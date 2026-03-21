from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from config import (
    ACTUAL_TIMINGS_PATH,
    AZURE_SPEECH_KEY,
    AZURE_SPEECH_REGION,
    AZURE_SPEECH_VOICE,
    DEFAULT_PACE,
    FINAL_VIDEO_PATH,
    NARRATION_PATH,
    OUTPUT_DIR,
    RECORDING_PATH,
    REPO_ROOT,
    S3_BUCKET,
    S3_ENDPOINT,
    SEGMENTS,
    TIMINGS_PATH,
    WINDOW_TITLES,
    WINDOWS_CLI,
    WINDOWS_MOUNT,
    WINDOWS_MOUNT_ROOT,
    WSL_DISTRO,
    WSL_MOUNT,
    WSL_USER,
    wsl_cli_path,
    wsl_repo_root,
)
from narration import SCENES

# Lock the chosen drive letter so all child processes use the same mount point.
os.environ["POSIXLAKE_DEMO_WINDOWS_MOUNT"] = str(WINDOWS_MOUNT)

SETTLE_SECONDS = 2.0
WINDOWS_CLIENT_TEMP = OUTPUT_DIR / "windows_update.csv"
SCENE_RUNNER = Path(__file__).with_name("scene_runner.py")


@dataclass
class SegmentTiming:
    scene_id: str
    seconds: float


class CandycamClient:
    def __init__(self) -> None:
        self.proc: subprocess.Popen[str] | None = None
        self.buffer = ""

    def start(self) -> None:
        helper = Path(__file__).with_name("candycam_helper.py")
        venv_python = Path(__file__).with_name(".venv") / "Scripts" / "python.exe"
        python = str(venv_python) if venv_python.exists() else sys.executable
        self.proc = subprocess.Popen(
            [python, str(helper)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            env={**os.environ, "CANDYCAM_BACKEND": "xcap"},
        )
        # Log helper stderr to console (like duckcells)
        import threading
        def _drain_stderr():
            assert self.proc is not None and self.proc.stderr is not None
            for line in self.proc.stderr:
                stripped = line.strip()
                if stripped:
                    print(f"[candycam] {stripped}", file=sys.stderr, flush=True)
        threading.Thread(target=_drain_stderr, daemon=True).start()
        self._wait_for("READY")

    def _send(self, command: str) -> None:
        assert self.proc is not None and self.proc.stdin is not None
        self.proc.stdin.write(command + "\n")
        self.proc.stdin.flush()

    def _wait_for(self, *prefixes: str, timeout: float = 20.0) -> str:
        assert self.proc is not None and self.proc.stdout is not None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = self.proc.stdout.readline()
            if not line:
                time.sleep(0.05)
                continue
            text = line.strip()
            if any(text.startswith(prefix) for prefix in prefixes):
                return text
        raise TimeoutError(f"Timed out waiting for {prefixes}")

    def list_windows(self) -> list[str]:
        self._send("LIST_WINDOWS")
        line = self._wait_for("WINDOWS:")
        payload = line.removeprefix("WINDOWS: ")
        return [] if not payload else payload.split(" | ")

    def record_window(self, output_path: Path, title: str) -> None:
        self._send(f"RECORD_WINDOW {output_path} {title}")
        response = self._wait_for("RECORDING", "ERROR")
        if response.startswith("ERROR"):
            raise RuntimeError(response)

    def stop(self) -> None:
        self._send("STOP")
        response = self._wait_for("STOPPED", "ERROR", timeout=30.0)
        if response.startswith("ERROR"):
            raise RuntimeError(response)

    def quit(self) -> None:
        if self.proc is None:
            return
        self._send("QUIT")
        self._wait_for("QUIT", timeout=10.0)
        if self.proc.stdin:
            self.proc.stdin.close()
        self.proc.wait(timeout=10)
        self.proc = None


def run(cmd: list[str], *, timeout: int = 120, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout, encoding="utf-8", errors="replace")
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def strip_nuls(text: str) -> str:
    return text.replace("\x00", "")



def run_wsl_bash(script: str, *, timeout: int = 120, check: bool = True) -> subprocess.CompletedProcess[str]:
    user_script = (
        f"if [ \"$(whoami)\" != {shlex.quote(WSL_USER)} ]; then "
        f"exec su - {shlex.quote(WSL_USER)} -c {shlex.quote(f'bash -lc {shlex.quote(script)}')}; "
        f"else exec bash -lc {shlex.quote(script)}; fi"
    )
    return run(
        ["wsl.exe", "-d", WSL_DISTRO, "--", "bash", "-lc", user_script],
        timeout=timeout,
        check=check,
    )



def terminate_process_tree(proc: subprocess.Popen[str] | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], timeout=20, check=False)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()



def wait_for_window(title: str, cam: CandycamClient, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        windows = cam.list_windows()
        if any(title.lower() in name.lower() for name in windows):
            print(f"  [cam] found window: {title}")
            return
        time.sleep(0.5)
    # Dump visible windows for diagnosis
    windows = cam.list_windows()
    print(f"  [cam] TIMEOUT — visible windows: {windows}", file=sys.stderr)
    raise TimeoutError(f"Window '{title}' did not appear within {timeout}s")



def wait_for_windows_mount(timeout: float = 60.0) -> None:
    target = WINDOWS_MOUNT_ROOT / "data" / "data.csv"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if target.exists():
            return
        time.sleep(0.25)
    raise TimeoutError(f"Windows mount did not appear at {target}")



def wait_for_wsl_mount(timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    command = f"test -f {shlex.quote(WSL_MOUNT)}/data/data.csv"
    while time.monotonic() < deadline:
        result = run(
            ["wsl.exe", "-d", WSL_DISTRO, "-u", "root", "--", "bash", "-lc", command],
            timeout=10,
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(0.25)
    raise TimeoutError(f"WSL mount did not appear at {WSL_MOUNT}")



def ensure_output_dirs() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)



def clean_output() -> None:
    ensure_output_dirs()

    # Kill any lingering posixlake-cli processes inside WSL — they survive
    # Windows-side taskkill and hold stale NFS mounts.
    run(
        ["wsl.exe", "-d", WSL_DISTRO, "-u", "root", "--", "bash", "-lc",
         "pkill -f posixlake-cli || true; umount -f " + shlex.quote(WSL_MOUNT) + " 2>/dev/null; true"],
        timeout=15,
        check=False,
    )
    # Also clean Windows NFS mount if stale
    run(["net", "use", str(WINDOWS_MOUNT), "/delete", "/y"], timeout=10, check=False)

    # Reset the MinIO bucket so s3-test creates fresh (not "already exists")
    run(
        ["podman", "exec", "posixlake-minio", "mc", "rb", "--force", f"local/{S3_BUCKET}"],
        timeout=15, check=False,
    )
    run(
        ["podman", "exec", "posixlake-minio", "mc", "mb", f"local/{S3_BUCKET}"],
        timeout=15, check=False,
    )

    for path in [
        TIMINGS_PATH,
        ACTUAL_TIMINGS_PATH,
        RECORDING_PATH,
        NARRATION_PATH,
        FINAL_VIDEO_PATH,
        WINDOWS_CLIENT_TEMP,
        *SEGMENTS.values(),
    ]:
        if path.exists():
            path.unlink()



def preflight(require_recording: bool) -> None:
    ensure_output_dirs()
    if not WINDOWS_CLI.exists():
        raise RuntimeError(
            f"Windows CLI not found at {WINDOWS_CLI}. Build it with: cargo build --release -p posixlake"
        )

    result = run(["wsl.exe", "-l", "-q"], timeout=10)
    available_distros = [line.strip() for line in strip_nuls(result.stdout).splitlines() if line.strip()]
    if WSL_DISTRO not in available_distros:
        raise RuntimeError(f"WSL distro '{WSL_DISTRO}' not found. Available: {', '.join(available_distros)}")

    linux_cli_check = run(
        ["wsl.exe", "-d", WSL_DISTRO, "-u", "root", "--", "bash", "-lc",
         f"test -x {shlex.quote(wsl_cli_path())}"],
        timeout=15,
        check=False,
    )
    if linux_cli_check.returncode != 0:
        raise RuntimeError(
            "WSL Linux CLI binary is missing. Build it inside WSL with: "
            f"wsl -d {WSL_DISTRO} -- bash -lc 'cd {shlex.quote(wsl_repo_root())} && cargo build --release -p posixlake'"
        )

    root_check = run(
        ["wsl.exe", "-d", WSL_DISTRO, "-u", "root", "--", "bash", "-lc", "whoami"],
        timeout=10,
        check=False,
    )
    if root_check.returncode != 0 or strip_nuls(root_check.stdout).strip() != "root":
        raise RuntimeError(
            "WSL root access is required for the Linux mount flow. "
            "Run the distro as root or enable passwordless sudo before running the demo."
        )

    run(["ffmpeg", "-version"], timeout=10)
    run(["ffprobe", "-version"], timeout=10)

    if require_recording:
        if not AZURE_SPEECH_KEY or not AZURE_SPEECH_REGION:
            raise RuntimeError("Azure Speech credentials are required for record mode")
        capture_check = subprocess.run(
            [sys.executable, "-c", "from capture import DemoRecorder; print('ok')"],
            text=True,
            capture_output=True,
            encoding="utf-8",
        )
        if capture_check.returncode != 0:
            raise RuntimeError(
                "candycam capture module is not importable in the current uv environment"
            )

    # Verify MinIO/S3 is reachable for the S3 cloud scene
    import urllib.request
    try:
        urllib.request.urlopen(f"{S3_ENDPOINT}/minio/health/live", timeout=5)
    except Exception:
        raise RuntimeError(
            f"MinIO is not reachable at {S3_ENDPOINT}. "
            "Start it with: podman run -d --name posixlake-minio -p 9000:9000 -p 9001:9001 "
            "-e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin "
            "minio/minio:latest server /data --console-address ':9001'"
        )


def launch_scene(scene_id: str, pace: float, *, visible: bool) -> subprocess.Popen[str]:
    """Launch a scene in a subprocess.

    When visible=True, uses the duckcells pattern:
      cmd.exe /C start "Window Title" /MAX /WAIT uv run python scene_runner.py ...
    This opens a maximized console window with an exact title that
    candycam can match for per-window recording.
    """
    scene_cmd = [
        "uv", "run", "python", str(SCENE_RUNNER), scene_id, "--pace", str(pace),
    ]
    if visible:
        title = WINDOW_TITLES[scene_id]
        # start "title" /MAX /WAIT launches a maximized, titled console
        # that stays open until the child exits — same as duckcells Phase 4.
        inner = " ".join(scene_cmd)
        return subprocess.Popen(
            f'cmd.exe /C start "{title}" /MAX /WAIT {inner}',
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=SCENE_RUNNER.parent,
        )
    else:
        return subprocess.Popen(
            scene_cmd,
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=SCENE_RUNNER.parent,
        )



def measure_recorded_server_scene(start_proc: subprocess.Popen[str], wait_fn, extra_seconds: float) -> float:
    started = time.monotonic()
    wait_fn()
    time.sleep(extra_seconds)
    return time.monotonic() - started



def measure_scene_runtime(scene_id: str, pace: float, *, timeout: int = 180) -> float:
    started = time.monotonic()
    result = subprocess.run(
        ["uv", "run", "python", str(SCENE_RUNNER), scene_id, "--pace", str(pace)],
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        cwd=SCENE_RUNNER.parent,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Scene failed: {scene_id}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return time.monotonic() - started



def write_timings(path: Path, timings: Iterable[SegmentTiming]) -> None:
    data = {entry.scene_id: round(entry.seconds, 3) for entry in timings}
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")



def dry_run(pace: float) -> list[SegmentTiming]:
    print("[demo] dry run starting")
    clean_output()
    timings: list[SegmentTiming] = []

    timings.append(SegmentTiming("s3_cloud", measure_scene_runtime("s3_cloud", pace, timeout=120)))

    windows_server = launch_scene("windows_server", pace, visible=False)
    try:
        seconds = measure_recorded_server_scene(windows_server, wait_for_windows_mount, SETTLE_SECONDS * pace)
        timings.append(SegmentTiming("windows_server", seconds))
        timings.append(SegmentTiming("windows_client", measure_scene_runtime("windows_client", pace)))
    finally:
        terminate_process_tree(windows_server)

    wsl_server = launch_scene("wsl_server", pace, visible=False)
    try:
        seconds = measure_recorded_server_scene(wsl_server, wait_for_wsl_mount, SETTLE_SECONDS * pace)
        timings.append(SegmentTiming("wsl_server", seconds))
        timings.append(SegmentTiming("wsl_client", measure_scene_runtime("wsl_client", pace, timeout=240)))
    finally:
        terminate_process_tree(wsl_server)

    write_timings(TIMINGS_PATH, timings)
    print(f"[demo] dry run complete -> {TIMINGS_PATH}")
    return timings



def is_valid_media(path: Path) -> bool:
    result = run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "csv=p=0",
            str(path),
        ],
        timeout=20,
        check=False,
    )
    return result.returncode == 0 and bool(result.stdout.strip()) and float(result.stdout.strip()) > 0



def concat_segments() -> None:
    segment_list = OUTPUT_DIR / "segments.txt"
    ordered = [SEGMENTS[scene["id"]] for scene in SCENES]
    for segment in ordered:
        if not segment.exists() or not is_valid_media(segment):
            raise RuntimeError(f"Recorded segment missing or invalid: {segment}")
    segment_list.write_text(
        "\n".join(f"file '{segment.as_posix()}'" for segment in ordered),
        encoding="utf-8",
    )
    run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(segment_list),
            "-c:v",
            "copy",
            "-an",
            "-movflags",
            "+faststart",
            str(RECORDING_PATH),
        ],
        timeout=120,
    )



def synthesize_narration(actual_timings: dict[str, float]) -> None:
    import azure.cognitiveservices.speech as speechsdk  # type: ignore

    speech_config = speechsdk.SpeechConfig(subscription=AZURE_SPEECH_KEY, region=AZURE_SPEECH_REGION)
    speech_config.speech_synthesis_voice_name = AZURE_SPEECH_VOICE
    speech_config.set_speech_synthesis_output_format(
        speechsdk.SpeechSynthesisOutputFormat.Riff16Khz16BitMonoPcm
    )

    padded_segments: list[Path] = []
    for scene in SCENES:
        scene_id = scene["id"]
        raw_path = OUTPUT_DIR / f"{scene_id}.wav"
        padded_path = OUTPUT_DIR / f"{scene_id}_padded.wav"
        audio_config = speechsdk.audio.AudioOutputConfig(filename=str(raw_path))
        synthesizer = speechsdk.SpeechSynthesizer(speech_config=speech_config, audio_config=audio_config)
        result = synthesizer.speak_text_async(scene["narration"]).get()
        if result.reason != speechsdk.ResultReason.SynthesizingAudioCompleted:
            raise RuntimeError(f"Speech synthesis failed for {scene_id}: {result.reason}")

        target = actual_timings[scene_id]
        duration_check = run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "csv=p=0",
                str(raw_path),
            ],
            timeout=20,
        )
        source_duration = float(duration_check.stdout.strip())

        # Fit audio to video duration: pad if short, speed up if long (like duckcells)
        if source_duration > target + 0.5:
            tempo = min(2.0, source_duration / target)
            af_filter = f"atempo={tempo:.4f},apad=whole_dur={target:.3f}"
            fit_label = f"tempo={tempo:.2f}x"
        else:
            af_filter = f"apad=whole_dur={target:.3f}"
            fit_label = "pad"

        run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(raw_path),
                "-af",
                af_filter,
                "-ar", "16000",
                "-ac", "1",
                str(padded_path),
            ],
            timeout=60,
        )
        print(f"  {scene_id}: speech={source_duration:.1f}s -> {fit_label} -> {target:.1f}s")
        padded_segments.append(padded_path)

    concat_list = OUTPUT_DIR / "narration_segments.txt"
    concat_list.write_text(
        "\n".join(f"file '{segment.as_posix()}'" for segment in padded_segments),
        encoding="utf-8",
    )
    run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_list),
            "-c",
            "copy",
            str(NARRATION_PATH),
        ],
        timeout=120,
    )



def merge_final_video() -> None:
    run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(RECORDING_PATH),
            "-i",
            str(NARRATION_PATH),
            "-c:v", "libx264",
            "-preset", "medium",
            "-crf", "18",
            "-tune", "stillimage",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac",
            "-b:a", "192k",
            "-map", "0:v:0",
            "-map", "1:a:0",
            "-shortest",
            "-movflags", "+faststart",
            str(FINAL_VIDEO_PATH),
        ],
        timeout=600,
    )



def load_or_create_timings(pace: float) -> dict[str, float]:
    if not TIMINGS_PATH.exists():
        dry_run(pace)
    return json.loads(TIMINGS_PATH.read_text(encoding="utf-8"))



def record(pace: float) -> None:
    base_timings = load_or_create_timings(pace)
    print(f"[demo] loaded dry-run timings: {base_timings}")
    cam = CandycamClient()
    actual: dict[str, float] = {}
    clean_output()
    # Allow ports to release after cleanup
    time.sleep(3)
    cam.start()
    windows_server: subprocess.Popen[str] | None = None
    wsl_server: subprocess.Popen[str] | None = None
    try:
        print("[record] === S3 Cloud ===")
        s3_scene = launch_scene("s3_cloud", pace, visible=True)
        try:
            wait_for_window(WINDOW_TITLES["s3_cloud"], cam)
            cam.record_window(SEGMENTS["s3_cloud"], WINDOW_TITLES["s3_cloud"])
            started = time.monotonic()
            s3_scene.wait(timeout=120)
            actual["s3_cloud"] = time.monotonic() - started
            cam.stop()
            print(f"  recorded {actual['s3_cloud']:.1f}s")
        finally:
            terminate_process_tree(s3_scene)

        print("[record] === Windows Server ===")
        windows_server = launch_scene("windows_server", pace, visible=True)
        wait_for_window(WINDOW_TITLES["windows_server"], cam)
        cam.record_window(SEGMENTS["windows_server"], WINDOW_TITLES["windows_server"])
        started = time.monotonic()
        wait_for_windows_mount()
        time.sleep(SETTLE_SECONDS * pace)
        actual["windows_server"] = time.monotonic() - started
        cam.stop()
        print(f"  recorded {actual['windows_server']:.1f}s")

        print("[record] === Windows Client ===")
        client = launch_scene("windows_client", pace, visible=True)
        try:
            wait_for_window(WINDOW_TITLES["windows_client"], cam)
            cam.record_window(SEGMENTS["windows_client"], WINDOW_TITLES["windows_client"])
            started = time.monotonic()
            client.wait(timeout=240)
            actual["windows_client"] = time.monotonic() - started
            cam.stop()
            print(f"  recorded {actual['windows_client']:.1f}s")
        finally:
            terminate_process_tree(client)
            terminate_process_tree(windows_server)

        # Clean stale Windows mount before WSL phase
        run(["net", "use", str(WINDOWS_MOUNT), "/delete", "/y"], timeout=10, check=False)

        print("[record] === WSL Server ===")
        wsl_server = launch_scene("wsl_server", pace, visible=True)
        wait_for_window(WINDOW_TITLES["wsl_server"], cam)
        cam.record_window(SEGMENTS["wsl_server"], WINDOW_TITLES["wsl_server"])
        started = time.monotonic()
        wait_for_wsl_mount()
        time.sleep(SETTLE_SECONDS * pace)
        actual["wsl_server"] = time.monotonic() - started
        cam.stop()
        print(f"  recorded {actual['wsl_server']:.1f}s")

        print("[record] === WSL Client ===")
        wsl_client = launch_scene("wsl_client", pace, visible=True)
        try:
            wait_for_window(WINDOW_TITLES["wsl_client"], cam)
            cam.record_window(SEGMENTS["wsl_client"], WINDOW_TITLES["wsl_client"])
            started = time.monotonic()
            wsl_client.wait(timeout=300)
            actual["wsl_client"] = time.monotonic() - started
            cam.stop()
            print(f"  recorded {actual['wsl_client']:.1f}s")
        finally:
            terminate_process_tree(wsl_client)
            terminate_process_tree(wsl_server)
    finally:
        terminate_process_tree(windows_server)
        terminate_process_tree(wsl_server)
        cam.quit()

    write_timings(ACTUAL_TIMINGS_PATH, [SegmentTiming(k, v) for k, v in actual.items()])
    concat_segments()
    synthesize_narration(actual)
    merge_final_video()
    print(f"[demo] final video -> {FINAL_VIDEO_PATH}")



def main() -> None:
    parser = argparse.ArgumentParser(description="posixlake CLI demo orchestrator")
    parser.add_argument("mode", choices=["dry-run", "record"], help="dry-run timings or full recording")
    parser.add_argument("--pace", type=float, default=DEFAULT_PACE, help="visual pacing multiplier")
    args = parser.parse_args()

    preflight(require_recording=args.mode == "record")
    if args.mode == "dry-run":
        dry_run(args.pace)
    else:
        record(args.pace)


if __name__ == "__main__":
    main()
