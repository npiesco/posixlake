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

# Lock the Fabric table name so all scenes use the same table.
from config import _FABRIC_TABLE_SUFFIX
os.environ["POSIXLAKE_DEMO_FABRIC_TABLE"] = _FABRIC_TABLE_SUFFIX

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

    def record_window_by_pid(self, output_path: Path, pid: int) -> None:
        self._send(f"RECORD_WINDOW_PID {output_path} {pid}")
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


def get_child_pid(title_substring: str, timeout: float = 30.0) -> int:
    """Find the PID of a visible window matching title_substring."""
    import ctypes
    import ctypes.wintypes
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        results = []
        def cb(hwnd, _):
            pid = ctypes.wintypes.DWORD()
            ctypes.windll.user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
            length = ctypes.windll.user32.GetWindowTextLengthW(hwnd)
            if length > 0:
                buf = ctypes.create_unicode_buffer(length + 1)
                ctypes.windll.user32.GetWindowTextW(hwnd, buf, length + 1)
                if title_substring.lower() in buf.value.lower():
                    results.append(pid.value)
            return True
        WNDENUMPROC = ctypes.WINFUNCTYPE(
            ctypes.wintypes.BOOL, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM
        )
        ctypes.windll.user32.EnumWindows(WNDENUMPROC(cb), 0)
        if results:
            print(f"  [pid] found {title_substring} -> PID {results[0]}")
            return results[0]
        time.sleep(0.5)
    raise TimeoutError(f"No window with title '{title_substring}' found within {timeout}s")



def wait_for_windows_mount(timeout: float = 120.0) -> None:
    target = WINDOWS_MOUNT_ROOT / "data" / "data.csv"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if target.exists():
            return
        time.sleep(0.25)
    raise TimeoutError(f"Windows mount did not appear at {target}")



def wait_for_wsl_mount(timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    command = f"test -f {shlex.quote(WSL_MOUNT)}/data/data.csv"
    while time.monotonic() < deadline:
        try:
            result = subprocess.run(
                ["wsl.exe", "-d", WSL_DISTRO, "-u", "root", "--", "bash", "-lc", command],
                text=True, capture_output=True, timeout=15, encoding="utf-8", errors="replace",
            )
            if result.returncode == 0:
                return
        except subprocess.TimeoutExpired:
            pass
        time.sleep(0.5)
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
            stderr=None,
            cwd=SCENE_RUNNER.parent,
        )



def measure_recorded_server_scene(start_proc: subprocess.Popen[str], wait_fn, extra_seconds: float) -> float:
    started = time.monotonic()
    wait_fn()
    time.sleep(extra_seconds)
    return time.monotonic() - started


def start_merge_listener(server_proc: subprocess.Popen[str]) -> tuple:
    """Start a background thread that reads the server's stderr for MERGE_COMPLETE.

    Must be started BEFORE the client scene runs, so the event isn't missed.
    Returns a (thread, queue) tuple — pass to finish_merge_listener() after client completes.
    """
    import queue
    import threading

    if server_proc.stderr is None:
        raise RuntimeError("Server process stderr not piped — cannot read events")

    found: queue.Queue[str] = queue.Queue()

    def _reader() -> None:
        assert server_proc.stderr is not None
        for line in server_proc.stderr:
            stripped = line.strip()
            if "[EVENT] MERGE_COMPLETE" in stripped:
                found.put(stripped)
                print(f"  [server] {stripped}")
                return

    t = threading.Thread(target=_reader, daemon=True)
    t.start()
    return (t, found)


def finish_merge_listener(waiter: tuple, timeout: float = 120.0) -> None:
    """Wait for the merge listener thread to receive MERGE_COMPLETE."""
    t, found = waiter
    t.join(timeout=timeout)
    if found.empty():
        raise RuntimeError(f"Timed out after {timeout}s waiting for MERGE_COMPLETE event")
    print("[handshake] MERGE_COMPLETE confirmed")



def measure_scene_runtime(scene_id: str, pace: float, *, timeout: int = 180) -> float:
    sentinel = OUTPUT_DIR / f".done_{scene_id}"
    sentinel.unlink(missing_ok=True)

    started = time.monotonic()
    proc = subprocess.Popen(
        ["uv", "run", "python", str(SCENE_RUNNER), scene_id, "--pace", str(pace)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        errors="replace",
        cwd=SCENE_RUNNER.parent,
    )

    # Wait for process to finish — no arbitrary timeout, process drives completion
    stdout, stderr = proc.communicate()
    elapsed = time.monotonic() - started

    if proc.returncode != 0:
        raise RuntimeError(
            f"Scene failed: {scene_id}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        )
    return elapsed



def write_timings(path: Path, timings: Iterable[SegmentTiming]) -> None:
    data = {entry.scene_id: round(entry.seconds, 3) for entry in timings}
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")



def cleanup_stale_mounts() -> None:
    """Force-unmount any stale NFS mounts from previous runs."""
    # WSL mount — lazy unmount; handle timeout if WSL hangs on dead NFS mount
    try:
        subprocess.run(
            ["wsl.exe", "-d", WSL_DISTRO, "-u", "root", "--", "bash", "-lc",
             f"umount -l {WSL_MOUNT} 2>/dev/null; rm -rf {WSL_MOUNT} 2>/dev/null"],
            text=True, capture_output=True, timeout=5, encoding="utf-8", errors="replace",
        )
    except subprocess.TimeoutExpired:
        print("[cleanup] WSL umount timed out — dead NFS mount blocking, continuing")
    # Windows mount
    run(["net", "use", str(WINDOWS_MOUNT), "/delete", "/y"], timeout=10, check=False)


def validate_fabric_table(
    phase: str,
    expected_rows: int,
    expected_sensors: list[str] | None = None,
) -> None:
    """Gate: validate Fabric table state with exact schema, row count, and data checks."""
    from config import FABRIC_DB_PATH, WINDOWS_CLI

    print(f"[validate] {phase}: querying Fabric table...")
    result = run(
        [str(WINDOWS_CLI), "query", FABRIC_DB_PATH, "SELECT * FROM data ORDER BY id"],
        timeout=60,
        check=False,
    )
    output = result.stdout.strip()

    # Count data rows (skip header lines from pretty table: +---+, | header |, +---+)
    data_lines = [
        line for line in output.splitlines()
        if line.startswith("|") and "sensor" not in line.lower() and "id" not in line.split("|")[1].strip().lower()
    ]
    actual_rows = len(data_lines)

    if actual_rows != expected_rows:
        raise RuntimeError(
            f"[validate] {phase}: FAILED — expected {expected_rows} rows, got {actual_rows}\nOutput:\n{output}"
        )
    print(f"[validate] {phase}: row count OK ({actual_rows})")

    if expected_sensors:
        for sensor in expected_sensors:
            if sensor not in output:
                raise RuntimeError(
                    f"[validate] {phase}: FAILED — sensor '{sensor}' not found in output\nOutput:\n{output}"
                )
        print(f"[validate] {phase}: all {len(expected_sensors)} sensors present")

    # Schema check: must have exactly 4 columns (id, sensor, reading, location)
    if expected_rows > 0 and data_lines:
        cols = [c.strip() for c in data_lines[0].split("|") if c.strip()]
        if len(cols) != 4:
            raise RuntimeError(
                f"[validate] {phase}: FAILED — expected 4 columns, got {len(cols)}: {cols}"
            )
        print(f"[validate] {phase}: schema OK (4 columns)")

    print(f"[validate] {phase}: PASSED ✓")


def dry_run(pace: float) -> list[SegmentTiming]:
    print("[demo] dry run starting")
    cleanup_stale_mounts()
    clean_output()
    timings: list[SegmentTiming] = []

    timings.append(SegmentTiming("intro", measure_scene_runtime("intro", pace, timeout=60)))
    timings.append(SegmentTiming("fabric_origin", measure_scene_runtime("fabric_origin", pace, timeout=120)))
    validate_fabric_table("after fabric_origin", expected_rows=0)

    windows_server = launch_scene("windows_server", pace, visible=False)
    try:
        seconds = measure_recorded_server_scene(windows_server, wait_for_windows_mount, SETTLE_SECONDS * pace)
        timings.append(SegmentTiming("windows_server", seconds))
        timings.append(SegmentTiming("windows_client", measure_scene_runtime("windows_client", pace)))
    finally:
        terminate_process_tree(windows_server)
    validate_fabric_table("after windows_client", expected_rows=6, expected_sensors=["TEMP_01", "humidity_02", "pressure_03", "temp_04", "co2_05", "flow_06"])

    cleanup_stale_mounts()
    wsl_server = launch_scene("wsl_server", pace, visible=False)
    try:
        seconds = measure_recorded_server_scene(wsl_server, wait_for_wsl_mount, SETTLE_SECONDS * pace)
        timings.append(SegmentTiming("wsl_server", seconds))
        timings.append(SegmentTiming("wsl_client", measure_scene_runtime("wsl_client", pace, timeout=240)))
    finally:
        terminate_process_tree(wsl_server)
    validate_fabric_table("after wsl_client", expected_rows=6, expected_sensors=["temp_01", "humidity_02", "pressure_03", "temp_04", "co2_05", "flow_06"])

    timings.append(SegmentTiming("s3_interlude", measure_scene_runtime("s3_interlude", pace, timeout=120)))
    timings.append(SegmentTiming("fabric_homecoming", measure_scene_runtime("fabric_homecoming", pace, timeout=120)))
    validate_fabric_table("after fabric_homecoming", expected_rows=6, expected_sensors=["temp_01", "humidity_02", "pressure_03", "temp_04", "co2_05", "flow_06"])
    timings.append(SegmentTiming("outro", measure_scene_runtime("outro", pace, timeout=60)))

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
    """Concatenate video segments, speeding up any that are much longer than narration."""
    from narration import SCENES as NARRATION_SCENES

    # Load actual timings to know video durations
    actual_path = OUTPUT_DIR / "actual_timings.json"
    actual = {}
    if actual_path.exists():
        actual = json.loads(actual_path.read_text(encoding="utf-8"))

    # Estimate narration duration: ~150 words/min for Azure TTS
    narration_durations = {}
    for scene in NARRATION_SCENES:
        words = len(scene["narration"].split())
        narration_durations[scene["id"]] = max(5.0, words / 2.5)  # ~2.5 words/sec

    # Speed up video segments where video >> narration (add 50% breathing room)
    adjusted_segments: list[Path] = []
    ordered = [SEGMENTS[scene["id"]] for scene in NARRATION_SCENES]
    for scene, segment in zip(NARRATION_SCENES, ordered):
        scene_id = scene["id"]
        if not segment.exists() or not is_valid_media(segment):
            raise RuntimeError(f"Recorded segment missing or invalid: {segment}")

        narr_dur = narration_durations.get(scene_id, 0)
        video_dur = actual.get(scene_id, 0)

        # If video is >3x narration length, speed it up
        if narr_dur > 0 and video_dur > narr_dur * 3:
            target_dur = narr_dur * 1.5  # 50% breathing room
            speed_factor = video_dur / target_dur
            adjusted = OUTPUT_DIR / f"{segment.stem}_fast.mp4"
            print(f"  [speed] {scene_id}: {video_dur:.1f}s -> {target_dur:.1f}s ({speed_factor:.1f}x)")
            run(
                ["ffmpeg", "-y",
                 "-i", str(segment),
                 "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=mono",
                 "-filter:v", f"setpts=PTS/{speed_factor:.4f}",
                 "-map", "0:v", "-map", "1:a",
                 "-c:a", "aac", "-b:a", "128k",
                 "-shortest",
                 "-movflags", "+faststart", str(adjusted)],
                timeout=300,
            )
            adjusted_segments.append(adjusted)
        else:
            adjusted_segments.append(segment)

    segment_list = OUTPUT_DIR / "segments.txt"
    segment_list.write_text(
        "\n".join(f"file '{segment.as_posix()}'" for segment in adjusted_segments),
        encoding="utf-8",
    )

    # Use filter_complex concat to properly reset timestamps across mixed sources
    n = len(adjusted_segments)
    inputs: list[str] = []
    filter_parts: list[str] = []
    for i, seg in enumerate(adjusted_segments):
        inputs.extend(["-i", str(seg)])
        filter_parts.append(f"[{i}:v:0]")
    filter_str = "".join(filter_parts) + f"concat=n={n}:v=1:a=0[outv]"

    run(
        [
            "ffmpeg",
            "-y",
            *inputs,
            "-filter_complex",
            filter_str,
            "-map",
            "[outv]",
            "-c:v",
            "libx264",
            "-crf",
            "18",
            "-r",
            "30",
            "-g",
            "30",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            str(RECORDING_PATH),
        ],
        timeout=600,
    )

    # Return adjusted durations for narration sync
    adjusted_actual = dict(actual)
    for scene in NARRATION_SCENES:
        scene_id = scene["id"]
        narr_dur = narration_durations.get(scene_id, 0)
        video_dur = actual.get(scene_id, 0)
        if narr_dur > 0 and video_dur > narr_dur * 3:
            adjusted_actual[scene_id] = narr_dur * 1.5
    return adjusted_actual



def _narration_to_ssml(text: str, voice: str) -> str:
    """Wrap plain narration text in SSML with pronunciation hints."""
    import re
    escaped = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    escaped = re.sub(
        r"(?i)\bposixlake\b",
        '<phoneme alphabet="ipa" ph="ˈpɒzɪkˌleɪk">posixlake</phoneme>',
        escaped,
    )
    escaped = re.sub(
        r"(?i)\bnpiesco\b",
        '<phoneme alphabet="ipa" ph="ɛnˌpiːɛsˈkoʊ">npiesco</phoneme>',
        escaped,
    )
    return (
        '<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="en-US">'
        f'<voice name="{voice}">{escaped}</voice></speak>'
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
        ssml = _narration_to_ssml(scene["narration"], AZURE_SPEECH_VOICE)
        result = synthesizer.speak_ssml_async(ssml).get()
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
    cleanup_stale_mounts()
    clean_output()
    # Generate title cards (not recorded — pre-rendered)
    print("[record] === Generating title cards ===")
    measure_scene_runtime("intro", pace, timeout=60)
    measure_scene_runtime("outro", pace, timeout=60)

    # Allow ports to release after cleanup
    time.sleep(3)
    cam.start()
    windows_server: subprocess.Popen[str] | None = None
    wsl_server: subprocess.Popen[str] | None = None
    try:
        print("[record] === Fabric Origin ===")
        fabric_scene = launch_scene("fabric_origin", pace, visible=True)
        try:
            pid = get_child_pid(WINDOW_TITLES["fabric_origin"])
            cam.record_window_by_pid(SEGMENTS["fabric_origin"], pid)
            started = time.monotonic()
            fabric_scene.wait(timeout=120)
            actual["fabric_origin"] = time.monotonic() - started
            cam.stop()
            print(f"  recorded {actual['fabric_origin']:.1f}s")
        finally:
            terminate_process_tree(fabric_scene)

        print("[record] === Windows Server ===")
        windows_server = launch_scene("windows_server", pace, visible=True)
        pid = get_child_pid(WINDOW_TITLES["windows_server"])
        cam.record_window_by_pid(SEGMENTS["windows_server"], pid)
        started = time.monotonic()
        wait_for_windows_mount()
        time.sleep(SETTLE_SECONDS * pace)
        actual["windows_server"] = time.monotonic() - started
        cam.stop()
        print(f"  recorded {actual['windows_server']:.1f}s")

        print("[record] === Windows Client ===")
        client = launch_scene("windows_client", pace, visible=True)
        try:
            pid = get_child_pid(WINDOW_TITLES["windows_client"])
            cam.record_window_by_pid(SEGMENTS["windows_client"], pid)
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
        pid = get_child_pid(WINDOW_TITLES["wsl_server"])
        cam.record_window_by_pid(SEGMENTS["wsl_server"], pid)
        started = time.monotonic()
        wait_for_wsl_mount()
        time.sleep(SETTLE_SECONDS * pace)
        actual["wsl_server"] = time.monotonic() - started
        cam.stop()
        print(f"  recorded {actual['wsl_server']:.1f}s")

        print("[record] === WSL Client ===")
        wsl_client = launch_scene("wsl_client", pace, visible=True)
        try:
            pid = get_child_pid(WINDOW_TITLES["wsl_client"])
            cam.record_window_by_pid(SEGMENTS["wsl_client"], pid)
            started = time.monotonic()
            wsl_client.wait(timeout=300)
            actual["wsl_client"] = time.monotonic() - started
            cam.stop()
            print(f"  recorded {actual['wsl_client']:.1f}s")
        finally:
            terminate_process_tree(wsl_client)
            terminate_process_tree(wsl_server)

        print("[record] === S3 Interlude ===")
        s3_scene = launch_scene("s3_interlude", pace, visible=True)
        try:
            pid = get_child_pid(WINDOW_TITLES["s3_interlude"])
            cam.record_window_by_pid(SEGMENTS["s3_interlude"], pid)
            started = time.monotonic()
            s3_scene.wait(timeout=120)
            actual["s3_interlude"] = time.monotonic() - started
            cam.stop()
            print(f"  recorded {actual['s3_interlude']:.1f}s")
        finally:
            terminate_process_tree(s3_scene)

        print("[record] === Fabric Homecoming ===")
        homecoming = launch_scene("fabric_homecoming", pace, visible=True)
        try:
            pid = get_child_pid(WINDOW_TITLES["fabric_homecoming"])
            cam.record_window_by_pid(SEGMENTS["fabric_homecoming"], pid)
            started = time.monotonic()
            homecoming.wait(timeout=120)
            actual["fabric_homecoming"] = time.monotonic() - started
            cam.stop()
            print(f"  recorded {actual['fabric_homecoming']:.1f}s")
        finally:
            terminate_process_tree(homecoming)
    finally:
        terminate_process_tree(windows_server)
        terminate_process_tree(wsl_server)
        cam.quit()

    actual["intro"] = 20.0
    actual["outro"] = 22.0
    write_timings(ACTUAL_TIMINGS_PATH, [SegmentTiming(k, v) for k, v in actual.items()])
    adjusted = concat_segments()
    synthesize_narration(adjusted)
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
