"""
Candycam recording helper — started as a subprocess by orchestrate.ts.

Protocol (stdin commands, one per line):
  LIST_WINDOWS          → prints "WINDOWS: title1 | title2 | ..." to stdout
  RECORD_WINDOW <path> <title_substring>  → stops any current recording,
                          starts recording the window matching title_substring,
                          prints "RECORDING <path>" to stdout
  RECORD_WINDOW_PID <path> <pid>  → stops any current recording,
                          finds the window owned by <pid> via OS HWND lookup,
                          records that exact window by candycam window ID,
                          prints "RECORDING <path> (pid=<pid>, hwnd=<hwnd>, title=<title>)"
  RECORD_MONITOR <path> <index>  → same but records a full monitor
  STOP                  → stops current recording, prints "STOPPED <path>",
                          stays alive for more commands
  QUIT                  → stops any current recording, prints "QUIT", exits 0

Each segment produces a separate .mp4. The orchestrator concatenates them
with ffmpeg afterwards.
"""
import ctypes
import ctypes.wintypes
import io
import os
import subprocess
import sys
import time

# Force UTF-8 stdout on Windows (avoids cp1252 crashes on Unicode window titles)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

os.environ["CANDYCAM_BACKEND"] = "xcap"

from capture import DemoRecorder, QualityPreset  # type: ignore

# Sharp text for demo videos — CRF 18, medium preset, lanczos scaler
_QUALITY = QualityPreset.SCREEN_SHARE


def _validate_mp4(path: str) -> bool:
    """Return True if ffprobe can read a positive duration from the file."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "csv=p=0", path],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return False
        dur = result.stdout.strip()
        return dur != "" and float(dur) > 0
    except Exception:
        return False


def _wait_for_valid_mp4(path: str, timeout: float = 10.0) -> bool:
    """Poll until the file is a valid mp4 or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            if _validate_mp4(path):
                return True
        time.sleep(0.25)
    return False


def _get_hwnds_for_pid(target_pid: int) -> list[tuple[int, str]]:
    """Use Win32 EnumWindows to find all visible HWNDs owned by a PID.

    Returns list of (hwnd, title) for windows with non-empty titles,
    excluding IME windows.
    """
    results: list[tuple[int, str]] = []

    def cb(hwnd, _):
        pid = ctypes.wintypes.DWORD()
        ctypes.windll.user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
        if pid.value == target_pid:
            length = ctypes.windll.user32.GetWindowTextLengthW(hwnd)
            if length > 0:
                buf = ctypes.create_unicode_buffer(length + 1)
                ctypes.windll.user32.GetWindowTextW(hwnd, buf, length + 1)
                title = buf.value
                # Skip IME helper windows
                if "IME" not in title and "MSCTFIME" not in title:
                    results.append((hwnd, title))
        return True

    WNDENUMPROC = ctypes.WINFUNCTYPE(
        ctypes.wintypes.BOOL, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM
    )
    ctypes.windll.user32.EnumWindows(WNDENUMPROC(cb), 0)
    return results


def _find_candycam_window_for_pid(
    recorder: DemoRecorder, target_pid: int
) -> tuple[int, str] | None:
    """Map PID → HWND → candycam window ID.

    1. Uses Win32 EnumWindows to find the HWND owned by target_pid
    2. Matches that HWND against candycam list_window_info() IDs
    3. Returns (window_id, title) or None if no match
    """
    hwnds = _get_hwnds_for_pid(target_pid)
    if not hwnds:
        return None

    # Get candycam's window list
    window_infos = recorder.list_window_info()
    candycam_ids = {w.id: w.title for w in window_infos}

    # Match OS HWND to candycam window ID (they should be equal)
    for hwnd, os_title in hwnds:
        if hwnd in candycam_ids:
            return (hwnd, candycam_ids[hwnd])

    return None


def main() -> None:
    recorder = DemoRecorder()
    current_path: str | None = None
    recording = False
    segment_paths: list[str] = []

    print("READY", flush=True)

    for line in sys.stdin:
        cmd = line.strip()
        if not cmd:
            continue

        if cmd == "LIST_WINDOWS":
            titles = recorder.list_windows()
            print(f"WINDOWS: {' | '.join(titles)}", flush=True)

        elif cmd.startswith("RECORD_WINDOW "):
            # Stop previous recording if active
            if recording:
                recorder.stop_recording()
                if current_path:
                    segment_paths.append(current_path)
                recording = False

            parts = cmd.split(" ", 2)
            if len(parts) < 3:
                print("ERROR: RECORD_WINDOW <path> <title>", flush=True)
                continue
            path, title = parts[1], parts[2]
            current_path = path

            # Validate the window actually exists BEFORE recording.
            # The capture library silently records the wrong thing
            # (desktop or best-guess window) when the title doesn't match.
            # NO FALLBACKS — fail hard if the target window is missing.
            all_titles = recorder.list_windows()
            matched = [t for t in all_titles if title.lower() in t.lower()]
            if not matched:
                print(
                    f"ERROR: No window matching '{title}'. "
                    f"Visible windows: {' | '.join(all_titles)}",
                    flush=True,
                )
                current_path = None
                continue

            try:
                recorder.start_recording_window_with_quality(path, title, _QUALITY)
                recording = True
                print(f"RECORDING {path} (matched: {matched[0]})", flush=True)
            except Exception as e:
                print(f"ERROR: {e}", flush=True)

        elif cmd.startswith("RECORD_WINDOW_PID "):
            # PID-based window targeting: find the exact window owned by
            # a given process ID, then record it by candycam window ID.
            # NO title matching, NO ambiguity, NO fallbacks.
            if recording:
                recorder.stop_recording()
                if current_path:
                    segment_paths.append(current_path)
                recording = False

            parts = cmd.split(" ", 2)
            if len(parts) < 3:
                print("ERROR: RECORD_WINDOW_PID <path> <pid>", flush=True)
                continue
            path = parts[1]
            try:
                target_pid = int(parts[2])
            except ValueError:
                print(f"ERROR: Invalid PID: {parts[2]}", flush=True)
                continue
            current_path = path

            match = _find_candycam_window_for_pid(recorder, target_pid)
            if match is None:
                hwnds = _get_hwnds_for_pid(target_pid)
                print(
                    f"ERROR: No candycam window for PID {target_pid}. "
                    f"OS HWNDs: {hwnds}",
                    flush=True,
                )
                current_path = None
                continue

            window_id, window_title = match
            try:
                recorder.start_recording_window_by_id_with_quality(path, window_id, _QUALITY)
                recording = True
                print(
                    f"RECORDING {path} (pid={target_pid}, hwnd={window_id}, title={window_title})",
                    flush=True,
                )
            except Exception as e:
                print(f"ERROR: {e}", flush=True)
                current_path = None

        elif cmd.startswith("RECORD_MONITOR "):
            if recording:
                recorder.stop_recording()
                if current_path:
                    segment_paths.append(current_path)
                recording = False

            parts = cmd.split(" ", 2)
            if len(parts) < 3:
                print("ERROR: RECORD_MONITOR <path> <index>", flush=True)
                continue
            path, idx = parts[1], int(parts[2])
            current_path = path
            try:
                recorder.start_recording_with_quality(path, idx, _QUALITY)
                recording = True
                print(f"RECORDING {path}", flush=True)
            except Exception as e:
                print(f"ERROR: {e}", flush=True)

        elif cmd == "STOP":
            if recording:
                recorder.stop_recording()
                recording = False
                if current_path:
                    # Wait for the mp4 to be fully written (moov atom)
                    if _wait_for_valid_mp4(current_path):
                        size = os.path.getsize(current_path)
                        segment_paths.append(current_path)
                        print(f"STOPPED {current_path} ({size} bytes, valid)", flush=True)
                    else:
                        size = os.path.getsize(current_path) if os.path.exists(current_path) else 0
                        print(f"ERROR STOP segment corrupt: {current_path} ({size} bytes, NO moov atom)", flush=True)
                else:
                    print("STOPPED (no path)", flush=True)
                current_path = None
            else:
                print("STOPPED (not recording)", flush=True)

        elif cmd == "QUIT":
            if recording:
                recorder.stop_recording()
                recording = False
                if current_path:
                    if _wait_for_valid_mp4(current_path):
                        segment_paths.append(current_path)
                    else:
                        print(f"ERROR QUIT segment corrupt: {current_path}", flush=True)
            # Print all segment paths for the orchestrator
            print(f"SEGMENTS: {' | '.join(segment_paths)}", flush=True)
            print("QUIT", flush=True)
            break

        else:
            print(f"UNKNOWN: {cmd}", flush=True)


if __name__ == "__main__":
    main()
