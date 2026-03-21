from __future__ import annotations

import ctypes
import os
import string
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEMO_DIR = Path(__file__).resolve().parent


def load_demo_env(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


load_demo_env(DEMO_DIR / ".env")


def choose_windows_mount() -> str:
    explicit = os.getenv("POSIXLAKE_DEMO_WINDOWS_MOUNT")
    if explicit:
        return explicit
    # Use GetLogicalDrives() bitmask — instant, no network timeouts.
    used_mask = ctypes.windll.kernel32.GetLogicalDrives()
    for drive in reversed(string.ascii_uppercase):
        bit = 1 << (ord(drive) - ord("A"))
        if not (used_mask & bit):
            return f"{drive}:"
    return "P:"

OUTPUT_DIR = DEMO_DIR / "output"
TIMINGS_PATH = OUTPUT_DIR / "timings.json"
ACTUAL_TIMINGS_PATH = OUTPUT_DIR / "actual_timings.json"
RECORDING_PATH = OUTPUT_DIR / "posixlake_cli_demo.mp4"
NARRATION_PATH = OUTPUT_DIR / "posixlake_cli_demo_narration.wav"
FINAL_VIDEO_PATH = OUTPUT_DIR / "posixlake_cli_demo_final.mp4"
WINDOWS_DB_PATH = OUTPUT_DIR / "demo_db"
WINDOWS_MOUNT = Path(choose_windows_mount())
WINDOWS_MOUNT_ROOT = Path(f"{WINDOWS_MOUNT}\\") if str(WINDOWS_MOUNT).endswith(":") else WINDOWS_MOUNT
WINDOWS_NFS_PORT = int(os.getenv("POSIXLAKE_DEMO_WINDOWS_PORT", "2049"))
WSL_DISTRO = os.getenv("WSL_DISTRO", "Ubuntu")
WSL_USER = os.getenv("POSIXLAKE_DEMO_WSL_USER", "npiesco")
WSL_HOME = os.getenv("POSIXLAKE_DEMO_WSL_HOME", f"/home/{WSL_USER}")
WSL_MOUNT = os.getenv("POSIXLAKE_DEMO_WSL_MOUNT", f"{WSL_HOME}/posixlake-demo")
WSL_NFS_PORT = int(os.getenv("POSIXLAKE_DEMO_WSL_PORT", "12049"))
WINDOWS_CLI = REPO_ROOT / "target" / "release" / "posixlake-cli.exe"
WSL_CLI = "/mnt/{drive}/{rest}/target/release/posixlake-cli"
DEFAULT_PACE = float(os.getenv("POSIXLAKE_DEMO_PACE", "1.0"))
AZURE_SPEECH_KEY = os.getenv("AZURE_SPEECH_KEY") or os.getenv("FOUNDRY_API_KEY")
AZURE_SPEECH_REGION = os.getenv("AZURE_SPEECH_REGION") or os.getenv("FOUNDRY_REGION")
AZURE_SPEECH_VOICE = os.getenv("POSIXLAKE_DEMO_VOICE", "en-US-AndrewMultilingualNeural")
WINDOW_TITLES = {
    "windows_server": "posixlake-demo-windows-server",
    "windows_client": "posixlake-demo-windows-client",
    "wsl_server": "posixlake-demo-wsl-server",
    "wsl_client": "posixlake-demo-wsl-client",
    "s3_cloud": "posixlake-demo-s3-cloud",
}
SEGMENTS = {
    "s3_cloud": OUTPUT_DIR / "seg_01_s3_cloud.mp4",
    "windows_server": OUTPUT_DIR / "seg_02_windows_server.mp4",
    "windows_client": OUTPUT_DIR / "seg_03_windows_client.mp4",
    "wsl_server": OUTPUT_DIR / "seg_04_wsl_server.mp4",
    "wsl_client": OUTPUT_DIR / "seg_05_wsl_client.mp4",
}

# S3/MinIO configuration
S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("MINIO_BUCKET", "posixlake-test")
S3_DB_PATH = os.getenv("POSIXLAKE_DEMO_S3_PATH", f"s3://{S3_BUCKET}/demo_iot_sensors")


def windows_to_wsl(path: Path) -> str:
    resolved = path.resolve()
    drive = resolved.drive.rstrip(":").lower()
    tail = resolved.as_posix().split(":", 1)[1].lstrip("/")
    return f"/mnt/{drive}/{tail}"


def wsl_cli_path() -> str:
    return WSL_CLI.format(
        drive=REPO_ROOT.drive.rstrip(":").lower(),
        rest=REPO_ROOT.as_posix().split(":", 1)[1].lstrip("/"),
    )


def windows_db_path_ps() -> str:
    return str(WINDOWS_DB_PATH)


def windows_cli_ps() -> str:
    return str(WINDOWS_CLI)


def wsl_repo_root() -> str:
    return windows_to_wsl(REPO_ROOT)


def wsl_db_path() -> str:
    return windows_to_wsl(WINDOWS_DB_PATH)
