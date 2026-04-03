from __future__ import annotations

import argparse
import ctypes
import os
import subprocess
import sys
import time
from pathlib import Path

from config import (
    FABRIC_CLIENT_ID,
    FABRIC_CLIENT_SECRET,
    FABRIC_DB_PATH,
    FABRIC_TENANT_ID,
    OUTPUT_DIR,
    REPO_ROOT,
    S3_ACCESS_KEY,
    S3_DB_PATH,
    S3_ENDPOINT,
    S3_SECRET_KEY,
    SEGMENTS,
    WINDOW_TITLES,
    WINDOWS_CLI,
    WINDOWS_MOUNT,
    WINDOWS_MOUNT_ROOT,
    WINDOWS_NFS_PORT,
    WSL_DISTRO,
    WSL_MOUNT,
    WSL_NFS_PORT,
    WSL_USER,
    wsl_cli_path,
)

SETTLE_SECONDS = 2.0
WINDOWS_CLIENT_TEMP = OUTPUT_DIR / "windows_update.csv"

# Pause after each command so viewers can read the output
READ_PAUSE = 1.5


def set_console_title(title: str) -> None:
    if sys.platform == "win32":
        ctypes.windll.kernel32.SetConsoleTitleW(title)


def pause_for_command(pace: float) -> float:
    return max(0.25, 0.8 * pace)


def settle_time(pace: float) -> float:
    return max(1.0, SETTLE_SECONDS * pace)


def show_command(prefix: str, command: str, pace: float) -> None:
    print()
    print(f"{prefix} {command}")
    sys.stdout.flush()
    time.sleep(pause_for_command(pace))


def run_capture(cmd: list[str], *, timeout: int = 240, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def print_result(result: subprocess.CompletedProcess[str]) -> None:
    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    if stdout:
        print(stdout)
    if stderr:
        print(stderr, file=sys.stderr)
    sys.stdout.flush()
    sys.stderr.flush()


def run_powershell(command: str, *, pace: float, timeout: int = 240) -> None:
    show_command("PS>", command, pace)
    result = run_capture(
        ["powershell.exe", "-NoLogo", "-NoProfile", "-Command", command],
        timeout=timeout,
    )
    print_result(result)


def run_process(cmd: list[str], *, display: str, pace: float, timeout: int = 240) -> None:
    show_command("PS>", display, pace)
    result = run_capture(cmd, timeout=timeout)
    print_result(result)


def wsl_root_command(command: str) -> list[str]:
    return ["wsl.exe", "-d", WSL_DISTRO, "-u", "root", "--", "bash", "-lc", command]


def wsl_user_command(command: str) -> list[str]:
    return ["wsl.exe", "-d", WSL_DISTRO, "-u", WSL_USER, "--", "bash", "-lc", command]


def run_wsl_root(command: str, *, pace: float, timeout: int = 240, display: str | None = None) -> None:
    show_command("$", display or command, pace)
    result = run_capture(wsl_root_command(command), timeout=timeout)
    print_result(result)


def run_wsl_user(command: str, *, pace: float, timeout: int = 240, display: str | None = None) -> None:
    show_command("$", display or command, pace)
    result = run_capture(wsl_user_command(command), timeout=timeout)
    print_result(result)


def run_fabric_origin(pace: float) -> None:
    set_console_title(WINDOW_TITLES["fabric_origin"])
    time.sleep(settle_time(pace))

    # Create the IoT Delta table directly on Fabric OneLake
    run_process(
        [
            str(WINDOWS_CLI),
            "create",
            FABRIC_DB_PATH,
            "--schema",
            "id:Int32,sensor:String,reading:Int32,location:String",
        ],
        display=(
            f"& {WINDOWS_CLI.name} create {FABRIC_DB_PATH}"
            f" --schema id:Int32,sensor:String,reading:Int32,location:String"
        ),
        pace=pace,
        timeout=120,
    )
    time.sleep(READ_PAUSE)

    # Verify with health check
    run_process(
        [str(WINDOWS_CLI), "health", FABRIC_DB_PATH],
        display=f"& {WINDOWS_CLI.name} health {FABRIC_DB_PATH}",
        pace=pace,
        timeout=60,
    )
    time.sleep(READ_PAUSE)
    time.sleep(settle_time(pace))


def run_windows_server(pace: float) -> None:
    set_console_title(WINDOW_TITLES["windows_server"])
    # Mount the Fabric OneLake table as a Windows NFS drive
    show_command(
        "PS>",
        f"& {WINDOWS_CLI.name} mount {FABRIC_DB_PATH} {WINDOWS_MOUNT} --port {WINDOWS_NFS_PORT}",
        pace,
    )
    env = os.environ.copy()
    env["AZURE_STORAGE_CLIENT_ID"] = FABRIC_CLIENT_ID
    env["AZURE_STORAGE_CLIENT_SECRET"] = FABRIC_CLIENT_SECRET
    env["AZURE_STORAGE_TENANT_ID"] = FABRIC_TENANT_ID
    subprocess.run(
        [
            str(WINDOWS_CLI),
            "mount",
            FABRIC_DB_PATH,
            str(WINDOWS_MOUNT),
            "--port",
            str(WINDOWS_NFS_PORT),
        ],
        env=env,
        check=True,
    )


def run_windows_client(pace: float) -> None:
    set_console_title(WINDOW_TITLES["windows_client"])
    target = WINDOWS_MOUNT_ROOT / "data" / "data.csv"
    time.sleep(settle_time(pace))

    # Read the empty CSV facade (header only)
    run_powershell(f"Get-Content '{target}'", pace=pace)
    time.sleep(READ_PAUSE)

    # Seed 6 IoT sensor readings across the facility
    run_powershell(f"Add-Content -Path '{target}' -Value '1,temp_01,72,plant_north'", pace=pace)
    run_powershell(f"Add-Content -Path '{target}' -Value '2,humidity_02,45,plant_south'", pace=pace)
    run_powershell(f"Add-Content -Path '{target}' -Value '3,pressure_03,1013,plant_east'", pace=pace)
    time.sleep(2)
    run_powershell(f"Add-Content -Path '{target}' -Value '4,temp_04,69,plant_west'", pace=pace)
    run_powershell(f"Add-Content -Path '{target}' -Value '5,co2_05,412,lab_01'", pace=pace)
    run_powershell(f"Add-Content -Path '{target}' -Value '6,flow_06,9,warehouse'", pace=pace)
    time.sleep(2)

    # Show all 6 readings
    run_powershell(f"Get-Content '{target}'", pace=pace)
    time.sleep(READ_PAUSE)

    # Flag anomaly — uppercase temp_01 to mark it for review (atomic Delta merge)
    run_powershell(
        f"Get-Content '{target}' | ForEach-Object {{ $_ -replace 'temp_01','TEMP_01' }} | Set-Content '{WINDOWS_CLIENT_TEMP}' -Encoding ascii",
        pace=pace,
    )
    show_command("PS>", f"cmd /c copy /Y {WINDOWS_CLIENT_TEMP} {target}", pace)
    copy_result = run_capture(
        ["cmd.exe", "/c", "copy", "/Y", str(WINDOWS_CLIENT_TEMP), str(target)],
        timeout=120,
    )
    print_result(copy_result)
    time.sleep(READ_PAUSE)

    # Verify the merge — TEMP_01 flag should appear
    run_powershell(f"Get-Content '{target}'", pace=pace)
    time.sleep(READ_PAUSE)

    # Show Delta Lake version history
    run_process(
        [str(WINDOWS_CLI), "status", str(WINDOWS_MOUNT)],
        display=f"& {WINDOWS_CLI.name} status {WINDOWS_MOUNT}",
        pace=pace,
    )
    time.sleep(READ_PAUSE)

    # Unmount
    run_process(
        [str(WINDOWS_CLI), "unmount", str(WINDOWS_MOUNT)],
        display=f"& {WINDOWS_CLI.name} unmount {WINDOWS_MOUNT}",
        pace=pace,
    )
    time.sleep(settle_time(pace))


def run_wsl_server(pace: float) -> None:
    set_console_title(WINDOW_TITLES["wsl_server"])
    run_wsl_root(f"mkdir -p {WSL_MOUNT}", pace=pace)
    time.sleep(settle_time(pace))
    # Mount the Fabric OneLake table via NFS from Linux
    mount_cmd = (
        f"AZURE_STORAGE_CLIENT_ID='{FABRIC_CLIENT_ID}'"
        f" AZURE_STORAGE_CLIENT_SECRET='{FABRIC_CLIENT_SECRET}'"
        f" AZURE_STORAGE_TENANT_ID='{FABRIC_TENANT_ID}'"
        f" {wsl_cli_path()} mount '{FABRIC_DB_PATH}' {WSL_MOUNT} --port {WSL_NFS_PORT}"
    )
    show_command("$", f"{wsl_cli_path()} mount {FABRIC_DB_PATH} {WSL_MOUNT} --port {WSL_NFS_PORT}", pace)
    subprocess.run(
        wsl_root_command(mount_cmd),
        check=True,
    )


def run_wsl_client(pace: float) -> None:
    set_console_title(WINDOW_TITLES["wsl_client"])
    target = f"{WSL_MOUNT}/data/data.csv"
    time.sleep(settle_time(pace))

    # Breathing room — narration plays over the mount output while viewer reads
    time.sleep(4)

    # All reads in rapid succession — OneLake NFS drops idle connections quickly
    run_wsl_user(f"cat {target}", pace=pace, display=f"cat {target}")
    run_wsl_user(f"grep TEMP_01 {target} || true", pace=pace, display=f"grep TEMP_01 {target}")
    run_wsl_user(
        f"awk -F, 'NR > 1 {{ print \\$2 }}' {target}",
        pace=pace,
        display=f"awk -F, 'NR > 1 {{ print $2 }}' {target}",
    )
    run_wsl_user(f"wc -l {target}", pace=pace, display=f"wc -l {target}")
    run_wsl_user(f"head -3 {target}", pace=pace, display=f"head -3 {target}")
    run_wsl_user(f"tail -2 {target}", pace=pace, display=f"tail -2 {target}")
    run_wsl_user(
        f"sort -t, -k2 {target}",
        pace=pace,
        display=f"sort -t, -k2 {target}",
    )

    # Delta Lake status from Linux
    run_wsl_user(
        f"{wsl_cli_path()} status {WSL_MOUNT}",
        pace=pace,
        display=f"posixlake-cli status {WSL_MOUNT}",
    )
    time.sleep(READ_PAUSE)

    # Unmount
    run_wsl_root(f"{wsl_cli_path()} unmount {WSL_MOUNT}", pace=pace)
    time.sleep(settle_time(pace))


def run_s3_interlude(pace: float) -> None:
    set_console_title(WINDOW_TITLES["s3_interlude"])
    time.sleep(settle_time(pace))

    # Create the IoT Delta table directly on S3
    run_process(
        [
            str(WINDOWS_CLI),
            "create",
            S3_DB_PATH,
            "--schema",
            "id:Int32,sensor:String,reading:Int32,location:String",
            "--endpoint",
            S3_ENDPOINT,
            "--access-key",
            S3_ACCESS_KEY,
            "--secret-key",
            S3_SECRET_KEY,
        ],
        display=(
            f"& {WINDOWS_CLI.name} create {S3_DB_PATH}"
            f" --schema id:Int32,sensor:String,reading:Int32,location:String"
            f" --endpoint {S3_ENDPOINT}"
        ),
        pace=pace,
        timeout=120,
    )
    time.sleep(READ_PAUSE)

    # Verify with health check
    run_process(
        [
            str(WINDOWS_CLI),
            "health",
            S3_DB_PATH,
            "--endpoint",
            S3_ENDPOINT,
            "--access-key",
            S3_ACCESS_KEY,
            "--secret-key",
            S3_SECRET_KEY,
        ],
        display=f"& {WINDOWS_CLI.name} health {S3_DB_PATH} --endpoint {S3_ENDPOINT}",
        pace=pace,
        timeout=60,
    )
    time.sleep(READ_PAUSE)
    time.sleep(settle_time(pace))


def run_fabric_homecoming(pace: float) -> None:
    set_console_title(WINDOW_TITLES["fabric_homecoming"])
    time.sleep(settle_time(pace))

    # Show health check on the Fabric table — proves data persisted
    run_process(
        [str(WINDOWS_CLI), "health", FABRIC_DB_PATH],
        display=f"& {WINDOWS_CLI.name} health {FABRIC_DB_PATH}",
        pace=pace,
        timeout=60,
    )
    time.sleep(READ_PAUSE)

    # Show the table contents via Fabric CLI
    run_powershell(
        "uv tool run --from ms-fabric-cli fab ls FabricDevWS.Workspace/devlake.Lakehouse",
        pace=pace,
    )
    time.sleep(READ_PAUSE)

    time.sleep(settle_time(pace))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a single posixlake demo scene")
    parser.add_argument(
        "scene",
        choices=[
            "intro",
            "fabric_origin",
            "windows_server",
            "windows_client",
            "wsl_server",
            "wsl_client",
            "s3_interlude",
            "fabric_homecoming",
            "outro",
        ],
    )
    parser.add_argument("--pace", type=float, default=1.0)
    args = parser.parse_args()

    scenes = {
        "intro": run_title_card_intro,
        "fabric_origin": run_fabric_origin,
        "windows_server": run_windows_server,
        "windows_client": run_windows_client,
        "wsl_server": run_wsl_server,
        "wsl_client": run_wsl_client,
        "s3_interlude": run_s3_interlude,
        "fabric_homecoming": run_fabric_homecoming,
        "outro": run_title_card_outro,
    }
    scenes[args.scene](args.pace)


def generate_title_card(output_path: Path, duration: float, title: str, subtitle: str) -> None:
    """Generate a title card video with logo + text using ffmpeg."""
    logo = REPO_ROOT / "posixlake-logo.png"
    # Dark background with centered logo and text
    filter_complex = (
        f"color=c=0x1a1a2e:s=2496x1664:d={duration}[bg];"
        f"[2:v]scale=300:300[logo];"
        f"[bg][logo]overlay=(W-w)/2:(H-h)/2-200[withlogo];"
        f"[withlogo]drawtext=text='{title}':fontsize=72:fontcolor=white:"
        f"x=(w-text_w)/2:y=(h/2)+100:fontfile=C\\\\:/Windows/Fonts/segoeui.ttf,"
        f"drawtext=text='{subtitle}':fontsize=36:fontcolor=0xaaaaaa:"
        f"x=(w-text_w)/2:y=(h/2)+190:fontfile=C\\\\:/Windows/Fonts/segoeui.ttf[out]"
    )
    subprocess.run(
        [
            "ffmpeg", "-y",
            "-f", "lavfi", "-i", f"color=c=0x1a1a2e:s=2496x1664:d={duration}:r=30",
            "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=mono:d={duration}",
            "-i", str(logo),
            "-filter_complex", filter_complex,
            "-map", "[out]",
            "-map", "1:a",
            "-c:v", "libx264", "-crf", "18",
            "-r", "30",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "128k",
            "-shortest",
            "-movflags", "+faststart",
            str(output_path),
        ],
        check=True, capture_output=True, timeout=60,
    )


def run_title_card_intro(pace: float) -> None:
    generate_title_card(
        SEGMENTS["intro"],
        duration=20.0,
        title="posixlake",
        subtitle="PowerShell and UNIX tools write Delta Lake",
    )


def run_title_card_outro(pace: float) -> None:
    generate_title_card(
        SEGMENTS["outro"],
        duration=22.0,
        title="posixlake",
        subtitle="Local | Fabric | Azure | S3  —  github.com/npiesco/posixlake",
    )


if __name__ == "__main__":
    main()
