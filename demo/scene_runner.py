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


def confirm_fabric_sensor(
    *,
    cli: str,
    db_path: str,
    env: dict[str, str],
    expected: str,
    sensor_id: int,
    max_attempts: int = 5,
    wsl: bool = False,
) -> None:
    """Confirm a sensor value landed in Fabric with incremental retry.

    Each attempt queries the Delta table via posixlake-cli (separate process,
    fresh table, no cached snapshot). Retries with incremental backoff to
    handle OneLake eventual consistency.
    """
    sql = f"SELECT sensor FROM data WHERE id = {sensor_id}"
    for attempt in range(max_attempts):
        if wsl:
            query_cmd = (
                f"AZURE_STORAGE_CLIENT_ID='{env.get('AZURE_STORAGE_CLIENT_ID', '')}'"
                f" AZURE_STORAGE_CLIENT_SECRET='{env.get('AZURE_STORAGE_CLIENT_SECRET', '')}'"
                f" AZURE_STORAGE_TENANT_ID='{env.get('AZURE_STORAGE_TENANT_ID', '')}'"
                f" {cli} query '{db_path}' \"{sql}\""
            )
            cmd = wsl_user_command(query_cmd)
        else:
            cmd = [cli, "query", db_path, sql]
        try:
            result = subprocess.run(
                cmd,
                text=True,
                capture_output=True,
                encoding="utf-8",
                errors="replace",
                env=env if not wsl else None,
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            result = type("R", (), {"stdout": "", "stderr": "timeout"})()
        if expected in result.stdout:
            print(f"[handshake] {expected} confirmed (attempt {attempt + 1})")
            return
        wait = 3 * (attempt + 1)
        print(f"[handshake] attempt {attempt + 1}: got {result.stdout.strip()!r}, retrying in {wait}s")
        time.sleep(wait)
    raise RuntimeError(
        f"[handshake] {expected} not found after {max_attempts} attempts.\n"
        f"Last output:\n{result.stdout}\nStderr:\n{result.stderr}"
    )


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


def run_process(cmd: list[str], *, display: str, pace: float, timeout: int = 240, env: dict[str, str] | None = None) -> None:
    show_command("PS>", display, pace)
    result = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
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

    # Flush Windows NFS client cache: unmount + remount (server stays running)
    run_powershell(f"net use {WINDOWS_MOUNT} /delete /y", pace=pace)
    time.sleep(1)
    run_powershell(
        f"C:\\Windows\\System32\\mount.exe -o anon,nolock,mtype=hard,fileaccess=6,lang=ansi,rsize=128,wsize=128,timeout=120,retry=5 \\\\localhost\\share {WINDOWS_MOUNT}",
        pace=pace,
    )
    time.sleep(1)

    # Show all 6 readings (fresh NFS handle — no stale cache)
    run_powershell(f"Get-Content '{target}'", pace=pace)
    time.sleep(READ_PAUSE)

    # Flag anomaly — uppercase temp_01 to mark it for review (atomic Delta merge)
    run_powershell(
        f"Get-Content '{target}' | ForEach-Object {{ $_ -replace 'temp_01','TEMP_01' }} | Set-Content '{WINDOWS_CLIENT_TEMP}' -Encoding ascii",
        pace=pace,
    )
    show_command("PS>", f"Copy-Item -Force '{WINDOWS_CLIENT_TEMP}' '{target}'", pace)
    copy_result = run_capture(
        ["powershell.exe", "-NoProfile", "-Command",
         f"Copy-Item -Force '{WINDOWS_CLIENT_TEMP}' '{target}'"],
        timeout=120,
    )
    print_result(copy_result)
    time.sleep(READ_PAUSE)

    # Flush cache again to verify the merge
    run_powershell(f"net use {WINDOWS_MOUNT} /delete /y", pace=pace)
    time.sleep(1)
    run_powershell(
        f"C:\\Windows\\System32\\mount.exe -o anon,nolock,mtype=hard,fileaccess=6,lang=ansi,rsize=128,wsize=128,timeout=120,retry=5 \\\\localhost\\share {WINDOWS_MOUNT}",
        pace=pace,
    )
    time.sleep(1)

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

    # Handshake: confirm TEMP_01 landed in Fabric Delta table before exiting.
    # Uses incremental retry — OneLake eventual consistency may delay visibility.
    fabric_env = os.environ.copy()
    fabric_env["AZURE_STORAGE_CLIENT_ID"] = FABRIC_CLIENT_ID
    fabric_env["AZURE_STORAGE_CLIENT_SECRET"] = FABRIC_CLIENT_SECRET
    fabric_env["AZURE_STORAGE_TENANT_ID"] = FABRIC_TENANT_ID
    confirm_fabric_sensor(
        cli=str(WINDOWS_CLI),
        db_path=FABRIC_DB_PATH,
        env=fabric_env,
        expected="TEMP_01",
        sensor_id=1,
    )


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

    # Confirm TEMP_01 is visible in Fabric before reading NFS —
    # WSL NFS server may have loaded a stale snapshot if OneLake hadn't replicated yet.
    fabric_env = os.environ.copy()
    fabric_env["AZURE_STORAGE_CLIENT_ID"] = FABRIC_CLIENT_ID
    fabric_env["AZURE_STORAGE_CLIENT_SECRET"] = FABRIC_CLIENT_SECRET
    fabric_env["AZURE_STORAGE_TENANT_ID"] = FABRIC_TENANT_ID
    confirm_fabric_sensor(
        cli=wsl_cli_path(),
        db_path=FABRIC_DB_PATH,
        env=fabric_env,
        expected="TEMP_01",
        sensor_id=1,
        wsl=True,
    )

    # Breathing room — wait for NFS mount to stabilize
    time.sleep(3)

    # First read — retry if NFS mount isn't ready yet
    for attempt in range(3):
        try:
            run_wsl_user(f"cat {target}", pace=pace, display=f"cat {target}")
            break
        except RuntimeError:
            if attempt < 2:
                time.sleep(2)
            else:
                raise
    run_wsl_user(f"grep TEMP_01 {target}", pace=pace, display=f"grep TEMP_01 {target}")
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

    # Resolve anomaly — flip TEMP_01 back to temp_01
    run_wsl_user(
        f"sed 's/TEMP_01/temp_01/' {target} > /tmp/_posixlake_sed.csv && sleep 2 && cp /tmp/_posixlake_sed.csv {target} && rm /tmp/_posixlake_sed.csv",
        pace=pace,
        display=f"sed -i 's/TEMP_01/temp_01/' {target}",
        timeout=120,
    )

    # Verify anomaly resolved via Delta query (bypasses NFS read, waits for merge)
    fabric_env = os.environ.copy()
    fabric_env["AZURE_STORAGE_CLIENT_ID"] = FABRIC_CLIENT_ID
    fabric_env["AZURE_STORAGE_CLIENT_SECRET"] = FABRIC_CLIENT_SECRET
    fabric_env["AZURE_STORAGE_TENANT_ID"] = FABRIC_TENANT_ID
    confirm_fabric_sensor(
        cli=wsl_cli_path(),
        db_path=FABRIC_DB_PATH,
        env=fabric_env,
        expected="temp_01",
        sensor_id=1,
        wsl=True,
    )
    time.sleep(READ_PAUSE)

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

    # Query the Fabric table — prove all 6 rows persisted from Windows + WSL
    run_process(
        [str(WINDOWS_CLI), "query", FABRIC_DB_PATH, "SELECT * FROM data ORDER BY id"],
        display=f"& {WINDOWS_CLI.name} query {FABRIC_DB_PATH} 'SELECT * FROM data ORDER BY id'",
        pace=pace,
        timeout=60,
    )
    time.sleep(READ_PAUSE)

    # Show row count
    run_process(
        [str(WINDOWS_CLI), "query", FABRIC_DB_PATH, "SELECT COUNT(*) as total_rows FROM data"],
        display=f"& {WINDOWS_CLI.name} query {FABRIC_DB_PATH} 'SELECT COUNT(*) as total_rows FROM data'",
        pace=pace,
        timeout=60,
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
        f"color=c=0x1a1a2e:s=1646x1038:d={duration}[bg];"
        f"[2:v]scale=200:200[logo];"
        f"[bg][logo]overlay=(W-w)/2:(H-h)/2-130[withlogo];"
        f"[withlogo]drawtext=text='{title}':fontsize=48:fontcolor=white:"
        f"x=(w-text_w)/2:y=(h/2)+65:fontfile=C\\\\:/Windows/Fonts/segoeui.ttf,"
        f"drawtext=text='{subtitle}':fontsize=24:fontcolor=0xaaaaaa:"
        f"x=(w-text_w)/2:y=(h/2)+125:fontfile=C\\\\:/Windows/Fonts/segoeui.ttf[out]"
    )
    subprocess.run(
        [
            "ffmpeg", "-y",
            "-f", "lavfi", "-i", f"color=c=0x1a1a2e:s=1646x1038:d={duration}:r=30",
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
