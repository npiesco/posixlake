from __future__ import annotations

import argparse
import ctypes
import shutil
import subprocess
import sys
import time
from pathlib import Path

from config import (
    REPO_ROOT,
    WINDOW_TITLES,
    WINDOWS_CLI,
    WINDOWS_DB_PATH,
    WINDOWS_MOUNT,
    WINDOWS_MOUNT_ROOT,
    WINDOWS_NFS_PORT,
    WSL_DISTRO,
    WSL_MOUNT,
    WSL_NFS_PORT,
    WSL_USER,
    windows_to_wsl,
    wsl_cli_path,
    wsl_db_path,
)

SETTLE_SECONDS = 2.0
WINDOWS_CLIENT_TEMP = WINDOWS_DB_PATH.parent / "windows_update.csv"


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


def run_windows_server(pace: float) -> None:
    set_console_title(WINDOW_TITLES["windows_server"])
    if WINDOWS_DB_PATH.exists():
        shutil.rmtree(WINDOWS_DB_PATH)
    run_process(
        [
            str(WINDOWS_CLI),
            "create",
            str(WINDOWS_DB_PATH),
            "--schema",
            "id:Int32,name:String,age:Int32,city:String",
        ],
        display=f"& {WINDOWS_CLI.name} create {WINDOWS_DB_PATH} --schema id:Int32,name:String,age:Int32,city:String",
        pace=pace,
    )
    time.sleep(settle_time(pace))
    show_command(
        "PS>",
        f"& {WINDOWS_CLI.name} mount {WINDOWS_DB_PATH} {WINDOWS_MOUNT} --port {WINDOWS_NFS_PORT}",
        pace,
    )
    subprocess.run(
        [
            str(WINDOWS_CLI),
            "mount",
            str(WINDOWS_DB_PATH),
            str(WINDOWS_MOUNT),
            "--port",
            str(WINDOWS_NFS_PORT),
        ],
        check=True,
    )


def run_windows_client(pace: float) -> None:
    set_console_title(WINDOW_TITLES["windows_client"])
    target = WINDOWS_MOUNT_ROOT / "data" / "data.csv"
    time.sleep(settle_time(pace))
    run_powershell(f"Get-Content '{target}'", pace=pace)
    run_powershell(f"Add-Content -Path '{target}' -Value '1,Alice,30,NYC'", pace=pace)
    run_powershell(f"Add-Content -Path '{target}' -Value '2,Bob,25,SF'", pace=pace)
    run_powershell(f"Get-Content '{target}'", pace=pace)
    run_powershell(
        f"Get-Content '{target}' | ForEach-Object {{ $_ -replace 'Alice','ALICE' }} | Set-Content '{WINDOWS_CLIENT_TEMP}' -Encoding ascii",
        pace=pace,
    )
    show_command("PS>", f"cmd /c copy /Y {WINDOWS_CLIENT_TEMP} {target}", pace)
    copy_result = run_capture(
        ["cmd.exe", "/c", "copy", "/Y", str(WINDOWS_CLIENT_TEMP), str(target)],
        timeout=120,
    )
    print_result(copy_result)
    run_powershell(f"Get-Content '{target}'", pace=pace)
    run_process(
        [str(WINDOWS_CLI), "status", str(WINDOWS_MOUNT)],
        display=f"& {WINDOWS_CLI.name} status {WINDOWS_MOUNT}",
        pace=pace,
    )
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
    show_command("$", f"{wsl_cli_path()} mount {wsl_db_path()} {WSL_MOUNT} --port {WSL_NFS_PORT}", pace)
    subprocess.run(
        wsl_root_command(f"{wsl_cli_path()} mount {wsl_db_path()} {WSL_MOUNT} --port {WSL_NFS_PORT}"),
        check=True,
    )


def run_wsl_client(pace: float) -> None:
    set_console_title(WINDOW_TITLES["wsl_client"])
    target = f"{WSL_MOUNT}/data/data.csv"
    time.sleep(settle_time(pace))
    run_wsl_user(f"cat {target}", pace=pace, display=f"su - {WSL_USER} -c \"cat {target}\"")
    run_wsl_user(f"grep ALICE {target}", pace=pace, display=f"su - {WSL_USER} -c \"grep ALICE {target}\"")
    run_wsl_user(
        f"awk -F, 'NR > 1 {{ print $2 }}' {target}",
        pace=pace,
        display=f"su - {WSL_USER} -c \"awk -F, 'NR > 1 {{ print $2 }}' {target}\"",
    )
    run_wsl_user(
        f"sed -i 's/ALICE,30/ALICE,31/' {target}",
        pace=pace,
        display=f"su - {WSL_USER} -c \"sed -i 's/ALICE,30/ALICE,31/' {target}\"",
    )
    run_wsl_user(
        f"echo '3,Charlie,28,LA' >> {target}",
        pace=pace,
        display=f"su - {WSL_USER} -c \"echo '3,Charlie,28,LA' >> {target}\"",
    )
    run_wsl_user(f"cat {target}", pace=pace, display=f"su - {WSL_USER} -c \"cat {target}\"")
    run_wsl_user(
        f"{wsl_cli_path()} status {WSL_MOUNT}",
        pace=pace,
        display=f"su - {WSL_USER} -c \"{wsl_cli_path()} status {WSL_MOUNT}\"",
    )
    run_wsl_root(f"{wsl_cli_path()} unmount {WSL_MOUNT}", pace=pace)
    time.sleep(settle_time(pace))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a single posixlake demo scene")
    parser.add_argument(
        "scene",
        choices=["windows_server", "windows_client", "wsl_server", "wsl_client"],
    )
    parser.add_argument("--pace", type=float, default=1.0)
    args = parser.parse_args()

    if args.scene == "windows_server":
        run_windows_server(args.pace)
    elif args.scene == "windows_client":
        run_windows_client(args.pace)
    elif args.scene == "wsl_server":
        run_wsl_server(args.pace)
    else:
        run_wsl_client(args.pace)


if __name__ == "__main__":
    main()
