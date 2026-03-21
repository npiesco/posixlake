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

    # Read the empty CSV facade (header only)
    run_powershell(f"Get-Content '{target}'", pace=pace)

    # Seed 6 rows — varied data for a real-looking demo
    run_powershell(f"Add-Content -Path '{target}' -Value '1,Alice,30,NYC'", pace=pace)
    run_powershell(f"Add-Content -Path '{target}' -Value '2,Bob,25,SF'", pace=pace)
    run_powershell(f"Add-Content -Path '{target}' -Value '3,Carol,28,Chicago'", pace=pace)
    run_powershell(f"Add-Content -Path '{target}' -Value '4,David,35,Seattle'", pace=pace)
    run_powershell(f"Add-Content -Path '{target}' -Value '5,Eve,22,Boston'", pace=pace)
    run_powershell(f"Add-Content -Path '{target}' -Value '6,Frank,40,Denver'", pace=pace)

    # Show all 6 rows
    run_powershell(f"Get-Content '{target}'", pace=pace)

    # Bulk update — uppercase Alice via file overwrite (atomic Delta merge)
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

    # Verify the merge — ALICE should appear
    run_powershell(f"Get-Content '{target}'", pace=pace)

    # Show Delta Lake version history
    run_process(
        [str(WINDOWS_CLI), "status", str(WINDOWS_MOUNT)],
        display=f"& {WINDOWS_CLI.name} status {WINDOWS_MOUNT}",
        pace=pace,
    )

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
    show_command("$", f"{wsl_cli_path()} mount {wsl_db_path()} {WSL_MOUNT} --port {WSL_NFS_PORT}", pace)
    subprocess.run(
        wsl_root_command(f"{wsl_cli_path()} mount {wsl_db_path()} {WSL_MOUNT} --port {WSL_NFS_PORT}"),
        check=True,
    )


def run_wsl_client(pace: float) -> None:
    set_console_title(WINDOW_TITLES["wsl_client"])
    target = f"{WSL_MOUNT}/data/data.csv"
    time.sleep(settle_time(pace))

    # Read — all 6 rows from Windows are visible
    run_wsl_user(f"cat {target}", pace=pace, display=f"cat {target}")

    # grep — find the uppercased row from the Windows merge
    run_wsl_user(f"grep ALICE {target}", pace=pace, display=f"grep ALICE {target}")

    # awk — extract just the name column
    run_wsl_user(
        f"awk -F, 'NR > 1 {{ print \\$2 }}' {target}",
        pace=pace,
        display=f"awk -F, 'NR > 1 {{ print $2 }}' {target}",
    )

    # wc — count rows
    run_wsl_user(f"wc -l {target}", pace=pace, display=f"wc -l {target}")

    # sed — in-place edit: bump ALICE's age 30→32
    # NFS doesn't support sed -i (temp-file rename), so stream through a tmp copy
    run_wsl_user(
        f"sed 's/ALICE,30/ALICE,32/' {target} > /tmp/_posixlake_sed.csv && cp /tmp/_posixlake_sed.csv {target} && rm /tmp/_posixlake_sed.csv",
        pace=pace,
        display=f"sed -i 's/ALICE,30/ALICE,32/' {target}",
    )

    # Append two new rows from Linux
    run_wsl_user(
        f"echo '7,Grace,29,Austin' >> {target}",
        pace=pace,
        display=f"echo '7,Grace,29,Austin' >> {target}",
    )
    run_wsl_user(
        f"echo '8,Hank,33,Portland' >> {target}",
        pace=pace,
        display=f"echo '8,Hank,33,Portland' >> {target}",
    )

    # Show final state — 8 rows from both platforms
    run_wsl_user(f"cat {target}", pace=pace, display=f"cat {target}")

    # Sort by name to prove full POSIX tool compatibility
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

    # Unmount
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
