from __future__ import annotations

SCENES = [
    {
        "id": "windows_server",
        "title": "Windows server",
        "narration": (
            "We start natively on Windows. The release CLI creates a fresh Delta Lake table, "
            "then mounts it as an NFS-backed drive so standard file tools can operate on live data."
        ),
    },
    {
        "id": "windows_client",
        "title": "Windows client",
        "narration": (
            "In PowerShell, every command is operating on the mounted drive. "
            "Get Content reads the generated CSV facade, Add Content appends rows, "
            "and a full file overwrite becomes an atomic Delta Lake merge."
        ),
    },
    {
        "id": "wsl_server",
        "title": "WSL server",
        "narration": (
            "Now the Linux binary inside WSL mounts the exact same database directory. "
            "Same project, same table, same posixlake workflow, but through the Linux NFS client."
        ),
    },
    {
        "id": "wsl_client",
        "title": "WSL client",
        "narration": (
            "Inside WSL, classic POSIX commands take over. Cat, grep, awk, sed, and echo all work against the mounted table, "
            "proving native Windows and Linux parity on one Delta Lake dataset."
        ),
    },
]
