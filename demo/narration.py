from __future__ import annotations

SCENES = [
    {
        "id": "windows_server",
        "title": "Windows server",
        "narration": (
            "The CLI creates a Delta Lake table and mounts it as a drive."
        ),
    },
    {
        "id": "windows_client",
        "title": "Windows client",
        "narration": (
            "PowerShell seeds six rows, reads the CSV, then overwrites the file. "
            "That overwrite becomes an atomic Delta Lake merge."
        ),
    },
    {
        "id": "wsl_server",
        "title": "WSL server",
        "narration": (
            "The same database, now mounted inside WSL."
        ),
    },
    {
        "id": "wsl_client",
        "title": "WSL client",
        "narration": (
            "Cat, grep, awk, sed, sort — standard POSIX tools operating on the same table. "
            "Two more rows are appended from Linux."
        ),
    },
]
