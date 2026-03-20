from __future__ import annotations

SCENES = [
    {
        "id": "windows_server",
        "title": "Windows server",
        "narration": (
            "We start natively on Windows. "
            "The release CLI creates a fresh Delta Lake table with a typed schema, "
            "then mounts it as a local drive letter through the built-in NFS server."
        ),
    },
    {
        "id": "windows_client",
        "title": "Windows client",
        "narration": (
            "In PowerShell, Get Content reads the CSV facade from the Delta table. "
            "We seed six rows of sample data — Alice, Bob, Carol, David, Eve, and Frank — "
            "each with an age and city. "
            "A file overwrite replaces Alice with uppercase ALICE. "
            "Posixlake turns that into an atomic Delta Lake merge. "
            "The status command confirms the full version history."
        ),
    },
    {
        "id": "wsl_server",
        "title": "WSL server",
        "narration": (
            "The Linux binary mounts the same database inside WSL."
        ),
    },
    {
        "id": "wsl_client",
        "title": "WSL client",
        "narration": (
            "Cat reads all six rows from Windows. "
            "Grep finds ALICE, awk extracts names, sed edits in place. "
            "Two more rows are appended from Linux, and sort confirms cross-platform parity."
        ),
    },
]
