from __future__ import annotations

SCENES = [
    {
        "id": "s3_cloud",
        "title": "S3 cloud",
        "narration": (
            "Data starts in S3 — the CLI creates a Delta table, inserts readings, and queries them."
        ),
    },
    {
        "id": "windows_server",
        "title": "Windows server",
        "narration": (
            "Operations pulls the table to a local drive. "
            "One command creates the schema, another mounts it as a Windows NFS share."
        ),
    },
    {
        "id": "windows_client",
        "title": "Windows client",
        "narration": (
            "From PowerShell, the operator loads six sensor readings — "
            "temperature, humidity, pressure, CO2, and flow. "
            "Temp oh one shows an anomaly, so the operator flags it. "
            "A file overwrite triggers an atomic Delta merge, and status confirms version tracking."
        ),
    },
    {
        "id": "wsl_server",
        "title": "WSL server",
        "narration": (
            "A Linux engineer mounts the exact same table from WSL — "
            "no export, no data copy."
        ),
    },
    {
        "id": "wsl_client",
        "title": "WSL client",
        "narration": (
            "Cat reads the latest state. Grep locates the flagged sensor, awk extracts names. "
            "Sed recalibrates the reading, two new sensors are appended, "
            "and sort confirms one ACID table — cloud, Windows, and Linux."
        ),
    },
]
