from __future__ import annotations

SCENES = [
    {
        "id": "s3_cloud",
        "title": "S3 cloud",
        "narration": (
            "Data starts in the cloud — "
            "posixlake creates and queries a Delta table directly on S3."
        ),
    },
    {
        "id": "windows_server",
        "title": "Windows server",
        "narration": (
            "Now the same CLI runs locally. "
            "It provisions a typed Delta table and mounts it as a Windows drive — "
            "no cloud console, no Spark cluster."
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
