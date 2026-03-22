from __future__ import annotations

SCENES = [
    {
        "id": "s3_cloud",
        "title": "S3 cloud",
        "narration": (
            "The table lives in S3. "
            "One command creates a typed Delta schema directly on object storage."
        ),
    },
    {
        "id": "windows_server",
        "title": "Windows server",
        "narration": (
            "Now an operator mounts that same S3 table as a Windows drive — "
            "no download, no local copy."
        ),
    },
    {
        "id": "windows_client",
        "title": "Windows client",
        "narration": (
            "From PowerShell, the operator loads six sensor readings — "
            "temperature, humidity, pressure, CO2, and flow. "
            "Temp oh one shows an anomaly, so the operator flags it. "
            "A file overwrite triggers an atomic Delta merge. "
            "Every change writes back to S3."
        ),
    },
    {
        "id": "wsl_server",
        "title": "WSL server",
        "narration": (
            "A Linux engineer mounts the exact same S3 table from WSL — "
            "no export, no data copy."
        ),
    },
    {
        "id": "wsl_client",
        "title": "WSL client",
        "narration": (
            "Cat reads the latest state. Grep locates the flagged sensor, awk extracts names. "
            "Sed recalibrates the reading, two new sensors are appended. "
            "Sort confirms one ACID table — S3, Windows, and Linux."
        ),
    },
]
