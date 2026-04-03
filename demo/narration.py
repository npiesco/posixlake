from __future__ import annotations

SCENES = [
    {
        "id": "fabric_origin",
        "title": "Fabric Origin",
        "narration": (
            "The table is born in Microsoft Fabric's OneLake. "
            "One command creates a typed Delta schema directly in the lakehouse — "
            "no Spark job, no notebook."
        ),
    },
    {
        "id": "windows_server",
        "title": "Windows Mount",
        "narration": (
            "An operator mounts that same Fabric table as a Windows drive letter. "
            "No download, no local copy — OneLake is the filesystem."
        ),
    },
    {
        "id": "windows_client",
        "title": "Windows Ops",
        "narration": (
            "PowerShell loads six sensor readings — "
            "temperature, humidity, pressure, CO2, and flow. "
            "Temp oh one shows an anomaly, so the operator flags it. "
            "A file overwrite triggers an atomic Delta merge. "
            "Every change writes directly to Fabric OneLake."
        ),
    },
    {
        "id": "wsl_server",
        "title": "WSL Mount",
        "narration": (
            "A Linux engineer mounts the exact same Fabric table from WSL — "
            "no export, no data copy — same OneLake Delta table, different OS."
        ),
    },
    {
        "id": "wsl_client",
        "title": "WSL Ops",
        "narration": (
            "Cat reads the latest state — all six rows from Windows are here. "
            "Grep locates the flagged sensor, awk extracts names. "
            "Head and tail slice the data, sort reorders by sensor. "
            "One ACID table — Fabric, Windows, and Linux."
        ),
    },
    {
        "id": "s3_interlude",
        "title": "S3 Interlude",
        "narration": (
            "The same workflow works with S3. "
            "One command creates the table on MinIO. "
            "Same engine, different cloud — backend portability."
        ),
    },
    {
        "id": "fabric_homecoming",
        "title": "Fabric Homecoming",
        "narration": (
            "Back in Fabric — the SQL endpoint shows every change. "
            "Eight rows from Windows and Linux. "
            "The table is ready for Power BI, Spark, or any Fabric workload. "
            "POSIX tools wrote production Delta Lake."
        ),
    },
]
