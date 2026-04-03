from __future__ import annotations

SCENES = [
    {
        "id": "intro",
        "title": "Intro",
        "narration": (
            "posixlake — a cross-platform database where PowerShell and UNIX tools "
            "write Delta Lake. Today we'll create a table in Microsoft Fabric, "
            "manipulate it from Windows and Linux using nothing but PowerShell and "
            "cat, grep, and awk, then verify the results back in Fabric."
        ),
    },
    {
        "id": "fabric_origin",
        "title": "Fabric Origin",
        "narration": (
            "The table is born in Microsoft Fabric's OneLake. "
            "One command creates a typed Delta schema directly in the lakehouse — "
            "no Spark job, no notebook, no portal clicks. "
            "The abfss path points straight to OneLake, authenticated with a Service Principal."
        ),
    },
    {
        "id": "windows_server",
        "title": "Windows Mount",
        "narration": (
            "An operator mounts that same Fabric table as a Windows drive letter. "
            "The NFS server connects to OneLake, and Windows sees it as a local filesystem. "
            "No download, no local copy — every read and write goes directly to Fabric."
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
            "Back in Fabric — a direct SQL query against OneLake shows every row. "
            "Six sensor readings, written from Windows PowerShell, "
            "read from Linux cat and grep, all persisted in Delta Lake format. "
            "The table is ready for Power BI, Spark, or any Fabric workload."
        ),
    },
    {
        "id": "outro",
        "title": "Outro",
        "narration": (
            "One Delta table. Three platforms. Zero special tools. "
            "posixlake turns PowerShell and POSIX commands into production Delta Lake — "
            "on local storage, Microsoft Fabric, Azure Blob, or S3. "
            "Learn more at github.com/npiesco/posixlake."
        ),
    },
]
