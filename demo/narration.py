from __future__ import annotations

SCENES = [
    {
        "id": "windows_server",
        "title": "Windows server",
        "narration": (
            "An ops team monitors IoT sensors across a facility. "
            "The CLI provisions a typed Delta Lake table and serves it as a mounted drive — "
            "no cloud console, no Spark cluster required."
        ),
    },
    {
        "id": "windows_client",
        "title": "Windows client",
        "narration": (
            "From PowerShell, the operator loads six sensor readings — "
            "temperature, humidity, pressure, CO2, and flow — from across the plant. "
            "Temp oh one shows an anomaly, so the operator flags it by uppercasing the sensor name. "
            "A file overwrite triggers an atomic Delta merge. "
            "The status command confirms every version is tracked automatically."
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
            "and sort confirms one ACID table across both platforms."
        ),
    },
]
