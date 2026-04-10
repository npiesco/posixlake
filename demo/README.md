# posixlake CLI Demo

Narrated 9-scene video demonstrating posixlake with Microsoft Fabric OneLake, Windows NFS, WSL Linux, and S3.

## Final Video

**`demo/output/posixlake_cli_demo_final.mp4`** — narrated, concatenated, ready to publish.

## Scenes

| # | Scene | What happens |
|---|-------|-------------|
| 0 | **Intro** | Title card, project overview |
| 1 | **Fabric Origin** | Connect to Fabric OneLake lakehouse, verify empty table |
| 2 | **Windows Server** | Start NFS server on Windows, mount to `Y:\` |
| 3 | **Windows Client** | PowerShell writes 6 IoT sensor rows via NFS, flags `temp_01` → `TEMP_01` (anomaly) |
| 4 | **WSL Server** | Start NFS server on Linux (WSL), mount same database |
| 5 | **WSL Client** | POSIX tools (`cat`, `grep`, `sed`) detect anomaly, flip `TEMP_01` → `temp_01` (resolved) |
| 6 | **S3 Interlude** | Same table accessed via S3-compatible storage |
| 7 | **Fabric Homecoming** | Query Fabric OneLake — all 6 rows present, anomaly resolved |
| 8 | **Outro** | Title card, links |

## Architecture

- **Per-window recording** via candycam using PID-based HWND matching (`RECORD_WINDOW_PID`)
- **Narration** via Azure Speech TTS with SSML pronunciation hints
- **Concat** via ffmpeg filter_complex with scale+pad normalization
- **Validation gates** after scenes 1, 3, 5, 7 — exact row count, sensor names, schema checks

## Requirements

- Windows build: `target/release/posixlake-cli.exe`
- WSL Linux build: `target/release/posixlake-cli`
- WSL distro (default: `Ubuntu`), passwordless sudo for mount/umount
- `ffmpeg` and `ffprobe` on `PATH`
- candycam `capture` wheel installed into the uv environment
- `AZURE_SPEECH_KEY` + `AZURE_SPEECH_REGION` (or `FOUNDRY_API_KEY` + `FOUNDRY_REGION`)
- Fabric OneLake credentials in `demo/.env` (see `config.py`)

## Setup

```powershell
cd demo
uv sync
uv pip install vendor\wheels\capture-0.1.0-cp313-cp313-win_arm64.whl
```

## Build Both Binaries

```powershell
# Windows
cargo build --release -p posixlake

# WSL Linux
wsl -d Ubuntu -- bash -lc 'cd /mnt/c/Users/npiesco/posixlake && cargo build --release -p posixlake'
```

## Run

### Dry run (headless, validates all 4 gates)

```powershell
uv run python orchestrate.py dry-run
```

### Record (opens windows, captures video, generates narration)

```powershell
uv run python orchestrate.py record
```

## Output Artifacts

| File | Description |
|------|-------------|
| `output/posixlake_cli_demo_final.mp4` | Final narrated video |
| `output/posixlake_cli_demo.mp4` | Video without narration |
| `output/posixlake_cli_demo_narration.wav` | Full narration audio track |
| `output/actual_timings.json` | Per-scene durations |
| `output/seg_*.mp4` | Individual scene segments |

## Files

| File | Purpose |
|------|---------|
| `orchestrate.py` | Main entry point — dry-run, record, concat, narration mux |
| `scene_runner.py` | Per-scene CLI commands (PowerShell, WSL bash) |
| `candycam_helper.py` | Subprocess protocol for per-window PID recording |
| `narration.py` | SSML narration text per scene |
| `config.py` | Paths, Fabric credentials, window titles, segment paths |
