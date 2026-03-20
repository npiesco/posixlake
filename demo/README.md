# posixlake CLI Demo

Recorded dual-surface CLI demo for `posixlake`:

- **Native Windows** using the Windows `posixlake-cli.exe` and PowerShell
- **WSL2 Linux** using the Linux `posixlake-cli` and POSIX tools
- **Screen capture** via candycam
- **Narration** via Azure Speech TTS
- **Final mux** via `ffmpeg`

The demo shows the same Delta Lake table from both environments, with every command printed on screen.

## What it demonstrates

1. Windows CLI creates and mounts a database to a drive letter.
2. PowerShell reads `data.csv`, appends rows, and performs a full-file overwrite that becomes a MERGE.
3. WSL mounts the exact same database directory with the Linux binary.
4. POSIX tools in WSL (`cat`, `grep`, `awk`, `sed`, `echo`) operate on the same live table.

## Requirements

- Windows build exists at `target/release/posixlake-cli.exe`
- WSL Linux build exists at `target/release/posixlake-cli`
- WSL distro available, default: `Ubuntu`
- WSL passwordless sudo for mount / umount flow
- `ffmpeg` and `ffprobe` on `PATH`
- candycam `capture` wheel installed into the uv environment
- `AZURE_SPEECH_KEY` + `AZURE_SPEECH_REGION` or `FOUNDRY_API_KEY` + `FOUNDRY_REGION`

## Setup with uv

From [demo/](demo):

```powershell
uv sync
uv pip install C:\Users\npiesco\forge\agents\demo-orchestrator\vendor\wheels\capture-0.1.0-cp313-cp313-win_arm64.whl
```

If the wheel path differs on this machine, point `uv pip install` at the correct file.

## Build both binaries

Native Windows:

```powershell
cargo build --release -p posixlake
```

WSL Linux:

```powershell
wsl -d Ubuntu -- bash -lc 'cd /mnt/c/Users/npiesco/posixlake && cargo build --release -p posixlake'
```

## Run the required dry run first

```powershell
uv run python demo/orchestrate.py dry-run
```

This executes the full workflow headlessly and writes timing data to [demo/output/timings.json](output/timings.json).

## Record the real demo

```powershell
uv run python demo/orchestrate.py record
```

Artifacts:

- [demo/output/posixlake_cli_demo.mp4](output/posixlake_cli_demo.mp4)
- [demo/output/posixlake_cli_demo_narration.wav](output/posixlake_cli_demo_narration.wav)
- [demo/output/posixlake_cli_demo_final.mp4](output/posixlake_cli_demo_final.mp4)
- [demo/output/timings.json](output/timings.json)
- [demo/output/actual_timings.json](output/actual_timings.json)

## Notes

- The Windows and WSL scenes are recorded by **window title**, not PID.
- Record mode regenerates scripts, records fresh segments, then pads narration to the measured segment durations.
- The WSL flow assumes the Linux binary can access the repo at `/mnt/c/...`.
