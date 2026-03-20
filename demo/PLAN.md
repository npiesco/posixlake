# posixlake CLI Demo Plan

## Goal

Show `posixlake-cli` operating on the same Delta Lake table from:

- native Windows
- WSL2 Linux

with:

- every command visible on screen
- candycam video capture
- Azure Speech narration padded to measured scene durations
- a required headless dry run before recording

## Scenes

1. **Windows server**
   - create database
   - mount to `P:`
2. **Windows client**
   - `Get-Content`
   - `Add-Content`
   - overwrite full CSV to trigger MERGE
   - `status`
   - `unmount`
3. **WSL server**
   - mount same database to `/mnt/posixlake-demo`
4. **WSL client**
   - `cat`
   - `grep`
   - `awk`
   - `sed -i`
   - `echo >>`
   - `status`
   - `unmount`

## Pipeline

1. Preflight native Windows binary, WSL binary, sudo, ffmpeg, speech credentials, candycam import.
2. Generate PowerShell and bash scripts for all four scenes.
3. Run a headless dry run and store timings in `demo/output/timings.json`.
4. Launch visible scene windows and record them by title.
5. Measure real segment durations during the recorded run.
6. Synthesize one narration file per scene.
7. Pad each narration segment to the actual scene duration.
8. Concatenate video segments.
9. Concatenate padded audio segments.
10. Merge final video and narration.
