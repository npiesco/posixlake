# posixlake CLI Demo Plan

## Goal

Show `posixlake-cli` operating on the same Delta Lake table across:

- **Microsoft Fabric OneLake** (cloud origin + homecoming)
- **native Windows** (PowerShell)
- **WSL2 Linux** (bash)
- **S3/MinIO** (backend portability interlude)

with:

- every command visible on screen
- candycam video capture
- Azure Speech narration padded to measured scene durations
- a required headless dry run before recording

## Story Arc — "From Cloud to Terminal and Back"

A sensor IoT dataset starts in Microsoft Fabric, gets manipulated through
posixlake on Windows and Linux using nothing but POSIX tools, then lands
back in Fabric ready for Power BI. The S3 interlude proves the same
workflow is cloud-agnostic.

## Scenes

1. **Fabric Origin** (PowerShell)
   - `posixlake-cli create abfss://...` with Service Principal env vars
   - `posixlake-cli health abfss://...`
   - Delta table born in OneLake — no Spark, no notebook

2. **Windows Mount** (PowerShell)
   - mount Fabric OneLake table to `P:` (or next free drive)
   - NFS server bridges OneLake ↔ Windows filesystem

3. **Windows Ops** (PowerShell)
   - `Get-Content` — read empty CSV facade
   - `Add-Content` × 6 — seed sensor readings
   - `Get-Content` — verify 6 rows
   - overwrite CSV → triggers atomic Delta MERGE (flag anomaly)
   - `Get-Content` — verify merge
   - `status` — show Delta version history
   - `unmount`

4. **WSL Mount** (bash)
   - mount same Fabric table from WSL
   - same OneLake Delta table, different OS

5. **WSL Ops** (bash)
   - `cat` — read latest state (all 6 readings from Windows)
   - `grep TEMP_01` — find flagged anomaly
   - `awk` — extract sensor names
   - `wc -l` — count readings
   - `sed` — recalibrate flagged sensor (72→73)
   - `echo >>` × 2 — append vibration + noise sensors
   - `cat` — show final 8-row state
   - `sort` — prove full POSIX tool compatibility
   - `status` + `unmount`

6. **S3 Interlude** (PowerShell)
   - `posixlake-cli create s3://...` on MinIO
   - `posixlake-cli health s3://...`
   - Same engine, different cloud — backend portability

7. **Fabric Homecoming** (PowerShell)
   - `fab table schema FabricDevWS.Workspace/devlake.Lakehouse` — show schema
   - Query via SQL endpoint — see all 8 rows from Windows + Linux
   - Data is ready for Power BI / Spark / any Fabric workload

## Pipeline

1. Preflight: native Windows binary, WSL binary, sudo, ffmpeg, speech
   credentials, candycam import, Fabric SP credentials, MinIO.
2. Generate PowerShell and bash scripts for all seven scenes.
3. Run a headless dry run and store timings in `demo/output/timings.json`.
4. Launch visible scene windows and record them by title.
5. Measure real segment durations during the recorded run.
6. Synthesize one narration file per scene.
7. Pad each narration segment to the actual scene duration.
8. Concatenate video segments.
9. Concatenate padded audio segments.
10. Merge final video and narration.

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `FABRIC_ONELAKE_TABLES_PATH` | OneLake abfss:// path to lakehouse Tables |
| `AZURE_STORAGE_CLIENT_ID` | Service Principal app ID |
| `AZURE_STORAGE_CLIENT_SECRET` | Service Principal password |
| `AZURE_STORAGE_TENANT_ID` | Entra tenant ID |
| `MINIO_ENDPOINT` | MinIO endpoint for S3 interlude |
| `MINIO_ACCESS_KEY` | MinIO access key |
| `MINIO_SECRET_KEY` | MinIO secret key |
| `AZURE_SPEECH_KEY` | Azure Speech TTS key |
| `AZURE_SPEECH_REGION` | Azure Speech TTS region |
