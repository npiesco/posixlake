# Runbook: Backup and Restore

## When to use

- Scheduled backup drill (recommended: weekly)
- Before schema migration or major upgrade
- Disaster recovery after data loss or corruption

## Prerequisites

- `posixlake-cli` binary on PATH
- Read access to the source database directory
- Write access to the backup destination

## Full Backup

```bash
# 1. Verify source is healthy
posixlake health /path/to/db
# Expect: {"status": "healthy", ...}

# 2. Create backup
# (Uses the Rust API — invoke via your application or a wrapper script)
# The backup copies _delta_log + Parquet files + _metadata atomically.

# 3. Verify backup directory exists
ls /path/to/backup/_delta_log/
ls /path/to/backup/_metadata/
```

## Restore

```bash
# 1. Stop any running mounts against the database
posixlake unmount /mnt/db

# 2. Restore from backup to a new path
# (Uses the Rust API — DatabaseOps::restore(backup_path, restore_path))

# 3. Verify restored database
posixlake health /path/to/restored

# 4. Spot-check data
# Mount and query to confirm row counts match expectations
```

## Incremental Backup

```bash
# Incremental backup only copies commits since the last backup.
# Use DatabaseOps::backup_incremental(backup_path) via the Rust API.
# The base backup must already exist.
```

## Verification Checklist

- [ ] `posixlake health` returns `"healthy"` on restored DB
- [ ] Row count matches pre-backup count
- [ ] Schema matches original
- [ ] Auth users/roles preserved (if auth was enabled)
- [ ] Audit log present in `_metadata/audit.json`

## Failure Modes

| Symptom | Cause | Action |
|---------|-------|--------|
| Restore fails with "delta log missing" | Incomplete backup | Re-run full backup from source |
| Row count mismatch | Backup taken during active writes | Stop writes, re-backup |
| Auth fails after restore | users.json missing | Check `_metadata/` was included in backup |
