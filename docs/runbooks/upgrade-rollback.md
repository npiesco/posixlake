# Runbook: Upgrade and Rollback

## When to use

- Upgrading posixlake to a new version
- Rolling back after a failed upgrade

## Pre-Upgrade Checklist

- [ ] Read release notes for breaking changes
- [ ] Take full backup (see [backup-restore runbook](./backup-restore.md))
- [ ] Verify current health: `posixlake health /path/to/db`
- [ ] Record current metrics: `posixlake metrics /path/to/db > pre-upgrade-metrics.json`
- [ ] Stop active mounts: `posixlake unmount /mnt/db`

## Upgrade Steps

```bash
# 1. Install new binary
# (Replace with your package manager or build process)
cargo build --release -p posixlake --bin posixlake-cli
cp target/release/posixlake-cli /usr/local/bin/

# 2. Verify new version
posixlake-cli --version

# 3. Test open on a copy (not production)
cp -r /path/to/db /tmp/upgrade-test-db
posixlake health /tmp/upgrade-test-db

# 4. Run against production
posixlake health /path/to/db

# 5. Re-mount if needed
posixlake mount /path/to/db /mnt/db
```

## Rollback

```bash
# 1. Stop mounts
posixlake unmount /mnt/db

# 2. Restore previous binary
cp /usr/local/bin/posixlake-cli.bak /usr/local/bin/posixlake-cli

# 3. If on-disk format changed, restore from pre-upgrade backup
# See backup-restore runbook

# 4. Verify
posixlake health /path/to/db
```

## Verification After Upgrade

- [ ] `posixlake --version` shows expected version and commit
- [ ] `posixlake health` returns `"healthy"`
- [ ] Query returns expected data
- [ ] Auth still works (if enabled)
- [ ] Metrics tracking resumes normally
