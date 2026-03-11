# Runbook: Incident Triage

## When to use

- Health check returns non-`"healthy"` status
- Error rate exceeds SLO budget (> 0.5% in 24h)
- Database unresponsive or producing unexpected results

## Step 1: Assess Severity

```bash
# Check health
posixlake health /path/to/db

# Check metrics for error rate
posixlake metrics /path/to/db | jq '{errors: .total_errors, queries: .total_queries, inserts: .total_inserts}'
```

| Health status | Severity | Action |
|---------------|----------|--------|
| `"healthy"` | Low | Investigate error metrics |
| `"degraded"` | Medium | Check delta log, disk space |
| `"unauthorized"` | Auth issue | Verify credentials |
| Open fails | Critical | Check file permissions, disk, corruption |

## Step 2: Common Issues

### Database won't open

```bash
# Check delta log exists
ls /path/to/db/_delta_log/

# Check permissions
ls -la /path/to/db/

# Check disk space
df -h /path/to/db
```

### High error rate

```bash
# Check audit log for patterns
tail -50 /path/to/db/_metadata/audit.json | jq 'select(.success == false)'
```

### Slow queries

```bash
# Check latency metrics
posixlake metrics /path/to/db | jq '{avg_ms: .avg_query_latency_ms, max_ms: .max_query_latency_ms}'

# If high: consider running optimize/vacuum
# (Via Rust API: db.optimize().await, db.vacuum().await)
```

### Mount failures

```bash
# Check if NFS port is in use
ss -tlnp | grep 12049

# Check mount status
posixlake status /mnt/db

# Force unmount and retry
sudo umount -f /mnt/db
posixlake mount /path/to/db /mnt/db
```

## Step 3: Escalation

If the issue cannot be resolved:

1. Capture full diagnostics:
   ```bash
   posixlake health /path/to/db > diag-health.json
   posixlake metrics /path/to/db > diag-metrics.json
   ls -laR /path/to/db/_delta_log/ > diag-delta-log.txt
   ```
2. Take backup if possible (see [backup-restore runbook](./backup-restore.md))
3. File issue with diagnostics attached

## Step 4: Post-Incident

- [ ] Root cause identified and documented
- [ ] Backup verified
- [ ] Monitoring confirmed healthy
- [ ] SLO impact assessed
