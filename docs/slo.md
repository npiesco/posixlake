# Service Level Objectives (SLOs)

These SLOs apply to posixlake CLI workloads on a single-node deployment.

## Availability

| Metric | Target | Measurement |
|--------|--------|-------------|
| Database open | 99.9% success rate | `posixlake health` returns `"healthy"` |
| Query success | 99.5% for valid SQL | Excludes malformed SQL / auth failures |
| Insert success | 99.5% | Excludes schema mismatch / auth failures |

## Latency

| Operation | p50 | p99 | Measurement |
|-----------|-----|-----|-------------|
| Point query (< 10K rows) | < 10 ms | < 100 ms | `posixlake metrics` avg/max latency |
| Full scan (< 1M rows) | < 500 ms | < 5 s | Wall clock via `posixlake metrics` |
| Insert (single batch) | < 50 ms | < 500 ms | Delta commit round-trip |
| Health check | < 5 ms | < 50 ms | `posixlake health` round-trip |

## Durability

| Metric | Target | Mechanism |
|--------|--------|-----------|
| Committed data loss | 0 rows | Delta Lake ACID transactions; fsync on commit |
| Backup RPO | Operator-defined | `posixlake backup` frequency |
| Backup RTO | < 5 min for < 1 GB | `posixlake restore` wall clock |

## Error Budget

- Allow up to 0.5% failed operations per rolling 24-hour window before triggering incident review.
- Track via `posixlake metrics` (`total_errors / (total_queries + total_inserts + total_deletes)`).

## Monitoring

Use `posixlake health` and `posixlake metrics` (JSON output) to feed external dashboards or alerting systems.

```bash
# Health probe (cron / systemd timer)
posixlake health /path/to/db | jq -e '.status == "healthy"'

# Metrics scrape
posixlake metrics /path/to/db | jq '{queries: .total_queries, errors: .total_errors, latency_avg: .avg_query_latency_ms}'
```
