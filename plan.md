# posixlake Enterprise Readiness Plan (Local CLI)

## Scope
This plan targets enterprise readiness for a local/self-hosted CLI component, not a managed cloud service.

## Goals
- Secure-by-default operation for local and team environments.
- Reproducible, auditable, supportable releases.
- Operational reliability with documented recovery procedures.
- Clear quality gates for shipping.

## Non-Goals
- Multi-tenant SaaS control plane.
- Centralized hosted auth service.
- Vendor-specific enterprise features without demand.

## Current Baseline (Observed)
- Strong core engine and broad test inventory.
- CI runs formatting, lint, build, unit tests, and S3 integration test.
- Security, RBAC, audit, backup/restore exist in code paths but need hardened defaults and operational guarantees.

## Phase 0 (Week 0-1): Readiness Definition
1. Define release tier and support statement for `v1.0.0`.
2. Freeze quality bars and release gates.
3. Assign owners per workstream.

### Exit Criteria
- `SUPPORT.md` drafted with support window and response targets.
- `RELEASE_POLICY.md` approved with versioning/deprecation rules.
- `PROD_READINESS.md` checklist created with pass/fail criteria.

## Phase 1 (Week 1-3): Security Defaults and Threat Model
1. Add `SECURITY.md` with reporting process and patch SLAs.
2. Document threat model for local NFS + CLI workflows.
3. Enforce explicit auth mode selection in CLI startup paths.
4. Add secure default profile (fail closed where auth is expected).
5. Add dependency vulnerability scanning and policy gates.

### Exit Criteria
- Security docs merged (`SECURITY.md`, threat model doc).
- CLI behavior documented and tested for auth-enabled vs auth-disabled modes.
- CI fails on high/critical dependency vulnerabilities.

## Phase 2 (Week 2-5): CI/CD and Release Integrity
1. Expand required CI from unit + one integration suite to a release matrix:
   - unit tests
   - core integration suites (auth, backup/restore, monitoring, NFS core)
   - platform smoke checks (Linux/macOS/Windows)
2. Add reproducible build metadata (commit, rustc, target, build flags).
3. Generate SBOM and attach to release artifacts.
4. Sign release artifacts and publish checksums.

### Exit Criteria
- Protected branch requires full release matrix to pass.
- Every release has checksums, SBOM, signature, and provenance metadata.
- Release job is deterministic and documented in `RELEASE.md`.

## Phase 3 (Week 4-8): Operations and Reliability
1. Define SLOs for CLI-backed workloads (success rate, error rate, latency targets where applicable).
2. Standardize metrics/log schema and add operator-facing health commands.
3. Create runbooks:
   - backup and restore drill
   - upgrade and rollback
   - incident triage
4. Run failure drills:
   - process crash during write
   - restore from backup
   - filesystem/NFS disruption

### Exit Criteria
- Runbooks exist and are executable by someone new to the project.
- Failure drills are automated or scripted and pass in CI/staging.
- Monitoring fields and health outputs are documented and stable.

## Phase 4 (Week 6-10): Compatibility and Upgrade Safety
1. Publish compatibility matrix (OS, architectures, Python binding versions).
2. Add schema/data migration safety tests for upgrades.
3. Add rollback validation for at least one prior minor version.
4. Define deprecation process and communication cadence.

### Exit Criteria
- Compatibility matrix published.
- Upgrade + rollback tests run in CI for supported paths.
- Deprecation policy included in docs and release notes template.

## Phase 5 (Week 8-12): v1.0 Readiness Review
1. Execute final readiness checklist.
2. Triage and close all blocker risks.
3. Publish `v1.0.0` release notes with known limitations.

### Exit Criteria
- All P0/P1 checklist items are green.
- No unresolved high-severity security/reliability issues.
- Release artifacts, docs, and support paths are complete.

## Workstreams and Owners
- Security and Compliance: TBD
- CI/CD and Release Engineering: TBD
- Runtime Reliability and NFS Hardening: TBD
- Docs and Support Operations: TBD

## Deliverables
- `SECURITY.md`
- `SUPPORT.md`
- `RELEASE_POLICY.md`
- `RELEASE.md`
- `PROD_READINESS.md`
- Threat model doc (`docs/threat-model.md`)
- Runbooks (`docs/runbooks/*.md`)
- Compatibility matrix (`docs/compatibility.md`)

## Risks
1. NFS behavior differences across OSes can cause flaky operational behavior.
2. Security controls may exist but not be enabled by default in all entry points.
3. Large-scale tests may be too slow for default CI and need tiered pipelines.

## Risk Mitigations
1. Keep a fast required CI tier and nightly exhaustive tier.
2. Add explicit startup mode checks and integration tests for auth paths.
3. Use release checklists and staged sign-off before publishing.

## Tracking Cadence
- Weekly readiness review with status per phase.
- Track blockers with severity labels: `P0`, `P1`, `P2`.
- Ship only when all `P0` and agreed `P1` items are closed.

## Progress Tree
### Feature: Auth fail-closed for unauthenticated opens (Phase 1)
1. Red: Added integration test `test_open_without_credentials_denies_auth_enabled_database` in `tests/tests/auth_test.rs` and validated failure.
2. Green: Implemented fail-closed auth initialization in `DatabaseOps::open_delta_native` for auth-enabled databases.
3. Regression: Ran `cargo test --workspace` and fixed unrelated CLI integration test regressions in `create_test.rs` and `s3_cli_test.rs` (binary path resolution and compose file resolution).
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.
7. Caps: Configured memoryStream lookup path for `.env.sudo`, then ran MCP `sudo_check` (success) and `sudo_set_caps` (success, but `set_count: 0` / `total: 0` — no matching binaries discovered by that tool).

### Current Status
- Completed: Steps 1-6 for the feature above.
- Completed: Sudo credentials and caps step executed via MCP; no binaries were modified by `sudo_set_caps` in this run because discovery returned zero targets.

### Feature: Auth fail-closed for audit log access (Phase 1)
1. Red: Added integration test `test_open_without_credentials_denies_audit_log_access` in `tests/tests/auth_test.rs`; validated failure.
2. Approach: Enforce admin permission gate for `get_audit_log()` so unauthenticated sessions in auth-enabled DBs fail with `Authentication required`.
3. Green: Added permission check in `DatabaseOps::get_audit_log()`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for parquet listing (Phase 1)
1. Red: Added integration test `test_open_without_credentials_denies_parquet_listing` in `tests/tests/auth_test.rs`; validated failure.
2. Approach: Enforce read permission gate in `DatabaseOps::list_parquet_files()` so unauthenticated sessions in auth-enabled DBs fail with `Authentication required`.
3. Green: Added permission check in `DatabaseOps::list_parquet_files()`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for backup operation (Phase 1)
1. Red: Added integration test `test_open_without_credentials_denies_backup` in `tests/tests/auth_test.rs`; validated failure (unauthenticated backup succeeded).
2. Approach: Enforce read permission gate in `DatabaseOps::backup()` before any backup filesystem work so unauthenticated sessions fail with `Authentication required`.
3. Green: Added permission check in `DatabaseOps::backup()`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for query_file operation (Phase 1)
1. Red: Added integration test `test_open_without_credentials_denies_query_file` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `query_file` succeeded).
2. Approach: Enforce read permission gate in `DatabaseOps::query_file()` so unauthenticated sessions fail with `Authentication required`.
3. Green: Added permission check in `DatabaseOps::query_file()`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for get_delta_table operation (Phase 1)
1. Red: Added integration test `test_open_without_credentials_denies_get_delta_table` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `get_delta_table` succeeded).
2. Approach: Enforce read permission gate in `DatabaseOps::get_delta_table()` so unauthenticated sessions fail with `Authentication required`.
3. Green: Added permission check in `DatabaseOps::get_delta_table()`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for set_primary_key operation (Phase 1)
1. Red: Added integration test `test_open_without_credentials_denies_set_primary_key` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `set_primary_key` succeeded).
2. Approach: Enforce write permission gate in `DatabaseOps::set_primary_key()` so unauthenticated sessions fail with `Authentication required`.
3. Green: Added permission check in `DatabaseOps::set_primary_key()`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for primary_key metadata read (Phase 1)
1. Red: Added integration test `test_open_without_credentials_hides_primary_key` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `primary_key()` returned configured key).
2. Approach: Enforce read-permission gating in `DatabaseOps::primary_key()` so unauthenticated sessions on auth-enabled DBs cannot read primary key metadata.
3. Green: Added permission check in `DatabaseOps::primary_key()` returning `None` when access is unauthorized.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for get_column_statistics operation (Phase 1)
1. Red: Added integration test `test_open_without_credentials_denies_column_statistics` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `get_column_statistics()` succeeded).
2. Approach: Enforce read permission gate in `DatabaseOps::get_column_statistics()` so unauthenticated sessions fail with `Authentication required`.
3. Green: Added permission check in `DatabaseOps::get_column_statistics()`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for get_pruning_statistics operation (Phase 1)
1. Red: Added integration test `test_open_without_credentials_denies_pruning_statistics` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `get_pruning_statistics()` succeeded).
2. Approach: Enforce read permission gate in `DatabaseOps::get_pruning_statistics()` so unauthenticated sessions fail with `Authentication required`.
3. Green: Added permission check in `DatabaseOps::get_pruning_statistics()`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for get_data_skipping_stats operation (Phase 1)
1. Red: Added integration test `test_open_without_credentials_denies_data_skipping_stats` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `get_data_skipping_stats()` succeeded).
2. Approach: Enforce read permission gate in `DatabaseOps::get_data_skipping_stats()` so unauthenticated sessions fail with `Authentication required`.
3. Green: Added permission check in `DatabaseOps::get_data_skipping_stats()`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for backup_incremental operation (Phase 1)
1. Red: Added integration test `test_open_without_credentials_denies_incremental_backup` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `backup_incremental()` succeeded).
2. Approach: Enforce read permission gate in `DatabaseOps::backup_incremental()` so unauthenticated sessions fail with `Authentication required`.
3. Green: Added permission check in `DatabaseOps::backup_incremental()`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for health_check status (Phase 1)
1. Red: Added integration test `test_open_without_credentials_hides_health_status` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `health_check()` returned `healthy`).
2. Approach: Enforce fail-closed behavior in `DatabaseOps::health_check()` so unauthenticated sessions return `status: unauthorized` and zeroed counters.
3. Green: Added permission gate in `DatabaseOps::health_check()` and unauthorized response.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for schema metadata read (Phase 1)
1. Red: Added integration test `test_open_without_credentials_hides_schema` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `schema()` returned full schema).
2. Approach: Enforce read-permission gate in `DatabaseOps::schema()` so unauthenticated sessions return an empty schema.
3. Green: Added permission check in `DatabaseOps::schema()` returning `Schema::empty()` when unauthorized.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for get_metrics observability read (Phase 1)
1. Red: Added integration test `test_open_without_credentials_hides_metrics` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `get_metrics()` leaked uptime).
2. Approach: Enforce read-permission gate in `DatabaseOps::get_metrics()` so unauthenticated sessions receive redacted metrics.
3. Green: Added permission check in `DatabaseOps::get_metrics()` returning zeroed metrics when unauthorized.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for delete operation (Phase 1)
1. Red: Added integration test `test_open_without_credentials_denies_delete_operation` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `delete()` returned Delta usage guidance instead of auth denial).
2. Approach: Enforce write-permission gate in `DatabaseOps::delete()` so unauthenticated sessions fail with `Authentication required` before any behavior is exposed.
3. Green: Added permission check in `DatabaseOps::delete()`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for health_check uptime metadata (Phase 1)
1. Red: Tightened integration test `test_open_without_credentials_hides_health_status` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `health_check()` leaked non-zero uptime).
2. Approach: Redact unauthorized `health_check()` metadata completely by setting `uptime_seconds` to `0.0`.
3. Green: Updated unauthorized branch in `DatabaseOps::health_check()` to return zero uptime.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for reset_data_skipping_stats operation (Phase 1)
1. Red: Strengthened integration test to `test_open_without_credentials_denies_reset_data_skipping_stats` in `tests/tests/auth_test.rs`; validated failure (method returned `()` and could not deny unauthenticated calls).
2. Approach: Change `DatabaseOps::reset_data_skipping_stats()` to return `Result<()>` and enforce write-permission checking before mutating stats.
3. Green: Updated `DatabaseOps::reset_data_skipping_stats()` signature to `Result<()>`, added `check_permission(Permission::Write)?`, and returned `Ok(())` after reset.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for reset_metrics operation (Phase 1)
1. Red: Strengthened integration test to `test_open_without_credentials_denies_reset_metrics` in `tests/tests/auth_test.rs`; validated failure (method returned `()` and could not deny unauthenticated calls).
2. Approach: Change `DatabaseOps::reset_metrics()` to return `Result<()>` and enforce write-permission checking before mutating metrics.
3. Green: Updated `DatabaseOps::reset_metrics()` signature to `Result<()>`, added `check_permission(Permission::Write)?`, and returned `Ok(())` after reset. Updated callsites in `tests/tests/monitoring_test.rs` and `posixlake/examples/monitoring.rs`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for begin_transaction operation (Phase 1)
1. Red: Added integration test `test_open_without_credentials_denies_begin_transaction` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `begin_transaction()` succeeded).
2. Approach: Enforce write-permission checking at `DatabaseOps::begin_transaction()` entry so unauthenticated sessions fail before transaction allocation.
3. Green: Added `check_permission(Permission::Write)?` at the start of `DatabaseOps::begin_transaction()`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for base_path metadata read (Phase 1)
1. Red: Added integration test `test_open_without_credentials_hides_base_path` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `base_path()` returned real filesystem path).
2. Approach: Enforce read-permission gating in `DatabaseOps::base_path()` so unauthenticated sessions receive a redacted path.
3. Green: Updated `DatabaseOps::base_path()` to return `Path::new(\"\")` when `check_permission(Permission::Read)` fails.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for backup metadata read (Phase 1)
1. Red: Added integration test `test_get_backup_metadata_without_credentials_denies_auth_enabled_backup` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `get_backup_metadata()` succeeded on auth-enabled backup).
2. Approach: Enforce auth-aware backup access checks for metadata reads; require credentials when backup contains auth metadata and verify caller permissions.
3. Green: Added backup authorization gate in `DatabaseOps::get_backup_metadata()`, plus `get_backup_metadata_with_credentials()` for credentialed access on auth-enabled backups.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for backup verification read (Phase 1)
1. Red: Added integration test `test_verify_backup_without_credentials_denies_auth_enabled_backup` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `verify_backup()` succeeded on auth-enabled backup).
2. Approach: Reuse backup authorization checks for verification paths; require credentials when backup includes auth metadata and permit credentialed verification flow.
3. Green: Added backup authorization gate in `DatabaseOps::verify_backup()`, plus `verify_backup_with_credentials()` for credentialed access.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for restore_to_transaction operation (Phase 1)
1. Red: Added integration test `test_restore_to_transaction_without_credentials_denies_auth_enabled_backup` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `restore_to_transaction()` succeeded on auth-enabled backup).
2. Approach: Reuse backup authorization checks for restore paths; require credentials when backup includes auth metadata and permit credentialed restore flow.
3. Green: Added backup authorization gate in `DatabaseOps::restore_to_transaction()`, plus `restore_to_transaction_with_credentials()` and shared implementation.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for restore operation (Phase 1)
1. Red: Added integration test `test_restore_without_credentials_denies_auth_enabled_backup` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `restore()` succeeded on auth-enabled backup).
2. Approach: Reuse backup authorization checks for restore paths; require credentials when backup includes auth metadata and permit credentialed restore flow.
3. Green: Added backup authorization gate in `DatabaseOps::restore()`, plus `restore_with_credentials()` and shared implementation.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for flush_write_buffer operation (Phase 1)
1. Red: Added integration test `test_open_without_credentials_denies_flush_write_buffer` in `tests/tests/auth_test.rs`; validated failure (unauthenticated `flush_write_buffer()` succeeded when buffer was empty).
2. Approach: Enforce write-permission check at `DatabaseOps::flush_write_buffer()` entry so unauthenticated sessions fail before any flush behavior.
3. Green: Added `check_permission(Permission::Write)?` at start of `DatabaseOps::flush_write_buffer()`.
4. Regression: Ran `cargo test --workspace`; first attempt failed due environment disk exhaustion (`/tmp` full, os error 28). Cleared generated `/tmp/posixlake_nfs_test_*.log` artifacts and re-ran successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for open_with_credentials(None) bypass (Phase 1)
1. Red: Added integration test `test_open_with_none_credentials_denies_auth_enabled_database` in `tests/tests/auth_test.rs`; validated failure (`open_with_credentials(..., None)` incorrectly succeeded on auth-enabled DBs).
2. Approach: Remove the `None => system admin` path in `DatabaseOps::open_with_credentials()` for auth-enabled databases and require explicit credentials; keep non-auth databases unchanged.
3. Green: Updated `DatabaseOps::open_with_credentials()` to return `Authentication required` when auth is enabled and credentials are `None`. Updated `test_permission_inheritance_and_revocation` to use explicit admin credentials.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for backup permission boundary (Phase 1)
1. Red: Added integration test `test_read_role_cannot_create_backup` in `tests/tests/auth_test.rs`; validated failure (read-only user could run `backup()`).
2. Approach: Enforce `Permission::Backup` for backup operations instead of `Permission::Read`.
3. Green: Updated permission checks in `DatabaseOps::backup()` and `DatabaseOps::backup_incremental()` from `Read` to `Backup`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for delete_rows_where permission boundary (Phase 1)
1. Red: Added integration test `test_write_role_cannot_delete_rows_without_delete_permission` in `tests/tests/auth_test.rs`; validated failure (write-only user could run `delete_rows_where()`).
2. Approach: Enforce `Permission::Delete` for row-level delete operations while preserving existing write permissions for insert/update paths.
3. Green: Updated `DatabaseOps::delete_rows_where()` to require `Permission::Delete`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for delete API permission boundary (Phase 1)
1. Red: Added integration test `test_write_role_cannot_call_delete_without_delete_permission` in `tests/tests/auth_test.rs`; validated failure (write-only user received operation guidance from `delete()` instead of permission denial).
2. Approach: Enforce `Permission::Delete` at `DatabaseOps::delete()` entry so unauthorized users fail before API behavior details are exposed.
3. Green: Updated `DatabaseOps::delete()` to require `Permission::Delete` instead of `Permission::Write`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.
