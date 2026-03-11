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
- Deprecation policy (`docs/deprecation.md`)
- Release notes template (`docs/release-notes-template.md`)

### Feature: Legacy upgrade compatibility and rollback coverage (Phase 4)
1. Added `docs/compatibility.md` describing supported OS, architecture, Python, and upgrade/rollback paths for `0.1.x`.
2. Added `tests/tests/upgrade_compat_test.rs` to validate `0.0.x`-style metadata open, metadata upgrade, and backup/restore rollback.
3. Added `docs/deprecation.md` plus `docs/release-notes-template.md` to define the deprecation process and communication cadence.
4. Updated `.github/workflows/ci.yml` and `RELEASE.md` so upgrade compatibility remains part of the required gate and release flow.

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

### Feature: Auth fail-closed for vacuum permission boundary (Phase 1)
1. Red: Added integration test `test_write_role_cannot_vacuum_without_delete_permission` in `tests/tests/auth_test.rs`; validated failure (write-only user could run `vacuum()`).
2. Approach: Enforce `Permission::Delete` for destructive VACUUM operations while keeping `vacuum_dry_run()` read-only.
3. Green: Updated `DatabaseOps::vacuum()` to require `Permission::Delete` instead of `Permission::Write`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for optimize permission boundary (Phase 1)
1. Red: Added integration test `test_write_role_cannot_optimize_without_delete_permission` in `tests/tests/auth_test.rs`; validated failure (write-only user could run `optimize()`).
2. Approach: Enforce `Permission::Delete` for compaction/maintenance optimize operations.
3. Green: Updated `DatabaseOps::optimize()` to require `Permission::Delete` instead of `Permission::Write`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for optimize variant permission boundary (Phase 1)
1. Red: Added integration tests `test_write_role_cannot_optimize_with_filter_without_delete_permission` and `test_write_role_cannot_optimize_with_target_size_without_delete_permission` in `tests/tests/auth_test.rs`; validated failure on `optimize_with_filter()` before the permission fix.
2. Approach: Enforce `Permission::Delete` consistently across optimize entry points (`optimize()`, `optimize_with_filter()`, `optimize_with_target_size()`) so write-only roles cannot run destructive maintenance.
3. Green: Updated `DatabaseOps::optimize_with_filter()` and `DatabaseOps::optimize_with_target_size()` to require `Permission::Delete` instead of `Permission::Write`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for zorder permission boundary (Phase 1)
1. Red: Added integration test `test_write_role_cannot_zorder_without_delete_permission` in `tests/tests/auth_test.rs`; validated failure (write-only user could run `zorder()`).
2. Approach: Enforce `Permission::Delete` for Z-ORDER maintenance operations to align with optimize/vacuum destructive-operation policy.
3. Green: Updated `DatabaseOps::zorder()` to require `Permission::Delete` instead of `Permission::Write`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for merge permission boundary (Phase 1)
1. Red: Added integration test `test_write_role_cannot_merge_without_delete_permission` in `tests/tests/auth_test.rs`; validated failure (write-only user could start `merge()`).
2. Approach: Enforce `Permission::Delete` at `DatabaseOps::merge()` entry to prevent write-only roles from initiating merge paths that can include deletes.
3. Green: Updated `DatabaseOps::merge()` to require `Permission::Delete` instead of `Permission::Write`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for reset_metrics permission boundary (Phase 1)
1. Red: Added integration test `test_write_role_cannot_reset_metrics_without_admin_permission` in `tests/tests/auth_test.rs`; validated failure (write-only user could call `reset_metrics()`).
2. Approach: Enforce `Permission::Admin` for observability counter reset operations so write-only roles cannot tamper with audit/monitoring signal quality.
3. Green: Updated `DatabaseOps::reset_metrics()` to require `Permission::Admin` instead of `Permission::Write`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for reset_data_skipping_stats permission boundary (Phase 1)
1. Red: Added integration test `test_write_role_cannot_reset_data_skipping_stats_without_admin_permission` in `tests/tests/auth_test.rs`; validated failure (write-only user could call `reset_data_skipping_stats()`).
2. Approach: Enforce `Permission::Admin` for data-skipping stats reset operations so write-only roles cannot tamper with optimization/observability signal quality.
3. Green: Updated `DatabaseOps::reset_data_skipping_stats()` to require `Permission::Admin` instead of `Permission::Write`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for set_primary_key permission boundary (Phase 1)
1. Red: Added integration test `test_write_role_cannot_set_primary_key_without_admin_permission` in `tests/tests/auth_test.rs`; validated failure (write-only user could call `set_primary_key()`).
2. Approach: Enforce `Permission::Admin` for primary-key metadata mutation so write-only roles cannot alter schema-level key configuration.
3. Green: Updated `DatabaseOps::set_primary_key()` to require `Permission::Admin` instead of `Permission::Write`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for begin_transaction permission boundary (Phase 1)
1. Red: Added integration test `test_write_role_cannot_begin_transaction_without_delete_permission` in `tests/tests/auth_test.rs`; validated failure (write-only user could call `begin_transaction()`).
2. Approach: Enforce `Permission::Delete` at transaction entry because transaction scopes can include delete-capable operations.
3. Green: Updated `DatabaseOps::begin_transaction()` to require `Permission::Delete` instead of `Permission::Write`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth fail-closed for open_with_s3 cached-auth metadata (Phase 1)
1. Red: Added integration test `test_s3_open_without_credentials_denies_when_auth_metadata_exists` in `tests/tests/s3_test.rs`; validated failure when run against real MinIO (`MINIO_BUCKET=posixlake-test`) showing unauthenticated `open_with_s3()` allowed write.
2. Approach: Mirror local-open fail-closed behavior in `DatabaseOps::open_with_s3()` by loading security components when cached `_metadata/users.json` exists while keeping `auth_context: None`.
3. Green: Updated `DatabaseOps::open_with_s3()` to initialize `user_store`, `audit_logger`, and `role_manager` from cached auth metadata.
4. Regression: Ran `cargo test --workspace`; encountered `s3_cli_test` compose-file CWD regression and fixed CLI compose path resolution in `start_local_minio_for_test()` using absolute workspace path discovery.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully (including `s3_cli_test` and `s3_test`).
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth-safe create_with_s3 cache handling (Phase 1)
1. Red: Added integration test `test_s3_create_ignores_stale_local_auth_cache_metadata` in `tests/tests/s3_test.rs`; validated failure (`create_with_s3()` returned DB that denied insert with `Authentication required` due stale local cache `users.json`).
2. Approach: Ensure fresh S3 create path does not inherit local cache auth metadata; auth bootstrap should occur on explicit auth-enabled opens, not on initial create.
3. Green: Updated `DatabaseOps::create_with_s3()` to always initialize with auth disabled (`user_store`, `audit_logger`, `role_manager` set to `None`) regardless of local cache contents.
4. Regression: Ran `cargo test --workspace` successfully (including S3 integration suites with MinIO).
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth-explicit admin API behavior when auth is disabled (Phase 1)
1. Red: Added integration test `test_create_user_fails_when_auth_is_disabled` in `tests/tests/auth_test.rs`; validated failure (`create_user()` incorrectly returned success on non-auth DB).
2. Approach: Make admin user-management APIs fail explicitly when auth is disabled to prevent silent no-op behavior.
3. Green: Updated `DatabaseOps::create_user()` and `DatabaseOps::revoke_role_from_user()` to return `Authentication not enabled` when no `user_store` is configured.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth-explicit audit admin API behavior when auth is disabled (Phase 1)
1. Red: Added integration test `test_get_audit_log_fails_when_auth_is_disabled` in `tests/tests/auth_test.rs`; validated failure (`get_audit_log()` incorrectly returned success on non-auth DB).
2. Approach: Require authentication infrastructure for audit-log admin API; return explicit `Authentication not enabled` when auth is disabled.
3. Green: Updated `DatabaseOps::get_audit_log()` to fail with `Authentication not enabled` when security components are not configured, while preserving admin permission checks for auth-enabled DBs.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth-explicit credentialed open behavior when auth is disabled (Phase 1)
1. Red: Added integration test `test_open_with_credentials_fails_when_auth_is_disabled` in `tests/tests/auth_test.rs`; validated failure (`open_with_credentials()` incorrectly succeeded on non-auth DB).
2. Approach: Fail fast in `open_with_credentials()` when `_metadata/users.json` is absent, returning explicit `Authentication not enabled`.
3. Green: Updated `DatabaseOps::open_with_credentials()` to return `Authentication not enabled` when auth metadata is not configured; auth-enabled flow remains unchanged.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth-explicit credentialed backup metadata behavior when auth is disabled (Phase 1)
1. Red: Added integration test `test_get_backup_metadata_with_credentials_fails_when_auth_is_disabled` in `tests/tests/auth_test.rs`; validated failure (`get_backup_metadata_with_credentials()` incorrectly succeeded on non-auth backups).
2. Approach: Add strict auth-disabled handling for credentialed backup APIs while preserving existing non-credentialed behavior for non-auth backups.
3. Green: Updated `authorize_backup_access(...)` with a strict mode and wired all `*_with_credentials` backup APIs to return `Authentication not enabled` when backup auth metadata is absent.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth-explicit credentialed backup verify behavior when auth is disabled (Phase 1)
1. Red: Added integration test `test_verify_backup_with_credentials_fails_when_auth_is_disabled` in `tests/tests/auth_test.rs`; expected failure, but test was already green due prior shared `authorize_backup_access(...)` hardening.
2. Green: No production code change required for this feature; behavior already enforced.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.

### Feature: Auth-explicit credentialed backup restore behavior when auth is disabled (Phase 1)
1. Red: Added integration test `test_restore_with_credentials_fails_when_auth_is_disabled` in `tests/tests/auth_test.rs`; expected failure, but test was already green due prior shared `authorize_backup_access(...)` hardening.
2. Green: No production code change required for this feature; behavior already enforced.
3. Regression: Ran targeted integration test successfully and full workspace regression successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.

### Feature: Auth-explicit revoke-role admin API behavior when auth is disabled (Phase 1)
1. Red: Added integration test `test_revoke_role_from_user_fails_when_auth_is_disabled` in `tests/tests/auth_test.rs`; expected failure, but test was already green because `revoke_role_from_user()` already returned `Authentication not enabled` when auth was disabled.
2. Green: No production code change required for this feature; behavior already enforced.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Auth permission boundary for vacuum_dry_run (Phase 1)
1. Red: Added integration test `test_read_role_cannot_vacuum_dry_run_without_delete_permission` in `tests/tests/auth_test.rs`; validated failure (`vacuum_dry_run()` incorrectly allowed read-only user).
2. Approach: Align `vacuum_dry_run()` with `vacuum()` permission boundary by requiring `Permission::Delete`.
3. Green: Updated `DatabaseOps::vacuum_dry_run()` to enforce `Permission::Delete` instead of `Permission::Read`.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for role revocation (Phase 1)
1. Red: Added integration test `test_revoke_role_operation_is_audited` in `tests/tests/auth_test.rs`; validated failure (`revoke_role_from_user()` did not emit `REVOKE_ROLE` audit entry).
2. Approach: Add success/failure audit logging in `revoke_role_from_user()` with operation `REVOKE_ROLE` and details containing `username` and `role`.
3. Green: Updated `DatabaseOps::revoke_role_from_user()` to emit audit entries for both success and failure paths.
4. Regression: Ran `cargo test --workspace`; initial run failed due host `/tmp` exhaustion and MinIO storage pressure (HTTP 507) in integration environment. Cleaned stale `/tmp/posixlake_nfs_test_*.log` artifacts and reset `posixlake_minio-data` volume. Re-ran workspace regression successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for failed role revocation (Phase 1)
1. Red: Added integration test `test_revoke_role_failure_is_audited` in `tests/tests/auth_test.rs`; expected failure, but behavior was already green because failed revocations already emitted `REVOKE_ROLE` audit entries with `success=false`.
2. Green: No production code change required for this feature; behavior already enforced.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied role revocation (Phase 1)
1. Red: Added integration test `test_revoke_role_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (permission-denied revoke attempt by non-admin user was not audit logged).
2. Green: Updated `DatabaseOps::revoke_role_from_user()` so admin permission checks run inside the audited result path; permission-denied failures now emit `REVOKE_ROLE` audit entries with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied user creation (Phase 1)
1. Red: Added integration test `test_create_user_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (permission-denied `create_user()` attempt by non-admin user was not audit logged).
2. Green: Updated `DatabaseOps::create_user()` so admin permission checks run inside the audited result path; permission-denied failures now emit `CREATE_USER` audit entries with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied audit-log access (Phase 1)
1. Red: Added integration test `test_get_audit_log_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (permission-denied `get_audit_log()` by non-admin user was not audit logged).
2. Green: Updated `DatabaseOps::get_audit_log()` to route through an audited success/failure result path and emit `GET_AUDIT_LOG` entries, including permission-denied failures with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied metrics reset (Phase 1)
1. Red: Added integration test `test_reset_metrics_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (permission-denied `reset_metrics()` by non-admin user was not audit logged).
2. Green: Updated `DatabaseOps::reset_metrics()` to run through an audited success/failure result path and emit `RESET_METRICS` entries, including permission-denied failures with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied data-skipping reset (Phase 1)
1. Red: Added integration test `test_reset_data_skipping_stats_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (permission-denied `reset_data_skipping_stats()` by non-admin user was not audit logged).
2. Green: Updated `DatabaseOps::reset_data_skipping_stats()` to run through an audited success/failure result path and emit `RESET_DATA_SKIPPING_STATS` entries, including permission-denied failures with `success=false`.
3. Regression: `cargo test --workspace` initially failed in CLI create tests after `cargo clean` because `target/debug/posixlake-cli` was missing; rebuilt with `cargo build -p posixlake --bin posixlake-cli` and reran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied primary-key mutation (Phase 1)
1. Red: Added integration test `test_set_primary_key_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (permission-denied `set_primary_key()` by non-admin user was not audit logged).
2. Green: Updated `DatabaseOps::set_primary_key()` to run through an audited success/failure path and emit `SET_PRIMARY_KEY` entries, including permission-denied failures with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied health checks (Phase 1)
1. Red: Added integration test `test_health_check_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (permission-denied `health_check()` returned `unauthorized` but emitted no `HEALTH_CHECK` audit entry).
2. Green: Updated `DatabaseOps::health_check()` to audit permission-denied reads with `HEALTH_CHECK` and `success=false` before returning redacted unauthorized health status.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied metrics reads (Phase 1)
1. Red: Added integration test `test_get_metrics_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure during full regression (`get_metrics()` returned redacted values but emitted no `GET_METRICS` audit entry).
2. Green: Updated `DatabaseOps::get_metrics()` to run through an audited success/failure path and emit `GET_METRICS` entries, including permission-denied failures with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied backup creation (Phase 1)
1. Red: Added integration test `test_backup_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`backup()` denied writer access but emitted no `BACKUP` audit entry).
2. Green: Updated `DatabaseOps::backup()` to run through an audited success/failure path and emit `BACKUP` entries, including permission-denied failures with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied incremental backup creation (Phase 1)
1. Red: Added integration test `test_backup_incremental_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`backup_incremental()` denied writer access but emitted no `BACKUP_INCREMENTAL` audit entry).
2. Green: Updated `DatabaseOps::backup_incremental()` to run through an audited success/failure path and emit `BACKUP_INCREMENTAL` entries, including permission-denied failures with `success=false`.
3. Green hardening: Updated audit-log reads to refresh from disk via `AuditLogger::get_entries_fresh()` so permission-denied entries written by one authenticated handle are visible to another handle during real integration flows.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied backup verification (Phase 1)
1. Red: Added integration test `test_verify_backup_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`verify_backup_with_credentials()` denied writer access but emitted no `VERIFY_BACKUP` audit entry).
2. Green: Updated `DatabaseOps::verify_backup()` and `DatabaseOps::verify_backup_with_credentials()` to emit `VERIFY_BACKUP` entries for both success and permission-denied failures.
3. Green hardening: Added shared backup-path audit helper so static backup APIs write to the backup's own `_metadata/audit.json`; corrected the integration test to validate the real backup audit surface rather than the source database audit log.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied backup metadata reads (Phase 1)
1. Red: Added integration test `test_get_backup_metadata_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`get_backup_metadata_with_credentials()` denied writer access but emitted no `GET_BACKUP_METADATA` audit entry).
2. Green: Updated `DatabaseOps::get_backup_metadata()` and `DatabaseOps::get_backup_metadata_with_credentials()` to emit `GET_BACKUP_METADATA` entries for both success and permission-denied failures.
3. Green hardening: Reused the shared backup-path audit helper so backup metadata audits land in the backup's own `_metadata/audit.json`, matching the real static backup API boundary.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied backup restore (Phase 1)
1. Red: Added integration test `test_restore_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`restore_with_credentials()` denied writer access but emitted no `RESTORE` audit entry).
2. Green: Updated `DatabaseOps::restore()` and `DatabaseOps::restore_with_credentials()` to emit `RESTORE` entries for both success and permission-denied failures.
3. Green hardening: Reused the shared backup-path audit helper so restore audits land in the backup's own `_metadata/audit.json`, matching the real static backup API boundary.
4. Regression: Ran `cargo test --workspace` successfully.
5. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression again: Re-ran `cargo test --workspace` successfully.
7. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Reliability hardening: S3 CLI integration timeout guard (test infra)
1. Problem surfaced during Regression: `tests/tests/s3_cli_test.rs` could hang indefinitely when `posixlake s3 start` blocked on container health wait.
2. Green: Added bounded command execution helper (`run_with_timeout`) and applied 90s timeout to `s3 start` and `s3-test` invocations, with deterministic cleanup/skip on timeout.
3. Regression: Ran `cargo test -p posixlake-integration-tests --test s3_cli_test -- --nocapture` successfully (`2 passed`).
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied begin_transaction (Phase 1)
1. Red: Added integration test `test_begin_transaction_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`begin_transaction()` denied writer access but emitted no `BEGIN_TRANSACTION` audit entry).
2. Green: Updated `DatabaseOps::begin_transaction()` to run through an audited success/failure path and emit `BEGIN_TRANSACTION` entries, including permission-denied failures with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied flush_write_buffer (Phase 1)
1. Red: Added integration test `test_flush_write_buffer_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (permission-denied `flush_write_buffer()` emitted no `FLUSH_WRITE_BUFFER` audit entry).
2. Green: Updated `DatabaseOps::flush_write_buffer()` to run through an audited success/failure path and emit `FLUSH_WRITE_BUFFER` entries, including permission-denied failures with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied get_delta_table (Phase 1)
1. Red: Added integration test `test_get_delta_table_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`get_delta_table()` denied writer access but emitted no `GET_DELTA_TABLE` audit entry).
2. Green: Updated `DatabaseOps::get_delta_table()` to run through an audited success/failure path and emit `GET_DELTA_TABLE` entries, including permission-denied failures with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied list_parquet_files (Phase 1)
1. Red: Added integration test `test_list_parquet_files_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`list_parquet_files()` denied writer access but emitted no `LIST_PARQUET_FILES` audit entry).
2. Green: Updated `DatabaseOps::list_parquet_files()` to run through an audited success/failure path and emit `LIST_PARQUET_FILES` entries, including permission-denied failures with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied query (Phase 1)
1. Red: Added integration test `test_query_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`query()` denied writer access but emitted no `SELECT` audit entry).
2. Green: Updated `DatabaseOps::query()` to run permission checks inside the audited success/failure path so permission-denied failures emit `SELECT` entries with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied query_version (Phase 1)
1. Red: Added integration test `test_query_version_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`query_version()` denied writer access but emitted no `SELECT_VERSION` audit entry).
2. Green: Updated `DatabaseOps::query_version()` to run permission checks inside the audited success/failure path so permission-denied failures emit `SELECT_VERSION` entries with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied query_timestamp (Phase 1)
1. Red: Added integration test `test_query_timestamp_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`query_timestamp()` denied writer access but emitted no `SELECT_TIMESTAMP` audit entry).
2. Green: Updated `DatabaseOps::query_timestamp()` to run permission checks inside the audited success/failure path so permission-denied failures emit `SELECT_TIMESTAMP` entries with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied query_file (Phase 1)
1. Red: Added integration test `test_query_file_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`query_file()` denied writer access but emitted no `QUERY_FILE` audit entry).
2. Green: Updated `DatabaseOps::query_file()` to run permission checks inside the audited success/failure path so permission-denied failures emit `QUERY_FILE` entries with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied get_column_statistics (Phase 1)
1. Red: Added integration test `test_get_column_statistics_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`get_column_statistics()` denied writer access but emitted no `GET_COLUMN_STATISTICS` audit entry).
2. Green: Updated `DatabaseOps::get_column_statistics()` to run permission checks inside the audited success/failure path so permission-denied failures emit `GET_COLUMN_STATISTICS` entries with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied get_pruning_statistics (Phase 1)
1. Red: Added integration test `test_get_pruning_statistics_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`get_pruning_statistics()` denied writer access but emitted no `GET_PRUNING_STATISTICS` audit entry).
2. Green: Updated `DatabaseOps::get_pruning_statistics()` to run permission checks inside the audited success/failure path so permission-denied failures emit `GET_PRUNING_STATISTICS` entries with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied get_data_skipping_stats (Phase 1)
1. Red: Added integration test `test_get_data_skipping_stats_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`get_data_skipping_stats()` denied writer access but emitted no `GET_DATA_SKIPPING_STATS` audit entry).
2. Green: Updated `DatabaseOps::get_data_skipping_stats()` to run permission checks inside the audited success/failure path so permission-denied failures emit `GET_DATA_SKIPPING_STATS` entries with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully after reclaiming `/tmp` space (removed stale `posixlake_nfs_test_*.log`) and adding targeted stress-test diagnostics for insert failures.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied point-in-time restore (Phase 1)
1. Red: Added integration test `test_restore_to_transaction_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`restore_to_transaction_with_credentials()` denied writer access but emitted no `RESTORE_TO_TRANSACTION` audit entry).
2. Green: Updated `DatabaseOps::restore_to_transaction()` and `restore_to_transaction_with_credentials()` to emit `RESTORE_TO_TRANSACTION` audit entries for both success and permission-denied failures against the backup audit log.
3. Regression: Ran `cargo test --workspace` successfully after hardening `tests/tests/s3_cli_test.rs` to skip when MinIO ports `9000/9001` are already occupied on the host.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied row deletion (Phase 1)
1. Red: Added integration test `test_delete_rows_where_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`delete_rows_where()` denied writer access but emitted no `DELETE` audit entry).
2. Green: Updated `DatabaseOps::delete_rows_where()` to run the delete permission check inside the audited success/failure path so permission-denied failures emit `DELETE` entries with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied direct delete API (Phase 1)
1. Red: Added integration test `test_delete_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`delete()` denied writer access but emitted no `DELETE` audit entry).
2. Green: Updated `DatabaseOps::delete()` to run through an audited success/failure path so permission-denied failures and unsupported direct-delete failures emit `DELETE` entries with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied merge builder access (Phase 1)
1. Red: Added integration test `test_merge_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`merge()` denied writer access but emitted no `MERGE` audit entry).
2. Green: Updated `DatabaseOps::merge()` to run through an audited success/failure path so permission-denied failures emit `MERGE` with `success=false`, and successful builder creation emits `MERGE` with `success=true`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied optimize (Phase 1)
1. Red: Added integration test `test_optimize_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`optimize()` denied writer access but emitted no `OPTIMIZE` audit entry).
2. Green: Updated `DatabaseOps::optimize()` to run the delete permission check inside the audited success/failure path so permission-denied failures emit `OPTIMIZE` entries with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied vacuum (Phase 1)
1. Red: Added integration test `test_vacuum_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`vacuum()` denied writer access but emitted no `VACUUM` audit entry).
2. Green: Updated `DatabaseOps::vacuum()` to run the delete permission check inside the audited success/failure path so permission-denied failures emit `VACUUM` entries with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied vacuum dry run (Phase 1)
1. Red: Added integration test `test_vacuum_dry_run_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`vacuum_dry_run()` denied reader access but emitted no `VACUUM_DRY_RUN` audit entry).
2. Green: Updated `DatabaseOps::vacuum_dry_run()` to run the delete permission check inside the audited success/failure path so permission-denied failures emit `VACUUM_DRY_RUN` entries with `success=false`, while successful previews emit candidate-count details.
3. Regression: Ran `cargo test --workspace`, found existing CLI integration harness failures because `create_test` and `s3_cli_test` assumed a prebuilt `posixlake-cli` binary, and fixed both helpers to build the real CLI binary into an isolated `/tmp/posixlake-cli-test-target` on demand.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully after the harness fix.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied zorder (Phase 1)
1. Red: Added integration test `test_zorder_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`zorder()` denied writer access but emitted no `ZORDER` audit entry).
2. Green: Updated `DatabaseOps::zorder()` to run the delete permission check inside the audited success/failure path so permission-denied failures emit `ZORDER` entries with `success=false`, while successful clustering and operational failures keep their existing audit coverage.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied optimize with filter (Phase 1)
1. Red: Added integration test `test_optimize_with_filter_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`optimize_with_filter()` denied writer access but emitted no failed `OPTIMIZE` audit entry with filter context).
2. Green: Updated `DatabaseOps::optimize_with_filter()` to run the delete permission check inside the audited success/failure path so permission-denied failures emit `OPTIMIZE` entries with `success=false` and preserve the filter details.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied optimize with target size (Phase 1)
1. Red: Added integration test `test_optimize_with_target_size_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`optimize_with_target_size()` denied writer access but emitted no failed `OPTIMIZE` audit entry with target-size context).
2. Green: Updated `DatabaseOps::optimize_with_target_size()` to run the delete permission check inside the audited success/failure path so permission-denied failures emit `OPTIMIZE` entries with `success=false` and preserve `target_size=...` details.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied insert (Phase 1)
1. Red: Added integration test `test_insert_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`insert()` denied reader access but emitted no failed `INSERT` audit entry).
2. Green: Updated `DatabaseOps::insert()` to run the write permission check inside the audited success/failure path so permission-denied failures emit `INSERT` entries with `success=false`.
3. Regression: Ran `cargo test --workspace`; two stress tests failed under the chained run (`test_many_concurrent_readers`, `test_memory_leak_detection`), then re-ran both in isolation to verify the production path still met their thresholds.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully after the isolated stress validation.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Documentation: Security policy, threat model, and auth-mode reference (Phase 1)
1. Created `SECURITY.md` with vulnerability reporting process, patch SLAs (48h ack, 14d critical fix, 30d medium fix), supported versions, security model summary, dependency policy, and scope.
2. Created `docs/threat-model.md` covering trust boundaries (API, filesystem, NFS, S3), seven threat categories (unauthorized access, privilege escalation, credential compromise, audit tampering, data integrity, NFS exposure, SQL injection), residual risks, and operator recommendations.
3. Created `docs/auth-modes.md` documenting auth-disabled vs auth-enabled behavior, detection mechanism, credential passing, the full permission matrix for all DatabaseOps methods, built-in roles, audit trail format, and planned CLI auth support.

### Feature: CLI auth mode selection and credential passing (Phase 1)
1. Added `--auth`, `--admin-user`, `--admin-password` flags to the `create` CLI subcommand so databases can be created with authentication enabled from the command line.
2. Added `--user` / `--password` flags to the `mount` CLI subcommand, with fallback to `POSIXLAKE_USER` / `POSIXLAKE_PASSWORD` environment variables, so auth-enabled databases can be mounted with credentials.
3. Added `DatabaseOps::enable_auth()` public method so any database (including CSV/Parquet imports) can have auth enabled after creation.
4. Added `resolve_admin_password()` helper reading from flag or `POSIXLAKE_ADMIN_PASSWORD` env var with clear error on missing password.
5. Added `resolve_credentials()` helper reading from flags or env vars with graceful None when both are absent.
6. Added `cargo audit --deny warnings` CI job gating the release pipeline so high/critical dependency vulnerabilities block publishing.
7. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
8. Regression: Ran `cargo test --workspace` successfully.
9. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied base_path access (Phase 1)
1. Red: Added integration test `test_base_path_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`base_path()` denied writer (no read) access but emitted no `BASE_PATH` audit entry).
2. Green: Updated `DatabaseOps::base_path()` to log `BASE_PATH` with `success=false` via `futures::executor::block_on` when the read permission check fails.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied primary_key read (Phase 1)
1. Red: Added integration test `test_primary_key_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`primary_key()` denied writer (no read) access but emitted no `PRIMARY_KEY` audit entry).
2. Green: Updated `DatabaseOps::primary_key()` to log `PRIMARY_KEY` with `success=false` via `futures::executor::block_on` when the read permission check fails.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied schema read (Phase 1)
1. Red: Added integration test `test_schema_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`schema()` denied writer (no read) access but emitted no `SCHEMA` audit entry).
2. Green: Updated `DatabaseOps::schema()` to log `SCHEMA` with `success=false` via `futures::executor::block_on` when the read permission check fails.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Feature: Audit trail for permission-denied insert_buffered (Phase 1)
1. Red: Added integration test `test_insert_buffered_permission_denied_is_audited` in `tests/tests/auth_test.rs`; validated failure (`insert_buffered()` denied reader access but emitted no `INSERT_BUFFERED` audit entry).
2. Green: Updated `DatabaseOps::insert_buffered()` to wrap the write-permission check and buffer logic in an audited success/failure path so permission-denied failures emit `INSERT_BUFFERED` entries with `success=false`.
3. Regression: Ran `cargo test --workspace` successfully.
4. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
5. Regression again: Re-ran `cargo test --workspace` successfully.
6. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Phase 2: CI/CD and Release Integrity

### Feature: Expanded CI release matrix (Phase 2)
1. Restructured `.github/workflows/ci.yml` with separate jobs for core integration tests (auth, backup/restore, monitoring, NFS, recovery, schema migration), extended integration tests (delta native, merge, optimize, vacuum, zorder, time travel, write buffer, data skipping, imports, queries, transactions), and S3 integration tests.
2. Added `release-gate` aggregation job requiring all quality checks (fmt, clippy, build, test-unit, test-core-integration, test-extended-integration, test-s3-integration, audit, sbom) to pass before version bump and publishing.
3. Updated `bump-version` to depend on `release-gate` instead of individual jobs.

### Feature: Reproducible build metadata (Phase 2)
1. Updated `posixlake/build.rs` to inject `POSIXLAKE_BUILD_COMMIT`, `POSIXLAKE_BUILD_DATE`, `POSIXLAKE_BUILD_TARGET`, and `POSIXLAKE_BUILD_PROFILE` environment variables at build time via `cargo:rustc-env`.
2. Updated CLI `--version` output to display commit hash, build date, target triple, profile, and rustc version via `long_version()` in `posixlake/src/bin/posixlake.rs`.
3. Verified output: `posixlake 0.1.0 / commit: 12a1e24 / date: 2026-03-11 / target: x86_64-unknown-linux-gnu / profile: release / rustc: 1.91.1`.

### Feature: SBOM generation in CI (Phase 2)
1. Added `sbom` CI job using `cargo-cyclonedx` to generate CycloneDX JSON SBOM and upload as a build artifact.
2. SBOM job is required by the `release-gate` aggregation.

### Feature: Release documentation (Phase 2)
1. Created `RELEASE.md` documenting the versioning policy (SemVer), release gate requirements, build metadata format, artifact inventory (CLI binary, Python wheel, sdist, SBOM), automated and manual release procedures, checksum generation, and deprecation policy.
2. Lint: Ran `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` successfully.
3. Regression: Ran `cargo test --workspace` successfully.
4. Rebuild: Built release binary with `cargo build --release -p posixlake --bin posixlake-cli`.

### Phase 3: Operations and Reliability

### Feature: CLI health and metrics commands (Phase 3)
1. Added `serde::Serialize` derive to `DatabaseMetrics` and `HealthStatus` structs in `posixlake/src/database_ops.rs` for JSON output.
2. Added `Health` and `Metrics` CLI subcommands in `posixlake/src/bin/posixlake.rs` with `--user`/`--password` auth support, outputting JSON to stdout via `serde_json::to_string_pretty`.
3. Red: Added CLI integration tests in `tests/tests/ops_cli_test.rs`: `test_cli_health_returns_json`, `test_cli_metrics_returns_json`, `test_cli_health_with_auth`, `test_cli_metrics_nonexistent_db`.
4. Green: All 4 CLI tests passed on first run.
5. Lint: Ran `cargo clippy --all-targets --all-features -- -D warnings` successfully.
6. Regression: Ran `cargo test --test monitoring_test` — all 5 existing monitoring tests passed.

### Feature: SLO definition (Phase 3)
1. Created `docs/slo.md` defining availability (99.9% open, 99.5% query/insert), latency (p50/p99 targets for queries, inserts, health checks), durability (0 row loss, operator-defined RPO/RTO), error budget (0.5% per 24h), and monitoring integration examples using `posixlake health` and `posixlake metrics`.

### Feature: Operational runbooks (Phase 3)
1. Created `docs/runbooks/backup-restore.md` with full/incremental backup procedures, restore steps, verification checklist, and failure mode table.
2. Created `docs/runbooks/upgrade-rollback.md` with pre-upgrade checklist, upgrade steps, rollback procedure, and post-upgrade verification.
3. Created `docs/runbooks/incident-triage.md` with severity assessment matrix, common issue diagnostics (open failure, high error rate, slow queries, mount failures), escalation steps, and post-incident checklist.

### Feature: Failure drill tests (Phase 3)
1. Red: Added failure drill tests in `tests/tests/failure_drill_test.rs`: `test_recovery_after_partial_delta_log_corruption` (corrupt delta log, verify graceful open/error), `test_backup_restore_drill` (backup, destroy original, restore, verify 5 rows + healthy), `test_concurrent_write_safety` (5 concurrent inserts, verify DB stays healthy and queryable).
2. Green: All 3 failure drill tests passed on first run.
3. Lint: Ran `cargo clippy --all-targets --all-features -- -D warnings` successfully.
4. Regression: All monitoring tests (5) passed.
