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
