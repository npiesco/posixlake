# Azure Blob / ADLS Gen2 Storage Backend — Implementation Plan

## Problem

posixlake currently supports **Local** and **S3/MinIO** storage backends. We need to add
**Azure Blob Storage / ADLS Gen2** as a third backend so Delta Lake tables can be stored
on Azure-compatible storage (including the Azurite emulator for local testing).

## Approach

Mirror the existing S3 integration 1:1. The S3 pattern uses `deltalake`'s
`open_table_with_storage_options` with an `AWS_*` env-var map — Azure uses the same
mechanism with `AZURE_*` keys. We also add `object_store`'s `azure` feature for the
`MicrosoftAzureBuilder` and `deltalake`'s `azure` feature for `deltalake-azure`.

Rather than keep separate `s3_url` / `azure_url` fields and duplicate every branch,
we introduce a `CloudStorageConfig` enum that unifies both, then refactor the ~12
existing S3 branch sites to use it. This keeps future backends trivial to add.

## Key Insight — No `azure_storage_blob` Crate Needed

Delta Lake + object_store natively handle Azure auth/transport. The raw Azure SDK
(`azure_storage_blob`) is unnecessary. Storage options map keys:
- `AZURE_STORAGE_ACCOUNT_NAME`
- `AZURE_STORAGE_ACCOUNT_KEY` (or `AZURE_STORAGE_SAS_TOKEN`, or managed identity)

## Checklist

### Phase 0 — Baseline Verification
[x] 0.1 Run `cargo nextest run` — confirm all existing tests pass before any changes
[x] 0.2 Run `cargo build` — confirm clean compilation with no warnings

### Phase 1 — Dependencies (`cargo add` only, never hand-edit Cargo.toml)
[x] 1.1 `cargo add object_store --features azure` in `posixlake/` — adds `MicrosoftAzureBuilder` (`posixlake/Cargo.toml:37`)
[x] 1.2 `cargo add deltalake --features azure` in `posixlake/` — adds `deltalake-azure` crate (`posixlake/Cargo.toml:49`)
[x] 1.3 Verify `cargo build` compiles cleanly after dependency additions

### Phase 2 — Storage Abstraction (azure.rs + module registration)
[x] 2.1 Create `posixlake/src/storage/azure.rs` — `parse_azure_url`, `create_delta_storage_options`, `get_azure_cache_path` + unit tests
[x] 2.2 Register module: add `pub mod azure;` to `posixlake/src/storage/mod.rs`

### Phase 3 — Skipped (deferred refactor to CloudStorageConfig — reuses s3_url/s3_storage_options fields for now)

### Phase 4 — Azure Create/Open APIs in `DatabaseOps`
[x] 4.1 Add `create_with_azure(azure_path, schema, account_name, account_key, endpoint)` to `DatabaseOps` (`database_ops.rs`)
[x] 4.2 Add `open_with_azure(azure_path, account_name, account_key, endpoint)` to `DatabaseOps` (`database_ops.rs`)
[x] 4.3 `cargo build` — compiles cleanly
[x] 4.4 `cargo test --lib -p posixlake` — 91 passed, 0 failed
[x] 4.5 `cargo clippy` — clean, no warnings
[x] 4.6 `cargo fmt --all` — clean

### Phase 8 — Integration Tests (partial)
[x] 8.1 Create `tests/tests/azure_test.rs` — 4 integration tests hitting live Azurite:
    - `test_azure_create_database` ✅
    - `test_azure_insert_and_query` ✅
    - `test_azure_reopen_database` ✅
    - `test_azure_multiple_inserts` ✅
    - `vacuum_inner` (lines 2368-2370)
    - `vacuum_inner_dry_run` (lines 2380-2382)
    - `zorder_inner` (lines 2442-2444)
[ ] 3.7 Update `delta_lake/operations.rs` function signatures to accept
    `cloud_config: Option<&CloudStorageConfig>` instead of separate `s3_url` / `s3_storage_options`:
    - `optimize_table` (lines 24-27)
    - `vacuum_table` (lines 82-85)
    - `vacuum_dry_run` (lines 149-153)
    - `zorder_table` (lines 204-208)
[ ] 3.8 `cargo build` — confirm Phase 3 compiles
### Phase 3 — Skipped (deferred refactor to CloudStorageConfig — reuses s3_url/s3_storage_options fields for now)

### Phase 4 — Azure Create/Open APIs in `DatabaseOps`
[x] 4.1 Add `create_with_azure(azure_path, schema, account_name, account_key, endpoint)` to `DatabaseOps` (`database_ops.rs`)
[x] 4.2 Add `open_with_azure(azure_path, account_name, account_key, endpoint)` to `DatabaseOps` (`database_ops.rs`)
[x] 4.3 `cargo build` — compiles cleanly
[x] 4.4 `cargo test --lib -p posixlake` — 91 passed, 0 failed
[x] 4.5 `cargo clippy` — clean, no warnings
[x] 4.6 `cargo fmt --all` — clean

### Phase 8 — Integration Tests
[x] 8.1 Create `tests/tests/azure_test.rs` — 4 integration tests hitting live Azurite:
    - `test_azure_create_database` ✅
    - `test_azure_insert_and_query` ✅
    - `test_azure_reopen_database` ✅
    - `test_azure_multiple_inserts` ✅
[x] 8.2 Create `tests/tests/azure_cli_test.rs` — 3 CLI integration tests:
    - `test_create_with_azure_uri` ✅
    - `test_create_with_azure_env_vars` ✅
    - `test_health_with_azure_uri` ✅
[x] 8.3 `cargo test` — all 98 tests pass (91 lib + 4 Azure + 3 Azure CLI)

### Phase 5 — CLI Support
[x] 5.1 Add `az://` detection + `--azure-account`, `--azure-key`, `--azure-endpoint` flags to Create, Mount, Health, Metrics
[x] 5.2 Add `resolve_azure_credentials()` helper
[x] 5.3 Wire Create handler for Azure
[x] 5.4 Wire Mount handler for Azure
[x] 5.5 Wire Health handler for Azure
[x] 5.6 Wire Metrics handler for Azure
[x] 5.7 Add `AzureTest` subcommand
[x] 5.8 Add `Azure` management subcommand (Start/Stop Azurite via Podman)
[x] 5.9 `cargo build` — compiles cleanly

### Phase 6 — Python Bindings (UniFFI)
[x] 6.1 Add `AzureConfig` struct in `python.rs`
[x] 6.2 Add `create_with_azure` constructor
[x] 6.3 Add `open_with_azure` constructor
[x] 6.4 Export `AzureConfig` from `__init__.py`
[x] 6.5 `cargo build` — compiles cleanly

### Phase 7 — Podman / Azurite Setup
[x] 7.1 Add `azurite` service to `docker-compose.yml`
[x] 7.2 Add `azurite-init` service to create test container

### Phase 9 — Documentation
[x] 9.1 Update `README.md` — add Azure sections (mount, feature list, storage abstraction)
[x] 9.2 CLI `--help` text updated with Azure flag descriptions
[x] 9.3 Create `tests/AZURE_TEST_SETUP.md` — Azurite setup instructions

### Phase 10 — Final Verification
[x] 10.1 `cargo build` — clean build ✅
[x] 10.2 `cargo test` — 98 tests pass, 0 failures ✅
[x] 10.3 `cargo clippy` — no warnings ✅
[x] 10.4 `cargo fmt` — clean ✅

## Files Modified (Summary)

| File | Change |
|------|--------|
| `posixlake/Cargo.toml` | Add `azure` features to `object_store` + `deltalake` (via `cargo add`) |
| `posixlake/src/storage/mod.rs` | Add `Azure` variant, `CloudStorageConfig` enum, `pub mod azure` |
| `posixlake/src/storage/azure.rs` | **NEW** — URI parsing, storage options, cache path |
| `posixlake/src/storage/path.rs` | Add `Azure(String)` variant to `ResolvedPath` |
| `posixlake/src/database_ops.rs` | Replace `s3_url`/`s3_storage_options` → `CloudStorageConfig`; add `create_with_azure`/`open_with_azure`; add `open_delta_table` helper |
| `posixlake/src/delta_lake/operations.rs` | Accept `CloudStorageConfig` instead of separate S3 params |
| `posixlake/src/bin/posixlake.rs` | Add `AzureTest`, `Azure` subcommands; `abfss://` detection; `resolve_azure_credentials` |
| `posixlake/src/python.rs` | Add `AzureConfig` struct + `create_with_azure`/`open_with_azure` constructors |
| `docker-compose.yml` | Add `azurite` + `azurite-init` services |
| `tests/tests/azure_test.rs` | **NEW** — integration tests mirroring `s3_test.rs` |
| `tests/tests/azure_cli_test.rs` | **NEW** — CLI integration tests mirroring `s3_cli_test.rs` |
| `tests/AZURE_TEST_SETUP.md` | **NEW** — Azurite setup instructions |
| `bindings/python/posixlake/__init__.py` | Export `AzureConfig` |
| `README.md` | Azure usage documentation |

## Notes

- **`azure_storage_blob` is NOT needed** — `deltalake` + `object_store` handle Azure natively
- **Azurite** is the local emulator (same role MinIO plays for S3)
- Default Azurite well-known credentials: account=`devstoreaccount1`, key=`Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==`
- **URI scheme**: `abfss://container@account.dfs.core.windows.net/path` (ADLS Gen2) or `https://account.blob.core.windows.net/container/path` (Blob)
- The `CloudStorageConfig` refactor is the biggest risk — it touches ~12 branch sites, but each is a mechanical transformation
