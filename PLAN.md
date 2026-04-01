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

### Phase 8 — Integration Tests (partial)
[x] 8.1 Create `tests/tests/azure_test.rs` — 4 integration tests hitting live Azurite:
    - `test_azure_create_database` ✅
    - `test_azure_insert_and_query` ✅
    - `test_azure_reopen_database` ✅
    - `test_azure_multiple_inserts` ✅

### Phase 5 — CLI Support (NEXT)
[ ] 5.1 Add Azure detection to `Commands::Create` — detect `az://` prefix alongside `s3://`
    (`posixlake.rs:300`), add `--azure-account`, `--azure-key`, `--azure-endpoint` flags
    (or env vars `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`, `AZURITE_ENDPOINT`)
[ ] 5.2 Add `resolve_azure_credentials()` helper — mirror `resolve_s3_credentials` (`posixlake.rs:1526-1549`)
[ ] 5.3 Wire `Commands::Create` handler: if `is_azure`, call `DatabaseOps::create_with_azure()`
    (`posixlake.rs:306-316`)
[ ] 5.4 Wire `Commands::Mount` handler: if `is_azure`, call `DatabaseOps::open_with_azure()`
    (`posixlake.rs:389-396`)
[ ] 5.5 Wire `Commands::Health` handler — add Azure open path (`posixlake.rs:711-716`)
[ ] 5.6 Wire `Commands::Metrics` handler — add Azure open path (`posixlake.rs:740-744`)
[ ] 5.7 Add `AzureTest` subcommand — mirror `S3Test` (`posixlake.rs:211-228`)
[ ] 5.8 Add `Azure` management subcommand with `Start`/`Stop` for Azurite via Podman
    — mirror `S3`/`S3Commands` (`posixlake.rs:230-268`)
[ ] 5.9 `cargo build` — confirm Phase 5 compiles

### Phase 6 — Python Bindings (UniFFI)
[ ] 6.1 Add `AzureConfig` struct in `python.rs` — mirror `S3Config` (`python.rs:529-536`):
    ```rust
    pub struct AzureConfig {
        pub account_name: String,
        pub account_key: String,
    }
    ```
[ ] 6.2 Add `create_with_azure(azure_path, schema, azure_config)` constructor
    — mirror `create_with_s3` (`python.rs:589-614`)
[ ] 6.3 Add `open_with_azure(azure_path, azure_config)` constructor
    — mirror `open_with_s3` (`python.rs:705-724`)
[ ] 6.4 Export `AzureConfig` from Python `__init__.py` (`bindings/python/posixlake/__init__.py`)
[ ] 6.5 `cargo build` — confirm Phase 6 compiles

### Phase 7 — Podman / Azurite Setup
[ ] 7.1 Add `azurite` service to `docker-compose.yml` — mirror `minio` service (lines 2-20):
    ```yaml
    azurite:
      image: mcr.microsoft.com/azure-storage/azurite:latest
      container_name: posixlake-azurite
      ports:
        - "10000:10000"   # Blob
        - "10001:10001"   # Queue
        - "10002:10002"   # Table
      command: azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0
    ```
[ ] 7.2 Add `azurite-init` service to create a test container — mirror `minio-init` (lines 22-35)

### Phase 8 — Integration Tests
[ ] 8.1 Create `tests/tests/azure_test.rs` — mirror `s3_test.rs` structure:
    - `is_azurite_available()` — probe `http://localhost:10000` (mirrors `s3_test.rs:23-40`)
    - `get_azurite_config()` — read `AZURITE_ENDPOINT`, `AZURE_STORAGE_ACCOUNT_NAME`,
      `AZURE_STORAGE_ACCOUNT_KEY` env vars (mirrors `s3_test.rs:68-76`)
    - `create_test_schema()` / `create_test_batch()` — reuse pattern (mirrors `s3_test.rs:78-100`)
    - `test_azure_create_database` (mirrors `s3_test.rs:102-145`)
    - `test_azure_insert_and_query` (mirrors `s3_test.rs:147-200`)
    - `test_azure_reopen_database` (mirrors `s3_test.rs:202-260`)
    - `test_azure_multiple_inserts` (mirrors `s3_test.rs:262-307`)
[ ] 8.2 Create `tests/tests/azure_cli_test.rs` — mirror `s3_cli_test.rs`:
    - Azurite start/stop via compose
    - CLI azure-test auto-start
[ ] 8.3 Add `reqwest` dep to `tests/Cargo.toml` if not already present (it is — line 20)
[ ] 8.4 `cargo nextest run` — confirm all new + existing tests pass

### Phase 9 — Documentation
[ ] 9.1 Update `README.md` — add Azure section alongside S3 documentation
[ ] 9.2 Update CLI `--help` text — ensure Azure flags have clear descriptions
[ ] 9.3 Add `AZURE_TEST_SETUP.md` in `tests/` — mirror `tests/POSIX_TEST_SETUP.md` for Azure/Azurite

### Phase 10 — Final Verification
[ ] 10.1 `cargo build` — clean build
[ ] 10.2 `cargo nextest run` — full regression suite passes
[ ] 10.3 `cargo clippy` — no new warnings (warnings are errors via `.cargo/config.toml`)

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
