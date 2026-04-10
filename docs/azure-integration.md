# Azure Blob Storage / ADLS Gen2 / Fabric OneLake Integration

## Overview

posixlake supports three Azure-compatible storage backends:

| Backend | URI Scheme | Auth Method | Status |
|---------|-----------|-------------|--------|
| **Azure Blob / Azurite** | `az://<container>` | Account Key | ✅ Tested |
| **Microsoft Fabric OneLake** | `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/<table>` | Service Principal | ✅ Tested live |
| **ADLS Gen2** | `abfss://<container>@<account>.dfs.core.windows.net/<path>` | Account Key | Supported via the Azure Blob backend |

## Architecture

```
posixlake CLI / API
        │
        ▼
  DatabaseOps::create_with_azure()    ← Account key auth (Azurite / Azure Blob)
  DatabaseOps::open_with_azure()
  DatabaseOps::create_with_onelake()  ← Service Principal auth (Fabric OneLake)
  DatabaseOps::open_with_onelake()
        │
        ▼
  Delta Lake (deltalake-rs + deltalake-azure)
        │
        ▼
  object_store MicrosoftAzureBuilder
        │
        ▼
  Azure Blob REST API / OneLake ADLS Gen2 API
```

## Storage Options

### Account Key Auth (Azurite / Azure Blob)

Set by `storage::azure::create_delta_storage_options()`:

| Key | Value | Notes |
|-----|-------|-------|
| `AZURE_STORAGE_ACCOUNT_NAME` | Account name | `devstoreaccount1` for Azurite |
| `AZURE_STORAGE_ACCOUNT_KEY` | Base64 key | Well-known for Azurite |
| `AZURE_STORAGE_ENDPOINT` | Endpoint URL with account | `http://127.0.0.1:10000/devstoreaccount1` for Azurite |
| `azure_allow_http` | `true` | Only for local/emulator endpoints |

### Service Principal Auth (Fabric OneLake)

Set by `storage::azure::create_delta_storage_options_sp()`:

| Key | Value |
|-----|-------|
| `AZURE_STORAGE_ACCOUNT_NAME` | `onelake` |
| `AZURE_STORAGE_CLIENT_ID` | SP application ID |
| `AZURE_STORAGE_CLIENT_SECRET` | SP password |
| `AZURE_STORAGE_TENANT_ID` | Entra tenant ID |
| `AZURE_STORAGE_AUTHORITY_HOST` | `https://login.microsoftonline.com` |

## Key Design Decisions

### 1. Container = Database (Azurite)

With the `az://` scheme, Delta Lake treats the container as the table root. **Each database maps to its own Azure container.** The URI format is:

```
az://<container-name>
```

NOT `az://container/subpath` — Delta Lake with `object_store` writes the `_delta_log/` at the container root regardless of the path suffix in the `az://` URL. This is a known behavior of `deltalake-azure` with `object_store` 0.12.

For Fabric OneLake, the `abfss://` scheme supports full paths correctly because the endpoint is different (OneLake ADLS Gen2 compatible).

### 2. Container Auto-Creation

`create_with_azure()` automatically creates the Azure container if it doesn't exist. This uses SharedKey HMAC-SHA256 auth against the Azure Blob REST API (`PUT /<account>/<container>?restype=container`).

**Azurite HMAC quirk:** When using path-style URLs (`http://host:port/<account>/<container>`), the canonical resource for HMAC signing must use a **double account prefix**: `/<account>/<account>/<container>`. This is because Azurite's path-style routing prepends the account to the container path in its internal canonical resource computation.

### 3. No `AZURE_STORAGE_USE_EMULATOR` Flag

We explicitly do NOT use `AZURE_STORAGE_USE_EMULATOR=true` in storage options. This flag changes how `object_store` resolves blob paths within containers, causing writes to go to the container root instead of the specified prefix. Instead, we:

- Set `AZURE_STORAGE_ENDPOINT` to `http://127.0.0.1:10000/<account>` (account in endpoint path)
- Set `azure_allow_http=true` to permit non-HTTPS connections
- Let `object_store` use standard path resolution

### 4. No `--skipApiVersionCheck` on Azurite

Azurite's `--skipApiVersionCheck` flag causes Delta Lake writes to succeed (HTTP 201) but the data is not persisted correctly — subsequent reads return "No files in log segment". Always run Azurite without this flag.

## Testing

### Local (Azurite via npx)

```bash
# Start Azurite natively (no Docker/Podman needed)
npx azurite --blobHost 0.0.0.0 --blobPort 10000

# Run Rust integration tests (auto-creates containers)
cargo test --test azure_test

# Run Python integration tests
bindings\python\.venv\Scripts\python.exe scripts\test_python_azure.py
```

### Local (Azurite via Podman)

```bash
# Start Azurite in Podman
podman run -d --name azurite -p 10000:10000 \
  mcr.microsoft.com/azure-storage/azurite:latest \
  azurite --blobHost 0.0.0.0

# Run tests
cargo test --test azure_test
```

### Fabric OneLake (requires Azure subscription)

```bash
# Set environment variables
export FABRIC_ONELAKE_TABLES_PATH="abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>/Tables"
export AZURE_STORAGE_CLIENT_ID="<sp-app-id>"
export AZURE_STORAGE_CLIENT_SECRET="<sp-password>"
export AZURE_STORAGE_TENANT_ID="<tenant-id>"

# Run Fabric integration tests
cargo test --test fabric_test
```

## CLI Usage

```bash
# Create database on Azurite
posixlake-cli create az://my-database --schema "id:Int32,name:String" \
  --azure-account devstoreaccount1 \
  --azure-key "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
  --azure-endpoint http://127.0.0.1:10000

# Health check
posixlake-cli health az://my-database \
  --azure-account devstoreaccount1 \
  --azure-key "..." \
  --azure-endpoint http://127.0.0.1:10000

# End-to-end test
posixlake-cli azure-test az://my-test-db
```

## Python Usage

```python
from posixlake import DatabaseOps, Schema, Field, AzureConfig

schema = Schema(fields=[
    Field(name="id", data_type="Int32", nullable=False),
    Field(name="name", data_type="String", nullable=False),
], primary_key="id")

azure_config = AzureConfig(
    account_name="devstoreaccount1",
    account_key="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
    endpoint="http://127.0.0.1:10000"
)

db = DatabaseOps.create_with_azure("az://my-database", schema, azure_config)
db.insert_json('[{"id": 1, "name": "Alice"}]')
print(db.query_json("SELECT * FROM data"))
```

## Troubleshooting

| Issue | Root Cause | Fix |
|-------|-----------|-----|
| `ContainerNotFound` | Container doesn't exist | `create_with_azure()` auto-creates; or create manually |
| `No files in log segment` after create | `--skipApiVersionCheck` on Azurite | Remove the flag, restart Azurite clean |
| `BadScheme` error | HTTP endpoint without `azure_allow_http` | Ensure storage options include `azure_allow_http=true` |
| `AuthorizationFailure` on container create | HMAC signing mismatch | Azurite path-style needs double-account canonical resource |
| Write succeeds but read finds wrong path | `AZURE_STORAGE_USE_EMULATOR=true` | Don't use USE_EMULATOR; set endpoint with account in path |
| `az://container/subpath` writes to root | Delta Lake path resolution with `az://` | Use `az://container` (one container per database) |
| Fabric `401 Unauthorized` | SP missing workspace access | Grant SP Contributor via `fab acl set` |

## Files

| File | Purpose |
|------|---------|
| `posixlake/src/storage/azure.rs` | URI parsing, storage options, container auto-creation, HMAC signing |
| `posixlake/src/database_ops.rs` | `create_with_azure`, `open_with_azure`, `create_with_onelake`, `open_with_onelake` |
| `posixlake/src/bin/posixlake.rs` | CLI `--azure-*` flags, `AzureTest`, `Azure` subcommands |
| `posixlake/src/python.rs` | `AzureConfig` UniFFI binding |
| `tests/tests/azure_test.rs` | Rust integration tests (Azurite) |
| `tests/tests/azure_cli_test.rs` | CLI integration tests (Azurite) |
| `tests/tests/fabric_test.rs` | Fabric OneLake integration tests |
| `scripts/test_python_azure.py` | Python integration tests (Azurite) |
| `docker-compose.yml` | Azurite service definition |
| `tests/AZURE_TEST_SETUP.md` | Setup instructions for running Azure tests |
