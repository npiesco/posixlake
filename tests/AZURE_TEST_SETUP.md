# Azure / Azurite Test Setup

This document covers setup for running posixlake's Azure Blob Storage integration tests locally using **Azurite** (Microsoft's official Azure Storage emulator).

## Quick Start

```bash
# Start Azurite via Podman (WSL)
posixlake-cli azure start --engine podman

# Or via docker-compose directly
podman compose -f docker-compose.yml up -d azurite

# Run Azure integration tests
cargo test --test azure_test
cargo test --test azure_cli_test
```

## Prerequisites

- **Podman** or **Docker** installed (Podman in WSL recommended)
- Azurite image: `mcr.microsoft.com/azure-storage/azurite:latest`

## Azurite Configuration

Azurite uses well-known development credentials (hardcoded, not secrets):

| Setting | Value |
|---------|-------|
| Account Name | `devstoreaccount1` |
| Account Key | `Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==` |
| Blob Endpoint | `http://127.0.0.1:10000` |
| Queue Endpoint | `http://127.0.0.1:10001` |
| Table Endpoint | `http://127.0.0.1:10002` |
| Container | `posixlake-test` |

## Environment Variables

Tests read these env vars (all have defaults for Azurite):

| Variable | Default | Description |
|----------|---------|-------------|
| `AZURITE_ENDPOINT` | `http://127.0.0.1:10000` | Azurite blob endpoint |
| `AZURE_STORAGE_ACCOUNT_NAME` | `devstoreaccount1` | Storage account name |
| `AZURE_STORAGE_ACCOUNT_KEY` | *(Azurite well-known key)* | Storage account key |
| `AZURE_STORAGE_CONTAINER` | `posixlake-test` | Test container name |
| `SKIP_AZURE_TESTS` | *(unset)* | Set to any value to skip Azure tests |

## Managing Azurite

### Via posixlake CLI

```bash
# Start Azurite (defaults to Podman)
posixlake-cli azure start

# Stop Azurite
posixlake-cli azure stop

# Test Azure backend end-to-end
posixlake-cli azure-test az://my-test-db
```

### Via docker-compose

```bash
# Start Azurite + init container (creates posixlake-test container)
podman compose -f docker-compose.yml up -d azurite azurite-init

# Stop Azurite
podman compose -f docker-compose.yml rm -sf azurite azurite-init
```

### Manual container

```bash
podman run -d --name azurite \
  -p 10000:10000 -p 10001:10001 -p 10002:10002 \
  mcr.microsoft.com/azure-storage/azurite:latest \
  azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0
```

## Creating the Test Container

Azurite starts with no containers. The `azurite-init` compose service creates `posixlake-test` automatically. To create it manually:

```bash
# Using Azure CLI
az storage container create \
  --name posixlake-test \
  --connection-string "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
```

## Test Files

| File | Description |
|------|-------------|
| `tests/tests/azure_test.rs` | Core integration tests (create, insert, query, reopen, multiple inserts) |
| `tests/tests/azure_cli_test.rs` | CLI integration tests (create with flags, env vars, health check) |

## Troubleshooting

- **Tests skip with "Azurite not reachable"**: Azurite isn't running on port 10000. Start it with `posixlake-cli azure start`.
- **Container creation fails**: Ensure Azurite is healthy before creating containers. The compose healthcheck handles this automatically.
- **Port conflicts**: Azurite uses ports 10000-10002. Check for conflicts with `ss -tlnp | grep 1000`.
