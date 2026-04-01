# Microsoft Fabric Setup

Provisions a Fabric PAYG capacity, workspace (`FabricDevWS`), and lakehouse (`devlake`) using the Azure CLI and Fabric CLI.

## Prerequisites

| Tool | Install |
|------|---------|
| [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) | `winget install Microsoft.AzureCLI` |
| [uv](https://docs.astral.sh/uv/) | `winget install astral-sh.uv` |
| Fabric CLI | `uv tool install ms-fabric-cli` |

## Required Azure/Entra Setup

The capacity admin **must** be a native Entra tenant member — B2B guest accounts (`#EXT#` UPNs) are rejected by the Fabric API.

### 1. Create a native tenant-member user (if needed)

Skip this if you already have a native member account in your tenant.

```powershell
az ad user create `
  --display-name "Fabric Capacity Admin" `
  --user-principal-name "fabricadmin@<your-tenant>.onmicrosoft.com" `
  --password "<strong-password>" `
  --force-change-password-next-sign-in false
```

### 2. Grant Azure subscription access

The admin user needs Contributor on the subscription to create the capacity ARM resource:

```powershell
az role assignment create `
  --assignee "fabricadmin@<your-tenant>.onmicrosoft.com" `
  --role Contributor `
  --scope "/subscriptions/<subscription-id>"
```

### 3. Activate a Power BI license

The admin user must have at least a Power BI (Free) license. Without it, the Fabric capacity API returns `401 Unauthorized`.

1. Sign in to https://app.powerbi.com as the admin user
2. Complete the self-service signup flow
3. Verify: **Settings > Account > License** should show "Free account" or higher

### 4. Request Fabric capacity quota

New subscriptions start with **0 CU** (Capacity Units). An F2 SKU requires 2 CU.

1. Go to https://portal.azure.com → search **Quotas** → select **Microsoft Fabric**
2. Click **New Quota Request**
3. Select the subscription, region, and request at least **2 CU**
4. Submit — auto-approves within minutes

Verify via CLI:
```powershell
az rest --method get `
  --url "https://management.azure.com/subscriptions/<subscription-id>/providers/Microsoft.Fabric/locations/<region>/usages?api-version=2023-11-01"
```

## Authentication

Two separate logins are required — Azure CLI for ARM operations and Fabric CLI for workspace/lakehouse creation.

### Azure CLI

```powershell
az login --tenant <tenant-id> --use-device-code
```

Sign in as the native admin user.

### Fabric CLI

```powershell
uv tool run --from ms-fabric-cli fab auth login --tenant <tenant-id>
```

Select **Interactive with a web browser** and sign in as the same native admin user.

Verify both:
```powershell
az account show --query "{user:user.name, tenant:tenantId}" --output json
uv tool run --from ms-fabric-cli fab auth status
```

## Run the Script

```powershell
$env:FABRIC_CAPACITY_NAME = 'posixlake'
$env:FABRIC_CAPACITY_ADMIN_MEMBER = 'fabricadmin@<your-tenant>.onmicrosoft.com'
uv run python scripts/setup_fabric.py
```

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `FABRIC_CAPACITY_NAME` | No | Derived from subscription ID | Lowercase alphanumeric, globally unique |
| `FABRIC_CAPACITY_ADMIN_MEMBER` | No | Auto-discovered from Entra | Native tenant member UPN |
| `AZ_SUBSCRIPTION_ID` | No | Current `az` subscription | Azure subscription ID |

### What the Script Does

1. Verifies Fabric CLI is authenticated
2. Registers the `Microsoft.Fabric` resource provider
3. Creates resource group `rg-fabric-dev` (skips if exists)
4. Creates F2 PAYG capacity via ARM PUT (skips if exists)
5. Creates workspace `FabricDevWS` on the capacity via Fabric CLI
6. Creates lakehouse `devlake` in the workspace via Fabric CLI

All steps are idempotent for capacity and resource group. Workspace and lakehouse creation will fail if names collide — delete them first via `fab delete` if re-running.

## Teardown

```powershell
# Delete lakehouse and workspace
uv tool run --from ms-fabric-cli fab delete FabricDevWS.Workspace/devlake.Lakehouse
uv tool run --from ms-fabric-cli fab delete FabricDevWS.Workspace

# Delete capacity (stops billing)
az resource delete `
  --resource-group rg-fabric-dev `
  --resource-type "Microsoft.Fabric/capacities" `
  --name posixlake

# Delete resource group
az group delete --name rg-fabric-dev --yes
```

## Gotchas

| Issue | Root Cause | Fix |
|-------|-----------|-----|
| `401 Unable to authorize with Azure Active Directory` | Capacity admin is a B2B guest user | Use a native tenant member (no `#EXT#` in UPN) |
| `401 Unable to authorize` even with native user | User has no Power BI license | Sign in to https://app.powerbi.com to activate free license |
| `RegionalQuota: 0, RequestedSku: F2` | Subscription has 0 Fabric CU quota | Request quota increase in Azure Portal → Quotas → Microsoft Fabric |
| `InvalidResourceLocation` on capacity PUT | Capacity exists in a different region | Set `LOCATION` in script to match, or use existing capacity name |
| `Failed to decode JWT token` from `fab create` | Fabric CLI ignores `FAB_TOKEN` env vars | Use `fab auth login` interactive flow instead |
| `az` not found in Python subprocess on Windows | `az` is `az.cmd` on Windows, not on PATH for subprocess | Script resolves `az.cmd` via `shutil.which` + fallback paths |
| Fabric CLI hangs on install with Python 3.13 ARM64 | `pymsalruntime` doesn't build on ARM64/3.13 | Use `uv tool install --python <3.11-path> ms-fabric-cli` |
