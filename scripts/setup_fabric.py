"""Provision a Microsoft Fabric PAYG capacity, workspace, and lakehouse."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import time

# ========== CONFIG ==========
SUBSCRIPTION_ID = os.environ.get("AZ_SUBSCRIPTION_ID", "").strip()
RESOURCE_GROUP = "rg-fabric-dev"
LOCATION = "centralus"
CAPACITY_NAME = os.environ.get("FABRIC_CAPACITY_NAME", "").strip()
CAPACITY_SKU = "F2"  # smallest PAYG SKU
CAPACITY_API_VERSION = "2023-11-01"
CAPACITY_ADMIN_MEMBER = os.environ.get("FABRIC_CAPACITY_ADMIN_MEMBER", "").strip()
WORKSPACE_NAME = "FabricDevWS"
LAKEHOUSE_NAME = "devlake"
# ============================


def run(
    args: list[str], *, check: bool = True, capture_output: bool = False, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        check=check,
        text=True,
        capture_output=capture_output,
        env=env,
    )


def resolve_subscription_id() -> str:
    if SUBSCRIPTION_ID:
        return SUBSCRIPTION_ID

    az = resolve_az_executable()
    result = run(
        [az, "account", "show", "--query", "id", "--output", "tsv"],
        capture_output=True,
    )
    subscription_id = result.stdout.strip()
    if not subscription_id:
        raise SystemExit(
            "Set AZ_SUBSCRIPTION_ID or sign in with `az login` before running this script."
        )
    return subscription_id


def resolve_tenant_id(az: str) -> str:
    result = run(
        [az, "account", "show", "--query", "tenantId", "--output", "tsv"],
        capture_output=True,
    )
    tenant_id = result.stdout.strip()
    if not tenant_id:
        raise SystemExit("Azure CLI did not return a tenant ID for the current account.")
    return tenant_id


def resolve_az_executable() -> str:
    for executable_name in ("az", "az.cmd", "az.exe"):
        az_path = shutil.which(executable_name)
        if az_path:
            return az_path

    windows_candidates = []
    for env_var in ("ProgramFiles", "ProgramFiles(x86)"):
        base_dir = os.environ.get(env_var)
        if base_dir:
            windows_candidates.append(
                Path(base_dir) / "Microsoft SDKs" / "Azure" / "CLI2" / "wbin" / "az.cmd"
            )

    for candidate in windows_candidates:
        if candidate.exists():
            return str(candidate)

    raise SystemExit("Azure CLI was not found. Install Azure CLI or add `az.cmd` to PATH.")


def resolve_uv_executable() -> str:
    for executable_name in ("uv", "uv.exe", "uv.cmd"):
        uv_path = shutil.which(executable_name)
        if uv_path:
            return uv_path

    raise SystemExit("uv was not found. Install uv or add it to PATH before running this script.")


def resolve_fab_command() -> list[str]:
    return [resolve_uv_executable(), "tool", "run", "--from", "ms-fabric-cli", "fab"]


def resolve_capacity_admin_member(az: str) -> str:
    if CAPACITY_ADMIN_MEMBER:
        return validate_capacity_admin_member(az, CAPACITY_ADMIN_MEMBER)

    candidates: list[str] = []

    signed_in_user = run(
        [az, "ad", "signed-in-user", "show", "--query", "userPrincipalName", "--output", "tsv"],
        check=False,
        capture_output=True,
    )
    user_principal_name = signed_in_user.stdout.strip()
    if user_principal_name:
        candidates.append(user_principal_name)

    directory_users = run(
        [az, "ad", "user", "list", "--filter", "userType eq 'Member'", "--query", "[].userPrincipalName", "--output", "tsv"],
        check=False,
        capture_output=True,
    )
    if directory_users.stdout.strip():
        candidates.extend(
            user.strip() for user in directory_users.stdout.splitlines() if user.strip()
        )

    result = run(
        [az, "account", "show", "--query", "user.name", "--output", "tsv"],
        capture_output=True,
    )
    member = result.stdout.strip()
    if member:
        candidates.append(member)

    unique_candidates: list[str] = []
    seen_candidates: set[str] = set()
    for candidate in candidates:
        normalized_candidate = candidate.strip()
        if not normalized_candidate:
            continue
        comparison_key = normalized_candidate.casefold()
        if comparison_key in seen_candidates:
            continue
        seen_candidates.add(comparison_key)
        unique_candidates.append(normalized_candidate)

    valid_candidates: list[str] = []
    for candidate in unique_candidates:
        try:
            valid_candidates.append(validate_capacity_admin_member(az, candidate))
        except SystemExit:
            continue

    if len(valid_candidates) == 1:
        return valid_candidates[0]

    if len(valid_candidates) > 1:
        raise SystemExit(
            "Azure CLI found multiple tenant member users that could be Fabric capacity admins. Set FABRIC_CAPACITY_ADMIN_MEMBER to the desired Entra UPN."
        )

    raise SystemExit(
        "Azure CLI could not discover a native Entra member user for Fabric capacity administration. Set FABRIC_CAPACITY_ADMIN_MEMBER to a tenant member UPN or sign in with a tenant member account."
    )


def validate_capacity_admin_member(az: str, member: str) -> str:
    result = run(
        [
            az,
            "ad",
            "user",
            "show",
            "--id",
            member,
            "--query",
            "{upn:userPrincipalName,userType:userType}",
            "--output",
            "json",
        ],
        check=False,
        capture_output=True,
    )

    if result.returncode != 0 or not result.stdout.strip():
        raise SystemExit(
            "Fabric capacity admins must be tenant member users. Set FABRIC_CAPACITY_ADMIN_MEMBER to a native Entra UPN in the target tenant."
        )

    payload = json.loads(result.stdout)
    user_principal_name = str(payload.get("upn") or member).strip()
    user_type = str(payload.get("userType") or "").strip().lower()

    if "#EXT#" in user_principal_name.upper() or user_type == "guest":
        raise SystemExit(
            "Fabric capacity admins cannot be B2B guest users. Set FABRIC_CAPACITY_ADMIN_MEMBER to a native Entra member UPN in the target tenant."
        )

    return user_principal_name


def resolve_capacity_name(subscription_id: str) -> str:
    if CAPACITY_NAME:
        return CAPACITY_NAME

    normalized_subscription_id = subscription_id.replace("-", "")
    return f"fabric{normalized_subscription_id}"


def wait_for_provider_registration(az: str, namespace: str) -> None:
    for _ in range(30):
        result = run(
            [az, "provider", "show", "--namespace", namespace, "--query", "registrationState", "--output", "tsv"],
            capture_output=True,
        )
        if result.stdout.strip() == "Registered":
            return
        time.sleep(2)

    raise SystemExit(f"Azure provider {namespace} is not fully registered yet. Try again in a minute.")


def get_access_token(
    az: str, *, resource: str | None = None, resource_type: str | None = None
) -> str:
    args = [az, "account", "get-access-token"]
    if resource:
        args.extend(["--resource", resource])
    if resource_type:
        args.extend(["--resource-type", resource_type])
    args.extend(["--query", "accessToken", "--output", "tsv"])

    result = run(args, capture_output=True)
    token = result.stdout.strip()
    if not token:
        raise SystemExit("Azure CLI did not return an access token required for Fabric CLI.")
    return token


def build_fabric_environment(az: str, tenant_id: str) -> dict[str, str]:
    fabric_env = os.environ.copy()
    fabric_env["FAB_TOKEN"] = get_access_token(
        az, resource="https://api.fabric.microsoft.com/"
    )
    fabric_env["FAB_TOKEN_ONELAKE"] = get_access_token(
        az, resource_type="data-lake"
    )
    fabric_env["FAB_TOKEN_AZURE"] = get_access_token(
        az, resource="https://management.azure.com/"
    )
    fabric_env["FAB_TENANT_ID"] = tenant_id
    return fabric_env


def main() -> None:
    workspace_path = f"{WORKSPACE_NAME}.Workspace"
    lakehouse_path = f"{workspace_path}/{LAKEHOUSE_NAME}.Lakehouse"
    az = resolve_az_executable()

    print("== Resolve Fabric CLI ==")
    fab_command = resolve_fab_command()

    subscription_id = resolve_subscription_id()
    tenant_id = resolve_tenant_id(az)
    capacity_name = resolve_capacity_name(subscription_id)
    capacity_admin_member = resolve_capacity_admin_member(az)

    print("== Azure context ==")
    run([az, "account", "set", "--subscription", subscription_id])

    print("== Verify Fabric access ==")
    fab_status = run([*fab_command, "auth", "status"], check=False)
    if fab_status.returncode != 0:
        raise SystemExit(
            "Fabric CLI is not logged in. Run:\n"
            f"  uv tool run --from ms-fabric-cli fab auth login --tenant {tenant_id}\n"
            f"Sign in as: {capacity_admin_member}"
        )

    print("== Ensure Fabric provider registered ==")
    run([az, "provider", "register", "--namespace", "Microsoft.Fabric"])
    wait_for_provider_registration(az, "Microsoft.Fabric")

    print("== Create resource group (idempotent) ==")
    rg_check = run(
        [az, "group", "exists", "--name", RESOURCE_GROUP, "--subscription", subscription_id],
        capture_output=True,
    )
    if rg_check.stdout.strip().lower() == "true":
        print(f"  Resource group '{RESOURCE_GROUP}' already exists — skipping creation.")
    else:
        run([
            az, "group", "create",
            "--name", RESOURCE_GROUP,
            "--location", LOCATION,
            "--subscription", subscription_id,
            "--output", "none",
        ])

    print("== Create Fabric PAYG capacity ==")
    capacity_url = (
        "https://management.azure.com/"
        f"subscriptions/{subscription_id}/resourceGroups/{RESOURCE_GROUP}"
        f"/providers/Microsoft.Fabric/capacities/{capacity_name}"
        f"?api-version={CAPACITY_API_VERSION}"
    )
    # Check if capacity already exists before trying to create it.
    capacity_check = run(
        [az, "rest", "--method", "get", "--url", capacity_url],
        check=False,
        capture_output=True,
    )
    if capacity_check.returncode == 0:
        print(f"  Capacity '{capacity_name}' already exists — skipping creation.")
    else:
        capacity_body = {
            "location": LOCATION,
            "sku": {"name": CAPACITY_SKU, "tier": "Fabric"},
            "properties": {
                "administration": {
                    "members": [capacity_admin_member],
                }
            },
        }
        run([
            az, "rest",
            "--method", "put",
            "--url", capacity_url,
            "--body", json.dumps(capacity_body),
            "--output", "none",
        ])

    print("== Create Fabric workspace on capacity ==")
    run([*fab_command, "create", workspace_path, "-P", f"capacityname={capacity_name}"])

    print("== Create lakehouse ==")
    run([*fab_command, "create", lakehouse_path])

    print("== DONE ==")
    print(f"Workspace:  {WORKSPACE_NAME}")
    print(f"Capacity:   {capacity_name} ({CAPACITY_SKU} PAYG)")
    print(f"Lakehouse:  {LAKEHOUSE_NAME}")


if __name__ == "__main__":
    main()
