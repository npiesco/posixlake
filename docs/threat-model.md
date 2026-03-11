# Threat Model: posixlake Local CLI

## System Overview

posixlake is a local/self-hosted columnar database engine. Data lives on the local filesystem or S3-compatible object storage. Users interact via the CLI, Rust API, Python bindings, or NFS mount.

```
 User
  |
  +-- CLI (posixlake-cli)
  +-- Rust API (DatabaseOps)
  +-- Python bindings (UniFFI)
  +-- NFS mount (cat, grep, sed, vim on mounted data)
  |
  v
 Database Directory
  +-- _delta_log/       (Delta Lake transaction log)
  +-- _metadata/        (users.json, audit.json, schema.json)
  +-- *.parquet         (columnar data files)
```

## Trust Boundaries

### Boundary 1: Caller to DatabaseOps API

All operations flow through `DatabaseOps`. When authentication is enabled, every public method checks permissions before executing. The boundary is the `check_permission()` gate.

**Trust assumption**: The calling process is not compromised. posixlake does not defend against a process that bypasses the API and reads Parquet files directly from disk.

### Boundary 2: Database directory on filesystem

The `_metadata/` directory contains credentials (`users.json`) and audit logs (`audit.json`). These files are readable by any user with filesystem access to the database directory.

**Trust assumption**: The OS-level file permissions on the database directory restrict access to authorized users. posixlake does not encrypt data at rest on local disk.

### Boundary 3: NFS mount point

When a database is mounted via NFS, the mount point exposes CSV/JSON/Parquet views. The NFS server runs in-process and binds to localhost by default.

**Trust assumption**: The NFS mount is accessed only by local users on the same machine. Network-exposed NFS mounts require external firewall rules.

### Boundary 4: S3 transport

S3-backed databases send data over HTTPS to the configured endpoint. Credentials are passed via environment variables or storage options.

**Trust assumption**: S3 credentials are managed securely outside posixlake (IAM roles, env vars, credential files). posixlake does not store S3 credentials persistently.

## Threat Categories

### T1: Unauthorized data access

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Unauthenticated user reads data from auth-enabled DB | Fail-closed: all ops return "Authentication required" without valid credentials | Implemented |
| User with `write` role reads data | Read permission check on query, schema, primary_key, base_path, list_parquet_files, get_delta_table | Implemented |
| Direct filesystem read bypasses auth | Out of scope; mitigated by OS file permissions on the database directory | By design |

### T2: Privilege escalation

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Non-admin user creates users or grants roles | Admin permission required for create_user, grant_role, revoke_role | Implemented |
| Non-admin user accesses audit log | Admin permission required for get_audit_log | Implemented |
| Reader user performs write/delete operations | Write/Delete permission checks on insert, delete, merge, optimize, vacuum, zorder | Implemented |

### T3: Credential compromise

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Password extracted from users.json | Passwords stored as bcrypt hashes (cost 12); plaintext never stored | Implemented |
| Brute-force password attack | bcrypt cost factor makes offline brute-force expensive; no rate limiting on API calls (local-only tool) | Partial |
| Credential leakage in audit log | Audit log records usernames and operations, never passwords | Implemented |

### T4: Audit log tampering

| Threat | Mitigation | Status |
|--------|-----------|--------|
| User deletes or modifies audit.json | Audit file lives in _metadata/; protected by OS file permissions | Partial |
| Permission-denied operations go unlogged | All permission-checked API surfaces emit audit entries on denial | Implemented |
| Attacker suppresses audit by opening DB without auth | Unauthenticated opens on auth-enabled DBs are fail-closed; no operations succeed to suppress | Implemented |

### T5: Data integrity

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Concurrent writers corrupt Delta log | Delta Lake ACID transactions with optimistic concurrency | Implemented |
| Malformed RecordBatch corrupts table | Schema validation on insert; Arrow type checking | Implemented |
| Path traversal via database path | Database paths are resolved to absolute paths; no symlink following in metadata | Implemented |

### T6: NFS exposure

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Remote host accesses NFS mount | NFS server binds to localhost (127.0.0.1) by default | Implemented |
| NFS mount exposes auth-enabled DB without credentials | NFS mount inherits database auth posture; operations through mount are subject to same permission checks | Implemented |
| Write via NFS CSV facade bypasses auth | CSV writes trigger Delta Lake operations through the same DatabaseOps API with permission checks | Implemented |

### T7: SQL injection

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Malicious SQL via query() API | Queries execute through DataFusion query planner; no raw SQL passthrough to external DB | Low risk |
| SQL via .query file on NFS mount | Same DataFusion execution path; read-only by default unless write permission granted | Low risk |

## Residual Risks

1. **No encryption at rest**: Data files (Parquet) and metadata (users.json, audit.json) are unencrypted on disk. Mitigation: use filesystem-level encryption (LUKS, FileVault, BitLocker) or encrypted S3 buckets.

2. **No network authentication for NFS**: The NFS server does not authenticate connecting clients beyond localhost binding. Mitigation: do not expose NFS port to untrusted networks.

3. **No API rate limiting**: The local API has no rate limiting on authentication attempts. Mitigation: acceptable for local CLI tool; not intended for network-exposed deployments.

4. **Audit log is append-only by convention, not cryptographically**: A user with filesystem write access to `_metadata/audit.json` could modify it. Mitigation: restrict filesystem permissions; consider log signing in a future phase.

## Recommendations for Operators

1. Set database directory permissions to `700` (owner only) or `750` (owner + group).
2. Enable authentication on all databases that contain sensitive data.
3. Create separate users with least-privilege roles (use `read` role for analytics users).
4. Back up `_metadata/` directory separately for audit trail preservation.
5. Use filesystem encryption for data at rest.
6. Do not expose the NFS mount port beyond localhost without a VPN or firewall.
