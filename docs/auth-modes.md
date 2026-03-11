# Authentication Modes

posixlake databases operate in one of two modes: **auth-disabled** (default) or **auth-enabled**.

## Auth-Disabled Mode (Default)

When a database is created without authentication, all operations are permitted without credentials.

```rust
let db = DatabaseOps::create(&path, schema).await?;
// All operations succeed without authentication
db.insert(batch).await?;
db.query("SELECT * FROM data").await?;
```

**Characteristics:**
- No `_metadata/users.json` file exists.
- No permission checks on any operation.
- No audit logging of operations.
- Suitable for single-user local development.

## Auth-Enabled Mode

When a database is created with authentication enabled, every operation requires valid credentials and appropriate permissions.

```rust
let db = DatabaseOps::create_with_auth(&path, schema, true).await?;
db.create_user("admin", "password", &["admin"]).await?;

// Must open with credentials to perform operations
let authed = DatabaseOps::open_with_credentials(&path, Some(("admin", "password"))).await?;
authed.query("SELECT * FROM data").await?;
```

**Characteristics:**
- `_metadata/users.json` and `_metadata/audit.json` are created.
- All operations check the caller's permissions before executing.
- Denied operations are audited with `success=false`.
- Opening without credentials succeeds, but every subsequent operation fails with "Authentication required".

### Detection

posixlake determines the auth mode by checking for `_metadata/users.json` in the database directory:
- **File exists** -> auth-enabled mode; security components are loaded.
- **File absent** -> auth-disabled mode; all permission checks are bypassed.

## Opening an Auth-Enabled Database

### With Valid Credentials

```rust
let db = DatabaseOps::open_with_credentials(&path, Some(("alice", "secret"))).await?;
// Operations permitted based on alice's roles
```

The caller's roles determine which operations are allowed. See the permission matrix below.

### Without Credentials

```rust
let db = DatabaseOps::open(&path).await?;
// db opens successfully, but:
db.query("SELECT * FROM data").await;
// -> Error: "Authentication required"
```

The database opens (metadata is loaded) but `auth_context` is `None`. Every operation that touches data or metadata returns an error.

### With Invalid Credentials

```rust
let db = DatabaseOps::open_with_credentials(&path, Some(("alice", "wrong"))).await;
// -> Error: authentication failure
```

The open call itself fails. No database handle is returned.

## Permission Matrix

| Operation | Required Permission |
|-----------|-------------------|
| `query()`, `query_file()` | Read |
| `schema()`, `primary_key()`, `base_path()` | Read |
| `list_parquet_files()`, `get_delta_table()` | Read |
| `insert()`, `insert_buffered()` | Write |
| `flush_write_buffer()` | Write |
| `set_primary_key()` | Admin |
| `delete_rows_where()` | Delete |
| `merge()` | Write |
| `optimize()`, `zorder()`, `vacuum()` | Delete |
| `backup()`, `backup_incremental()` | Read |
| `verify_backup()`, `get_backup_metadata()` | Read |
| `restore()`, `restore_to_transaction()` | Admin |
| `create_user()`, `grant_role()`, `revoke_role()` | Admin |
| `get_audit_log()` | Admin |
| `begin_transaction()` | Write |
| `health_check()` | Read (for uptime metadata) |
| `reset_metrics()`, `reset_data_skipping_stats()` | Admin |

## Built-in Roles

| Role | Permissions | Typical Use |
|------|-------------|-------------|
| `admin` | Read, Write, Delete, Admin, Backup, Restore | Database administrators |
| `read` | Read | Analysts, dashboards |
| `write` | Write | ETL/ingestion pipelines |

Users can hold multiple roles. A user with both `read` and `write` roles can query and insert but cannot delete or manage users.

## Audit Trail

Every permission-checked operation produces an audit entry in `_metadata/audit.json`:

```json
{
  "timestamp": 1710172800000,
  "user": "alice",
  "operation": "INSERT",
  "details": "10 rows",
  "success": true
}
```

Denied operations are also logged:

```json
{
  "timestamp": 1710172801000,
  "user": "bob",
  "operation": "INSERT",
  "details": "failed: Permission denied: Write",
  "success": false
}
```

## CLI Support (Planned)

The CLI does not currently expose authentication flags. Auth is available through the Rust API and Python bindings. Planned CLI additions:

- `posixlake create --auth` to create auth-enabled databases.
- `--user` / `--password` flags or `POSIXLAKE_USER` / `POSIXLAKE_PASSWORD` environment variables for credential passing.
- `posixlake user create|grant|revoke` subcommands for user management.
