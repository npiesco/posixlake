# IMPORTANT: Testing Without Interference/Flaky/Fail

## Critical Testing Patterns for NFS-Based Systems

This document explains how to write reliable, non-flaky tests for FSDB's NFS server and filesystem mounts.

---

## The Golden Rules: Event-Based, Port-Isolated NFS Testing

**MANDATORY**: 
1. Every test MUST use a **unique NFS server port** to prevent interference when tests run in parallel
2. All synchronization MUST be **event-based**, never timeout-based (no arbitrary sleeps)
3. Server readiness is verified by actual NFS handshake, not timeouts
4. Mount points must be unique per test to avoid conflicts

### Why This Is Critical

NFS servers bind to network ports and mount points are system-level resources. When tests run in parallel (Rust's default behavior), they can:
- **Port conflicts** - multiple servers trying to bind to same port
- **Mount conflicts** - tests trying to mount at the same location
- **Hang** waiting for NFS responses
- **Fail intermittently** due to race conditions
- **Block** each other's file operations

### The Solution

Use unique ports and mount points for each test:

```rust
/// Helper to create unique NFS server port based on thread ID
/// This allows parallel test execution without port conflicts
fn create_unique_port(base_port: u16) -> u16 {
    let thread_id = format!("{:?}", thread::current().id());
    let hash: u16 = thread_id.bytes().map(|b| b as u16).sum::<u16>() % 10000;
    base_port + hash
}

/// Helper to create unique mount point based on thread ID
fn create_unique_mount_point(base_dir: &std::path::Path, test_name: &str) -> std::path::PathBuf {
    let thread_id = format!("{:?}", thread::current().id());
    let unique_name = format!("mount_{}_{}", test_name, thread_id.replace("ThreadId(", "").replace(")", ""));
    base_dir.join(unique_name)
}
```

---

## Correct Test Pattern

Every NFS test should follow this pattern:

```rust
#[tokio::test]
#[serial]  // Serial execution for NFS server tests
async fn test_my_nfs_feature() {
    let _ = tracing_subscriber::fmt::try_init();
    
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");
    
    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    
    // Create database with test data
    let db = DatabaseOps::create(&db_path, schema.clone()).unwrap();
    
    // Insert test data
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    ).unwrap();
    db.insert(batch).await.unwrap();
    
    // CRITICAL: Use unique port and mount point
    let nfs_port = create_unique_port(12049); // Base port above privileged range
    let mount_point = create_unique_mount_point(temp_dir.path(), "my_feature");
    fs::create_dir(&mount_point).unwrap();
    
    // Start NFS server
    let server = FsdbNfsServer::new(Arc::new(db), nfs_port).await.unwrap();
    
    // Wait for server to be ready (event-based)
    server.wait_ready().await.unwrap();
    
    // Mount using OS NFS client
    mount_nfs("localhost", nfs_port, &mount_point).unwrap();
    
    // Perform test operations on mount_point
    // ...
    
    // Cleanup: unmount and stop server
    unmount_nfs(&mount_point).unwrap();
    server.shutdown().await.unwrap();
}
```

---

## WRONG Patterns (DO NOT USE)

### Shared NFS Port Across Tests

```rust
// WRONG - All tests use same port
let nfs_port = 2049; // Will cause "Address already in use" errors
```

**Problem**: Tests running in parallel will try to bind to the same port, causing:
- "Address already in use" errors
- Tests hanging indefinitely waiting for port
- Random test failures

### Hardcoded Mount Paths

```rust
// WRONG - Hardcoded path shared across all tests
let mount_point = PathBuf::from("/tmp/fsdb_mount");
```

**Problem**: Multiple tests will try to mount at the same location, causing system-level conflicts.

### Missing `#[serial]` Annotation

```rust
#[tokio::test]
// WRONG - Missing #[serial] for NFS tests
async fn test_something() {
    // ...
}
```

**Problem**: NFS server tests should run serially to avoid port conflicts and resource exhaustion, even with unique ports.

---

## Testing Checklist

Before submitting an NFS test, verify:

- [x] Uses `#[serial]` annotation for NFS server operations
- [x] Uses `create_unique_port()` for NFS server port
- [x] Uses `create_unique_mount_point()` with descriptive test name
- [x] Uses `TempDir` for database path (automatic cleanup)
- [x] Mount point is created inside `TempDir` (automatic cleanup)
- [x] Test passes when run alone: `cargo test test_name`
- [x] Test passes with all other tests: `cargo test --test nfs_test`
- [x] Test passes 3+ times in a row without failures
- [x] No hanging or timeout issues

---

## Key Insights

### Why Unique Ports and Paths Work

1. **Parallel Safety**: Each test gets a unique port and mount point automatically
2. **No Port Conflicts**: Port hashing based on thread ID prevents collisions
3. **Automatic Cleanup**: Server shutdown and unmount happen automatically
4. **Deterministic**: Thread-based naming is consistent and predictable

### Why `#[serial]` Is Required

Even with unique ports, `#[serial]` is mandatory for NFS tests because:
- NFS servers consume system resources (file descriptors, memory)
- Parallel NFS servers can exhaust kernel resources
- Mount/unmount operations need exclusive access to mount table
- `#[serial]` ensures tests run one at a time for stability

### NFS Server Considerations

NFSv3 protocol specifics to keep in mind:
- Server must respond to NFS handshake within timeout
- Requires event-based readiness signaling (no polling/timeouts)
- Stateless protocol - server doesn't track client state
- Mount operations may require sudo/admin privileges
- localhost binding (127.0.0.1) for security

---

## Reference Examples

See these tests for correct patterns:
- `tests/tests/nfs_test.rs::test_nfs_server_starts_and_stops`
- `tests/tests/nfs_test.rs::test_database_export_via_nfs`
- `tests/tests/nfs_test.rs::test_read_csv_content`
- `tests/tests/nfs_test.rs::test_directory_structure`

---
## Summary

**The Golden Rules:**
1. Use `create_unique_port()` for every NFS server test
2. Use `create_unique_mount_point()` for every mount
3. Use `#[serial]` on ALL NFS tests for stability
4. Use event-based server readiness (not timeouts)
5. Clean up: unmount then shutdown server

Follow these rules religiously. No exceptions.