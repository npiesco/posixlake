# POSIX Interface Test Setup

## Prerequisites for Running POSIX Tests

The POSIX interface uses a **pure Rust NFS server** - NO external drivers required!

### Zero Installation Required

**All Operating Systems**: Your OS already has a built-in NFS client. No additional software needed!

- **macOS**: Built-in NFS client (mount_nfs)
- **Linux**: Built-in NFS client (mount.nfs)
- **Windows**: Built-in NFS client (Windows Pro/Enterprise/Education)

**Why NFS Server?**
- No external drivers - no FUSE-T, no WinFsp, no kernel extensions
- No installation - OS has native NFS client built-in
- No security prompts - no system modifications required
- True cross-platform - works identically on all OSes
- CI/CD friendly - headless environments fully supported
- No crashes or dialogs - stable NFS protocol, not experimental drivers

### How It Works

FSDB runs a localhost NFS server in pure Rust. Your OS mounts it using its native NFS client:

```bash
# macOS/Linux
sudo mount -t nfs -o nolocks,vers=3 localhost:/fsdb /mnt/fsdb

# Windows (mount as network drive)
mount -o anon \\localhost\fsdb Z:
```

The `fsdb` CLI handles all mounting automatically - you just run:

```bash
fsdb mount /path/to/database /mnt/fsdb
```

## Running the Tests

### Prerequisites: Passwordless Sudo Setup

The NFS mounting tests **require passwordless sudo** for `mount_nfs` and `umount` commands. This is a one-time setup:

```bash
# Run the automated setup script (ONE TIME):
python3 scripts/setup_nfs_sudo.py

# The script will:
# 1. Create a sudoers file in /etc/sudoers.d/
# 2. Configure passwordless sudo for mount_nfs and umount only
# 3. Verify the setup is working
# 4. Prompt for your password once during setup

# After setup completes, NFS tests run without any password prompts
```

**Manual Setup** (if you prefer):
```bash
# Run this ONCE to setup passwordless sudo for NFS commands only:
sudo visudo
# Add this line (replace YOUR_USERNAME):
YOUR_USERNAME ALL=(ALL) NOPASSWD: /sbin/mount_nfs, /sbin/umount
```

### Running Tests

```bash
# Run all NFS tests (21 tests passing)
cargo test --test nfs_test

# All tests pass with passwordless sudo configured:
# - test_nfs_server_starts_and_stops
# - test_nfs_server_exports_database
# - test_nfs_directory_structure
# - test_nfs_read_csv_file
# - test_nfs_write_to_csv
# - test_nfs_file_attributes
# - test_real_os_mount_and_posix_commands (requires sudo)
# - test_real_os_write_through_mount (requires sudo)
# - test_nfs_list_individual_parquet_files
# - test_nfs_read_individual_parquet_file
# - test_nfs_parquet_file_accurate_sizes
# - test_nfs_parquet_file_modification_times
# - test_nfs_parquet_file_row_count_metadata
# ... and more

# Build normally
cargo build
```

## Troubleshooting

### "Permission denied" during mount

On macOS/Linux, NFS mounting requires root permissions:

```bash
# Run tests with sudo (preserves RUST_LOG)
sudo -E cargo test --test posix_test
```

Alternatively, configure `/etc/exports` to allow user-space mounts (advanced).

### Windows: NFS Client Not Available

Windows Home doesn't include NFS client. Options:
1. **Upgrade to Windows Pro/Enterprise** (includes NFS client)
2. **Install third-party NFS client** (e.g., NFS Client from Microsoft Store)
3. **Use WSL2** (Linux NFS client works perfectly)

### Mount errors

If you see mount errors:
- **Port conflict**: Ensure no other NFS server is running on port 2049
- **Firewall**: Allow localhost NFS traffic
- **Mount point**: Ensure directory exists and is empty

## Architecture

```
┌─────────────────────────────────────────┐
│  POSIX Commands (ls, cat, grep, sed)   │
└──────────────┬──────────────────────────┘
               │ (Native OS mount)
               │ mount -t nfs localhost:/fsdb
               │
┌──────────────▼──────────────────────────┐
│  OS Built-in NFS Client                 │
│  macOS: mount_nfs / Linux: mount.nfs    │
│  Windows: NFS client service            │
└──────────────┬──────────────────────────┘
               │ (NFSv3 protocol)
               │
┌──────────────▼──────────────────────────┐
│  FSDB NFS Server (Pure Rust)            │
│  Port: localhost:2049                   │
│  Implementation: nfs3_server crate      │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│  FSDB Core (Delta Lake Native)         │
│  Parquet, MVCC, Delta Lake, SQL         │
└─────────────────────────────────────────┘
```

## What We're Testing

1. **P6.1**: NFS server startup and export listing
2. **P6.2**: Read operations (`cat`, `grep`, `head`, `tail`)
3. **P6.3**: Write operations (`echo >>`, `sed -i`, `vim`)
4. **P6.4**: CSV views of Parquet data
5. **P6.5**: Performance and concurrent access

## References

- NFS Protocol: RFC 1813 (NFSv3)
- nfsserve crate: https://github.com/xetdata/nfsserve
- FSDB Architecture: See PROGRESS_TREE_TRACKER.md

