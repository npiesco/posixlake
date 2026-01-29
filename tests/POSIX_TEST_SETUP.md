# POSIX Test Setup

This document covers platform-specific prerequisites for running posixlake's integration test suite, particularly the NFS mount tests.

## Prerequisites by Platform

### Linux

NFS mount tests require either root access or passwordless sudo for `mount`/`umount`:

```bash
# Option 1: Run tests as root
sudo cargo test

# Option 2: Configure passwordless sudo (recommended)
python3 scripts/setup_nfs_sudo.py
```

The setup script configures `/etc/sudoers.d/` to allow passwordless `mount` and `umount` for your user.

### macOS

NFS mount tests require either root access or passwordless sudo for `mount_nfs`:

```bash
# Option 1: Run tests as root
sudo cargo test

# Option 2: Configure passwordless sudo (recommended)
python3 scripts/setup_nfs_sudo.py
```

### Windows

NFS mount tests require the **Client for NFS** Windows feature, which provides `mount.exe` and `umount.exe`.

#### 1. Enable NFS Client Feature

Open an **Administrator PowerShell** and run:

```powershell
Enable-WindowsOptionalFeature -Online -FeatureName ServicesForNFS-ClientOnly
```

A reboot may be required after enabling the feature.

#### 2. Verify Installation

After enabling and rebooting, confirm `mount.exe` is available:

```cmd
where mount
```

Expected output:

```
C:\Windows\system32\mount.exe
```

#### 3. Run Tests

```cmd
cargo test
```

The test harness will automatically detect whether the NFS client is available and skip NFS mount tests if `mount.exe` is not found.

#### Notes

- The NFS Client feature is available on Windows 10/11 Pro, Enterprise, and Education editions. It is **not** available on Home editions.
- Windows Server uses a different feature name: `Install-WindowsFeature -Name NFS-Client`
- The Windows `mount` command syntax used by the tests: `mount -o port=<port> \\<host>\/ <mount_point>`
- Unmount uses: `umount <mount_point>`

## Running Tests

```bash
# Run all tests
cargo test

# Run only NFS mount tests
cargo test -p posixlake-integration-tests --test nfs_test

# Run only non-NFS integration tests (no mount required)
cargo test -p posixlake-integration-tests --test auth_test
cargo test -p posixlake-integration-tests --test backup_restore_test
cargo test -p posixlake-integration-tests --test column_stats_test
```

## Temporary Directories

All integration tests use `std::env::temp_dir()` for cross-platform temporary file storage:
- **Linux/macOS**: `/tmp/`
- **Windows**: `C:\Users\<user>\AppData\Local\Temp\`
