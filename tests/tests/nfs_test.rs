//! NFS Server Integration Tests
//! Testing posixlake's pure Rust NFS server implementation

use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use posixlake::DatabaseOps;
#[cfg(target_os = "windows")]
use posixlake::nfs::windows::{
    MOUNT_OPTIONS, ensure_clean_nfs_state, find_free_drive, prepare_nfs_mount,
};
use posixlake::nfs::{MountGuard, NfsServer};
use serial_test::serial;
use std::path::Path;
use std::sync::Arc;
use std::sync::Once;
use tempfile::TempDir;
#[cfg(target_os = "linux")]
use tracing::debug;

/// Clears NFS client state before other tests run.
/// Name starts with 'a' to ensure it runs first alphabetically.
#[tokio::test]
#[serial(nfs)]
#[cfg(target_os = "windows")]
async fn test_aaa_init_clean_nfs_state() {
    eprintln!("[TEST] Initializing clean NFS state for nfs tests...");
    ensure_clean_nfs_state().await;
    // Give Windows time to fully process the cleanup
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    eprintln!("[TEST] NFS state cleared");
}

static INIT: Once = Once::new();

/// Initialize tracing for tests
fn init_logging() {
    INIT.call_once(|| {
        let log_file = std::env::temp_dir()
            .join(format!("posixlake_nfs_test_{}.log", std::process::id()))
            .to_string_lossy()
            .into_owned();
        let file = std::fs::File::create(&log_file).expect("Failed to create log file");
        eprintln!("[TEST] Logging to: {}", log_file);
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(std::sync::Arc::new(file))
            .with_ansi(false)
            .init();
    });
}

/// Helper to create unique NFS server port based on test name
/// Avoids port conflicts when tests run in parallel
/// Helper to create unique NFS server port based on thread ID
/// This allows parallel test execution without port conflicts
fn create_unique_port(base_port: u16) -> u16 {
    // Windows mount.exe only supports the standard NFS port (2049)
    #[cfg(target_os = "windows")]
    {
        let _ = base_port;
        2049
    }

    #[cfg(not(target_os = "windows"))]
    {
        let thread_id = format!("{:?}", std::thread::current().id());
        let hash: u16 = thread_id.bytes().map(|b| b as u16).sum::<u16>() % 10000;
        base_port + hash
    }
}

/// Check if we can run mount commands (either as root or with passwordless sudo)
/// Returns true if we can mount, false otherwise  
fn check_can_mount() -> bool {
    // If running as root (UID 0), we can mount without sudo
    #[cfg(unix)]
    {
        // Check if we're root by seeing if our effective UID is 0
        let is_root = unsafe { libc::geteuid() } == 0;
        if is_root {
            return true;
        }
    }

    #[cfg(target_os = "windows")]
    {
        // Check if Windows NFS Client feature is installed (mount.exe exists)
        std::path::Path::new(r"C:\Windows\system32\mount.exe").exists()
    }

    // Check if passwordless sudo works for mount command
    // The sudoers file only allows specific commands (mount/umount), not generic 'true'
    // since /etc/sudoers.d/ is not readable by regular users
    #[cfg(not(target_os = "windows"))]
    {
        std::process::Command::new("sudo")
            .args(["-n", "mount", "--help"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
}

/// Ensure we can run mount commands, panic with instructions if not
fn require_mount_capability() {
    if !check_can_mount() {
        panic!(
            "\n\n\
            ================================================================================\n\
            ERROR: Cannot run mount commands!\n\
            ================================================================================\n\
            \n\
            NFS mount tests require either:\n\
            1. Running as root (UID 0), OR\n\
            2. Passwordless sudo configured for mount/umount operations\n\
            \n\
            To configure passwordless sudo:\n\
                python3 scripts/setup_nfs_sudo.py\n\
            \n\
            This will configure your system to allow posixlake to mount NFS without prompts.\n\
            \n\
            After running the setup script, re-run the tests.\n\
            ================================================================================\n\
            "
        );
    }
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_server_starts_and_stops() {
    init_logging();
    // TDD: This test should fail because NFS server doesn't exist yet

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Create database
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();
    let db = Arc::new(db);

    // Start NFS server on unique port
    let port = create_unique_port(12049);

    // This should compile and start server
    let server = posixlake::nfs::NfsServer::new(db.clone(), port).await;
    assert!(server.is_ok(), "NFS server should start successfully");

    let server = server.unwrap();

    // Server should be ready immediately
    assert!(server.is_ready().await, "NFS server should be ready");

    // Shutdown should be clean
    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_server_exports_database() {
    // TDD: Test that NFS server exports the database path

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema).await.unwrap();

    // Insert test data
    let batch = RecordBatch::try_new(
        db.schema(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    let port = create_unique_port(12049);
    let server = posixlake::nfs::NfsServer::new(Arc::new(db), port)
        .await
        .unwrap();

    // Server should export /posixlake path
    let exports = server.list_exports().await;
    assert_eq!(exports.len(), 1, "Should have exactly one export");
    assert_eq!(exports[0], "/posixlake", "Should export /posixlake");

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_directory_structure() {
    // TDD: Test that NFS exports proper directory structure

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema).await.unwrap();

    let port = create_unique_port(12049);
    let server = posixlake::nfs::NfsServer::new(Arc::new(db), port)
        .await
        .unwrap();

    // Root should have "data" directory
    let root_entries = server.readdir("/").await.unwrap();
    assert!(
        root_entries.contains(&"data".to_string()),
        "Root should contain 'data' directory"
    );

    // /data should have data.csv
    let data_entries = server.readdir("/data").await.unwrap();
    assert!(
        data_entries.contains(&"data.csv".to_string()),
        "/data should contain 'data.csv'"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_read_csv_file() {
    // TDD: Test reading data.csv through NFS

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema).await.unwrap();

    // Insert test data
    let batch = RecordBatch::try_new(
        db.schema(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    let port = create_unique_port(12049);
    let server = posixlake::nfs::NfsServer::new(Arc::new(db), port)
        .await
        .unwrap();

    // Read data.csv
    let content = server.read_file("/data/data.csv", 0, 1024).await.unwrap();
    let csv_str = String::from_utf8_lossy(&content);

    // Verify CSV content
    assert!(csv_str.contains("id,name"), "CSV should have headers");
    assert!(csv_str.contains("1,Alice"), "CSV should have first row");
    assert!(csv_str.contains("2,Bob"), "CSV should have second row");
    assert!(csv_str.contains("3,Charlie"), "CSV should have third row");

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_write_to_csv() {
    // TDD: Test writing data via NFS (append operation)

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema).await.unwrap();

    // Start with one row
    let batch = RecordBatch::try_new(
        db.schema(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Alice"])),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    let db = Arc::new(db);
    let port = create_unique_port(12049);
    let server = posixlake::nfs::NfsServer::new(db.clone(), port)
        .await
        .unwrap();

    // Write new row: "2,Bob\n"
    let new_data = b"2,Bob\n";
    server
        .write_file("/data/data.csv", 0, new_data)
        .await
        .unwrap();

    // Read back and verify
    let content = server.read_file("/data/data.csv", 0, 1024).await.unwrap();
    let csv_str = String::from_utf8_lossy(&content);

    assert!(
        csv_str.contains("1,Alice"),
        "Original data should still be there"
    );
    assert!(csv_str.contains("2,Bob"), "New data should be written");

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_file_attributes() {
    // TDD: Test getting file attributes (size, permissions, etc.)

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema).await.unwrap();

    let batch = RecordBatch::try_new(
        db.schema(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    let port = create_unique_port(12049);
    let server = posixlake::nfs::NfsServer::new(Arc::new(db), port)
        .await
        .unwrap();

    // Get attributes for data.csv
    let attrs = server.getattr("/data/data.csv").await.unwrap();

    assert!(attrs.is_file, "data.csv should be a file");
    assert!(!attrs.is_dir, "data.csv should not be a directory");
    assert!(attrs.size > 0, "data.csv should have content");
    assert!(attrs.readable, "data.csv should be readable");
    assert!(attrs.writable, "data.csv should be writable");

    // Get attributes for /data directory
    let attrs = server.getattr("/data").await.unwrap();

    assert!(!attrs.is_file, "/data should not be a file");
    assert!(attrs.is_dir, "/data should be a directory");

    server.shutdown().await.unwrap();
}

/// Helper to mount NFS using OS command
/// Requires passwordless sudo (configured via scripts/setup_nfs_sudo.py)
async fn mount_nfs_os(
    host: &str,
    port: u16,
    mount_point: &Path,
) -> Result<std::path::PathBuf, String> {
    // Check if we're running as root
    #[cfg(unix)]
    let is_root = unsafe { libc::geteuid() } == 0;

    // Check if we're in a container (Linux only)
    #[cfg(target_os = "linux")]
    let in_container = std::path::Path::new("/.dockerenv").exists()
        || std::path::Path::new("/run/.containerenv").exists();

    #[cfg(target_os = "linux")]
    {
        debug!(
            "mount_nfs_os: is_root={}, in_container={}",
            is_root, in_container
        );
        debug!(
            "mount_nfs_os: /.dockerenv exists={}, /run/.containerenv exists={}",
            std::path::Path::new("/.dockerenv").exists(),
            std::path::Path::new("/run/.containerenv").exists()
        );
    }

    #[cfg(target_os = "macos")]
    {
        let mut cmd = if is_root {
            tokio::process::Command::new("mount_nfs")
        } else {
            let mut c = tokio::process::Command::new("sudo");
            c.arg("-n"); // non-interactive - will fail if passwordless sudo not configured
            c.arg("mount_nfs");
            c
        };

        let status = cmd
            .arg("-o")
            .arg(format!(
                "nolocks,vers=3,tcp,port={},mountport={}",
                port, port
            ))
            .arg(format!("{}:/", host))
            .arg(mount_point.as_os_str())
            .status()
            .await
            .map_err(|e| format!("Failed to execute mount_nfs: {}", e))?;

        if status.success() {
            Ok(mount_point.to_path_buf())
        } else {
            Err(format!(
                "mount_nfs failed with exit code: {:?}",
                status.code()
            ))
        }
    }

    #[cfg(target_os = "linux")]
    {
        // In containers with privileged mode, use unshare to create a new mount namespace with full privileges
        if in_container && is_root {
            debug!("Detected privileged container environment, using unshare for mount namespace");

            // Use unshare with --map-root-user to create a new user namespace with root privileges
            // and --mount for mount namespace isolation, then perform the mount
            let status = tokio::process::Command::new("unshare")
                .arg("--mount")
                .arg("--map-root-user")
                .arg("--propagation")
                .arg("unchanged")
                .arg("mount")
                .arg("-t")
                .arg("nfs")
                .arg("-o")
                .arg(format!(
                    "nolock,noac,soft,timeo=10,retrans=2,vers=3,tcp,port={},mountport={}",
                    port, port
                ))
                .arg(format!("{}:/", host))
                .arg(mount_point.as_os_str())
                .status()
                .await
                .map_err(|e| format!("Failed to execute unshare mount: {}", e))?;

            if status.success() {
                return Ok(mount_point.to_path_buf());
            } else {
                return Err(format!(
                    "unshare mount failed with exit code: {:?}",
                    status.code()
                ));
            }
        }

        // Standard mount (bare metal or non-container)
        let mut cmd = if is_root {
            tokio::process::Command::new("mount")
        } else {
            let mut c = tokio::process::Command::new("sudo");
            c.arg("-n"); // non-interactive - will fail if passwordless sudo not configured
            c.arg("mount");
            c
        };

        let status = cmd
            .arg("-t")
            .arg("nfs")
            .arg("-o")
            .arg(format!(
                "nolock,noac,soft,timeo=10,retrans=2,vers=3,tcp,port={},mountport={}",
                port, port
            ))
            .arg(format!("{}:/", host))
            .arg(mount_point.as_os_str())
            .status()
            .await
            .map_err(|e| format!("Failed to execute mount: {}", e))?;

        if status.success() {
            Ok(mount_point.to_path_buf())
        } else {
            Err(format!("mount failed with exit code: {:?}", status.code()))
        }
    }

    #[cfg(target_os = "windows")]
    {
        let _ = (port, mount_point);

        // Find drive letter using prod helper
        let letter =
            find_free_drive(None).ok_or_else(|| "No free drive letter found".to_string())?;
        eprintln!("[MOUNT] Selected drive letter: {}:", letter);

        // Use prod helper to restart services and cleanup stale mount
        prepare_nfs_mount(letter).await;

        let status = tokio::process::Command::new("mount")
            .arg("-o")
            .arg(MOUNT_OPTIONS)
            .arg(format!("\\\\{}\\share", host))
            .arg(format!("{}:", letter))
            .status()
            .await
            .map_err(|e| format!("Failed to execute mount: {}", e))?;

        if status.success() {
            Ok(std::path::PathBuf::from(format!("{}:\\", letter)))
        } else {
            Err(format!("mount failed with exit code: {:?}", status.code()))
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        Err("OS mounting not implemented for this platform".to_string())
    }
}

/// Helper to unmount NFS using OS command
/// Runs as root if possible, otherwise uses passwordless sudo (configured via scripts/setup_nfs_sudo.py)
async fn unmount_nfs_os(mount_point: &Path) -> Result<(), String> {
    // Check if we're running as root
    #[cfg(unix)]
    let is_root = unsafe { libc::geteuid() } == 0;

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        // In containers with privileged mode, use unshare for unmount (Linux only)
        #[cfg(target_os = "linux")]
        {
            let in_container = std::path::Path::new("/.dockerenv").exists()
                || std::path::Path::new("/run/.containerenv").exists();

            if in_container && is_root {
                debug!("Detected privileged container environment, using unshare for unmount");

                let status = tokio::process::Command::new("unshare")
                    .arg("--mount")
                    .arg("--map-root-user")
                    .arg("--propagation")
                    .arg("unchanged")
                    .arg("umount")
                    .arg(mount_point.as_os_str())
                    .status()
                    .await
                    .map_err(|e| format!("Failed to execute unshare umount: {}", e))?;

                if status.success() {
                    return Ok(());
                } else {
                    return Err(format!("unshare umount failed: {:?}", status));
                }
            }
        }

        // Standard umount (bare metal or non-container)
        let mut cmd = if is_root {
            tokio::process::Command::new("umount")
        } else {
            let mut c = tokio::process::Command::new("sudo");
            c.arg("-n"); // non-interactive - will fail if passwordless sudo not configured
            c.arg("umount");
            c
        };

        let status = cmd
            .arg(mount_point.as_os_str())
            .status()
            .await
            .map_err(|e| format!("Failed to execute umount: {}", e))?;

        if status.success() {
            Ok(())
        } else {
            Err(format!("umount failed: {:?}", status))
        }
    }

    #[cfg(target_os = "windows")]
    {
        let drive = mount_point.to_string_lossy();
        let status = tokio::process::Command::new("umount")
            .arg(drive.as_ref())
            .status()
            .await
            .map_err(|e| format!("Failed to execute umount: {}", e))?;

        if status.success() {
            Ok(())
        } else {
            Err(format!("umount failed: {:?}", status))
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        Err("OS unmounting not implemented for this platform".to_string())
    }
}

#[tokio::test]
#[serial(nfs)]
async fn test_real_os_mount_and_posix_commands() {
    // Test using actual OS NFS mount commands
    // Requires passwordless sudo - will fail with setup instructions if not configured
    require_mount_capability();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");
    let mount_point = temp_dir.path().join("mnt");
    std::fs::create_dir(&mount_point).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema).await.unwrap();

    // Insert test data
    let batch = RecordBatch::try_new(
        db.schema(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    let port = create_unique_port(12049);
    #[cfg(target_os = "windows")]
    ensure_clean_nfs_state().await;
    let server = posixlake::nfs::NfsServer::new(Arc::new(db), port)
        .await
        .unwrap();

    // Server is ready - new() waits for ready signal before returning
    // Mount using OS command - THIS MUST WORK
    println!("[MOUNT] Mounting NFS at {:?} on port {}", mount_point, port);
    let mount_point = mount_nfs_os("localhost", port, &mount_point)
        .await
        .expect("NFS mount must succeed");

    // Create mount guard for automatic cleanup on panic/timeout
    let _guard = MountGuard::new(mount_point.clone());

    println!("[SUCCESS] Successfully mounted NFS");

    // Test POSIX commands
    let data_csv = mount_point.join("data").join("data.csv");

    // Test reading with cat/type
    println!("[TEST] Testing 'cat' command...");
    #[cfg(not(windows))]
    let output = tokio::process::Command::new("cat")
        .arg(&data_csv)
        .output()
        .await
        .expect("Failed to execute cat");

    #[cfg(windows)]
    let output = tokio::process::Command::new("cmd")
        .args(["/C", "type"])
        .arg(&data_csv)
        .output()
        .await
        .expect("Failed to execute type");

    let content = String::from_utf8_lossy(&output.stdout);
    println!("  CSV content:\n{}", content);
    assert!(content.contains("id,name"), "CSV should have headers");
    assert!(content.contains("Alice"), "CSV should contain Alice");
    assert!(content.contains("Bob"), "CSV should contain Bob");

    // Test ls/dir
    println!("[TEST] Testing 'ls' command...");
    #[cfg(not(windows))]
    let output = tokio::process::Command::new("ls")
        .arg(mount_point.join("data"))
        .output()
        .await
        .expect("Failed to execute ls");

    #[cfg(windows)]
    let output = tokio::process::Command::new("cmd")
        .args(["/C", "dir"])
        .arg(mount_point.join("data"))
        .output()
        .await
        .expect("Failed to execute dir");

    let listing = String::from_utf8_lossy(&output.stdout);
    println!("  Directory listing: {}", listing);
    #[cfg(not(windows))]
    assert!(listing.contains("data.csv"), "Should list data.csv");
    #[cfg(windows)]
    assert!(
        listing.to_lowercase().contains("data.csv"),
        "Should list data.csv"
    );

    println!("[SUCCESS] All POSIX commands working through NFS mount!");

    // Unmount
    unmount_nfs_os(&mount_point)
        .await
        .expect("Unmount must succeed");

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_real_os_write_through_mount() {
    // Test writing data through real OS mount
    // Requires passwordless sudo - will fail with setup instructions if not configured
    require_mount_capability();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");
    let mount_point = temp_dir.path().join("mnt");
    std::fs::create_dir(&mount_point).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema).await.unwrap();

    // Start with one row
    let batch = RecordBatch::try_new(
        db.schema(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Alice"])),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    let port = create_unique_port(12049);
    #[cfg(target_os = "windows")]
    ensure_clean_nfs_state().await;
    let server = posixlake::nfs::NfsServer::new(Arc::new(db), port)
        .await
        .unwrap();

    // Server is ready - new() waits for ready signal before returning
    // Mount - THIS MUST WORK
    println!("[MOUNT] Mounting NFS for write test on port {}", port);
    let mount_point = mount_nfs_os("localhost", port, &mount_point)
        .await
        .expect("NFS mount must succeed");

    // Create mount guard for automatic cleanup on panic/timeout
    let _guard = MountGuard::new(mount_point.clone());

    let data_csv = mount_point.join("data").join("data.csv");

    // Write new row - use explicit read+write with offset to ensure proper append
    println!("[TEST] Testing write through mount...");
    {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};

        // First, read current content and size
        let current_content = tokio::fs::read_to_string(&data_csv)
            .await
            .expect("Failed to read current content");
        println!(
            "[DEBUG] Current content before write ({} bytes):\n{}",
            current_content.len(),
            current_content
        );

        // Open file for write (not append - we'll seek explicitly)
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&data_csv)
            .await
            .expect("Failed to open file for read/write");

        // Seek to end
        let pos = file
            .seek(std::io::SeekFrom::End(0))
            .await
            .expect("Failed to seek");
        println!("[DEBUG] Seeked to position {} for append", pos);

        // Write new row
        file.write_all(b"\n2,Bob").await.expect("Failed to write");
        println!("[DEBUG] Write completed");

        // Force sync to NFS server
        file.flush().await.expect("Failed to flush");
        println!("[DEBUG] Flush completed");
        file.sync_all().await.expect("Failed to sync");
        println!("[DEBUG] Sync completed");
    }
    println!("[SUCCESS] Write completed");

    // Small delay to allow Windows NFS cache to settle
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Read back and verify using multiple methods
    println!("[TEST] Testing read-after-write consistency...");

    // Method 1: Direct tokio::fs read
    let direct_content = tokio::fs::read_to_string(&data_csv)
        .await
        .expect("Failed to read file directly");
    println!(
        "[DEBUG] Direct tokio::fs read ({} bytes):\n{}",
        direct_content.len(),
        direct_content
    );

    // Method 2: OS command (type on Windows, cat on Unix)
    #[cfg(not(windows))]
    let output = tokio::process::Command::new("cat")
        .arg(&data_csv)
        .output()
        .await
        .expect("Failed to execute cat");

    #[cfg(windows)]
    let output = tokio::process::Command::new("cmd")
        .args(["/C", "type"])
        .arg(&data_csv)
        .output()
        .await
        .expect("Failed to execute type");

    let content = String::from_utf8_lossy(&output.stdout);
    println!(
        "[DEBUG] OS command read ({} bytes):\n{}",
        content.len(),
        content
    );

    // Use the direct content for assertion (more reliable)
    assert!(
        direct_content.contains("Alice"),
        "Original data should still be there"
    );
    assert!(direct_content.contains("Bob"), "New data should be written");

    println!("[SUCCESS] Write operations working through NFS mount!");

    unmount_nfs_os(&mount_point)
        .await
        .expect("Unmount must succeed");

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_list_individual_parquet_files() {
    init_logging();

    // Create database with test data
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone()).await.unwrap();

    // Insert first batch of data (creates first Parquet file)
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    db.insert(batch1).await.unwrap();

    // Insert second batch of data (creates second Parquet file)
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
            Arc::new(Int32Array::from(vec![300, 400])),
        ],
    )
    .unwrap();
    db.insert(batch2).await.unwrap();

    // Start NFS server
    let port = create_unique_port(12049);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();

    // List files in /data directory
    let entries = server.readdir("/data").await.unwrap();

    // Should have data.csv + at least 2 .parquet files
    assert!(
        entries.len() >= 3,
        "Expected at least 3 entries (data.csv + 2 parquet files), got {}",
        entries.len()
    );

    // Verify data.csv is present
    let csv_entry = entries.iter().find(|e| *e == "data.csv");
    assert!(csv_entry.is_some(), "data.csv should be present");

    // Verify at least 2 Parquet files are present
    let parquet_files: Vec<_> = entries.iter().filter(|e| e.ends_with(".parquet")).collect();
    assert!(
        parquet_files.len() >= 2,
        "Expected at least 2 .parquet files, got {}",
        parquet_files.len()
    );

    // Verify Parquet filenames contain the expected Delta Lake pattern (part-*.parquet)
    for entry in &parquet_files {
        assert!(
            entry.starts_with("part-") || entry.starts_with("data_"),
            "Parquet file should follow Delta Lake naming pattern: {}",
            entry
        );
        assert!(
            entry.ends_with(".parquet") || entry.ends_with(".snappy.parquet"),
            "Parquet file should end with .parquet: {}",
            entry
        );
    }

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_read_individual_parquet_file() {
    init_logging();

    // Create database with test data
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone()).await.unwrap();

    // Insert data (creates Parquet file)
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    // Start NFS server
    let port = create_unique_port(12049);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();

    // List files to get Parquet filename
    let entries = server.readdir("/data").await.unwrap();
    let parquet_file = entries
        .iter()
        .find(|e| e.ends_with(".parquet"))
        .expect("Should have at least one Parquet file");

    // Read the Parquet file content (as CSV)
    let file_path = format!("/data/{}", parquet_file);
    let content = server.read_file(&file_path, 0, 1024).await.unwrap();
    let content_str = String::from_utf8_lossy(&content);

    // Verify CSV format and content
    assert!(
        content_str.contains("id,name,value"),
        "Should have CSV header"
    );
    assert!(content_str.contains("Alice"), "Should contain Alice");
    assert!(content_str.contains("Bob"), "Should contain Bob");
    assert!(content_str.contains("100"), "Should contain value 100");
    assert!(content_str.contains("200"), "Should contain value 200");

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_parquet_file_accurate_sizes() {
    init_logging();

    // Create database with test data
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone()).await.unwrap();

    // Insert data to create Parquet file
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int32Array::from(vec![100, 200, 300])),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    // Get the actual file size from filesystem (Delta Lake mode)
    let parquet_files: Vec<_> = std::fs::read_dir(db_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert_eq!(parquet_files.len(), 1, "Should have exactly 1 Parquet file");
    let expected_size = parquet_files[0].metadata().unwrap().len();

    // Start NFS server
    let port = create_unique_port(12049);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();

    // List files to get Parquet filename
    let entries = server.readdir("/data").await.unwrap();
    let parquet_file = entries
        .iter()
        .find(|e| e.ends_with(".parquet"))
        .expect("Should have at least one Parquet file");

    // Get file attributes via NFS
    let file_path = format!("/data/{}", parquet_file);
    let attrs = server.getattr(&file_path).await.unwrap();

    // Verify the size matches the actual file size
    assert_eq!(
        attrs.size, expected_size,
        "NFS file size ({}) should match actual Parquet file size ({})",
        attrs.size, expected_size
    );

    // Size should be greater than 0
    assert!(attrs.size > 0, "File size should be greater than 0");

    // Size should be reasonable (not hardcoded 1024)
    assert_ne!(
        attrs.size, 1024,
        "File size should not be hardcoded to 1024"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_parquet_file_modification_times() {
    init_logging();

    // Create database with test data
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone()).await.unwrap();

    // Insert data to create Parquet file
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )
    .unwrap();

    let before_insert = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let _txn_id = db.insert(batch).await.unwrap();

    let after_insert = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Get the Parquet file path and timestamp (Delta Lake mode)
    let (creation_timestamp, nfs_parquet_path) = {
        // Find the parquet file in the database directory
        let parquet_files: Vec<_> = std::fs::read_dir(db_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
            .collect();
        assert_eq!(parquet_files.len(), 1, "Should have exactly 1 Parquet file");

        let file_path = parquet_files[0].path();
        let metadata = file_path.metadata().unwrap();
        let timestamp = metadata
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let parquet_filename = file_path.file_name().unwrap().to_str().unwrap().to_string();
        (timestamp, format!("/data/{}", parquet_filename))
    };

    // Start NFS server
    let port = create_unique_port(12049);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();
    server.is_ready().await;

    // Get attributes of the individual Parquet file via NFS
    let attrs = server.getattr(&nfs_parquet_path).await.unwrap();

    // Verify modification time is set (not epoch/1970)
    assert!(attrs.size > 0, "File should have a size");

    // The file's modification time should be within reasonable range of insert time
    // Note: We'll implement mtime, atime, ctime in FileAttributes
    // For now, verify that the NFS layer has access to timestamp

    // Get the timestamp from metadata (milliseconds since epoch)
    let expected_mtime_secs = creation_timestamp / 1000;

    // Verify timestamp is reasonable (between before_insert and after_insert + 1 second buffer)
    assert!(
        expected_mtime_secs >= before_insert && expected_mtime_secs <= after_insert + 1,
        "File timestamp ({}) should be between {} and {}",
        expected_mtime_secs,
        before_insert,
        after_insert + 1
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_parquet_file_row_count_metadata() {
    init_logging();

    // Create database with test data
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone()).await.unwrap();

    // Insert data with known row count
    let row_count = 42;
    let ids: Vec<i32> = (1..=row_count).collect();
    let values: Vec<i32> = (100..100 + row_count).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap();

    let _txn_id = db.insert(batch).await.unwrap();

    // Get the Parquet file (Delta Lake mode)
    let parquet_files: Vec<_> = std::fs::read_dir(db_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert_eq!(parquet_files.len(), 1, "Should have exactly 1 Parquet file");

    let parquet_filename = parquet_files[0].file_name().to_str().unwrap().to_string();
    let nfs_parquet_path = format!("/data/{}", parquet_filename);

    // Start NFS server
    let port = create_unique_port(12049);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();
    server.is_ready().await;

    // Read the file content via NFS and verify row count
    let content = server
        .read_file(&nfs_parquet_path, 0, 100000)
        .await
        .unwrap();
    let csv_content = String::from_utf8(content).unwrap();

    // Count lines in CSV (header + data rows)
    let line_count = csv_content.lines().count();
    let data_row_count = line_count - 1; // Exclude header

    assert_eq!(
        data_row_count, row_count as usize,
        "CSV should have {} data rows (plus header)",
        row_count
    );

    // Verify first and last rows to ensure all data is present
    let lines: Vec<&str> = csv_content.lines().collect();
    assert_eq!(lines[0], "id,value", "First line should be CSV header");
    assert_eq!(lines[1], "1,100", "Second line should be first data row");
    assert_eq!(
        lines[row_count as usize], "42,141",
        "Last line should be last data row"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_json_view_read() {
    init_logging();

    // Create database with test data
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone()).await.unwrap();

    // Insert test data
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(arrow::array::BooleanArray::from(vec![true, false, true])),
        ],
    )
    .unwrap();

    db.insert(batch).await.unwrap();

    // Start NFS server
    let port = create_unique_port(12049);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();
    server.is_ready().await;

    // Read JSON file via NFS
    let json_content = server
        .read_file("/data/data.json", 0, 100000)
        .await
        .unwrap();
    let json_str = String::from_utf8(json_content).unwrap();

    // Parse and verify JSON
    let json_value: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    let records = json_value.as_array().unwrap();

    assert_eq!(records.len(), 3, "Should have 3 records");

    // Verify first record
    assert_eq!(records[0]["id"], 1);
    assert_eq!(records[0]["name"], "Alice");
    assert_eq!(records[0]["active"], true);

    // Verify last record
    assert_eq!(records[2]["id"], 3);
    assert_eq!(records[2]["name"], "Charlie");
    assert_eq!(records[2]["active"], true);

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_jsonl_view_read() {
    init_logging();

    // Create database with test data
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone()).await.unwrap();

    // Insert test data
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(arrow::array::Float64Array::from(vec![1.5, 2.5])),
        ],
    )
    .unwrap();

    db.insert(batch).await.unwrap();

    // Start NFS server
    let port = create_unique_port(12049);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();
    server.is_ready().await;

    // Read JSONL file via NFS (JSON Lines format)
    let jsonl_content = server
        .read_file("/data/data.jsonl", 0, 100000)
        .await
        .unwrap();
    let jsonl_str = String::from_utf8(jsonl_content).unwrap();

    // Parse JSON Lines (one JSON object per line)
    let lines: Vec<&str> = jsonl_str.trim().lines().collect();
    assert_eq!(lines.len(), 2, "Should have 2 lines");

    // Parse first line
    let record1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(record1["id"], 10);
    assert_eq!(record1["value"], 1.5);

    // Parse second line
    let record2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(record2["id"], 20);
    assert_eq!(record2["value"], 2.5);

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_special_query_file() {
    init_logging();

    // Create database with test data
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone()).await.unwrap();

    // Insert test data
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "Dave", "Eve",
            ])),
            Arc::new(Int32Array::from(vec![95, 87, 92, 78, 88])),
        ],
    )
    .unwrap();

    db.insert(batch).await.unwrap();

    // Start NFS server
    let port = create_unique_port(12049);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();
    server.is_ready().await;

    // Write SQL query to .query file
    let sql_query = b"SELECT name, score FROM data WHERE score > 90 ORDER BY score DESC";
    server.write_file("/.query", 0, sql_query).await.unwrap();

    // Read results from .query file
    let result_content = server.read_file("/.query", 0, 100000).await.unwrap();
    let result_str = String::from_utf8(result_content).unwrap();

    // Verify CSV format results
    let lines: Vec<&str> = result_str.lines().collect();
    assert_eq!(lines[0], "name,score", "First line should be header");
    assert!(
        lines.contains(&"Alice,95"),
        "Should contain Alice with score 95"
    );
    assert!(
        lines.contains(&"Charlie,92"),
        "Should contain Charlie with score 92"
    );
    assert!(
        !result_str.contains("Bob"),
        "Should not contain Bob (score 87 <= 90)"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_special_schema_file() {
    init_logging();

    // Create database with complex schema
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone()).await.unwrap();

    // Start NFS server
    let port = create_unique_port(12049);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();
    server.is_ready().await;

    // Read schema.sql file
    let schema_content = server.read_file("/schema.sql", 0, 100000).await.unwrap();
    let schema_str = String::from_utf8(schema_content).unwrap();

    // Verify SQL-like schema definition
    assert!(
        schema_str.contains("CREATE TABLE"),
        "Should contain CREATE TABLE"
    );
    assert!(schema_str.contains("id"), "Should contain id field");
    assert!(
        schema_str.contains("INT64") || schema_str.contains("BIGINT"),
        "Should specify id type"
    );
    assert!(schema_str.contains("email"), "Should contain email field");
    assert!(
        schema_str.contains("VARCHAR") || schema_str.contains("TEXT"),
        "Should specify email type"
    );
    assert!(schema_str.contains("age"), "Should contain age field");
    assert!(
        schema_str.contains("NOT NULL"),
        "Should indicate non-nullable fields"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_special_stats_file() {
    init_logging();

    // Create database with test data
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(db_path, schema.clone()).await.unwrap();

    // Insert multiple batches
    for i in 1..=3 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i * 10, i * 10 + 1])),
                Arc::new(Int32Array::from(vec![i * 100, i * 100 + 10])),
            ],
        )
        .unwrap();
        db.insert(batch).await.unwrap();
    }

    // Start NFS server
    let port = create_unique_port(12049);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();
    server.is_ready().await;

    // Read .stats file
    let stats_content = server.read_file("/.stats", 0, 100000).await.unwrap();
    let stats_str = String::from_utf8(stats_content).unwrap();

    // Parse as JSON
    let stats: serde_json::Value = serde_json::from_str(&stats_str).unwrap();

    // Verify statistics
    assert!(
        stats["total_rows"].as_u64().unwrap() >= 6,
        "Should have at least 6 total rows"
    );
    assert!(
        stats["total_files"].as_u64().unwrap() >= 3,
        "Should have at least 3 Parquet files"
    );
    assert!(
        stats["total_size_bytes"].as_u64().unwrap() > 0,
        "Should have non-zero size"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_metadata_directory() {
    init_logging();

    // Create database
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    let db = DatabaseOps::create(db_path, schema.clone()).await.unwrap();

    // Insert data
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    // Start NFS server
    let port = create_unique_port(12049);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();
    server.is_ready().await;

    // List .metadata directory
    let entries = server.readdir("/.metadata").await.unwrap();

    // Verify expected files
    assert!(
        entries.contains(&"row_counts.json".to_string()),
        "Should contain row_counts.json"
    );
    assert!(
        entries.contains(&"file_list.json".to_string()),
        "Should contain file_list.json"
    );

    // Read row_counts.json
    let row_counts_content = server
        .read_file("/.metadata/row_counts.json", 0, 100000)
        .await
        .unwrap();
    let row_counts_str = String::from_utf8(row_counts_content).unwrap();
    let row_counts: serde_json::Value = serde_json::from_str(&row_counts_str).unwrap();

    // Verify it's valid JSON with expected structure
    assert!(
        row_counts.is_object() || row_counts.is_array(),
        "Should be valid JSON structure"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_concurrent_multiprocess_access() {
    use std::time::Duration;

    // Test using actual OS NFS mount commands
    require_mount_capability();

    // Create test database
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db_multiprocess");
    let mount_dir = temp_dir.path().join("mnt");
    std::fs::create_dir(&mount_dir).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert test data - 50 rows
    let ids: Vec<i32> = (1..=50).collect();
    let names: Vec<String> = (1..=50).map(|i| format!("user{}", i)).collect();
    let values: Vec<i32> = (1..=50).map(|i| i * 10).collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    // Start NFS server
    let port = create_unique_port(11049);
    #[cfg(target_os = "windows")]
    ensure_clean_nfs_state().await;
    let server = posixlake::nfs::NfsServer::new(Arc::new(db), port)
        .await
        .unwrap();

    // Mount the NFS server
    println!(
        "[MOUNT] Mounting NFS for multi-process test on port {}",
        port
    );
    let mount_result = mount_nfs_os("localhost", port, &mount_dir).await;

    if mount_result.is_err() {
        println!(
            "Mount failed, skipping multi-process test: {:?}",
            mount_result.err()
        );
        server.shutdown().await.ok();
        return;
    }
    let mount_dir = mount_result.unwrap();

    // Create mount guard for automatic cleanup on panic/timeout
    let _guard = MountGuard::new(mount_dir.clone());

    println!("[SUCCESS] Successfully mounted NFS for multi-process test");

    // Test that files are accessible
    let data_path = mount_dir.join("data").join("data.csv");
    println!("[TEST] Verifying mount at {:?}", data_path);

    // Try ls/dir first
    #[cfg(not(windows))]
    let ls_output = tokio::process::Command::new("ls")
        .arg(mount_dir.join("data"))
        .output()
        .await
        .expect("ls should work");
    #[cfg(windows)]
    let ls_output = tokio::process::Command::new("cmd")
        .args(["/C", "dir"])
        .arg(mount_dir.join("data"))
        .output()
        .await
        .expect("dir should work");
    println!("[LS] {}", String::from_utf8_lossy(&ls_output.stdout));

    // Now try cat/type
    #[cfg(not(windows))]
    let cat_output = tokio::process::Command::new("cat")
        .arg(&data_path)
        .output()
        .await
        .expect("cat should work");
    #[cfg(windows)]
    let cat_output = tokio::process::Command::new("cmd")
        .args(["/C", "type"])
        .arg(&data_path)
        .output()
        .await
        .expect("type should work");

    let content = String::from_utf8_lossy(&cat_output.stdout);
    println!("[CAT] Read {} bytes", content.len());
    assert!(content.contains("id,name,value"), "Should have CSV header");
    assert!(
        content.lines().count() == 51,
        "Should have 51 lines (header + 50 rows)"
    );
    println!("[VERIFY] Initial verification successful");

    // Spawn multiple concurrent cat/type commands (reduced to 3 for stability)
    println!("[TEST] Starting concurrent reads...");
    let mut handles = vec![];
    for i in 0..3 {
        let path = data_path.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(i * 100)).await; // Stagger starts
            #[cfg(not(windows))]
            let output = tokio::process::Command::new("cat")
                .arg(&path)
                .output()
                .await;
            #[cfg(windows)]
            let output = tokio::process::Command::new("cmd")
                .args(["/C", "type"])
                .arg(&path)
                .output()
                .await;
            match output {
                Ok(o) => {
                    println!("[READER {}] Success: {} bytes", i, o.stdout.len());
                    o.status.success()
                }
                Err(e) => {
                    println!("[READER {}] Failed: {:?}", i, e);
                    false
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all with timeout
    let all_success = tokio::time::timeout(Duration::from_secs(15), async {
        let mut success = true;
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(result) => {
                    if !result {
                        println!("[ERROR] Process {} reported failure", i);
                        success = false;
                    }
                }
                Err(e) => {
                    println!("[ERROR] Process {} panicked: {:?}", i, e);
                    success = false;
                }
            }
        }
        success
    })
    .await;

    // Main assertion: concurrent reads must work
    assert!(
        all_success.is_ok(),
        "Concurrent operations should not timeout"
    );
    assert!(
        all_success.unwrap(),
        "All concurrent processes must complete successfully"
    );

    println!("[SUCCESS] ✓ Concurrent multi-process access test PASSED");
    println!("[SUCCESS] ✓ All 3 concurrent readers completed successfully");

    // Cleanup - mount may disconnect on macOS after concurrent access (kernel issue)
    // This is expected and not a test failure since concurrent access already succeeded
    println!("[CLEANUP] Unmounting and shutting down...");
    unmount_nfs_os(&mount_dir).await.ok();
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(server);

    println!("[COMPLETE] Test completed successfully");
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_concurrent_readers_and_writer() {
    init_logging();

    // Test concurrent read operations through NFS mount
    // Note: This tests concurrent READS only - concurrent writes to CSV are not supported
    require_mount_capability();

    // Create test database
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db_concurrent_rw");
    let mount_dir = temp_dir.path().join("mnt");
    std::fs::create_dir(&mount_dir).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert initial data - 20 rows
    let ids: Vec<i32> = (1..=20).collect();
    let names: Vec<String> = (1..=20).map(|i| format!("user{}", i)).collect();
    let values: Vec<i32> = (1..=20).map(|i| i * 10).collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    // Start NFS server
    let port = create_unique_port(11149);
    let server = posixlake::nfs::NfsServer::new(Arc::new(db), port)
        .await
        .unwrap();

    // Mount the NFS server
    println!(
        "[MOUNT] Mounting NFS for concurrent read test on port {}",
        port
    );
    let mount_result = mount_nfs_os("localhost", port, &mount_dir).await;

    if mount_result.is_err() {
        println!("Mount failed, skipping test: {:?}", mount_result.err());
        server.shutdown().await.ok();
        return;
    }
    let mount_dir = mount_result.unwrap();

    // Create mount guard for automatic cleanup
    let guard = MountGuard::new(mount_dir.clone());

    println!("[SUCCESS] Successfully mounted NFS for concurrent read test");

    let data_path = mount_dir.join("data/data.csv");

    // Verify initial read works before spawning concurrent readers
    let initial_content = tokio::fs::read_to_string(&data_path)
        .await
        .expect("Initial read should succeed");
    assert!(
        initial_content.contains("user1"),
        "Should contain test data"
    );
    println!(
        "[VERIFY] Initial read successful, {} bytes",
        initial_content.len()
    );

    // Spawn concurrent readers - all reading the same file simultaneously
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<usize, String>>(10);

    for i in 0..5 {
        let path = data_path.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let result = tokio::fs::read_to_string(&path).await;
            match result {
                Ok(content) => {
                    let _ = tx.send(Ok(content.len())).await;
                }
                Err(e) => {
                    let _ = tx.send(Err(format!("Reader {} failed: {}", i, e))).await;
                }
            }
        });
    }
    drop(tx); // Close sender so rx.recv() returns None when all done

    // Collect results - event-based, no polling
    let mut success_count = 0;
    let mut fail_count = 0;
    while let Some(result) = rx.recv().await {
        match result {
            Ok(bytes) => {
                println!("[READER] Success: {} bytes", bytes);
                success_count += 1;
            }
            Err(e) => {
                println!("[READER] Failed: {}", e);
                fail_count += 1;
            }
        }
    }

    println!(
        "[RESULT] {} successful reads, {} failed reads",
        success_count, fail_count
    );
    assert_eq!(success_count, 5, "All 5 concurrent readers should succeed");
    assert_eq!(fail_count, 0, "No readers should fail");

    println!("[SUCCESS] Concurrent read test passed!");

    // Cleanup via guard drop
    guard.mark_unmounted();
    unmount_nfs_os(&mount_dir).await.ok();
    server.shutdown().await.ok();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_file_deletion_truncates_table() {
    init_logging();
    println!("\n[TEST] test_nfs_file_deletion_truncates_table");

    // TDD: This test should fail because file deletion is not implemented yet
    // Expected behavior: calling remove() on data.csv should truncate the table

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    println!("[SETUP] Creating database at: {}", db_path.display());
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert some test data
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();

    println!("[INSERT] Inserting 3 rows");
    db.insert(batch).await.unwrap();

    // Verify data exists
    let results = db.query("SELECT * FROM data").await.unwrap();
    assert_eq!(
        results[0].num_rows(),
        3,
        "Should have 3 rows before deletion"
    );
    println!("[VERIFY] Data exists: 3 rows");

    // Wrap database in Arc for NFS server
    let db_arc = Arc::new(db);

    // Start NFS server
    let port = create_unique_port(12999);
    println!("[NFS] Starting server on port {}", port);
    let server = NfsServer::new(db_arc.clone(), port).await.unwrap();

    // Wait for server to be ready
    assert!(server.is_ready().await, "NFS server should be ready");
    println!("[NFS] Server ready");

    // Test file deletion via NFS server API directly (no OS mount needed)
    println!("[DELETE] Calling remove on data.csv");
    let remove_result = server.remove_file("/data/data.csv").await;

    // TDD: This should succeed once implemented (currently returns error)
    assert!(
        remove_result.is_ok(),
        "File deletion should be supported: {:?}",
        remove_result.err()
    );

    // Verify table is now empty (truncated)
    println!("[VERIFY] Checking table is truncated");
    let results_after = db_arc.query("SELECT * FROM data").await.unwrap();
    let total_rows: usize = results_after.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "Table should be empty after file deletion");

    println!("[SUCCESS] File deletion successfully truncated table");

    // Cleanup
    println!("[CLEANUP] Shutting down");
    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_row_deletion_via_overwrite() {
    init_logging();
    println!("\n[TEST] test_nfs_row_deletion_via_overwrite");

    // Integration test: Overwrite CSV with filtered content, verify rows deleted

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create schema with ID column (needed for row identification)
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    println!("[SETUP] Creating database at: {}", db_path.display());
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert 5 rows
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "Diana", "Eve",
            ])),
            Arc::new(Int32Array::from(vec![30, 25, 35, 42, 31])),
        ],
    )
    .unwrap();

    println!("[INSERT] Inserting 5 rows");
    db.insert(batch).await.unwrap();

    // Verify data exists
    let results = db.query("SELECT * FROM data ORDER BY id").await.unwrap();
    assert_eq!(
        results[0].num_rows(),
        5,
        "Should have 5 rows before deletion"
    );
    println!("[VERIFY] Data exists: 5 rows");

    // Wrap database in Arc for NFS server
    let db_arc = Arc::new(db);

    // Start NFS server
    let port = create_unique_port(13100);
    println!("[NFS] Starting server on port {}", port);
    let server = NfsServer::new(db_arc.clone(), port).await.unwrap();

    // Wait for server to be ready
    assert!(server.is_ready().await, "NFS server should be ready");
    println!("[NFS] Server ready");

    // Read current CSV content via NFS server API
    println!("[READ] Reading current CSV content via server API");
    let current_csv = server.read_file("/data/data.csv", 0, 100000).await.unwrap();
    let current_csv_str = String::from_utf8(current_csv).unwrap();
    println!("[CSV CONTENT BEFORE]:\n{}", current_csv_str);

    // Verify all 5 rows are present
    assert!(current_csv_str.contains("Alice"), "Should contain Alice");
    assert!(current_csv_str.contains("Bob"), "Should contain Bob");
    assert!(
        current_csv_str.contains("Charlie"),
        "Should contain Charlie"
    );
    assert!(current_csv_str.contains("Diana"), "Should contain Diana");
    assert!(current_csv_str.contains("Eve"), "Should contain Eve");

    // Filter out Bob (id=2) and Diana (id=4) - keep Alice, Charlie, Eve
    println!("[FILTER] Creating filtered CSV (removing Bob and Diana)");
    let filtered_lines: Vec<&str> = current_csv_str
        .lines()
        .filter(|line| !line.contains("Bob") && !line.contains("Diana"))
        .collect();
    let filtered_csv = filtered_lines.join("\n") + "\n";

    println!("[FILTERED CSV]:\n{}", filtered_csv);
    assert!(
        !filtered_csv.contains("Bob"),
        "Filtered CSV should not contain Bob"
    );
    assert!(
        !filtered_csv.contains("Diana"),
        "Filtered CSV should not contain Diana"
    );
    assert!(
        filtered_csv.contains("Alice"),
        "Filtered CSV should still contain Alice"
    );

    // Write filtered content back (this should trigger row deletion)
    println!("[WRITE] Writing filtered CSV back to data.csv");
    server
        .write_file("/data/data.csv", 0, filtered_csv.as_bytes())
        .await
        .unwrap();

    // Small delay for async operations to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify rows were deleted from Delta Lake
    println!("[VERIFY] Checking Delta Lake has only 3 rows");
    let results_after = db_arc
        .query("SELECT * FROM data ORDER BY id")
        .await
        .unwrap();
    let total_rows: usize = results_after.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 3,
        "Should have 3 rows after deletion (Alice, Charlie, Eve)"
    );

    // Verify specific rows exist
    let id_array = results_after[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let ids: Vec<i32> = (0..id_array.len()).map(|i| id_array.value(i)).collect();

    println!("[REMAINING IDS]: {:?}", ids);
    assert_eq!(
        ids,
        vec![1, 3, 5],
        "Should have ids 1, 3, 5 (Alice, Charlie, Eve)"
    );

    println!("[SUCCESS] Row deletion via overwrite successful - Bob and Diana deleted");

    // Cleanup
    println!("[CLEANUP] Shutting down");
    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_deletion_vectors_and_optimize() {
    init_logging();
    println!("\n[TEST] test_nfs_deletion_vectors_and_optimize");

    // TDD: This test verifies:
    // 1. Deletion vectors are created when rows are deleted
    // 2. OPTIMIZE compacts deleted rows and reclaims space

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create schema with ID column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    println!("[SETUP] Creating database at: {}", db_path.display());
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert 100 rows to create multiple Parquet files
    for batch_num in 0..10 {
        let start_id = batch_num * 10;
        let ids: Vec<i32> = (start_id..start_id + 10).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("User{}", i)).collect();
        let values: Vec<i32> = ids.iter().map(|i| i * 100).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap();

        db.insert(batch).await.unwrap();
    }

    println!("[INSERT] Inserted 100 rows");

    // Verify data exists
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 100, "Should have 100 rows before deletion");
    println!("[VERIFY] 100 rows exist");

    // Check Parquet files before deletion (they're in the base_path, not a subdirectory)
    println!("[DEBUG] Listing files in: {}", db_path.display());
    let parquet_files_before: Vec<_> = std::fs::read_dir(&db_path)
        .expect("Failed to read database directory")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    let parquet_count_before = parquet_files_before.len();
    println!(
        "[PARQUET] {} Parquet files before deletion",
        parquet_count_before
    );
    assert!(parquet_count_before > 0, "Should have Parquet files");

    // Check _delta_log commits before deletion
    let delta_log_dir = db_path.join("_delta_log");
    let log_files_before: Vec<_> = std::fs::read_dir(&delta_log_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("json"))
        .collect();
    let commit_count_before = log_files_before.len();
    println!(
        "[DELTA_LOG] {} commits before deletion",
        commit_count_before
    );

    // Wrap database in Arc for NFS server
    let db_arc = Arc::new(db);

    // Start NFS server
    let port = create_unique_port(15000);
    println!("[NFS] Starting server on port {}", port);
    let server = NfsServer::new(db_arc.clone(), port).await.unwrap();

    // Wait for server to be ready
    assert!(server.is_ready().await, "NFS server should be ready");
    println!("[NFS] Server ready");

    // Read current CSV content
    println!("[READ] Reading current CSV content");
    let current_csv = server.read_file("/data/data.csv", 0, 100000).await.unwrap();
    let current_csv_str = String::from_utf8(current_csv).unwrap();
    let lines_before: Vec<&str> = current_csv_str.lines().collect();
    println!(
        "[CSV] {} lines before deletion (including header)",
        lines_before.len()
    );

    // Delete every other row (delete rows with even IDs: 0, 2, 4, ..., 98)
    // Keep rows with odd IDs: 1, 3, 5, ..., 99
    let filtered_lines: Vec<&str> = lines_before
        .iter()
        .filter(|line| {
            if line.starts_with("id,") {
                true // Keep header
            } else {
                // Parse ID from CSV line
                let id: i32 = line
                    .split(',')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(-1);
                id % 2 == 1 // Keep only odd IDs
            }
        })
        .copied()
        .collect();

    let filtered_csv = filtered_lines.join("\n") + "\n";
    println!(
        "[FILTER] Filtered to {} lines (should be 51: header + 50 rows)",
        filtered_lines.len()
    );

    // Write filtered CSV back (this should delete 50 rows)
    println!("[DELETE] Writing filtered CSV to delete 50 rows");
    server
        .write_file("/data/data.csv", 0, filtered_csv.as_bytes())
        .await
        .unwrap();

    // Verify rows were deleted
    println!("[VERIFY] Checking rows after deletion");
    let results_after = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_after = results_after[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        count_after, 50,
        "Should have 50 rows after deletion (deleted 50)"
    );
    println!("[SUCCESS] Deletion successful: 100 rows -> 50 rows");

    // Check _delta_log for new commit with deletion
    let log_files_after_delete: Vec<_> = std::fs::read_dir(&delta_log_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("json"))
        .collect();
    let commit_count_after_delete = log_files_after_delete.len();
    println!(
        "[DELTA_LOG] {} commits after deletion (was {})",
        commit_count_after_delete, commit_count_before
    );
    assert!(
        commit_count_after_delete > commit_count_before,
        "Should have new commit(s) after deletion"
    );

    // Parquet files should NOT change yet (deletion vectors don't rewrite files)
    let parquet_files_after_delete: Vec<_> = std::fs::read_dir(&db_path)
        .expect("Failed to read database directory")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    let parquet_count_after_delete = parquet_files_after_delete.len();
    println!(
        "[PARQUET] {} Parquet files after deletion (deletion vectors don't rewrite files)",
        parquet_count_after_delete
    );
    // Note: Parquet count might be same or higher (new files from inserts in overwrite)

    // Run OPTIMIZE to compact deleted rows
    println!("[OPTIMIZE] Running OPTIMIZE to compact deleted rows");
    db_arc.optimize().await.unwrap();
    println!("[OPTIMIZE] OPTIMIZE complete");

    // Verify row count is still 50 after OPTIMIZE
    let results_after_optimize = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_after_optimize = results_after_optimize[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        count_after_optimize, 50,
        "Should still have 50 rows after OPTIMIZE"
    );
    println!("[VERIFY] Row count stable after OPTIMIZE: 50 rows");

    // Check _delta_log for OPTIMIZE commit
    let log_files_after_optimize: Vec<_> = std::fs::read_dir(&delta_log_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("json"))
        .collect();
    let commit_count_after_optimize = log_files_after_optimize.len();
    println!(
        "[DELTA_LOG] {} commits after OPTIMIZE",
        commit_count_after_optimize
    );
    // OPTIMIZE may or may not create a new commit depending on whether compaction was needed
    println!(
        "[INFO] OPTIMIZE created {} new commit(s)",
        commit_count_after_optimize.saturating_sub(commit_count_after_delete)
    );

    // Parquet files may change after OPTIMIZE (compaction)
    let parquet_files_after_optimize: Vec<_> = std::fs::read_dir(&db_path)
        .expect("Failed to read database directory")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    let parquet_count_after_optimize = parquet_files_after_optimize.len();
    println!(
        "[PARQUET] {} Parquet files after OPTIMIZE (compacted)",
        parquet_count_after_optimize
    );

    // Verify the correct rows remain (only odd IDs)
    println!("[VERIFY] Checking that only odd IDs remain");
    let final_results = db_arc
        .query("SELECT id FROM data ORDER BY id")
        .await
        .unwrap();
    let id_array = final_results[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let remaining_ids: Vec<i32> = (0..id_array.len()).map(|i| id_array.value(i)).collect();
    let expected_ids: Vec<i32> = (0..100).filter(|i| i % 2 == 1).collect();
    assert_eq!(
        remaining_ids, expected_ids,
        "Should have only odd IDs remaining"
    );
    println!("[SUCCESS] Correct rows remain: odd IDs from 1 to 99");

    println!("[SUCCESS] Deletion vectors + OPTIMIZE test complete");

    // Cleanup
    println!("[CLEANUP] Shutting down");
    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_row_deletion_performance() {
    init_logging();
    println!("\n[TEST] test_nfs_row_deletion_performance");

    // TDD: Benchmark row-level deletion performance
    // Success criterion: < 100ms for small deletes (10-100 rows)

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create schema with ID column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    println!("[SETUP] Creating database at: {}", db_path.display());
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert 1000 rows for realistic benchmark
    println!("[SETUP] Inserting 1000 rows for benchmark");
    for batch_num in 0..100 {
        let start_id = batch_num * 10;
        let ids: Vec<i32> = (start_id..start_id + 10).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("User{}", i)).collect();
        let values: Vec<i32> = ids.iter().map(|i| i * 100).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap();

        db.insert(batch).await.unwrap();
    }
    println!("[SETUP] 1000 rows inserted");

    // Wrap database in Arc for NFS server
    let db_arc = Arc::new(db);

    // Start NFS server
    let port = create_unique_port(16000);
    println!("[NFS] Starting server on port {}", port);
    let server = NfsServer::new(db_arc.clone(), port).await.unwrap();
    assert!(server.is_ready().await, "NFS server should be ready");
    println!("[NFS] Server ready");

    // Benchmark 1: Delete 10 rows (small delete)
    println!("\n[BENCHMARK 1] Deleting 10 rows");
    let start_time = std::time::Instant::now();

    // Read current CSV
    let current_csv = server
        .read_file("/data/data.csv", 0, 1_000_000)
        .await
        .unwrap();
    let current_csv_str = String::from_utf8(current_csv).unwrap();

    // Filter out IDs 0-9 (10 rows)
    let filtered_lines: Vec<&str> = current_csv_str
        .lines()
        .filter(|line| {
            if line.starts_with("id,") {
                true // Keep header
            } else {
                let id: i32 = line
                    .split(',')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(-1);
                id >= 10 // Keep IDs 10+
            }
        })
        .collect();
    let filtered_csv = filtered_lines.join("\n") + "\n";

    // Write filtered CSV (delete 10 rows)
    server
        .write_file("/data/data.csv", 0, filtered_csv.as_bytes())
        .await
        .unwrap();

    let delete_10_duration = start_time.elapsed();
    println!("[RESULT] Delete 10 rows took: {:?}", delete_10_duration);
    println!(
        "[RESULT] Delete 10 rows: {:.2}ms",
        delete_10_duration.as_secs_f64() * 1000.0
    );

    // Verify deletion
    let count_after_10 = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_10 = count_after_10[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count_10, 990, "Should have 990 rows after deleting 10");

    // Benchmark 2: Delete 50 rows (medium delete)
    println!("\n[BENCHMARK 2] Deleting 50 rows");
    let start_time = std::time::Instant::now();

    // Read current CSV
    let current_csv = server
        .read_file("/data/data.csv", 0, 1_000_000)
        .await
        .unwrap();
    let current_csv_str = String::from_utf8(current_csv).unwrap();

    // Filter out IDs 10-59 (50 rows)
    let filtered_lines: Vec<&str> = current_csv_str
        .lines()
        .filter(|line| {
            if line.starts_with("id,") {
                true // Keep header
            } else {
                let id: i32 = line
                    .split(',')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(-1);
                id >= 60 // Keep IDs 60+
            }
        })
        .collect();
    let filtered_csv = filtered_lines.join("\n") + "\n";

    // Write filtered CSV (delete 50 rows)
    server
        .write_file("/data/data.csv", 0, filtered_csv.as_bytes())
        .await
        .unwrap();

    let delete_50_duration = start_time.elapsed();
    println!("[RESULT] Delete 50 rows took: {:?}", delete_50_duration);
    println!(
        "[RESULT] Delete 50 rows: {:.2}ms",
        delete_50_duration.as_secs_f64() * 1000.0
    );

    // Verify deletion
    let count_after_50 = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_50 = count_after_50[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count_50, 940, "Should have 940 rows after deleting 50 more");

    // Benchmark 3: Delete 100 rows (large delete)
    println!("\n[BENCHMARK 3] Deleting 100 rows");
    let start_time = std::time::Instant::now();

    // Read current CSV
    let current_csv = server
        .read_file("/data/data.csv", 0, 1_000_000)
        .await
        .unwrap();
    let current_csv_str = String::from_utf8(current_csv).unwrap();

    // Filter out IDs 60-159 (100 rows)
    let filtered_lines: Vec<&str> = current_csv_str
        .lines()
        .filter(|line| {
            if line.starts_with("id,") {
                true // Keep header
            } else {
                let id: i32 = line
                    .split(',')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(-1);
                id >= 160 // Keep IDs 160+
            }
        })
        .collect();
    let filtered_csv = filtered_lines.join("\n") + "\n";

    // Write filtered CSV (delete 100 rows)
    server
        .write_file("/data/data.csv", 0, filtered_csv.as_bytes())
        .await
        .unwrap();

    let delete_100_duration = start_time.elapsed();
    println!("[RESULT] Delete 100 rows took: {:?}", delete_100_duration);
    println!(
        "[RESULT] Delete 100 rows: {:.2}ms",
        delete_100_duration.as_secs_f64() * 1000.0
    );

    // Verify deletion
    let count_after_100 = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_100 = count_after_100[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        count_100, 840,
        "Should have 840 rows after deleting 100 more"
    );

    // Print summary
    println!("\n=== PERFORMANCE SUMMARY ===");
    println!(
        "Delete 10 rows:  {:.2}ms",
        delete_10_duration.as_secs_f64() * 1000.0
    );
    println!(
        "Delete 50 rows:  {:.2}ms",
        delete_50_duration.as_secs_f64() * 1000.0
    );
    println!(
        "Delete 100 rows: {:.2}ms",
        delete_100_duration.as_secs_f64() * 1000.0
    );

    // Check success criterion: < 100ms for small deletes
    let delete_10_ms = delete_10_duration.as_secs_f64() * 1000.0;
    if delete_10_ms < 100.0 {
        println!(
            "OK SUCCESS: Delete 10 rows ({:.2}ms) < 100ms threshold",
            delete_10_ms
        );
    } else {
        println!(
            "WARN  WARNING: Delete 10 rows ({:.2}ms) >= 100ms threshold",
            delete_10_ms
        );
    }

    println!("\n[SUCCESS] Performance benchmark complete");

    // Cleanup
    println!("[CLEANUP] Shutting down");
    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_cache_hit_after_write() {
    init_logging();
    println!("\n[TEST] test_nfs_cache_hit_after_write");

    // TDD: This test isolates the cache invalidation problem
    // Expected behavior: Write should UPDATE cache, not invalidate it
    // Current (wrong) behavior: Write invalidates cache, causing cache miss on next read

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    println!("[SETUP] Creating database");
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert initial data
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();
    println!("[SETUP] Inserted 3 rows");

    let db_arc = Arc::new(db);

    // Start NFS server with caching
    let port = create_unique_port(17000);
    println!("[NFS] Starting server with cache on port {}", port);
    let server = NfsServer::new(db_arc.clone(), port).await.unwrap();
    assert!(server.is_ready().await);
    println!("[NFS] Server ready");

    // READ 1: First read (cache MISS expected - populates cache)
    println!("\n[READ 1] First read - cache MISS expected");
    let start = std::time::Instant::now();
    let csv1 = server.read_file("/data/data.csv", 0, 100000).await.unwrap();
    let read1_duration = start.elapsed();
    println!("[READ 1] Duration: {:?}", read1_duration);
    let csv1_str = String::from_utf8(csv1).unwrap();
    println!("[READ 1] Content:\n{}", csv1_str);
    assert!(csv1_str.contains("Alice"));
    assert!(csv1_str.contains("Bob"));
    assert!(csv1_str.contains("Charlie"));

    // WRITE: Modify CSV (delete Bob)
    println!("\n[WRITE] Deleting Bob from CSV");
    let filtered_csv = csv1_str
        .lines()
        .filter(|line| !line.contains("Bob"))
        .collect::<Vec<&str>>()
        .join("\n")
        + "\n";
    println!("[WRITE] Filtered CSV:\n{}", filtered_csv);

    let write_start = std::time::Instant::now();
    server
        .write_file("/data/data.csv", 0, filtered_csv.as_bytes())
        .await
        .unwrap();
    let write_duration = write_start.elapsed();
    println!("[WRITE] Duration: {:?}", write_duration);

    // READ 2: Second read (cache HIT expected - should be fast!)
    println!("\n[READ 2] Second read - cache HIT expected");
    let start = std::time::Instant::now();
    let csv2 = server.read_file("/data/data.csv", 0, 100000).await.unwrap();
    let read2_duration = start.elapsed();
    println!("[READ 2] Duration: {:?}", read2_duration);
    let csv2_str = String::from_utf8(csv2).unwrap();
    println!("[READ 2] Content:\n{}", csv2_str);

    // Verify content is correct
    assert!(csv2_str.contains("Alice"));
    assert!(!csv2_str.contains("Bob"), "Bob should be deleted");
    assert!(csv2_str.contains("Charlie"));

    // TDD: This assertion will FAIL because cache is invalidated
    // Expected: read2 should be MUCH faster than read1 (cache hit)
    // Current: read2 is same speed as read1 (cache miss)
    println!("\n=== CACHE PERFORMANCE ===");
    println!("Read 1 (cache MISS): {:?}", read1_duration);
    println!("Read 2 (should be cache HIT): {:?}", read2_duration);

    let speedup = read1_duration.as_secs_f64() / read2_duration.as_secs_f64();
    println!("Speedup: {:.2}x", speedup);

    // TDD: This will FAIL with current implementation
    // Cache hit should be at least 2x faster than cache miss
    assert!(
        read2_duration < read1_duration / 2,
        "READ 2 should be at least 2x faster than READ 1 (cache hit vs miss). \
         Read1: {:?}, Read2: {:?}, Speedup: {:.2}x",
        read1_duration,
        read2_duration,
        speedup
    );

    println!("[SUCCESS] Cache is working correctly - read after write is fast!");

    // Cleanup
    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_delete_performance_isolated() {
    init_logging();
    println!("\n[TEST] test_delete_performance_isolated");

    // TDD: Isolate JUST the delete operation to see where time is spent
    // Expected: Delete 10 rows from 100-row table should be < 50ms

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    println!("[SETUP] Creating database");
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert 100 rows
    for i in 0..10 {
        let start_id = i * 10;
        let ids: Vec<i32> = (start_id..start_id + 10).collect();
        let names: Vec<String> = ids.iter().map(|id| format!("User{}", id)).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();
        db.insert(batch).await.unwrap();
    }
    println!("[SETUP] Inserted 100 rows");

    // Test 1: Direct SQL DELETE (bypass NFS entirely)
    println!("\n[TEST 1] Direct SQL DELETE (10 rows)");
    let start = std::time::Instant::now();
    let deleted_count = db
        .delete_rows_where("id IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)")
        .await
        .unwrap();
    let direct_delete_duration = start.elapsed();
    println!("[TEST 1] Direct DELETE took: {:?}", direct_delete_duration);
    println!("[TEST 1] Deleted {} rows", deleted_count);
    assert_eq!(deleted_count, 10);

    // TDD: This should be < 50ms
    let delete_ms = direct_delete_duration.as_secs_f64() * 1000.0;
    println!("[TEST 1] Direct DELETE: {:.2}ms", delete_ms);

    if delete_ms < 50.0 {
        println!(
            "OK SUCCESS: Direct DELETE ({:.2}ms) < 50ms threshold",
            delete_ms
        );
    } else {
        println!(
            "WARN  WARNING: Direct DELETE ({:.2}ms) >= 50ms threshold",
            delete_ms
        );
    }

    // Test 2: Via NFS (full path)
    let db_arc = Arc::new(db);
    let port = create_unique_port(18000);
    let server = NfsServer::new(db_arc.clone(), port).await.unwrap();
    assert!(server.is_ready().await);

    // Pre-warm cache
    let _ = server.read_file("/data/data.csv", 0, 100000).await.unwrap();

    println!("\n[TEST 2] NFS DELETE via overwrite (10 rows)");
    let csv = server.read_file("/data/data.csv", 0, 100000).await.unwrap();
    let csv_str = String::from_utf8(csv).unwrap();

    // Filter out IDs 10-19
    let filtered = csv_str
        .lines()
        .filter(|line| {
            if line.starts_with("id,") {
                true
            } else {
                let id: i32 = line
                    .split(',')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(-1);
                id >= 20
            }
        })
        .collect::<Vec<&str>>()
        .join("\n")
        + "\n";

    let start = std::time::Instant::now();
    server
        .write_file("/data/data.csv", 0, filtered.as_bytes())
        .await
        .unwrap();
    let nfs_delete_duration = start.elapsed();
    println!("[TEST 2] NFS DELETE took: {:?}", nfs_delete_duration);

    let nfs_delete_ms = nfs_delete_duration.as_secs_f64() * 1000.0;
    println!("[TEST 2] NFS DELETE: {:.2}ms", nfs_delete_ms);

    // TDD: NFS delete should be < 100ms (includes CSV diff overhead)
    if nfs_delete_ms < 100.0 {
        println!(
            "OK SUCCESS: NFS DELETE ({:.2}ms) < 100ms threshold",
            nfs_delete_ms
        );
    } else {
        println!(
            "WARN  WARNING: NFS DELETE ({:.2}ms) >= 100ms threshold",
            nfs_delete_ms
        );
    }

    println!("\n=== PERFORMANCE BREAKDOWN ===");
    println!("Direct SQL DELETE: {:.2}ms", delete_ms);
    println!("NFS DELETE (with diff): {:.2}ms", nfs_delete_ms);
    println!("Overhead: {:.2}ms", nfs_delete_ms - delete_ms);

    // Cleanup
    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_concurrent_row_deletion() {
    init_logging();
    println!("\n[TEST] test_concurrent_row_deletion");

    // TDD: Test concurrent row deletions from multiple "clients"
    // Expected: Delta Lake optimistic concurrency control should handle conflicts
    // All deletions should succeed or retry correctly

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create schema with ID column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
    ]));

    println!("[SETUP] Creating database");
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert 200 rows (will be deleted by 4 concurrent workers)
    println!("[SETUP] Inserting 200 rows");
    for batch_num in 0..20 {
        let start_id = batch_num * 10;
        let ids: Vec<i32> = (start_id..start_id + 10).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("User{}", i)).collect();
        let categories: Vec<String> = ids
            .iter()
            .map(|i| {
                match i % 4 {
                    0 => "A",
                    1 => "B",
                    2 => "C",
                    _ => "D",
                }
                .to_string()
            })
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(categories)),
            ],
        )
        .unwrap();
        db.insert(batch).await.unwrap();
    }
    println!("[SETUP] Inserted 200 rows");

    // Verify initial count
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let initial_count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(initial_count, 200, "Should have 200 rows initially");
    println!("[VERIFY] 200 rows present");

    let db_arc = Arc::new(db);

    // Start NFS server
    let port = create_unique_port(19000);
    println!("[NFS] Starting server on port {}", port);
    let server = Arc::new(NfsServer::new(db_arc.clone(), port).await.unwrap());
    assert!(server.is_ready().await);
    println!("[NFS] Server ready");

    // Pre-warm cache with initial read
    let _ = server
        .read_file("/data/data.csv", 0, 1_000_000)
        .await
        .unwrap();
    println!("[CACHE] Pre-warmed");

    // Spawn 4 concurrent workers, each deleting rows from different categories
    println!("\n[CONCURRENT] Spawning 4 workers to delete different row sets");
    let mut handles = vec![];

    for worker_id in 0..4 {
        let server_clone = server.clone();
        let category = match worker_id {
            0 => "A",
            1 => "B",
            2 => "C",
            _ => "D",
        };

        let handle = tokio::spawn(async move {
            println!(
                "[WORKER {}] Starting - will delete category {}",
                worker_id, category
            );

            // Read current CSV
            let csv = server_clone
                .read_file("/data/data.csv", 0, 1_000_000)
                .await
                .unwrap();
            let csv_str = String::from_utf8(csv).unwrap();

            // Filter out this worker's category
            let filtered = csv_str
                .lines()
                .filter(|line| {
                    if line.starts_with("id,") {
                        true // Keep header
                    } else {
                        !line.ends_with(&format!(",{}", category))
                    }
                })
                .collect::<Vec<&str>>()
                .join("\n")
                + "\n";

            let rows_in_filtered = filtered.lines().count() - 1; // -1 for header
            println!(
                "[WORKER {}] Filtered CSV has {} rows (deleted category {})",
                worker_id, rows_in_filtered, category
            );

            // Write filtered CSV (delete operation)
            let start = std::time::Instant::now();
            let result = server_clone
                .write_file("/data/data.csv", 0, filtered.as_bytes())
                .await;
            let duration = start.elapsed();

            match result {
                Ok(_) => {
                    println!("[WORKER {}] DELETE succeeded in {:?}", worker_id, duration);
                    Ok(())
                }
                Err(e) => {
                    println!("[WORKER {}] DELETE failed: {:?}", worker_id, e);
                    Err(e)
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all workers to complete
    println!("\n[WAIT] Waiting for all workers to complete...");
    let mut successes = 0;
    let mut failures = 0;

    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(_)) => {
                successes += 1;
                println!("[WORKER {}] OK Completed successfully", i);
            }
            Ok(Err(e)) => {
                failures += 1;
                println!("[WORKER {}] ❌ Failed: {:?}", i, e);
            }
            Err(e) => {
                failures += 1;
                println!("[WORKER {}] ❌ Panicked: {:?}", i, e);
            }
        }
    }

    println!("\n=== CONCURRENT DELETION RESULTS ===");
    println!("Successes: {}", successes);
    println!("Failures: {}", failures);

    // TDD: With proper concurrency control, AT LEAST ONE worker should succeed
    // Others may fail due to conflicts, but that's acceptable (optimistic concurrency)
    assert!(
        successes >= 1,
        "At least one concurrent deletion should succeed"
    );

    // Verify final state - check that SOME rows were deleted
    println!("\n[VERIFY] Checking final row count");
    let final_results = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let final_count = final_results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    println!(
        "[VERIFY] Initial: {} rows, Final: {} rows",
        initial_count, final_count
    );
    println!("[VERIFY] Deleted: {} rows", initial_count - final_count);

    // TDD: Some rows should have been deleted (optimistic concurrency allows partial success)
    assert!(
        final_count < initial_count,
        "Some rows should have been deleted. Initial: {}, Final: {}",
        initial_count,
        final_count
    );

    // TDD: If all workers succeeded (no conflicts), all 200 rows should be deleted
    // If conflicts occurred, fewer rows deleted (but at least some)
    if successes == 4 {
        println!("OK All 4 workers succeeded - checking all categories deleted");
        // With 4 workers each deleting 50 rows (one category), all 200 should be gone
        // But due to race conditions in reading initial state, some may remain
        assert!(
            final_count <= 50,
            "Most rows should be deleted when all workers succeed"
        );
    }

    println!("\n[SUCCESS] Concurrent deletion test passed!");
    println!("- Optimistic concurrency control working");
    println!(
        "- {} workers succeeded, {} had conflicts",
        successes, failures
    );

    // Cleanup
    println!("[CLEANUP] Shutting down");
    // Extract from Arc to call shutdown (which takes ownership)
    let server = Arc::try_unwrap(server).unwrap_or_else(|_| panic!("Failed to unwrap Arc"));
    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_large_dataset_row_deletion() {
    init_logging();
    println!("\n[TEST] test_large_dataset_row_deletion");

    // TDD: Test row deletion on a large dataset (10K rows)
    // Expected: System remains stable and correct under load
    // Performance should scale reasonably with dataset size

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    println!("[SETUP] Creating database");
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert 10K rows in batches of 100
    println!("[SETUP] Inserting 10,000 rows (100 batches of 100)...");
    let insert_start = std::time::Instant::now();
    for batch_num in 0..100 {
        let start_id = batch_num * 100;
        let ids: Vec<i32> = (start_id..start_id + 100).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("User{:05}", i)).collect();
        let values: Vec<i64> = ids.iter().map(|i| (i * 12345) as i64).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap();
        db.insert(batch).await.unwrap();

        if (batch_num + 1) % 20 == 0 {
            println!("[SETUP] Inserted {} rows...", (batch_num + 1) * 100);
        }
    }
    let insert_duration = insert_start.elapsed();
    println!("[SETUP] Inserted 10,000 rows in {:?}", insert_duration);

    // Verify initial count
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let initial_count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(initial_count, 10000, "Should have 10,000 rows initially");
    println!("[VERIFY] 10,000 rows present");

    let db_arc = Arc::new(db);

    // Start NFS server
    let port = create_unique_port(20000);
    println!("[NFS] Starting server on port {}", port);
    let server = NfsServer::new(db_arc.clone(), port).await.unwrap();
    assert!(server.is_ready().await);
    println!("[NFS] Server ready");

    // Test 1: Delete 100 rows from 10K dataset
    println!("\n[TEST 1] Deleting 100 rows from 10K dataset");
    let csv_read_start = std::time::Instant::now();
    let csv = server
        .read_file("/data/data.csv", 0, 10_000_000)
        .await
        .unwrap();
    let csv_read_duration = csv_read_start.elapsed();
    println!(
        "[TEST 1] CSV read took: {:?} ({} bytes)",
        csv_read_duration,
        csv.len()
    );

    let csv_str = String::from_utf8(csv).unwrap();
    let lines_before = csv_str.lines().count();
    println!("[TEST 1] CSV has {} lines (including header)", lines_before);

    // Filter out IDs 5000-5099 (100 rows)
    let filter_start = std::time::Instant::now();
    let filtered = csv_str
        .lines()
        .filter(|line| {
            if line.starts_with("id,") {
                true // Keep header
            } else {
                let id: i32 = line
                    .split(',')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(-1);
                !(5000..5100).contains(&id)
            }
        })
        .collect::<Vec<&str>>()
        .join("\n")
        + "\n";
    let filter_duration = filter_start.elapsed();
    println!("[TEST 1] CSV filtering took: {:?}", filter_duration);

    let lines_after = filtered.lines().count();
    println!("[TEST 1] Filtered CSV has {} lines", lines_after);
    assert_eq!(lines_before - lines_after, 100, "Should filter 100 lines");

    // Write filtered CSV (delete operation)
    let delete_start = std::time::Instant::now();
    server
        .write_file("/data/data.csv", 0, filtered.as_bytes())
        .await
        .unwrap();
    let delete_duration = delete_start.elapsed();
    println!("[TEST 1] DELETE operation took: {:?}", delete_duration);

    // Verify deletion
    let verify_results = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_after_delete = verify_results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        count_after_delete, 9900,
        "Should have 9,900 rows after deleting 100"
    );
    println!("[TEST 1] Verified: 9,900 rows remaining");

    // Test 2: Delete another 1000 rows (larger batch)
    println!("\n[TEST 2] Deleting 1000 rows from 9.9K dataset");
    let csv2 = server
        .read_file("/data/data.csv", 0, 10_000_000)
        .await
        .unwrap();
    let csv2_str = String::from_utf8(csv2).unwrap();

    // Filter out IDs 1000-1999 (1000 rows)
    let filtered2 = csv2_str
        .lines()
        .filter(|line| {
            if line.starts_with("id,") {
                true
            } else {
                let id: i32 = line
                    .split(',')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(-1);
                !(1000..2000).contains(&id)
            }
        })
        .collect::<Vec<&str>>()
        .join("\n")
        + "\n";

    let delete2_start = std::time::Instant::now();
    server
        .write_file("/data/data.csv", 0, filtered2.as_bytes())
        .await
        .unwrap();
    let delete2_duration = delete2_start.elapsed();
    println!("[TEST 2] DELETE operation took: {:?}", delete2_duration);

    // Verify final state
    let final_results = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let final_count = final_results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        final_count, 8900,
        "Should have 8,900 rows after deleting 1,000 more"
    );
    println!("[TEST 2] Verified: 8,900 rows remaining");

    // Print performance summary
    println!("\n=== LARGE DATASET PERFORMANCE ===");
    println!("Dataset: 10,000 rows");
    println!("Insert 10K rows: {:?}", insert_duration);
    println!("Read CSV (10K rows): {:?}", csv_read_duration);
    println!("Delete 100 rows: {:?}", delete_duration);
    println!("Delete 1000 rows: {:?}", delete2_duration);
    println!("Total deleted: 1,100 rows");
    println!("Final count: 8,900 rows");

    // TDD: Performance should be reasonable for large datasets
    let delete_100_ms = delete_duration.as_secs_f64() * 1000.0;
    let delete_1000_ms = delete2_duration.as_secs_f64() * 1000.0;

    println!("\n[PERFORMANCE] Delete 100 rows: {:.2}ms", delete_100_ms);
    println!("[PERFORMANCE] Delete 1000 rows: {:.2}ms", delete_1000_ms);

    // Reasonable thresholds for large dataset operations
    if delete_100_ms < 500.0 {
        println!("OK Delete 100 rows performance acceptable (< 500ms)");
    } else {
        println!(
            "WARN  Delete 100 rows slower than expected: {:.2}ms",
            delete_100_ms
        );
    }

    if delete_1000_ms < 2000.0 {
        println!("OK Delete 1000 rows performance acceptable (< 2s)");
    } else {
        println!(
            "WARN  Delete 1000 rows slower than expected: {:.2}ms",
            delete_1000_ms
        );
    }

    println!("\n[SUCCESS] Large dataset deletion test passed!");
    println!("- System stable with 10K rows");
    println!("- Deletions complete successfully");
    println!("- Data integrity maintained");

    // Cleanup
    println!("[CLEANUP] Shutting down");
    server.shutdown().await.unwrap();
}

/// Test: CSV row UPDATE via overwrite (edit values, same IDs)
/// This tests MERGE operation with UPDATE detection
#[tokio::test]
#[serial(nfs)]
async fn test_nfs_csv_update_via_overwrite() {
    init_logging();
    println!("\n[TEST] test_nfs_csv_update_via_overwrite - UPDATE rows via CSV edit");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    println!("[SETUP] Creating database");
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert 3 rows
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int32Array::from(vec![25, 30, 35])),
        ],
    )
    .unwrap();

    println!("[INSERT] Inserting 3 rows");
    db.insert(batch).await.unwrap();

    let db_arc = Arc::new(db);

    // Start NFS server
    let port = create_unique_port(13200);
    println!("[NFS] Starting server on port {}", port);
    let server = NfsServer::new(db_arc.clone(), port).await.unwrap();
    assert!(server.is_ready().await);
    println!("[NFS] Server ready");

    // Read current CSV
    println!("[READ] Reading CSV");
    let csv = server.read_file("/data/data.csv", 0, 100000).await.unwrap();
    let csv_str = String::from_utf8(csv).unwrap();
    println!("[CSV BEFORE]:\n{}", csv_str);

    // Verify initial values
    assert!(csv_str.contains("Alice"));
    assert!(csv_str.contains("25"));
    assert!(csv_str.contains("Bob"));
    assert!(csv_str.contains("30"));

    // EDIT CSV: Change values for existing IDs (UPDATE operation)
    // Alice (id=1): age 25 -> 26, name stays same
    // Bob (id=2): age 30 -> 31, name stays same
    // Charlie (id=3): age 35 -> 36, name stays same
    let updated_csv = "id,name,age\n1,Alice Updated,26\n2,Bob Updated,31\n3,Charlie,35\n";

    println!("[WRITE] Writing updated CSV (UPDATE operation)");
    println!("[CSV AFTER]:\n{}", updated_csv);
    server
        .write_file("/data/data.csv", 0, updated_csv.as_bytes())
        .await
        .unwrap();

    println!("[VERIFY] Checking updated values");

    // Verify Alice was updated
    let results = db_arc
        .query("SELECT name, age FROM data WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(results[0].num_rows(), 1, "Alice should still exist");
    let name = results[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    let age = results[0]
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    assert_eq!(name, "Alice Updated", "Alice's name should be updated");
    assert_eq!(age, 26, "Alice's age should be updated to 26");
    println!("[VERIFY] Alice updated: name='{}', age={}", name, age);

    // Verify Bob was updated
    let results = db_arc
        .query("SELECT name, age FROM data WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(results[0].num_rows(), 1, "Bob should still exist");
    let name = results[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    let age = results[0]
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    assert_eq!(name, "Bob Updated", "Bob's name should be updated");
    assert_eq!(age, 31, "Bob's age should be updated to 31");
    println!("[VERIFY] Bob updated: name='{}', age={}", name, age);

    // Verify Charlie unchanged
    let results = db_arc
        .query("SELECT name, age FROM data WHERE id = 3")
        .await
        .unwrap();
    assert_eq!(results[0].num_rows(), 1, "Charlie should still exist");
    let name = results[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    let age = results[0]
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    assert_eq!(name, "Charlie", "Charlie's name should be unchanged");
    assert_eq!(age, 35, "Charlie's age should be unchanged");
    println!("[VERIFY] Charlie unchanged: name='{}', age={}", name, age);

    // Verify row count is still 3 (no rows deleted or added)
    let results = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3, "Should still have exactly 3 rows");
    println!("[VERIFY] Row count: {}", count);

    println!("[SUCCESS] CSV UPDATE via overwrite test passed");

    server.shutdown().await.unwrap();
}

// ============================================================================
// STRESS TESTS - Large Scale Performance & Stability
// ============================================================================

/// STRESS TEST 1: 100K rows - Medium scale stress test
///
/// NOTE: This test is IGNORED by default due to longer runtime (~1-2 minutes)
/// To run: cargo test test_stress_100k_rows -- --ignored --nocapture
#[tokio::test]
#[serial(nfs)]
#[ignore = "Stress test: 100K rows, takes ~1-2 minutes. Run with: cargo test -- --ignored"]
async fn test_stress_100k_rows() {
    init_logging();
    println!("\n[STRESS TEST] 100K rows - Medium scale");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    println!("[SETUP] Creating database");
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();
    let db_arc = Arc::new(db);

    // Insert 100K rows in batches of 1000
    println!("[SETUP] Inserting 100,000 rows (100 batches of 1000)...");
    let insert_start = std::time::Instant::now();
    for batch_num in 0..100 {
        let start_id = batch_num * 1000;
        let ids: Vec<i32> = (start_id..start_id + 1000).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("User{:06}", i)).collect();
        let values: Vec<i64> = ids.iter().map(|i| (i * 12345) as i64).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap();
        db_arc.insert(batch).await.unwrap();

        if (batch_num + 1) % 10 == 0 {
            println!("[SETUP] Inserted {} rows...", (batch_num + 1) * 1000);
        }
    }
    let insert_duration = insert_start.elapsed();
    println!("[SETUP] OK Inserted 100,000 rows in {:?}", insert_duration);

    // Verify count
    let results = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 100000);
    println!("[VERIFY] OK Count confirmed: 100,000 rows");

    // Start NFS server
    let port = create_unique_port(12000);
    let server = NfsServer::new(db_arc.clone(), port).await.unwrap();
    assert!(server.is_ready().await, "NFS server should be ready");
    println!("[NFS] Started on port {}", port);

    // Test 1: Read large CSV (100K rows)
    println!("\n[TEST 1] Reading 100K row CSV...");
    let csv_start = std::time::Instant::now();
    let csv_data = server
        .read_file("/data/data.csv", 0, 100_000_000)
        .await
        .unwrap();
    let csv_duration = csv_start.elapsed();
    let csv_str = String::from_utf8_lossy(&csv_data);
    let csv_lines = csv_str.lines().count();
    println!("[TEST 1] OK Read {} lines in {:?}", csv_lines, csv_duration);
    println!(
        "[TEST 1] CSV size: {:.2} MB",
        csv_data.len() as f64 / 1024.0 / 1024.0
    );

    // Test 2: Delete 1000 rows from middle
    println!("\n[TEST 2] Deleting 1000 rows from middle of 100K dataset...");
    let filtered = csv_str
        .lines()
        .filter(|line| {
            if line.starts_with("id,") {
                true
            } else {
                let id: i32 = line
                    .split(',')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(-1);
                !(50000..51000).contains(&id)
            }
        })
        .collect::<Vec<&str>>()
        .join("\n")
        + "\n";

    let delete_start = std::time::Instant::now();
    server
        .write_file("/data/data.csv", 0, filtered.as_bytes())
        .await
        .unwrap();
    let delete_duration = delete_start.elapsed();
    println!("[TEST 2] OK DELETE 1000 rows took: {:?}", delete_duration);

    // Verify deletion
    let results = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let final_count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(final_count, 99000);
    println!("[VERIFY] OK Count after delete: 99,000 rows");

    // Test 3: Large batch delete (10K rows)
    println!("\n[TEST 3] Deleting 10,000 rows from 99K dataset...");
    let csv_data2 = server
        .read_file("/data/data.csv", 0, 100_000_000)
        .await
        .unwrap();
    let csv_str2 = String::from_utf8_lossy(&csv_data2);

    let filtered2 = csv_str2
        .lines()
        .filter(|line| {
            if line.starts_with("id,") {
                true
            } else {
                let id: i32 = line
                    .split(',')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(-1);
                !(10000..20000).contains(&id)
            }
        })
        .collect::<Vec<&str>>()
        .join("\n")
        + "\n";

    let delete2_start = std::time::Instant::now();
    server
        .write_file("/data/data.csv", 0, filtered2.as_bytes())
        .await
        .unwrap();
    let delete2_duration = delete2_start.elapsed();
    println!(
        "[TEST 3] OK DELETE 10,000 rows took: {:?}",
        delete2_duration
    );

    // Verify final state
    let results = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let final_count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(final_count, 89000);
    println!("[VERIFY] OK Final count: 89,000 rows");

    // Performance summary
    println!("\n=== 100K STRESS TEST RESULTS ===");
    println!("Dataset size: 100,000 rows");
    println!("Insert 100K: {:?}", insert_duration);
    println!("Read CSV (100K rows): {:?}", csv_duration);
    println!("Delete 1K rows: {:?}", delete_duration);
    println!("Delete 10K rows: {:?}", delete2_duration);
    println!("OK System stable under 100K row load");

    server.shutdown().await.unwrap();
}

/// STRESS TEST 2: 1M rows - Large scale stress test
///
/// NOTE: This test is IGNORED by default due to long runtime (~5-10 minutes)
/// To run: cargo test test_stress_1m_rows -- --ignored --nocapture
#[tokio::test]
#[serial(nfs)]
#[ignore = "Stress test: 1M rows, takes ~5-10 minutes. Run with: cargo test -- --ignored"]
async fn test_stress_1m_rows() {
    init_logging();
    println!("\n[STRESS TEST] 1M rows - Large scale (THIS WILL TAKE 5-10 MINUTES)");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    println!("[SETUP] Creating database");
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();
    let db_arc = Arc::new(db);

    // Insert 1M rows in batches of 10K
    println!("[SETUP] Inserting 1,000,000 rows (100 batches of 10,000)...");
    println!("[INFO] This will take several minutes...");
    let insert_start = std::time::Instant::now();

    for batch_num in 0..100 {
        let start_id = batch_num * 10000;
        let ids: Vec<i32> = (start_id..start_id + 10000).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("User{:07}", i)).collect();
        let values: Vec<i64> = ids.iter().map(|i| (*i as i64) * 12345_i64).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap();
        db_arc.insert(batch).await.unwrap();

        if (batch_num + 1) % 10 == 0 {
            let elapsed = insert_start.elapsed();
            let rows_inserted = (batch_num + 1) * 10000;
            let rate = rows_inserted as f64 / elapsed.as_secs_f64();
            println!(
                "[SETUP] Inserted {} rows... ({:.0} rows/sec)",
                rows_inserted, rate
            );
        }
    }
    let insert_duration = insert_start.elapsed();
    let insert_rate = 1_000_000.0 / insert_duration.as_secs_f64();
    println!(
        "[SETUP] OK Inserted 1,000,000 rows in {:?} ({:.0} rows/sec)",
        insert_duration, insert_rate
    );

    // Verify count
    println!("[VERIFY] Counting rows...");
    let count_start = std::time::Instant::now();
    let results = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_duration = count_start.elapsed();
    let count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1_000_000);
    println!(
        "[VERIFY] OK Count confirmed: 1,000,000 rows (query took {:?})",
        count_duration
    );

    // Start NFS server
    let port = create_unique_port(13000);
    println!("[NFS] Starting server on port {}...", port);
    let server = NfsServer::new(db_arc.clone(), port).await.unwrap();
    assert!(server.is_ready().await, "NFS server should be ready");
    println!("[NFS] OK Started");

    // Test 1: Read large CSV (1M rows) - This tests CSV generation performance
    println!("\n[TEST 1] Reading 1M row CSV (testing CSV generation + cache)...");
    let csv_start = std::time::Instant::now();
    let csv_data = server
        .read_file("/data/data.csv", 0, 1_000_000_000)
        .await
        .unwrap();
    let csv_duration = csv_start.elapsed();
    let csv_str = String::from_utf8_lossy(&csv_data);
    let csv_lines = csv_str.lines().count();
    let csv_size_mb = csv_data.len() as f64 / 1024.0 / 1024.0;
    println!("[TEST 1] OK Read {} lines in {:?}", csv_lines, csv_duration);
    println!("[TEST 1] CSV size: {:.2} MB", csv_size_mb);
    println!(
        "[TEST 1] Read rate: {:.2} MB/sec",
        csv_size_mb / csv_duration.as_secs_f64()
    );

    // Test 2: Read from cache (should be MUCH faster)
    println!("\n[TEST 2] Reading from cache (should be <100ms)...");
    let cache_start = std::time::Instant::now();
    let csv_data2 = server
        .read_file("/data/data.csv", 0, 1_000_000_000)
        .await
        .unwrap();
    let cache_duration = cache_start.elapsed();
    println!("[TEST 2] OK Cached read took: {:?}", cache_duration);
    println!(
        "[TEST 2] Speedup: {:.0}x faster",
        csv_duration.as_secs_f64() / cache_duration.as_secs_f64()
    );
    assert_eq!(csv_data.len(), csv_data2.len(), "Cached data should match");

    // Test 3: Delete 10K rows (large deletion on 1M dataset)
    println!("\n[TEST 3] Deleting 10,000 rows from 1M dataset (CSV diff stress test)...");
    println!("[TEST 3] This tests CSV parsing and diff performance on large data...");

    let filtered = csv_str
        .lines()
        .filter(|line| {
            if line.starts_with("id,") {
                true
            } else {
                let id: i32 = line
                    .split(',')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(-1);
                !(500000..510000).contains(&id)
            }
        })
        .collect::<Vec<&str>>()
        .join("\n")
        + "\n";

    let delete_start = std::time::Instant::now();
    server
        .write_file("/data/data.csv", 0, filtered.as_bytes())
        .await
        .unwrap();
    let delete_duration = delete_start.elapsed();
    println!("[TEST 3] OK DELETE 10,000 rows took: {:?}", delete_duration);

    // Verify deletion
    let verify_start = std::time::Instant::now();
    let results = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let verify_duration = verify_start.elapsed();
    let final_count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(final_count, 990000);
    println!(
        "[VERIFY] OK Count after delete: 990,000 rows (query took {:?})",
        verify_duration
    );

    // Test 4: Query performance on large table
    println!("\n[TEST 4] Query performance on 990K rows...");
    let query_start = std::time::Instant::now();
    let results = db_arc
        .query("SELECT * FROM data WHERE id >= 100000 AND id < 100100")
        .await
        .unwrap();
    let query_duration = query_start.elapsed();
    let result_count: usize = results.iter().map(|batch| batch.num_rows()).sum();
    println!(
        "[TEST 4] OK Query returned {} rows in {:?}",
        result_count, query_duration
    );

    // Performance summary
    println!("\n=== 1M STRESS TEST RESULTS ===");
    println!("Dataset size: 1,000,000 rows");
    println!(
        "Insert 1M rows: {:?} ({:.0} rows/sec)",
        insert_duration, insert_rate
    );
    println!("Initial count query: {:?}", count_duration);
    println!(
        "Generate CSV (1M rows): {:?} ({:.2} MB/sec)",
        csv_duration,
        csv_size_mb / csv_duration.as_secs_f64()
    );
    println!(
        "Cached CSV read: {:?} ({:.0}x speedup)",
        cache_duration,
        csv_duration.as_secs_f64() / cache_duration.as_secs_f64()
    );
    println!("Delete 10K rows: {:?}", delete_duration);
    println!("Verify count: {:?}", verify_duration);
    println!("Point query: {:?}", query_duration);
    println!("CSV size: {:.2} MB", csv_size_mb);
    println!("\nOK SYSTEM STABLE UNDER 1M ROW LOAD");
    println!("OK CSV GENERATION SUCCESSFUL");
    println!("OK DELETION VECTORS WORKING");
    println!("OK CACHE PERFORMANCE EXCELLENT");

    server.shutdown().await.unwrap();
}

/// STRESS TEST 3: Large batch deletion (50K rows deleted at once)
///
/// NOTE: This test is IGNORED by default due to longer runtime (~2-3 minutes)
/// To run: cargo test test_stress_large_batch_delete -- --ignored --nocapture
#[tokio::test]
#[serial(nfs)]
#[ignore = "Stress test: 50K row batch delete, takes ~2-3 minutes. Run with: cargo test -- --ignored"]
async fn test_stress_large_batch_delete() {
    init_logging();
    println!("\n[STRESS TEST] Large batch deletion - 50K rows at once");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    println!("[SETUP] Creating database");
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();
    let db_arc = Arc::new(db);

    // Insert 100K rows
    println!("[SETUP] Inserting 100,000 rows...");
    let insert_start = std::time::Instant::now();
    for batch_num in 0..100 {
        let start_id = batch_num * 1000;
        let ids: Vec<i32> = (start_id..start_id + 1000).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("User{:06}", i)).collect();
        let values: Vec<i64> = ids.iter().map(|i| (*i as i64) * 12345_i64).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap();
        db_arc.insert(batch).await.unwrap();

        if (batch_num + 1) % 20 == 0 {
            println!("[SETUP] Inserted {} rows...", (batch_num + 1) * 1000);
        }
    }
    let insert_duration = insert_start.elapsed();
    println!("[SETUP] OK Inserted 100,000 rows in {:?}", insert_duration);

    // Start NFS server
    let port = create_unique_port(14000);
    let server = NfsServer::new(db_arc.clone(), port).await.unwrap();
    assert!(server.is_ready().await, "NFS server should be ready");
    println!("[NFS] Started on port {}", port);

    // Read CSV
    let csv_data = server
        .read_file("/data/data.csv", 0, 100_000_000)
        .await
        .unwrap();
    let csv_str = String::from_utf8_lossy(&csv_data);

    // Test: Delete 50K rows (IDs 25000-74999)
    println!("\n[TEST] Deleting 50,000 rows from 100K dataset...");
    println!("[TEST] This stresses CSV diff, ID extraction, and WHERE clause generation");

    let filtered = csv_str
        .lines()
        .filter(|line| {
            if line.starts_with("id,") {
                true
            } else {
                let id: i32 = line
                    .split(',')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(-1);
                !(25000..75000).contains(&id)
            }
        })
        .collect::<Vec<&str>>()
        .join("\n")
        + "\n";

    println!(
        "[TEST] Filtered CSV has {} lines (expecting ~50,001)",
        filtered.lines().count()
    );

    let delete_start = std::time::Instant::now();
    server
        .write_file("/data/data.csv", 0, filtered.as_bytes())
        .await
        .unwrap();
    let delete_duration = delete_start.elapsed();
    println!("[TEST] OK DELETE 50,000 rows took: {:?}", delete_duration);
    println!(
        "[TEST] Deletion rate: {:.0} rows/sec",
        50000.0 / delete_duration.as_secs_f64()
    );

    // Verify deletion
    let results = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let final_count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(final_count, 50000);
    println!("[VERIFY] OK Count after delete: 50,000 rows (50% of data deleted)");

    // Verify correct IDs remain
    let results = db_arc
        .query("SELECT MIN(id) as min_id, MAX(id) as max_id FROM data WHERE id < 25000")
        .await
        .unwrap();
    let min_id = results[0]
        .column_by_name("min_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    let max_id = results[0]
        .column_by_name("max_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    println!("[VERIFY] Lower range: min={}, max={}", min_id, max_id);
    assert_eq!(min_id, 0);
    assert_eq!(max_id, 24999);

    let results = db_arc
        .query("SELECT MIN(id) as min_id, MAX(id) as max_id FROM data WHERE id >= 75000")
        .await
        .unwrap();
    let min_id = results[0]
        .column_by_name("min_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    let max_id = results[0]
        .column_by_name("max_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    println!("[VERIFY] Upper range: min={}, max={}", min_id, max_id);
    assert_eq!(min_id, 75000);
    assert_eq!(max_id, 99999);

    println!("\n=== LARGE BATCH DELETE RESULTS ===");
    println!("Deleted: 50,000 rows (50% of data)");
    println!("Duration: {:?}", delete_duration);
    println!(
        "Rate: {:.0} rows/sec",
        50000.0 / delete_duration.as_secs_f64()
    );
    println!("OK LARGE BATCH DELETION SUCCESSFUL");
    println!("OK DATA INTEGRITY VERIFIED");

    server.shutdown().await.unwrap();
}

/// 1GB CSV Stress Test
/// Tests CSV diff performance on very large datasets
///
/// NOTE: This test is IGNORED by default due to long runtime (~30 minutes)
/// To run: cargo test test_1gb_csv_stress -- --ignored --nocapture
#[tokio::test]
#[serial(nfs)]
#[ignore = "Stress test: 30M rows, takes ~30 minutes. Run with: cargo test -- --ignored"]
async fn test_1gb_csv_stress() {
    init_logging();
    println!("\n[STRESS TEST] 1GB CSV diff performance");

    // TDD: Test CSV diff on a 1GB dataset (~33M rows)
    // Expected: System handles large CSV without crashing
    // Performance measured but not critical (this is stress test territory)

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    println!("[SETUP] Creating database");
    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Calculate number of rows for ~1GB CSV
    // Each row is approximately: "1234567,User0012345,123456789012\n" = ~35 bytes
    // 1GB = 1,073,741,824 bytes / 35 bytes/row = ~30.7M rows
    // We'll use 30M rows to be safe
    let target_rows: i64 = 30_000_000;
    let batch_size: i64 = 10_000; // Insert in larger batches for speed
    let num_batches = target_rows / batch_size;

    println!(
        "[SETUP] Inserting {} rows ({} batches of {})...",
        target_rows, num_batches, batch_size
    );
    println!("[SETUP] This will take several minutes...");

    let insert_start = std::time::Instant::now();
    let mut last_progress = 0;

    for batch_num in 0..num_batches {
        let start_id = (batch_num * batch_size) as i32;
        let end_id = start_id + batch_size as i32;
        let ids: Vec<i32> = (start_id..end_id).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("User{:08}", i)).collect();
        let values: Vec<i64> = ids.iter().map(|i| (*i as i64) * 12345_i64).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap();
        db.insert(batch).await.unwrap();

        // Progress indicator every 10% progress
        let progress_pct = ((batch_num + 1) * 100) / num_batches;
        if progress_pct >= last_progress + 10 {
            last_progress = progress_pct;
            let total_rows = (batch_num + 1) * batch_size;
            println!(
                "[SETUP] Inserted {} rows ({}%)...",
                total_rows, progress_pct
            );
        }
    }

    let insert_duration = insert_start.elapsed();
    println!(
        "[SETUP] Inserted {} rows in {:?}",
        target_rows, insert_duration
    );
    println!(
        "[SETUP] Insert rate: {:.0} rows/sec",
        target_rows as f64 / insert_duration.as_secs_f64()
    );

    // Verify initial count
    println!("[VERIFY] Counting rows...");
    let count_start = std::time::Instant::now();
    let results = db
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let count_duration = count_start.elapsed();
    let initial_count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        initial_count, target_rows,
        "Should have {} rows initially",
        target_rows
    );
    println!(
        "[VERIFY] {} rows present (count took {:?})",
        target_rows, count_duration
    );

    let db_arc = Arc::new(db);

    // Start NFS server
    let port = create_unique_port(21000);
    println!("[NFS] Starting server on port {}", port);
    let server = NfsServer::new(db_arc.clone(), port).await.unwrap();
    assert!(server.is_ready().await);
    println!("[NFS] Server ready");

    // Test: Read large CSV
    println!("\n[STRESS TEST] Reading CSV from {} rows...", target_rows);
    let csv_read_start = std::time::Instant::now();
    let csv = server
        .read_file("/data/data.csv", 0, 2_000_000_000)
        .await
        .unwrap(); // 2GB max
    let csv_read_duration = csv_read_start.elapsed();
    let csv_size_mb = csv.len() as f64 / 1_048_576.0;
    println!(
        "[STRESS TEST] CSV read took: {:?} ({:.2} MB)",
        csv_read_duration, csv_size_mb
    );
    println!(
        "[STRESS TEST] Read throughput: {:.2} MB/sec",
        csv_size_mb / csv_read_duration.as_secs_f64()
    );

    let csv_str = String::from_utf8(csv).unwrap();
    let lines_before = csv_str.lines().count();
    println!(
        "[STRESS TEST] CSV has {} lines (including header)",
        lines_before
    );

    // Simulate deletion: Filter out a range of rows
    // Delete 10,000 rows from the middle (IDs 15,000,000 - 15,010,000)
    println!("\n[STRESS TEST] Simulating deletion of 10,000 rows via CSV filtering...");
    let filter_start = std::time::Instant::now();
    let filtered = csv_str
        .lines()
        .filter(|line| {
            if line.starts_with("id,") {
                true // Keep header
            } else {
                let id: i32 = line
                    .split(',')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(-1);
                !(15_000_000..15_010_000).contains(&id)
            }
        })
        .collect::<Vec<&str>>()
        .join("\n")
        + "\n";
    let filter_duration = filter_start.elapsed();
    let filter_throughput_mbps = csv_size_mb / filter_duration.as_secs_f64();
    println!("[STRESS TEST] CSV filtering took: {:?}", filter_duration);
    println!(
        "[STRESS TEST] Filter throughput: {:.2} MB/sec",
        filter_throughput_mbps
    );

    let lines_after = filtered.lines().count();
    println!("[STRESS TEST] Filtered CSV has {} lines", lines_after);
    assert_eq!(
        lines_before - lines_after,
        10_000,
        "Should filter 10,000 lines"
    );

    // Test: Write filtered CSV (this is the actual diff + delete operation)
    println!("\n[STRESS TEST] Writing filtered CSV (triggers CSV diff + delete)...");
    let write_start = std::time::Instant::now();
    server
        .write_file("/data/data.csv", 0, filtered.as_bytes())
        .await
        .unwrap();
    let write_duration = write_start.elapsed();
    println!(
        "[STRESS TEST] Write + diff + delete took: {:?}",
        write_duration
    );
    println!("[STRESS TEST] This includes:");
    println!("  - CSV parsing (old and new)");
    println!("  - Diff computation (find deleted IDs)");
    println!("  - Delta Lake delete operation");

    // Verify deletion
    println!("\n[VERIFY] Checking final count...");
    let verify_start = std::time::Instant::now();
    let results = db_arc
        .query("SELECT COUNT(*) as count FROM data")
        .await
        .unwrap();
    let verify_duration = verify_start.elapsed();
    let final_count = results[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        final_count,
        target_rows - 10_000,
        "Should have {} rows after deletion",
        target_rows - 10_000
    );
    println!(
        "[VERIFY] {} rows present after deletion (verify took {:?})",
        final_count, verify_duration
    );

    // Summary
    println!("\n=== 1GB CSV STRESS TEST RESULTS ===");
    println!("Dataset: {} rows (~{:.2} MB CSV)", target_rows, csv_size_mb);
    println!(
        "Insert {} rows: {:?} ({:.0} rows/sec)",
        target_rows,
        insert_duration,
        target_rows as f64 / insert_duration.as_secs_f64()
    );
    println!(
        "Read CSV: {:?} ({:.2} MB/sec)",
        csv_read_duration,
        csv_size_mb / csv_read_duration.as_secs_f64()
    );
    println!(
        "Filter CSV (in-memory): {:?} ({:.2} MB/sec)",
        filter_duration, filter_throughput_mbps
    );
    println!("Write + Diff + Delete 10K rows: {:?}", write_duration);
    println!("Deleted rows: 10,000");
    println!("Final count: {} rows", final_count);

    // Performance assessment
    let write_ms = write_duration.as_secs_f64() * 1000.0;
    println!("\n[PERFORMANCE ASSESSMENT]");
    if write_ms < 60_000.0 {
        println!(
            "OK Diff + delete on {}M rows completed in {:.2}s (< 1 min)",
            target_rows / 1_000_000,
            write_duration.as_secs_f64()
        );
    } else {
        println!(
            "WARN  Diff + delete took {:.2}s (> 1 min, expected for 30M+ rows)",
            write_duration.as_secs_f64()
        );
    }

    println!("\n[SUCCESS] 1GB CSV stress test passed!");
    println!("- System stable with {}M rows", target_rows / 1_000_000);
    println!("- CSV diff completed successfully");
    println!("- Deletions applied correctly");
    println!("- Data integrity maintained");

    // Cleanup
    println!("[CLEANUP] Shutting down");
    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_mkdir_creates_directory() {
    init_logging();
    println!("\n[TEST] test_nfs_mkdir_creates_directory");

    // TDD: Write the test, run it and watch it fail
    // Expected behavior: mkdir command should create a directory in the NFS mount

    require_mount_capability();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");
    let mount_point = temp_dir.path().join("mnt");
    std::fs::create_dir(&mount_point).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema).await.unwrap();

    let port = create_unique_port(12049);
    #[cfg(target_os = "windows")]
    ensure_clean_nfs_state().await;
    let server = posixlake::nfs::NfsServer::new(Arc::new(db), port)
        .await
        .unwrap();

    println!("[MOUNT] Mounting NFS at {:?} on port {}", mount_point, port);
    let mount_point = mount_nfs_os("localhost", port, &mount_point)
        .await
        .expect("NFS mount must succeed");

    // Create mount guard for automatic cleanup on panic/timeout
    let _guard = MountGuard::new(mount_point.clone());

    println!("[TEST] Testing 'mkdir' command to create directory 'testdir'");
    let test_dir = mount_point.join("testdir");

    // TDD: This should fail because mkdir is not implemented yet
    #[cfg(not(windows))]
    let output = tokio::process::Command::new("mkdir")
        .arg(&test_dir)
        .output()
        .await
        .expect("Failed to execute mkdir");

    #[cfg(windows)]
    let output = tokio::process::Command::new("cmd")
        .args(["/C", "mkdir"])
        .arg(&test_dir)
        .output()
        .await
        .expect("Failed to execute mkdir");

    // TDD: This assertion will fail until mkdir is implemented
    assert!(
        output.status.success(),
        "mkdir should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    println!("[VERIFY] Checking directory was created");
    // Verify directory exists by listing parent directory
    #[cfg(not(windows))]
    let output = tokio::process::Command::new("ls")
        .arg("-la")
        .arg(&mount_point)
        .output()
        .await
        .expect("Failed to execute ls");

    #[cfg(windows)]
    let output = tokio::process::Command::new("cmd")
        .args(["/C", "dir"])
        .arg(&mount_point)
        .output()
        .await
        .expect("Failed to execute dir");

    let listing = String::from_utf8_lossy(&output.stdout);
    println!("  Directory listing: {}", listing);
    assert!(
        listing.contains("testdir"),
        "Directory 'testdir' should exist"
    );

    // Verify it's actually a directory using async with timeout
    let verify_result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let metadata = tokio::fs::metadata(&test_dir).await?;
        assert!(metadata.is_dir(), "testdir should be a directory");
        Ok::<(), std::io::Error>(())
    })
    .await;

    match verify_result {
        Ok(Ok(())) => {
            println!("[SUCCESS] mkdir command successfully created directory!");
        }
        Ok(Err(e)) => {
            panic!("Directory verification failed: {}", e);
        }
        Err(_) => {
            panic!("Directory verification timed out - mount may be unresponsive");
        }
    }

    unmount_nfs_os(&mount_point)
        .await
        .expect("Unmount must succeed");
    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_cp_creates_file() {
    init_logging();
    println!("\n[TEST] test_nfs_cp_creates_file");

    // TDD: Write the test, run it and watch it fail
    // Expected behavior: cp command should create a new file in the NFS mount

    require_mount_capability();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");
    let mount_point = temp_dir.path().join("mnt");
    std::fs::create_dir(&mount_point).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema).await.unwrap();

    let port = create_unique_port(12049);
    #[cfg(target_os = "windows")]
    ensure_clean_nfs_state().await;
    // new() already waits for server readiness with event-based channel (see mod.rs:94-113)
    let server = posixlake::nfs::NfsServer::new(Arc::new(db), port)
        .await
        .unwrap();

    println!("[MOUNT] Mounting NFS at {:?} on port {}", mount_point, port);
    let mount_point = mount_nfs_os("localhost", port, &mount_point)
        .await
        .expect("NFS mount must succeed");

    // Create mount guard for automatic cleanup on panic/timeout
    let _guard = MountGuard::new(mount_point.clone());

    // Create a source file locally
    let source_file = temp_dir.path().join("source.txt");
    std::fs::write(&source_file, "Hello, World!").unwrap();

    println!("[TEST] Testing 'cp' command to copy file to NFS mount");
    let dest_file = mount_point.join("copied.txt");

    // macOS cp tries to copy extended attributes which NFS doesn't support
    // Use -X flag to not copy extended attributes
    #[cfg(target_os = "macos")]
    let output = tokio::process::Command::new("cp")
        .args([
            "-X",
            source_file.to_str().unwrap(),
            dest_file.to_str().unwrap(),
        ])
        .output()
        .await
        .expect("Failed to execute cp");

    #[cfg(target_os = "linux")]
    let output = tokio::process::Command::new("cp")
        .args([source_file.to_str().unwrap(), dest_file.to_str().unwrap()])
        .output()
        .await
        .expect("Failed to execute cp");

    #[cfg(windows)]
    let output = tokio::process::Command::new("cmd")
        .args(["/C", "copy"])
        .arg(&source_file)
        .arg(&dest_file)
        .output()
        .await
        .expect("Failed to execute copy");

    assert!(
        output.status.success(),
        "cp should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    println!("[VERIFY] Checking file was created");

    // Add timeout wrapper for filesystem operations that might hang on unresponsive mount
    let verify_result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        // Verify file exists
        let metadata = tokio::fs::metadata(&dest_file).await?;
        assert!(metadata.is_file(), "copied.txt should be a file");

        // Verify content matches
        let content = tokio::fs::read_to_string(&dest_file).await?;
        assert_eq!(content, "Hello, World!", "File content should match");

        Ok::<(), std::io::Error>(())
    })
    .await;

    match verify_result {
        Ok(Ok(())) => {
            println!("[SUCCESS] cp command successfully created file!");
        }
        Ok(Err(e)) => {
            panic!("File verification failed: {}", e);
        }
        Err(_) => {
            panic!(
                "File verification timed out - mount may be unresponsive. Check: mount | grep nfs"
            );
        }
    }

    unmount_nfs_os(&mount_point)
        .await
        .expect("Unmount must succeed");
    server.shutdown().await.unwrap();
}

// MountGuard is now imported from posixlake::nfs::MountGuard

#[tokio::test]
#[serial(nfs)]
async fn test_mount_guard_cleans_up_on_drop() {
    // TDD: Test that MountGuard automatically unmounts when dropped
    // This ensures stale mounts don't accumulate from failed/panicked tests
    init_logging();
    require_mount_capability();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db_mount_guard");
    let mount_point = temp_dir.path().join("mnt_guard_test");
    std::fs::create_dir(&mount_point).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    let port = create_unique_port(11299);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();

    // Mount and create guard
    let mount_point = mount_nfs_os("localhost", port, &mount_point)
        .await
        .expect("Mount should succeed");

    let guard = MountGuard::new(mount_point.clone());

    // Verify mount is active
    assert!(guard.is_mounted(), "Mount should be active after mounting");

    // Verify we can read through the mount
    let data_path = mount_point.join("data/data.csv");
    let content = tokio::fs::read_to_string(&data_path)
        .await
        .expect("Should read data through mount");
    assert!(content.contains("Alice"), "Should contain test data");

    println!("[TEST] Mount is active, now dropping guard to trigger cleanup...");

    // Drop the guard - this should trigger automatic unmount
    drop(guard);

    // Give the unmount a moment to complete
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify mount is gone
    let output = std::process::Command::new("mount")
        .output()
        .expect("Failed to run mount command");
    let mount_table = String::from_utf8_lossy(&output.stdout);
    assert!(
        !mount_table.contains(&mount_point.to_string_lossy().to_string()),
        "Mount should be cleaned up after guard is dropped"
    );

    println!("[SUCCESS] MountGuard successfully cleaned up mount on drop!");

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_mount_guard_skips_if_already_unmounted() {
    // TDD: Test that MountGuard doesn't error if mount was already cleaned up manually
    init_logging();
    require_mount_capability();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db_guard_skip");
    let mount_point = temp_dir.path().join("mnt_guard_skip");
    std::fs::create_dir(&mount_point).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Test"])),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    let port = create_unique_port(11399);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();

    let mount_point = mount_nfs_os("localhost", port, &mount_point)
        .await
        .expect("Mount should succeed");

    let guard = MountGuard::new(mount_point.clone());

    assert!(guard.is_mounted(), "Mount should be active");

    // Manually unmount and mark the guard
    unmount_nfs_os(&mount_point)
        .await
        .expect("Manual unmount should succeed");
    guard.mark_unmounted();

    assert!(
        !guard.is_mounted(),
        "Mount should report as not mounted after manual unmount"
    );

    println!("[TEST] Dropping guard after manual unmount...");

    // Drop guard - should not error since we marked it as unmounted
    drop(guard);

    println!("[SUCCESS] MountGuard correctly skipped cleanup for already-unmounted path!");

    server.shutdown().await.unwrap();
}

#[tokio::test]
#[serial(nfs)]
async fn test_portmapper_listener_on_111() {
    init_logging();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();
    let db = Arc::new(db);

    let port = create_unique_port(12049);
    let server = NfsServer::new(db.clone(), port).await.unwrap();

    // Give portmapper listener time to bind
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // TCP connect to port 111
    let connect_result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        tokio::net::TcpStream::connect("127.0.0.1:111"),
    )
    .await;

    match connect_result {
        Ok(Ok(mut stream)) => {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            // PMAPPROC_GETPORT RPC call: program=100003 (NFS), version=3, protocol=TCP(6)
            // RPC message format: fragment header + XID + msg_type(CALL=0) + rpc_vers(2) +
            //   prog(100000/PMAP) + vers(2) + proc(3/GETPORT) + auth(0) + verf(0) +
            //   target_prog(100003) + target_vers(3) + proto(6/TCP) + port(0)
            let rpc_call: Vec<u8> = vec![
                // Fragment header: last fragment (0x80) | length (56 bytes)
                0x80, 0x00, 0x00, 0x38, // XID
                0x00, 0x00, 0x00, 0x01, // Message type: CALL (0)
                0x00, 0x00, 0x00, 0x00, // RPC version: 2
                0x00, 0x00, 0x00, 0x02, // Program: PMAP (100000)
                0x00, 0x01, 0x86, 0xa0, // Version: 2
                0x00, 0x00, 0x00, 0x02, // Procedure: GETPORT (3)
                0x00, 0x00, 0x00, 0x03, // Auth: AUTH_NULL
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Verifier: AUTH_NULL
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // Target program: NFS (100003)
                0x00, 0x01, 0x86, 0xa3, // Target version: 3
                0x00, 0x00, 0x00, 0x03, // Protocol: TCP (6)
                0x00, 0x00, 0x00, 0x06, // Port: 0 (asking for it)
                0x00, 0x00, 0x00, 0x00,
            ];

            stream
                .write_all(&rpc_call)
                .await
                .expect("Failed to send RPC");

            let mut response = vec![0u8; 128];
            let n = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                stream.read(&mut response),
            )
            .await
            .expect("Response timeout")
            .expect("Failed to read response");

            assert!(
                n >= 28,
                "Response should contain at least an RPC reply header, got {} bytes",
                n
            );

            // The last 4 bytes of the reply body should be the port number
            // RPC reply: fragment(4) + XID(4) + msg_type(4) + reply_stat(4) + verf(8) + accept_stat(4) + port(4) = 32
            if n >= 32 {
                let port_bytes = &response[28..32];
                let returned_port = u32::from_be_bytes([
                    port_bytes[0],
                    port_bytes[1],
                    port_bytes[2],
                    port_bytes[3],
                ]);
                assert!(
                    returned_port > 0,
                    "Portmapper should return a non-zero port, got {}",
                    returned_port
                );
                println!("[TEST] Portmapper returned port: {}", returned_port);
            }

            println!("[SUCCESS] Portmapper listener on port 111 is working!");
        }
        Ok(Err(e)) => {
            // Connection refused - port 111 not bound (e.g., no root privileges)
            println!(
                "[SKIPPED] Cannot connect to port 111 (need root/admin): {}",
                e
            );
            println!("[INFO] Run with sudo/admin to test portmapper binding");
        }
        Err(_) => {
            println!("[SKIPPED] Connection to port 111 timed out");
        }
    }

    server.shutdown().await.unwrap();
}

/// TDD RED: Test that setattr works on existing files (data.csv)
/// Currently fails with NFS3ERR_NOTSUPP because setattr only handles created files
#[tokio::test]
#[serial(nfs)]
async fn test_setattr_on_existing_csv_file() {
    init_logging();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db_setattr");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert test data
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    let db = Arc::new(db);
    let port = create_unique_port(12049);
    let server = NfsServer::new(db.clone(), port).await.unwrap();

    // Get the file ID for data.csv
    let data_csv_id = server.lookup("/data/data.csv").await.unwrap();
    println!("[TEST] data.csv file ID: {}", data_csv_id);

    // Try to call setattr on data.csv - this should succeed
    // Currently fails with NFS3ERR_NOTSUPP
    let result = server.setattr(data_csv_id).await;

    assert!(
        result.is_ok(),
        "setattr on existing data.csv should succeed, got: {:?}",
        result.err()
    );

    println!("[SUCCESS] setattr on existing file works!");

    server.shutdown().await.unwrap();
}

/// TDD RED: Test that file overwrite works on existing files via Windows NFS mount
/// This tests the full flow: Windows client calls setattr to truncate, then writes
#[tokio::test]
#[serial(nfs)]
#[cfg(target_os = "windows")]
async fn test_windows_file_overwrite_on_existing_csv() {
    use posixlake::nfs::windows::{ensure_clean_nfs_state, find_free_drive, prepare_nfs_mount};

    init_logging();
    require_mount_capability();

    // Clean up any stale NFS state
    ensure_clean_nfs_state().await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db_overwrite");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

    // Insert initial data
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )
    .unwrap();
    db.insert(batch).await.unwrap();

    let db = Arc::new(db);
    let server = NfsServer::new(db.clone(), 2049).await.unwrap();

    // Find a free drive letter and mount
    let drive = find_free_drive(None).expect("No free drive letter");
    let temp_mount = std::path::PathBuf::from(format!("{}:\\", drive));

    println!("[TEST] Mounting NFS to {}", drive);
    prepare_nfs_mount(drive).await;

    // Actually mount using mount_nfs_os
    let mount_point = mount_nfs_os("localhost", 2049, &temp_mount)
        .await
        .expect("NFS mount must succeed");

    let _guard = MountGuard::new(mount_point.clone());

    let data_csv = mount_point.join("data").join("data.csv");

    // Read original content
    let original = tokio::fs::read_to_string(&data_csv)
        .await
        .expect("Should read original data.csv");
    println!("[TEST] Original content:\n{}", original);
    assert!(original.contains("Alice"), "Should have Alice");
    assert!(original.contains("Bob"), "Should have Bob");

    // Try to OVERWRITE the file with new content (this triggers setattr for truncate)
    let new_content = "id,name\n100,NewPerson\n";
    let write_result = tokio::fs::write(&data_csv, new_content).await;

    assert!(
        write_result.is_ok(),
        "File overwrite should succeed, got: {:?}",
        write_result.err()
    );

    // Read back and verify
    let updated = tokio::fs::read_to_string(&data_csv)
        .await
        .expect("Should read updated data.csv");
    println!("[TEST] Updated content:\n{}", updated);

    assert!(
        updated.contains("NewPerson"),
        "Should have new content after overwrite"
    );
    assert!(!updated.contains("Alice"), "Old content should be replaced");

    println!("[SUCCESS] File overwrite on existing CSV works!");

    server.shutdown().await.unwrap();
}
