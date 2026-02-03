//! NFS mv (rename) command integration test
//! TDD: Test-driven development for POSIX mv command support

use arrow::datatypes::{DataType, Field, Schema};
use posixlake::DatabaseOps;
#[cfg(target_os = "windows")]
use posixlake::nfs::windows::{
    MOUNT_OPTIONS, cleanup_after_test, ensure_clean_nfs_state, find_free_drive, prepare_nfs_mount,
};
use posixlake::nfs::{MountGuard, NfsServer};
use serial_test::serial;
use std::path::Path;
use std::sync::Arc;
use std::sync::Once;
use tempfile::TempDir;

/// Helper: If POSIXLAKE_PORT_LISTENER env var is set, act as a simple TCP listener
/// This allows the test to spawn itself as a child process to hold a port
#[cfg(target_os = "windows")]
fn maybe_run_as_port_listener() -> bool {
    if let Ok(port_str) = std::env::var("POSIXLAKE_PORT_LISTENER") {
        if let Ok(port) = port_str.parse::<u16>() {
            use std::net::TcpListener;
            if let Ok(_listener) = TcpListener::bind(format!("0.0.0.0:{}", port)) {
                // Hold the port for 60 seconds
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
        }
        return true;
    }
    false
}

/// TDD: Proves that ensure_clean_nfs_state works (restarts services + kills processes)
/// Starts a simple TCP listener on port 2049, then verifies we can kill it
#[tokio::test]
#[serial(nfs)]
#[cfg(target_os = "windows")]
async fn test_ensure_clean_nfs_state() {
    // Check if we're being run as a port listener subprocess
    if maybe_run_as_port_listener() {
        return;
    }

    // First, call the DRY function to restart services and clear state
    eprintln!("[TEST] Calling ensure_clean_nfs_state to restart services...");
    ensure_clean_nfs_state().await;

    // Check if ports are in use
    let output = tokio::process::Command::new("netstat")
        .args(["-ano"])
        .output()
        .await
        .expect("netstat failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let port_2049_in_use = stdout
        .lines()
        .any(|l| l.contains(":2049") && l.contains("LISTENING"));

    // If port is free, spawn ourselves as a child process to hold the port
    let _child = if !port_2049_in_use {
        eprintln!("[TEST] Port 2049 free, spawning child listener...");
        let exe = std::env::current_exe().expect("Failed to get current exe");
        let child = std::process::Command::new(&exe)
            .env("POSIXLAKE_PORT_LISTENER", "2049")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("Failed to spawn child listener");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Some(child)
    } else {
        None
    };

    // Verify port is now in use
    let output = tokio::process::Command::new("netstat")
        .args(["-ano"])
        .output()
        .await
        .expect("netstat failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let port_2049_in_use = stdout
        .lines()
        .any(|l| l.contains(":2049") && l.contains("LISTENING"));
    eprintln!("[TEST] Port 2049 in use before kill: {}", port_2049_in_use);
    assert!(port_2049_in_use, "Port 2049 should be in use");

    // Kill processes on NFS ports using prod function
    let killed = ensure_clean_nfs_state().await;
    eprintln!("[TEST] Clean state result: {}", killed);

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Verify ports are now free
    let output = tokio::process::Command::new("netstat")
        .args(["-ano"])
        .output()
        .await
        .expect("netstat failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let port_2049_in_use = stdout
        .lines()
        .any(|l| l.contains(":2049") && l.contains("LISTENING"));

    eprintln!("[TEST] Port 2049 in use after kill: {}", port_2049_in_use);
    assert!(
        !port_2049_in_use,
        "Port 2049 should be FREE after killing processes"
    );
}

/// TDD: Proves that RENAME operation is actually received by the NFS server
/// This test will FAIL if Windows refuses to send RENAME for directories
#[tokio::test]
#[serial(nfs)]
async fn test_single_filesystem_instance_for_all_operations() {
    init_logging();
    require_mount_capability();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");
    let mount_point = temp_dir.path().join("mnt");
    std::fs::create_dir(&mount_point).unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let db = DatabaseOps::create(&db_path, schema).await.unwrap();

    // Kill stale processes before starting server
    #[cfg(target_os = "windows")]
    ensure_clean_nfs_state().await;

    let server = NfsServer::new(Arc::new(db), 2049).await.unwrap();

    // Mount
    let mount_point = mount_nfs_os("localhost", 2049, &mount_point, Some('X'))
        .await
        .expect("Mount must succeed");
    let _guard = MountGuard::new(mount_point.clone());

    // Create directory
    let old_dir = mount_point.join("tdd_old_dir");
    tokio::fs::create_dir(&old_dir)
        .await
        .expect("MKDIR must succeed");

    // Verify directory exists
    let metadata = tokio::fs::metadata(&old_dir).await;
    assert!(metadata.is_ok(), "Directory must exist after MKDIR");
    assert!(
        metadata.unwrap().is_dir(),
        "Created path should be a directory"
    );

    // TDD: Attempt to rename the directory
    // This will FAIL if Windows doesn't send NFS RENAME operation
    let new_dir = mount_point.join("tdd_new_dir");

    #[cfg(unix)]
    let output = tokio::process::Command::new("mv")
        .arg(&old_dir)
        .arg(&new_dir)
        .output()
        .await
        .expect("Failed to execute mv command");

    #[cfg(windows)]
    let output = tokio::process::Command::new("cmd")
        .args(["/C", "move"])
        .arg(&old_dir)
        .arg(&new_dir)
        .output()
        .await
        .expect("Failed to execute move command");

    // TDD: This assertion will FAIL if RENAME is not supported
    assert!(
        output.status.success(),
        "RENAME must succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify rename worked
    assert!(
        tokio::fs::metadata(&old_dir).await.is_err(),
        "Old directory should not exist after rename"
    );
    assert!(
        tokio::fs::metadata(&new_dir).await.is_ok(),
        "New directory should exist after rename"
    );

    // Cleanup
    drop(_guard);
    server.shutdown().await.unwrap();
    #[cfg(target_os = "windows")]
    cleanup_after_test().await;
}

static INIT: Once = Once::new();

fn init_logging() {
    INIT.call_once(|| {
        let log_file = std::env::temp_dir()
            .join(format!("posixlake_nfs_mv_test_{}.log", std::process::id()))
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

fn check_can_mount() -> bool {
    #[cfg(unix)]
    {
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
    std::process::Command::new("sudo")
        .args(["-n", "mount", "--help"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn require_mount_capability() {
    if !check_can_mount() {
        panic!(
            "\n\n\
            ================================================================================\n\
            ERROR: Cannot run mount commands!\n\
            ================================================================================\n\
            \n\
            NFS mount tests require either:\n\
            1. (Unix) Running as root (UID 0), OR\n\
            2. (Unix) Passwordless sudo configured for mount/umount operations, OR\n\
            3. (Windows) NFS Client feature enabled\n\
            \n\
            Unix:  python3 scripts/setup_nfs_sudo.py\n\
            Windows (admin PowerShell):\n\
                Enable-WindowsOptionalFeature -Online -FeatureName ServicesForNFS-ClientOnly\n\
            ================================================================================\n\
            "
        );
    }
}

async fn mount_nfs_os(
    host: &str,
    port: u16,
    mount_point: &Path,
    preferred_drive: Option<char>,
) -> Result<std::path::PathBuf, String> {
    #[cfg(not(target_os = "windows"))]
    let _ = preferred_drive; // Only used on Windows

    #[cfg(unix)]
    let is_root = unsafe { libc::geteuid() } == 0;

    #[cfg(target_os = "macos")]
    {
        let mut cmd = if is_root {
            tokio::process::Command::new("mount_nfs")
        } else {
            let mut c = tokio::process::Command::new("sudo");
            c.arg("-n");
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
        let in_container = std::path::Path::new("/.dockerenv").exists()
            || std::path::Path::new("/run/.containerenv").exists();

        if in_container && is_root {
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

        let mut cmd = if is_root {
            tokio::process::Command::new("mount")
        } else {
            let mut c = tokio::process::Command::new("sudo");
            c.arg("-n");
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
        let letter = find_free_drive(preferred_drive)
            .ok_or_else(|| "No free drive letter found".to_string())?;
        eprintln!("[MOUNT] Selected drive letter: {}:", letter);

        // Use prod helper to restart services and cleanup stale mount
        prepare_nfs_mount(letter).await;

        // Mount with options from nfsserve README for Windows
        let output = tokio::process::Command::new("mount")
            .arg("-o")
            .arg(MOUNT_OPTIONS)
            .arg(format!("\\\\{}\\share", host))
            .arg(format!("{}:", letter))
            .output()
            .await
            .map_err(|e| format!("Failed to execute mount: {}", e))?;

        if output.status.success() {
            Ok(std::path::PathBuf::from(format!("{}:\\", letter)))
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            Err(format!(
                "mount failed (exit {:?}): stdout={}, stderr={}",
                output.status.code(),
                stdout.trim(),
                stderr.trim()
            ))
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        Err("OS mounting not implemented for this platform".to_string())
    }
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_mv_rename_file() {
    init_logging();
    println!("\n[TEST] test_nfs_mv_rename_file - TDD: Testing 'mv' command");

    // TDD: This test will FAIL until we implement the rename NFS procedure
    // Expected behavior: mv command should rename files in the NFS mount

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

    let port = create_unique_port(13049);
    #[cfg(target_os = "windows")]
    ensure_clean_nfs_state().await;
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();

    println!("[MOUNT] Mounting NFS at {:?} on port {}", mount_point, port);
    let mount_point = mount_nfs_os("localhost", port, &mount_point, Some('Z'))
        .await
        .expect("NFS mount must succeed");

    // Create mount guard for automatic cleanup on panic/timeout
    let _guard = MountGuard::new(mount_point.clone());

    println!("[SUCCESS] NFS mounted successfully");

    // Create a file in the mount
    println!("[CREATE] Creating test file 'original.txt'");
    let original_file = mount_point.join("original.txt");

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::fs::write(&original_file, "Test content for mv").await
    })
    .await
    .expect("File creation timed out")
    .expect("Failed to create original file");

    println!("[VERIFY] File created successfully");

    // Verify file exists before rename
    let exists_before = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::fs::metadata(&original_file).await
    })
    .await
    .expect("Metadata check timed out")
    .is_ok();

    assert!(exists_before, "Original file should exist before rename");
    println!("[VERIFY] Original file exists: {:?}", original_file);

    // TDD: Execute mv command - THIS WILL FAIL until rename is implemented
    println!("[TEST] Executing 'mv original.txt renamed.txt'");
    let renamed_file = mount_point.join("renamed.txt");

    #[cfg(unix)]
    let output = tokio::process::Command::new("mv")
        .arg(&original_file)
        .arg(&renamed_file)
        .output()
        .await
        .expect("Failed to execute mv command");

    #[cfg(windows)]
    let output = tokio::process::Command::new("cmd")
        .args(["/C", "move"])
        .arg(&original_file)
        .arg(&renamed_file)
        .output()
        .await
        .expect("Failed to execute move command");

    // TDD: This assertion will FAIL because rename returns NFS3ERR_NOTSUPP
    assert!(
        output.status.success(),
        "mv command should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    println!("[SUCCESS] mv command executed successfully");

    // Verify the rename worked
    println!("[VERIFY] Checking file was renamed");

    let verify_result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        // Original file should NOT exist
        let original_exists = tokio::fs::metadata(&original_file).await.is_ok();
        assert!(
            !original_exists,
            "Original file should not exist after rename"
        );

        // Renamed file SHOULD exist
        let renamed_metadata = tokio::fs::metadata(&renamed_file).await?;
        assert!(
            renamed_metadata.is_file(),
            "Renamed file should exist and be a file"
        );

        // Content should be preserved
        let content = tokio::fs::read_to_string(&renamed_file).await?;
        assert_eq!(
            content, "Test content for mv",
            "File content should be preserved after rename"
        );

        Ok::<(), std::io::Error>(())
    })
    .await;

    match verify_result {
        Ok(Ok(())) => {
            println!("[SUCCESS] File renamed successfully! mv command works!");
        }
        Ok(Err(e)) => {
            panic!("File verification failed: {}", e);
        }
        Err(_) => {
            panic!("File verification timed out - mount may be unresponsive");
        }
    }

    // MountGuard handles unmount on drop
    drop(_guard);

    server.shutdown().await.unwrap();
    #[cfg(target_os = "windows")]
    cleanup_after_test().await;
    println!("[SUCCESS] Test completed - mv command fully functional!");
}

#[tokio::test]
#[serial(nfs)]
async fn test_nfs_mv_rename_directory() {
    init_logging();
    println!("\n[TEST] test_nfs_mv_rename_directory - TDD: Testing 'mv' on directories");

    // TDD: Test renaming directories through NFS mount
    require_mount_capability();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");
    let mount_point = temp_dir.path().join("mnt");
    std::fs::create_dir(&mount_point).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create(&db_path, schema).await.unwrap();

    let port = create_unique_port(13050);
    #[cfg(target_os = "windows")]
    ensure_clean_nfs_state().await;
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();

    println!("[MOUNT] Mounting NFS for directory rename test");
    // Use different drive letter (Y:) than file test (Z:) to avoid Windows NFS client cache issues
    let mount_point = mount_nfs_os("localhost", port, &mount_point, Some('W')) // Use W: (file test uses Z:)
        .await
        .expect("NFS mount must succeed");

    // Create mount guard for automatic cleanup on panic/timeout
    let _guard = MountGuard::new(mount_point.clone());

    // Create a directory in the mount
    println!("[CREATE] Creating directory 'old_dir'");
    let old_dir = mount_point.join("old_dir");

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::fs::create_dir(&old_dir).await
    })
    .await
    .expect("Directory creation timed out")
    .expect("Failed to create directory");

    println!("[VERIFY] Directory created");

    // Verify directory exists before rename
    let exists_before = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::fs::metadata(&old_dir).await
    })
    .await
    .expect("Metadata check timed out")
    .map(|m| m.is_dir())
    .unwrap_or(false);

    assert!(
        exists_before,
        "Original directory should exist before rename"
    );

    // TDD: Execute rename on directory
    println!("[TEST] Executing 'mv old_dir new_dir'");
    let new_dir = mount_point.join("new_dir");

    // Log paths to verify they're on same mount
    println!("[DEBUG] old_dir path: {:?}", old_dir);
    println!("[DEBUG] new_dir path: {:?}", new_dir);
    println!("[DEBUG] mount_point path: {:?}", mount_point);

    // Verify source exists before rename
    match std::fs::metadata(&old_dir) {
        Ok(m) => println!("[DEBUG] old_dir exists, is_dir={}", m.is_dir()),
        Err(e) => println!("[DEBUG] old_dir metadata error: {}", e),
    }

    // Use platform-specific move command (tokio::fs::rename fails on Windows NFS mounts
    // because Windows returns CrossesDevices at VFS layer without making NFS calls)
    #[cfg(unix)]
    let output = tokio::process::Command::new("mv")
        .arg(&old_dir)
        .arg(&new_dir)
        .output()
        .await
        .expect("Failed to execute mv command");

    #[cfg(windows)]
    let output = tokio::process::Command::new("cmd")
        .args(["/C", "move"])
        .arg(&old_dir)
        .arg(&new_dir)
        .output()
        .await
        .expect("Failed to execute move command");

    // TDD: This should succeed after implementing rename
    assert!(
        output.status.success(),
        "mv command on directory should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    println!("[SUCCESS] mv command executed on directory");

    // Verify the directory rename worked
    let verify_result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        // Old directory should NOT exist
        let old_exists = tokio::fs::metadata(&old_dir).await.is_ok();
        assert!(!old_exists, "Old directory should not exist after rename");

        // New directory SHOULD exist
        let new_metadata = tokio::fs::metadata(&new_dir).await?;
        assert!(
            new_metadata.is_dir(),
            "New directory should exist and be a directory"
        );

        Ok::<(), std::io::Error>(())
    })
    .await;

    match verify_result {
        Ok(Ok(())) => {
            println!("[SUCCESS] Directory renamed successfully!");
        }
        Ok(Err(e)) => {
            panic!("Directory verification failed: {}", e);
        }
        Err(_) => {
            panic!("Directory verification timed out");
        }
    }

    // MountGuard handles unmount on drop
    drop(_guard);

    server.shutdown().await.unwrap();
    #[cfg(target_os = "windows")]
    cleanup_after_test().await;
    println!("[SUCCESS] Directory mv test completed!");
}
