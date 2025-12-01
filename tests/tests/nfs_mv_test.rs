//! NFS mv (rename) command integration test
//! TDD: Test-driven development for POSIX mv command support

use arrow::datatypes::{DataType, Field, Schema};
use posixlake::DatabaseOps;
use posixlake::nfs::NfsServer;
use serial_test::serial;
use std::path::Path;
use std::sync::Arc;
use std::sync::Once;
use tempfile::TempDir;

static INIT: Once = Once::new();

fn init_logging() {
    INIT.call_once(|| {
        let log_file = format!("/tmp/posixlake_nfs_mv_test_{}.log", std::process::id());
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
    let thread_id = format!("{:?}", std::thread::current().id());
    let hash: u16 = thread_id.bytes().map(|b| b as u16).sum::<u16>() % 10000;
    base_port + hash
}

fn check_can_mount() -> bool {
    #[cfg(unix)]
    {
        let is_root = unsafe { libc::geteuid() } == 0;
        if is_root {
            return true;
        }
    }

    let username = std::env::var("USER").unwrap_or_else(|_| "unknown".to_string());
    let username_safe = username.replace('.', "_");
    let sudoers_file = format!("/etc/sudoers.d/posixlake-nfs-{}", username_safe);
    std::path::Path::new(&sudoers_file).exists()
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
            1. Running as root (UID 0), OR\n\
            2. Passwordless sudo configured for mount/umount operations\n\
            \n\
            To configure passwordless sudo:\n\
                python3 scripts/setup_nfs_sudo.py\n\
            ================================================================================\n\
            "
        );
    }
}

async fn mount_nfs_os(host: &str, port: u16, mount_point: &Path) -> Result<(), String> {
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
            Ok(())
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
                    "nolock,vers=3,tcp,port={},mountport={}",
                    port, port
                ))
                .arg(format!("{}:/", host))
                .arg(mount_point.as_os_str())
                .status()
                .await
                .map_err(|e| format!("Failed to execute unshare mount: {}", e))?;

            if status.success() {
                return Ok(());
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
                "nolock,vers=3,tcp,port={},mountport={}",
                port, port
            ))
            .arg(format!("{}:/", host))
            .arg(mount_point.as_os_str())
            .status()
            .await
            .map_err(|e| format!("Failed to execute mount: {}", e))?;

        if status.success() {
            Ok(())
        } else {
            Err(format!("mount failed with exit code: {:?}", status.code()))
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        Err("OS mounting not implemented for this platform".to_string())
    }
}

async fn unmount_nfs_os(mount_point: &Path) -> Result<(), String> {
    #[cfg(unix)]
    let is_root = unsafe { libc::geteuid() } == 0;

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
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

        let mut cmd = if is_root {
            tokio::process::Command::new("umount")
        } else {
            let mut c = tokio::process::Command::new("sudo");
            c.arg("-n");
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

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        Err("OS unmounting not implemented for this platform".to_string())
    }
}

#[tokio::test]
#[serial]
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
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();

    println!("[MOUNT] Mounting NFS at {:?} on port {}", mount_point, port);
    mount_nfs_os("localhost", port, &mount_point)
        .await
        .expect("NFS mount must succeed");

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

    let output = tokio::process::Command::new("mv")
        .arg(&original_file)
        .arg(&renamed_file)
        .output()
        .await
        .expect("Failed to execute mv command");

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

    unmount_nfs_os(&mount_point)
        .await
        .expect("Unmount must succeed");

    server.shutdown().await.unwrap();
    println!("[SUCCESS] Test completed - mv command fully functional!");
}

#[tokio::test]
#[serial]
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
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();

    println!("[MOUNT] Mounting NFS for directory rename test");
    mount_nfs_os("localhost", port, &mount_point)
        .await
        .expect("NFS mount must succeed");

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

    // TDD: Execute mv command on directory
    println!("[TEST] Executing 'mv old_dir new_dir'");
    let new_dir = mount_point.join("new_dir");

    let output = tokio::process::Command::new("mv")
        .arg(&old_dir)
        .arg(&new_dir)
        .output()
        .await
        .expect("Failed to execute mv command");

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

    unmount_nfs_os(&mount_point)
        .await
        .expect("Unmount must succeed");

    server.shutdown().await.unwrap();
    println!("[SUCCESS] Directory mv test completed!");
}
