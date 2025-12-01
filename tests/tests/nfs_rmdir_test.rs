//! NFS rmdir (remove directory) command integration test
//! TDD: Test-driven development for POSIX rmdir command support

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
        let log_file = format!("/tmp/posixlake_nfs_rmdir_test_{}.log", std::process::id());
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
async fn test_nfs_rmdir_removes_empty_directory() {
    init_logging();
    println!("\n[TEST] test_nfs_rmdir_removes_empty_directory - TDD: Testing 'rmdir' command");

    // TDD: This test will FAIL until we implement the rmdir NFS procedure
    // Expected behavior: rmdir command should remove empty directories in the NFS mount

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

    let port = create_unique_port(14049);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();

    println!("[MOUNT] Mounting NFS at {:?} on port {}", mount_point, port);
    mount_nfs_os("localhost", port, &mount_point)
        .await
        .expect("NFS mount must succeed");

    println!("[SUCCESS] NFS mounted successfully");

    // Create a directory in the mount
    println!("[CREATE] Creating directory 'test_dir'");
    let test_dir = mount_point.join("test_dir");

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::fs::create_dir(&test_dir).await
    })
    .await
    .expect("Directory creation timed out")
    .expect("Failed to create directory");

    println!("[VERIFY] Directory created successfully");

    // Verify directory exists before removal
    let exists_before = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::fs::metadata(&test_dir).await
    })
    .await
    .expect("Metadata check timed out")
    .map(|m| m.is_dir())
    .unwrap_or(false);

    assert!(exists_before, "Directory should exist before removal");
    println!("[VERIFY] Directory exists: {:?}", test_dir);

    // TDD: Execute rmdir command - THIS WILL FAIL until rmdir is implemented
    println!("[TEST] Executing 'rmdir test_dir'");

    let output = tokio::process::Command::new("rmdir")
        .arg(&test_dir)
        .output()
        .await
        .expect("Failed to execute rmdir command");

    // TDD: This assertion will FAIL because rmdir returns NFS3ERR_NOTSUPP or similar
    assert!(
        output.status.success(),
        "rmdir command should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    println!("[SUCCESS] rmdir command executed successfully");

    // Verify the directory was removed
    println!("[VERIFY] Checking directory was removed");

    let verify_result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        // Directory should NOT exist
        let dir_exists = tokio::fs::metadata(&test_dir).await.is_ok();
        assert!(!dir_exists, "Directory should not exist after rmdir");

        Ok::<(), std::io::Error>(())
    })
    .await;

    match verify_result {
        Ok(Ok(())) => {
            println!("[SUCCESS] Directory removed successfully! rmdir command works!");
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
    println!("[SUCCESS] Test completed - rmdir command fully functional!");
}

#[tokio::test]
#[serial]
async fn test_nfs_rmdir_fails_on_non_empty_directory() {
    init_logging();
    println!(
        "\n[TEST] test_nfs_rmdir_fails_on_non_empty_directory - TDD: Testing rmdir on non-empty dir"
    );

    // TDD: rmdir should fail if directory is not empty (standard POSIX behavior)
    require_mount_capability();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");
    let mount_point = temp_dir.path().join("mnt");
    std::fs::create_dir(&mount_point).unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    let db = DatabaseOps::create(&db_path, schema).await.unwrap();

    let port = create_unique_port(14050);
    let server = NfsServer::new(Arc::new(db), port).await.unwrap();

    println!("[MOUNT] Mounting NFS for non-empty directory test");
    mount_nfs_os("localhost", port, &mount_point)
        .await
        .expect("NFS mount must succeed");

    // Create a directory with a file inside
    println!("[CREATE] Creating directory with file inside");
    let test_dir = mount_point.join("non_empty_dir");
    tokio::fs::create_dir(&test_dir)
        .await
        .expect("Failed to create directory");

    let file_inside = test_dir.join("file.txt");
    tokio::fs::write(&file_inside, "content")
        .await
        .expect("Failed to create file");

    println!("[VERIFY] Directory has file inside");

    // TDD: Execute rmdir command - should FAIL because directory is not empty
    println!("[TEST] Executing 'rmdir non_empty_dir' (should fail - directory not empty)");

    let output = tokio::process::Command::new("rmdir")
        .arg(&test_dir)
        .output()
        .await
        .expect("Failed to execute rmdir command");

    // rmdir should FAIL on non-empty directory
    assert!(
        !output.status.success(),
        "rmdir should fail on non-empty directory"
    );

    println!("[SUCCESS] rmdir correctly failed on non-empty directory");

    // Verify directory still exists
    let dir_exists = tokio::fs::metadata(&test_dir).await.is_ok();
    assert!(
        dir_exists,
        "Directory should still exist after failed rmdir"
    );

    unmount_nfs_os(&mount_point)
        .await
        .expect("Unmount must succeed");

    server.shutdown().await.unwrap();
    println!("[SUCCESS] Non-empty directory test completed!");
}
