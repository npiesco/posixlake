//! Windows NFS client helpers
//!
//! Utilities for managing Windows NFS client state, including service restart
//! and stale mount cleanup. Required for reliable NFS operation on Windows.

#[cfg(target_os = "windows")]
use std::path::Path;
#[cfg(target_os = "windows")]
use std::time::Duration;
#[cfg(target_os = "windows")]
use tokio::process::Command;
#[cfg(target_os = "windows")]
use tracing::info;

/// Mount options for Windows NFS client.
///
/// These options are required for reliable operation with posixlake's NFS server:
/// - anon: anonymous access
/// - nolock: disable NFS locking (not implemented)
/// - mtype=soft: soft mount for better error handling
/// - fileaccess=6: read/write access
/// - lang=ansi: ANSI character encoding
/// - rsize/wsize=128: small buffer sizes for compatibility
/// - timeout=60: 60 second timeout
/// - retry=2: retry twice on failure
pub const MOUNT_OPTIONS: &str =
    "anon,nolock,mtype=soft,fileaccess=6,lang=ansi,rsize=128,wsize=128,timeout=60,retry=2";

/// Kill any processes listening on NFS-related ports.
///
/// Uses netstat to find processes on ports 2049 (NFS) and 111 (portmapper),
/// then kills them with taskkill. Verifies ports are free after killing.
/// Returns true if ports are confirmed free.
#[cfg(target_os = "windows")]
pub async fn kill_processes_on_nfs_ports() -> bool {
    let ports = [2049u16, 111u16];

    // Try up to 3 times to ensure ports are free
    for attempt in 0..3 {
        let mut any_in_use = false;

        for port in ports {
            if let Ok(output) = Command::new("netstat").args(["-ano"]).output().await {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    let port_str = format!(":{}", port);
                    if line.contains(&port_str) && line.contains("LISTENING") {
                        any_in_use = true;
                        if let Some(pid_str) = line.split_whitespace().last() {
                            if pid_str.chars().all(|c| c.is_ascii_digit()) && !pid_str.is_empty() {
                                let _ = Command::new("taskkill")
                                    .args(["/F", "/PID", pid_str])
                                    .output()
                                    .await;
                                info!(
                                    "Killed PID {} on port {} (attempt {})",
                                    pid_str,
                                    port,
                                    attempt + 1
                                );
                            }
                        }
                    }
                }
            }
        }

        if !any_in_use {
            return true; // Ports are free
        }

        // Wait for OS to release the ports
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Final verification
    for port in ports {
        if let Ok(output) = Command::new("netstat").args(["-ano"]).output().await {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let port_str = format!(":{}", port);
            if stdout
                .lines()
                .any(|l| l.contains(&port_str) && l.contains("LISTENING"))
            {
                info!("Warning: Port {} still in use after kill attempts", port);
                return false;
            }
        }
    }

    true
}

#[cfg(not(target_os = "windows"))]
pub async fn kill_processes_on_nfs_ports() -> bool {
    false // No-op on non-Windows
}

/// Wait for NFS ports to be free (without killing processes).
/// Returns true if ports are free within timeout.
#[cfg(target_os = "windows")]
pub async fn wait_for_ports_free(timeout_secs: u64) -> bool {
    let ports = [2049u16, 111u16];
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout {
        let mut all_free = true;
        for port in ports {
            if let Ok(output) = Command::new("netstat").args(["-ano"]).output().await {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let port_str = format!(":{}", port);
                if stdout
                    .lines()
                    .any(|l| l.contains(&port_str) && l.contains("LISTENING"))
                {
                    all_free = false;
                    break;
                }
            }
        }
        if all_free {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

#[cfg(not(target_os = "windows"))]
pub async fn wait_for_ports_free(_timeout_secs: u64) -> bool {
    true
}

/// Clean up after a test completes. Call this after server shutdown
/// to clear Windows NFS client cached state before the next test.
#[cfg(target_os = "windows")]
pub async fn cleanup_after_test() {
    info!("Cleaning up after test...");
    // Wait for our server to fully release the port
    if !wait_for_ports_free(5).await {
        info!("Warning: Ports not free after shutdown, forcing cleanup");
        kill_processes_on_nfs_ports().await;
    }
    // Restart NFS client services to clear any cached connection state
    restart_nfs_services().await;
}

#[cfg(not(target_os = "windows"))]
pub async fn cleanup_after_test() {
    // No-op on non-Windows
}

/// Restart Windows NFS client services to clear stale state.
///
/// This stops and restarts NfsClnt and NfsRdr services to ensure
/// a clean state before mounting. Requires admin privileges.
#[cfg(target_os = "windows")]
pub async fn restart_nfs_services() {
    info!("Restarting Windows NFS client services...");

    let _ = Command::new("sc").args(["stop", "NfsClnt"]).output().await;
    let _ = Command::new("sc").args(["stop", "NfsRdr"]).output().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let _ = Command::new("sc").args(["start", "NfsRdr"]).output().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let start_result = Command::new("sc").args(["start", "NfsClnt"]).output().await;
    if let Ok(out) = &start_result {
        let stdout = String::from_utf8_lossy(&out.stdout);
        let first_line = stdout.lines().next().unwrap_or("");
        if first_line.contains("FAILED") {
            // 1056 = already running, which is fine
            info!("NfsClnt start result: {}", first_line);
        } else {
            info!("NfsClnt started successfully");
        }
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
}

#[cfg(not(target_os = "windows"))]
pub async fn restart_nfs_services() {
    // No-op on non-Windows
}

/// Clean up any stale NFS mount on the specified drive letter.
///
/// Attempts umount and net use /delete to ensure the drive is free.
#[cfg(target_os = "windows")]
pub async fn cleanup_stale_mount(drive_letter: char) {
    info!("Cleaning up drive {}:", drive_letter);

    let drive = format!("{}:", drive_letter);

    let umount_result = Command::new("umount").args(["-f", &drive]).output().await;
    if let Ok(out) = &umount_result {
        if !out.status.success() {
            info!("umount {}: failed (expected if not mounted)", drive_letter);
        } else {
            info!("umount {}: success", drive_letter);
        }
    }

    let net_result = Command::new("net")
        .args(["use", &drive, "/delete", "/y"])
        .output()
        .await;
    if let Ok(out) = &net_result {
        if !out.status.success() {
            info!(
                "net use /delete {}: failed (expected if not mapped)",
                drive_letter
            );
        } else {
            info!("net use /delete {}: success", drive_letter);
        }
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[cfg(not(target_os = "windows"))]
pub async fn cleanup_stale_mount(_drive_letter: char) {
    // No-op on non-Windows
}

/// Ensure Windows NFS client is in a clean state.
///
/// Restarts NFS services to clear cached connection state, then kills any
/// processes on NFS ports. Call this BEFORE starting an NFS server to ensure
/// no stale state from previous sessions.
///
/// Returns true if ports are confirmed free after cleanup.
#[cfg(target_os = "windows")]
pub async fn ensure_clean_nfs_state() -> bool {
    info!("Ensuring clean NFS state...");
    restart_nfs_services().await;
    kill_processes_on_nfs_ports().await
}

#[cfg(not(target_os = "windows"))]
pub async fn ensure_clean_nfs_state() -> bool {
    true // No-op on non-Windows
}

/// Prepare Windows NFS client for a fresh mount.
///
/// Restarts services and cleans up the specified drive letter.
#[cfg(target_os = "windows")]
pub async fn prepare_nfs_mount(drive_letter: char) {
    restart_nfs_services().await;
    cleanup_stale_mount(drive_letter).await;
}

#[cfg(not(target_os = "windows"))]
pub async fn prepare_nfs_mount(_drive_letter: char) {
    // No-op on non-Windows
}

/// Find a free drive letter for NFS mount.
///
/// Prefers the specified letter if available, otherwise searches Z down to D.
/// Skips Y (often used for other purposes).
#[cfg(target_os = "windows")]
pub fn find_free_drive(preferred: Option<char>) -> Option<char> {
    if let Some(pref) = preferred {
        if !Path::new(&format!("{}:\\", pref)).exists() {
            return Some(pref);
        }
    }

    ('D'..='Z')
        .rev()
        .filter(|&c| c != 'Y')
        .find(|c| !Path::new(&format!("{}:\\", c)).exists())
}

#[cfg(not(target_os = "windows"))]
pub fn find_free_drive(_preferred: Option<char>) -> Option<char> {
    None // Not applicable on non-Windows
}
