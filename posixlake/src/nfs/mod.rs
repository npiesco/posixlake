//! NFS server implementation for POSIX interface
//! This module provides a pure Rust NFSv3 server that exposes posixlake as a filesystem
//! No external drivers required - OS uses built-in NFS client

pub mod attr_cache;
pub mod cache;
pub mod file_views;
pub mod mmap_cache;
pub mod server;
pub mod windows;
pub mod write_buffer;

use crate::database_ops::DatabaseOps;
use crate::error::{Error, Result};
use crate::nfs::cache::NfsCache;
use crate::nfs::server::PosixLakeFilesystem;

use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

// Global storage for query results (simple approach for testing)
// In production, this would be per-server instance
lazy_static::lazy_static! {
    static ref QUERY_RESULTS: tokio::sync::Mutex<HashMap<String, Vec<u8>>> = tokio::sync::Mutex::const_new(HashMap::new());
}

/// NFS Server that exposes posixlake as a filesystem
/// Manages server lifecycle and provides simple API matching tests
pub struct NfsServer {
    db: Arc<DatabaseOps>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    portmap_shutdown_tx: Option<mpsc::Sender<()>>,
    ready: bool,
    /// Shared filesystem instance (must be same instance used by nfsserve listeners)
    filesystem: PosixLakeFilesystem,
    /// JoinHandles for spawned tasks - awaited on shutdown
    server_handle: Option<tokio::task::JoinHandle<()>>,
    portmap_handle: Option<tokio::task::JoinHandle<()>>,
}

impl NfsServer {
    /// Kill any processes listening on the specified ports (Windows only)
    #[cfg(target_os = "windows")]
    async fn kill_processes_on_ports(ports: &[u16]) {
        for port in ports {
            if let Ok(output) = tokio::process::Command::new("netstat")
                .args(["-ano"])
                .output()
                .await
            {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    let port_str = format!(":{}", port);
                    if line.contains(&port_str) && line.contains("LISTENING") {
                        if let Some(pid_str) = line.split_whitespace().last() {
                            if pid_str.chars().all(|c| c.is_ascii_digit()) && !pid_str.is_empty() {
                                let result = tokio::process::Command::new("taskkill")
                                    .args(["/F", "/PID", pid_str])
                                    .output()
                                    .await;
                                if let Ok(out) = result {
                                    if out.status.success() {
                                        info!("Killed process {} blocking port {}", pid_str, port);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Create and start a new NFS server
    pub async fn new(db: Arc<DatabaseOps>, port: u16) -> Result<Self> {
        info!(
            "Starting posixlake NFS server on port {} with caching enabled",
            port
        );

        // Kill any processes blocking NFS ports before we try to bind
        #[cfg(target_os = "windows")]
        {
            Self::kill_processes_on_ports(&[port, 111]).await;
        }

        // Create NFS cache (two-tier: moka in-memory + sled on-disk)
        // Use a unique temporary directory for the cache (includes timestamp for uniqueness)
        let cache_dir = std::env::temp_dir().join(format!(
            "fsdb_nfs_cache_{}_{}",
            port,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let cache = Arc::new(NfsCache::new(&cache_dir).await?);
        let fs = PosixLakeFilesystem::with_cache(db.clone(), cache.clone());
        // Clone for portmapper and NfsServer to share state (created_dirs, etc.)
        let portmap_fs = fs.clone();
        let server_fs = fs.clone();

        // Start the server in a background task
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        let (ready_tx, ready_rx) = mpsc::channel(1);

        let server_handle = tokio::spawn(async move {
            let addr = format!("127.0.0.1:{}", port);
            info!("Binding NFS server to {}", addr);

            let mut listener = match NFSTcpListener::bind(&addr, fs).await {
                Ok(l) => {
                    info!("NFS server bound successfully");
                    ready_tx.send(()).await.ok();
                    l
                }
                Err(e) => {
                    error!("Failed to bind NFS server: {:?}", e);
                    return;
                }
            };
            listener.with_export_name("share");

            // Run server until shutdown signal
            tokio::select! {
                result = listener.handle_forever() => {
                    if let Err(e) = result {
                        error!("NFS server error: {:?}", e);
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("NFS server received shutdown signal");
                }
            }

            info!("NFS server stopped");
        });

        // Spawn portmapper listener on port 111
        let (portmap_shutdown_tx, mut portmap_shutdown_rx) = mpsc::channel(1);
        let (portmap_ready_tx, portmap_ready_rx) = mpsc::channel::<bool>(1);
        let nfs_port = port; // Capture NFS port to tell portmapper
        let portmap_handle = tokio::spawn(async move {
            let addr = "127.0.0.1:111";
            info!("Binding portmapper listener to {}", addr);

            let mut listener = match NFSTcpListener::bind(addr, portmap_fs).await {
                Ok(l) => {
                    info!("Portmapper listener bound successfully on port 111");
                    portmap_ready_tx.send(true).await.ok();
                    l
                }
                Err(e) => {
                    warn!(
                        "Failed to bind portmapper on port 111 (needs root/admin): {:?}",
                        e
                    );
                    portmap_ready_tx.send(false).await.ok(); // Signal failure but don't block
                    return;
                }
            };
            // Tell portmapper to report NFS on the correct port
            listener.with_export_name("share");
            listener.with_nfs_port(nfs_port);

            tokio::select! {
                result = listener.handle_forever() => {
                    if let Err(e) = result {
                        error!("Portmapper listener error: {:?}", e);
                    }
                }
                _ = portmap_shutdown_rx.recv() => {
                    info!("Portmapper listener received shutdown signal");
                }
            }

            info!("Portmapper listener stopped");
        });

        // Wait for both NFS server and portmapper to be ready
        let mut ready_rx = ready_rx;
        let mut portmap_ready_rx = portmap_ready_rx;

        // Wait for NFS server first (required)
        let nfs_ready = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            ready_rx.recv().await
        })
        .await;

        match nfs_ready {
            Ok(Some(())) => {
                info!("NFS server is ready");
            }
            Ok(None) => {
                return Err(Error::InvalidOperation(
                    "NFS server failed to start".to_string(),
                ));
            }
            Err(_) => {
                return Err(Error::InvalidOperation(
                    "NFS server startup timeout".to_string(),
                ));
            }
        }

        // Wait for portmapper (optional - may fail without admin)
        let portmap_ready = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            portmap_ready_rx.recv().await
        })
        .await;

        match portmap_ready {
            Ok(Some(true)) => {
                info!("Portmapper is ready");
            }
            Ok(Some(false)) => {
                info!("Portmapper failed to bind (continuing without it)");
            }
            Ok(None) => {
                warn!("Portmapper channel closed unexpectedly");
            }
            Err(_) => {
                warn!("Portmapper startup timeout (continuing without it)");
            }
        }

        Ok(Self {
            db,
            shutdown_tx: Some(shutdown_tx),
            portmap_shutdown_tx: Some(portmap_shutdown_tx),
            ready: true,
            filesystem: server_fs,
            server_handle: Some(server_handle),
            portmap_handle: Some(portmap_handle),
        })
    }

    /// Check if server is ready
    pub async fn is_ready(&self) -> bool {
        self.ready
    }

    /// Shutdown the NFS server and wait for tasks to complete
    pub async fn shutdown(mut self) -> Result<()> {
        info!("Shutting down NFS server");

        // Send shutdown signals
        if let Some(tx) = self.shutdown_tx.take() {
            tx.send(()).await.ok();
        }
        if let Some(tx) = self.portmap_shutdown_tx.take() {
            tx.send(()).await.ok();
        }

        // Wait for tasks to complete (with timeout)
        if let Some(handle) = self.server_handle.take() {
            match tokio::time::timeout(std::time::Duration::from_secs(2), handle).await {
                Ok(Ok(())) => info!("NFS server task completed"),
                Ok(Err(e)) => error!("NFS server task panicked: {:?}", e),
                Err(_) => warn!("NFS server task shutdown timeout, aborting"),
            }
        }
        if let Some(handle) = self.portmap_handle.take() {
            match tokio::time::timeout(std::time::Duration::from_secs(2), handle).await {
                Ok(Ok(())) => info!("Portmapper task completed"),
                Ok(Err(e)) => error!("Portmapper task panicked: {:?}", e),
                Err(_) => warn!("Portmapper task shutdown timeout, aborting"),
            }
        }

        info!("NFS server shutdown complete");
        Ok(())
    }

    /// List exported paths (for testing)
    pub async fn list_exports(&self) -> Vec<String> {
        vec!["/posixlake".to_string()]
    }

    /// Read directory entries (for testing)
    pub async fn readdir(&self, path: &str) -> Result<Vec<String>> {
        // Handle special directories
        if path == "/.metadata" {
            return Ok(vec![
                "row_counts.json".to_string(),
                "file_list.json".to_string(),
            ]);
        }

        // Use shared filesystem instance
        let fs = &self.filesystem;
        use nfsserve::vfs::NFSFileSystem;

        let dirid = if path == "/" {
            1
        } else if path == "/data" {
            2
        } else {
            return Err(Error::InvalidOperation(format!("Unknown path: {}", path)));
        };

        let result = fs
            .readdir(dirid, 0, 100)
            .await
            .map_err(|e| Error::InvalidOperation(format!("readdir failed: {:?}", e)))?;

        let mut entries: Vec<String> = result
            .entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.name).to_string())
            .collect();

        // Add special files based on directory
        if path == "/" {
            entries.push(".query".to_string());
            entries.push(".stats".to_string());
            entries.push(".metadata".to_string());
            entries.push("schema.sql".to_string());
        } else if path == "/data" {
            entries.push("data.json".to_string());
            entries.push("data.jsonl".to_string());
        }

        Ok(entries)
    }

    /// Read file content (for testing)
    pub async fn read_file(&self, path: &str, offset: u64, size: u32) -> Result<Vec<u8>> {
        use crate::nfs::file_views::{JsonFileView, SchemaFile, StatsFile};

        // Handle special files that don't go through NFS filesystem layer
        match path {
            "/data/data.json" => {
                let mut view = JsonFileView::new(self.db.clone());
                return view.read(offset, size as u64).await;
            }
            "/data/data.jsonl" => {
                let mut view = JsonFileView::new_json_lines(self.db.clone());
                return view.read(offset, size as u64).await;
            }
            "/schema.sql" => {
                let view = SchemaFile::new(self.db.clone());
                return view.read(offset, size as u64).await;
            }
            "/.stats" => {
                let view = StatsFile::new(self.db.clone());
                return view.read(offset, size as u64).await;
            }
            "/.query" => {
                // Return query results from global storage
                let results = QUERY_RESULTS.lock().await;
                let content = results.get("/.query").cloned().unwrap_or_default();
                drop(results);

                let start = offset as usize;
                let end = std::cmp::min(start + size as usize, content.len());

                if start >= content.len() {
                    return Ok(Vec::new());
                }

                return Ok(content[start..end].to_vec());
            }
            _ if path.starts_with("/.metadata/") => {
                // Handle metadata directory files
                let filename = path.strip_prefix("/.metadata/").unwrap();
                match filename {
                    "row_counts.json" | "file_list.json" => {
                        let stats = StatsFile::new(self.db.clone());
                        return stats.read(offset, size as u64).await;
                    }
                    _ => {
                        return Err(Error::InvalidOperation(format!(
                            "Unknown metadata file: {}",
                            path
                        )))
                    }
                }
            }
            _ => {}
        }

        // Handle regular NFS files - use shared filesystem instance
        let fs = &self.filesystem;
        use nfsserve::vfs::NFSFileSystem;

        let fileid = if path == "/data/data.csv" {
            3
        } else if path.starts_with("/data/") && path.ends_with(".parquet") {
            // Handle individual Parquet files
            fs.refresh_parquet_files().await.map_err(|e| {
                Error::InvalidOperation(format!("Failed to refresh parquet files: {:?}", e))
            })?;

            let filename = path.strip_prefix("/data/").unwrap();
            let parquet_files = fs.parquet_files.lock().await;
            let fileid_opt = parquet_files
                .iter()
                .find(|(_id, file_path)| {
                    std::path::Path::new(file_path)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .map(|basename| basename == filename)
                        .unwrap_or(false)
                })
                .map(|(id, _)| *id);

            match fileid_opt {
                Some(id) => id,
                None => {
                    return Err(Error::InvalidOperation(format!(
                        "Parquet file not found: {}",
                        path
                    )))
                }
            }
        } else {
            return Err(Error::InvalidOperation(format!("Unknown file: {}", path)));
        };

        let (data, _eof) = fs
            .read(fileid, offset, size)
            .await
            .map_err(|e| Error::InvalidOperation(format!("read failed: {:?}", e)))?;

        Ok(data)
    }

    /// Write to file (for testing)
    pub async fn write_file(&self, path: &str, _offset: u64, data: &[u8]) -> Result<()> {
        use crate::nfs::file_views::QueryFile;

        // Handle special .query file
        if path == "/.query" {
            let mut query_file = QueryFile::new(self.db.clone());
            query_file.write_query(data).await?;

            // Read the result and store it in global storage
            let result = query_file.read(0, u64::MAX).await?;
            let mut results = QUERY_RESULTS.lock().await;
            results.insert(path.to_string(), result);
            drop(results);

            return Ok(());
        }

        // Handle regular files - use shared filesystem instance
        let fs = &self.filesystem;
        use nfsserve::vfs::NFSFileSystem;

        let fileid = if path == "/data/data.csv" {
            3
        } else {
            return Err(Error::InvalidOperation(format!("Unknown file: {}", path)));
        };

        fs.write(fileid, 0, data)
            .await
            .map_err(|e| Error::InvalidOperation(format!("write failed: {:?}", e)))?;

        Ok(())
    }

    /// Remove file (for testing) - truncates table if data.csv is deleted
    pub async fn remove_file(&self, path: &str) -> Result<()> {
        // Only support deletion of data.csv (truncate table operation)
        if path == "/data/data.csv" {
            info!("Removing {} - truncating table", path);
            // Delete all rows from the table using deletion vectors
            self.db.delete_rows_where("1=1").await?;
            Ok(())
        } else {
            Err(Error::InvalidOperation(format!(
                "File deletion not supported for: {}",
                path
            )))
        }
    }

    /// Get file attributes (for testing)
    pub async fn getattr(&self, path: &str) -> Result<FileAttributes> {
        let fs = &self.filesystem;
        use nfsserve::nfs::ftype3;
        use nfsserve::vfs::NFSFileSystem;

        let fileid = if path == "/" {
            1
        } else if path == "/data" {
            2
        } else if path == "/data/data.csv" {
            3
        } else if path.starts_with("/data/") && path.ends_with(".parquet") {
            // Handle individual Parquet files
            // First, populate the Parquet files cache
            fs.refresh_parquet_files().await.map_err(|e| {
                Error::InvalidOperation(format!("Failed to refresh parquet files: {:?}", e))
            })?;

            // Extract filename from path
            let filename = path.strip_prefix("/data/").unwrap();

            // Find the fileid for this Parquet file
            let parquet_files = fs.parquet_files.lock().await;
            let fileid_opt = parquet_files
                .iter()
                .find(|(_id, file_path)| {
                    // Extract basename from full path
                    std::path::Path::new(file_path)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .map(|basename| basename == filename)
                        .unwrap_or(false)
                })
                .map(|(id, _)| *id);

            match fileid_opt {
                Some(id) => id,
                None => {
                    return Err(Error::InvalidOperation(format!(
                        "Parquet file not found: {}",
                        path
                    )))
                }
            }
        } else {
            return Err(Error::InvalidOperation(format!("Unknown path: {}", path)));
        };

        let attr = fs
            .getattr(fileid)
            .await
            .map_err(|e| Error::InvalidOperation(format!("getattr failed: {:?}", e)))?;

        Ok(FileAttributes {
            is_file: matches!(attr.ftype, ftype3::NF3REG),
            is_dir: matches!(attr.ftype, ftype3::NF3DIR),
            size: attr.size,
            readable: true,
            writable: true,
        })
    }

    /// Lookup file ID by path (for testing)
    pub async fn lookup(&self, path: &str) -> Result<u64> {
        let fs = &self.filesystem;

        let fileid = if path == "/" {
            1
        } else if path == "/data" {
            2
        } else if path == "/data/data.csv" {
            3
        } else if path.starts_with("/data/") && path.ends_with(".parquet") {
            fs.refresh_parquet_files().await.map_err(|e| {
                Error::InvalidOperation(format!("Failed to refresh parquet files: {:?}", e))
            })?;

            let filename = path.strip_prefix("/data/").unwrap();
            let parquet_files = fs.parquet_files.lock().await;
            parquet_files
                .iter()
                .find(|(_id, file_path)| {
                    std::path::Path::new(file_path)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .map(|basename| basename == filename)
                        .unwrap_or(false)
                })
                .map(|(id, _)| *id)
                .ok_or_else(|| {
                    Error::InvalidOperation(format!("Parquet file not found: {}", path))
                })?
        } else {
            return Err(Error::InvalidOperation(format!("Unknown path: {}", path)));
        };

        Ok(fileid)
    }

    /// Set file attributes (for testing)
    /// This tests the setattr NFS operation which is needed for file truncation/overwrite
    pub async fn setattr(&self, fileid: u64) -> Result<()> {
        use nfsserve::nfs::sattr3;
        use nfsserve::vfs::NFSFileSystem;

        let fs = &self.filesystem;

        // Create a minimal setattr request (no changes, just test the operation succeeds)
        let setattr = sattr3::default();

        fs.setattr(fileid, setattr)
            .await
            .map_err(|e| Error::InvalidOperation(format!("setattr failed: {:?}", e)))?;

        Ok(())
    }
}

/// File attributes for testing
pub struct FileAttributes {
    pub is_file: bool,
    pub is_dir: bool,
    pub size: u64,
    pub readable: bool,
    pub writable: bool,
}

/// MountGuard ensures NFS mounts are cleaned up even if code panics or times out.
/// Implements Drop to automatically unmount when the guard goes out of scope.
///
/// # Example
/// ```no_run
/// use posixlake::nfs::MountGuard;
/// use std::path::PathBuf;
///
/// let mount_point = PathBuf::from("/mnt/nfs");
/// let guard = MountGuard::new(mount_point);
/// // ... do NFS operations ...
/// // On drop (normal or panic), mount is automatically cleaned up
/// guard.mark_unmounted(); // Call if you manually unmount
/// ```
pub struct MountGuard {
    mount_point: std::path::PathBuf,
    unmounted: std::sync::atomic::AtomicBool,
}

impl MountGuard {
    /// Create a new MountGuard for the given mount point
    pub fn new(mount_point: std::path::PathBuf) -> Self {
        Self {
            mount_point,
            unmounted: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Mark as already unmounted (call this if you manually unmount)
    pub fn mark_unmounted(&self) {
        self.unmounted
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Check if this mount point is currently mounted
    pub fn is_mounted(&self) -> bool {
        if self.unmounted.load(std::sync::atomic::Ordering::SeqCst) {
            return false;
        }

        #[cfg(target_os = "windows")]
        {
            // On Windows, check if the drive letter exists
            self.mount_point.exists()
        }

        #[cfg(not(target_os = "windows"))]
        {
            // Check actual mount table on Unix
            let output = std::process::Command::new("mount")
                .output()
                .expect("Failed to run mount command");
            let mount_table = String::from_utf8_lossy(&output.stdout);
            mount_table.contains(&self.mount_point.to_string_lossy().to_string())
        }
    }

    /// Get the mount point path
    pub fn mount_point(&self) -> &std::path::Path {
        &self.mount_point
    }
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        if self.unmounted.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }

        #[cfg(target_os = "windows")]
        {
            // On Windows, mount_point is a drive letter like "Z:\"
            // Use umount command from Windows NFS client
            let drive = self.mount_point.to_string_lossy();
            let drive_letter = drive.trim_end_matches('\\');

            eprintln!(
                "[MountGuard] Cleaning up Windows NFS mount: {}",
                drive_letter
            );

            // Try umount first (Windows NFS client command)
            let status = std::process::Command::new("umount")
                .arg("-f")
                .arg(drive_letter)
                .status();

            match status {
                Ok(s) if s.success() => {
                    eprintln!("[MountGuard] Successfully unmounted: {}", drive_letter);
                }
                _ => {
                    // Fallback to net use /delete
                    let fallback = std::process::Command::new("net")
                        .args(["use", drive_letter, "/delete", "/y"])
                        .status();

                    match fallback {
                        Ok(s) if s.success() => {
                            eprintln!(
                                "[MountGuard] Successfully disconnected via net use: {}",
                                drive_letter
                            );
                        }
                        Ok(s) => {
                            eprintln!(
                                "[MountGuard] Failed to unmount {} (exit code: {:?})",
                                drive_letter,
                                s.code()
                            );
                        }
                        Err(e) => {
                            eprintln!("[MountGuard] Error unmounting {}: {}", drive_letter, e);
                        }
                    }
                }
            }
        }

        #[cfg(not(target_os = "windows"))]
        {
            // Check if actually mounted before trying to unmount
            let output = std::process::Command::new("mount")
                .output()
                .expect("Failed to run mount command");
            let mount_table = String::from_utf8_lossy(&output.stdout);

            if !mount_table.contains(&self.mount_point.to_string_lossy().to_string()) {
                return;
            }

            eprintln!(
                "[MountGuard] Cleaning up stale mount: {}",
                self.mount_point.display()
            );

            // Synchronous unmount in Drop (can't use async in Drop)
            #[cfg(unix)]
            let is_root = unsafe { libc::geteuid() } == 0;

            // Use lazy unmount (-l) on Linux to avoid blocking if mount is busy
            #[cfg(target_os = "linux")]
            let unmount_args = vec!["-l", "-f"];

            #[cfg(not(target_os = "linux"))]
            let unmount_args = vec!["-f"];

            let status = if is_root {
                let mut cmd = std::process::Command::new("umount");
                for arg in &unmount_args {
                    cmd.arg(arg);
                }
                cmd.arg(&self.mount_point).status()
            } else {
                let mut cmd = std::process::Command::new("sudo");
                cmd.arg("-n").arg("umount");
                for arg in &unmount_args {
                    cmd.arg(arg);
                }
                cmd.arg(&self.mount_point).status()
            };

            match status {
                Ok(s) if s.success() => {
                    eprintln!(
                        "[MountGuard] Successfully unmounted: {}",
                        self.mount_point.display()
                    );
                }
                Ok(s) => {
                    eprintln!(
                        "[MountGuard] Failed to unmount {} (exit code: {:?})",
                        self.mount_point.display(),
                        s.code()
                    );
                }
                Err(e) => {
                    eprintln!(
                        "[MountGuard] Error unmounting {}: {}",
                        self.mount_point.display(),
                        e
                    );
                }
            }
        }
    }
}
