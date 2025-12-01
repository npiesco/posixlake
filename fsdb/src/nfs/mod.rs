//! NFS server implementation for POSIX interface
//! This module provides a pure Rust NFSv3 server that exposes FSDB as a filesystem
//! No external drivers required - OS uses built-in NFS client

pub mod attr_cache;
pub mod cache;
pub mod file_views;
pub mod mmap_cache;
pub mod server;
pub mod write_buffer;

use crate::database_ops::DatabaseOps;
use crate::error::{Error, Result};
use crate::nfs::cache::NfsCache;
use crate::nfs::server::FsdbFilesystem;

use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};

// Global storage for query results (simple approach for testing)
// In production, this would be per-server instance
lazy_static::lazy_static! {
    static ref QUERY_RESULTS: tokio::sync::Mutex<HashMap<String, Vec<u8>>> = tokio::sync::Mutex::const_new(HashMap::new());
}

/// NFS Server that exposes FSDB as a filesystem
/// Manages server lifecycle and provides simple API matching tests
pub struct NfsServer {
    db: Arc<DatabaseOps>,
    #[allow(dead_code)]
    port: u16,
    shutdown_tx: Option<mpsc::Sender<()>>,
    ready: bool,
    /// Cache for NFS operations (shared with filesystem)
    cache: Arc<NfsCache>,
}

impl NfsServer {
    /// Create and start a new NFS server
    pub async fn new(db: Arc<DatabaseOps>, port: u16) -> Result<Self> {
        info!(
            "Starting FSDB NFS server on port {} with caching enabled",
            port
        );

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
        let fs = FsdbFilesystem::with_cache(db.clone(), cache.clone());

        // Start the server in a background task
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        let (ready_tx, ready_rx) = mpsc::channel(1);

        tokio::spawn(async move {
            let addr = format!("127.0.0.1:{}", port);
            info!("Binding NFS server to {}", addr);

            let listener = match NFSTcpListener::bind(&addr, fs).await {
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

        // Wait for server to be ready
        let mut ready_rx = ready_rx;
        match tokio::time::timeout(std::time::Duration::from_secs(5), async {
            ready_rx.recv().await
        })
        .await
        {
            Ok(Some(())) => {
                info!("NFS server is ready");
                Ok(Self {
                    db,
                    port,
                    shutdown_tx: Some(shutdown_tx),
                    ready: true,
                    cache,
                })
            }
            Ok(None) => Err(Error::InvalidOperation(
                "NFS server failed to start".to_string(),
            )),
            Err(_) => Err(Error::InvalidOperation(
                "NFS server startup timeout".to_string(),
            )),
        }
    }

    /// Check if server is ready
    pub async fn is_ready(&self) -> bool {
        self.ready
    }

    /// Shutdown the NFS server
    pub async fn shutdown(mut self) -> Result<()> {
        info!("Shutting down NFS server");
        if let Some(tx) = self.shutdown_tx.take() {
            tx.send(()).await.ok();
        }
        Ok(())
    }

    /// List exported paths (for testing)
    pub async fn list_exports(&self) -> Vec<String> {
        vec!["/fsdb".to_string()]
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

        // Create temporary filesystem to query
        let fs = FsdbFilesystem::new(self.db.clone());
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

        // Handle regular NFS files - use cached filesystem
        let fs = FsdbFilesystem::with_cache(self.db.clone(), self.cache.clone());
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

        // Handle regular files - use cached filesystem
        let fs = FsdbFilesystem::with_cache(self.db.clone(), self.cache.clone());
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
        let fs = FsdbFilesystem::new(self.db.clone());
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
}

/// File attributes for testing
pub struct FileAttributes {
    pub is_file: bool,
    pub is_dir: bool,
    pub size: u64,
    pub readable: bool,
    pub writable: bool,
}
