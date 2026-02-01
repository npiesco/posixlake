//! NFS Server implementation for posixlake
//! Exposes database as NFSv3 filesystem with CSV file views

use crate::database_ops::DatabaseOps;
use crate::nfs::attr_cache::AttrCache;
use crate::nfs::cache::NfsCache;
use crate::nfs::file_views::CsvFileView;

use async_trait::async_trait;
use nfsserve::{
    nfs::{fattr3, fileid3, filename3, ftype3, nfsstat3, nfstime3, sattr3, specdata3},
    vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities},
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

/// Metadata for created files including stable timestamps
#[derive(Clone, Debug)]
struct FileMetadata {
    file_id: fileid3,
    content: Vec<u8>,
    atime: nfstime3,
    mtime: nfstime3,
    ctime: nfstime3,
}

// File ID constants
const ROOT_ID: fileid3 = 1;
const DATA_DIR_ID: fileid3 = 2;
const DATA_CSV_ID: fileid3 = 3;
const PARQUET_FILE_ID_START: fileid3 = 100;
const CREATED_DIR_START: fileid3 = 1000;
const CREATED_FILE_START: fileid3 = 2000;

/// posixlake NFS Filesystem
/// Maps database operations to NFS file operations
#[derive(Clone)]
pub struct PosixLakeFilesystem {
    db: Arc<DatabaseOps>,
    /// Cache of Parquet file IDs to paths
    pub(crate) parquet_files: Arc<Mutex<HashMap<fileid3, String>>>,
    /// Track created directories: (parent_dir_id, dir_name) -> dir_id
    created_dirs: Arc<Mutex<HashMap<(fileid3, String), fileid3>>>,
    /// Unique instance ID for debugging (shared across clones via Arc)
    instance_id: Arc<u64>,
    /// Next available directory ID
    next_dir_id: Arc<Mutex<fileid3>>,
    /// Track created files: (parent_dir_id, file_name) -> FileMetadata
    created_files: Arc<Mutex<HashMap<(fileid3, String), FileMetadata>>>,
    /// Next available file ID
    next_file_id: Arc<Mutex<fileid3>>,
    /// Two-tier cache (memory + disk) for file content
    cache: Option<Arc<NfsCache>>,
    /// Attribute cache to prevent mount disconnections during concurrent writes
    attr_cache: Arc<AttrCache>,
}

impl PosixLakeFilesystem {
    pub fn new(db: Arc<DatabaseOps>) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let instance_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        info!("Creating PosixLakeFilesystem instance {}", instance_id);
        Self {
            db,
            parquet_files: Arc::new(Mutex::new(HashMap::new())),
            created_dirs: Arc::new(Mutex::new(HashMap::new())),
            next_dir_id: Arc::new(Mutex::new(CREATED_DIR_START)),
            created_files: Arc::new(Mutex::new(HashMap::new())),
            next_file_id: Arc::new(Mutex::new(CREATED_FILE_START)),
            cache: None,
            attr_cache: Arc::new(AttrCache::new()),
            instance_id: Arc::new(instance_id),
        }
    }

    /// Create a new filesystem with caching enabled
    pub fn with_cache(db: Arc<DatabaseOps>, cache: Arc<NfsCache>) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let instance_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        info!("Creating PosixLakeFilesystem instance {} with cache", instance_id);
        Self {
            db,
            parquet_files: Arc::new(Mutex::new(HashMap::new())),
            created_dirs: Arc::new(Mutex::new(HashMap::new())),
            next_dir_id: Arc::new(Mutex::new(CREATED_DIR_START)),
            created_files: Arc::new(Mutex::new(HashMap::new())),
            next_file_id: Arc::new(Mutex::new(CREATED_FILE_START)),
            cache: Some(cache),
            attr_cache: Arc::new(AttrCache::new()),
            instance_id: Arc::new(instance_id),
        }
    }

    /// Get current timestamp for file attributes
    fn now() -> nfstime3 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        nfstime3 {
            seconds: now.as_secs() as u32,
            nseconds: now.subsec_nanos(),
        }
    }

    /// Create directory attributes
    fn dir_attr(id: fileid3) -> fattr3 {
        let now = Self::now();
        fattr3 {
            ftype: ftype3::NF3DIR,
            mode: 0o777, // World-writable so Windows anonymous NFS user can rename
            nlink: 2,
            uid: 0, // Match Windows NFS client auth user UID (-2 as unsigned)
            gid: 0, // Windows anonymous user GID (-2 as unsigned)
            size: 4096,
            used: 4096,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: id,
            atime: now,
            mtime: now,
            ctime: now,
        }
    }

    /// Create file attributes
    fn file_attr(id: fileid3, size: u64) -> fattr3 {
        let now = Self::now();
        fattr3 {
            ftype: ftype3::NF3REG,
            mode: 0o666,
            nlink: 1,
            uid: 0, // Match Windows NFS client auth
            gid: 0,
            size,
            used: size,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: id,
            atime: now,
            mtime: now,
            ctime: now,
        }
    }

    /// Refresh Parquet file cache (Delta Lake mode)
    pub(crate) async fn refresh_parquet_files(&self) -> std::result::Result<(), nfsstat3> {
        // For Delta Lake, scan the base directory for parquet files
        // Delta Lake stores parquet files in the root table directory
        let mut parquet_files = self.parquet_files.lock().await;
        parquet_files.clear();

        let base_path = self.db.base_path();

        // Scan for .parquet files in the base directory
        match std::fs::read_dir(base_path) {
            Ok(entries) => {
                let mut file_id = PARQUET_FILE_ID_START;

                for entry in entries.flatten() {
                    let path = entry.path();

                    // Check if it's a parquet file
                    if path.is_file()
                        && path.extension().and_then(|s| s.to_str()) == Some("parquet")
                    {
                        if let Some(filename) = path.file_name() {
                            let filename_str = filename.to_string_lossy().to_string();
                            parquet_files.insert(file_id, filename_str);
                            file_id += 1;
                        }
                    }
                }

                info!(
                    "NFS: Found {} parquet files in Delta Lake table",
                    parquet_files.len()
                );
                Ok(())
            }
            Err(e) => {
                error!("Failed to read Delta Lake directory: {}", e);
                Err(nfsstat3::NFS3ERR_IO)
            }
        }
    }
}

#[async_trait]
impl NFSFileSystem for PosixLakeFilesystem {
    fn root_dir(&self) -> fileid3 {
        ROOT_ID
    }

    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }

    async fn lookup(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> std::result::Result<fileid3, nfsstat3> {
        let name = String::from_utf8_lossy(filename.as_ref());
        info!("NFS LOOKUP: dir={}, filename={}", dirid, name);

        match dirid {
            ROOT_ID => {
                if name == "data" {
                    Ok(DATA_DIR_ID)
                } else {
                    // Check created directories
                    let created_dirs = self.created_dirs.lock().await;
                    debug!("NFS LOOKUP [inst={}]: created_dirs has {} entries: {:?}",
                           self.instance_id,
                           created_dirs.len(),
                           created_dirs.keys().collect::<Vec<_>>());
                    if let Some(&dir_id) = created_dirs.get(&(ROOT_ID, name.to_string())) {
                        debug!("NFS LOOKUP: found dir '{}' with id {}", name, dir_id);
                        return Ok(dir_id);
                    }
                    drop(created_dirs);
                    // Check created files
                    let created_files = self.created_files.lock().await;
                    if let Some(metadata) = created_files.get(&(ROOT_ID, name.to_string())) {
                        return Ok(metadata.file_id);
                    }
                    Err(nfsstat3::NFS3ERR_NOENT)
                }
            }
            DATA_DIR_ID => {
                if name == "data.csv" {
                    Ok(DATA_CSV_ID)
                } else {
                    // Check created directories
                    let created_dirs = self.created_dirs.lock().await;
                    if let Some(&dir_id) = created_dirs.get(&(DATA_DIR_ID, name.to_string())) {
                        return Ok(dir_id);
                    }
                    drop(created_dirs);
                    // Check created files
                    let created_files = self.created_files.lock().await;
                    if let Some(metadata) = created_files.get(&(DATA_DIR_ID, name.to_string())) {
                        return Ok(metadata.file_id);
                    }
                    drop(created_files);

                    // Check Parquet files
                    self.refresh_parquet_files().await?;
                    let parquet_files = self.parquet_files.lock().await;

                    for (id, path) in parquet_files.iter() {
                        let basename = std::path::Path::new(path)
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or(path);
                        if basename == name {
                            return Ok(*id);
                        }
                    }

                    Err(nfsstat3::NFS3ERR_NOENT)
                }
            }
            id if id >= CREATED_DIR_START => {
                // This is a created directory, check its children
                let created_dirs = self.created_dirs.lock().await;
                // Check if this directory exists (it's a created directory)
                let dir_exists = created_dirs.values().any(|&did| did == id);
                if dir_exists {
                    // Check if this directory has a child directory with the given name
                    if let Some(&child_id) = created_dirs.get(&(id, name.to_string())) {
                        return Ok(child_id);
                    }
                    drop(created_dirs);
                    // Check if this directory has a child file with the given name
                    let created_files = self.created_files.lock().await;
                    if let Some(metadata) = created_files.get(&(id, name.to_string())) {
                        return Ok(metadata.file_id);
                    }
                }
                Err(nfsstat3::NFS3ERR_NOENT)
            }
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }

    async fn getattr(&self, id: fileid3) -> std::result::Result<fattr3, nfsstat3> {
        info!("NFS GETATTR: id={}", id);

        // Check attribute cache first
        if let Some(cached_attr) = self.attr_cache.get(id).await {
            return Ok(cached_attr);
        }

        // Cache miss - compute attributes
        let attr = match id {
            ROOT_ID => Self::dir_attr(ROOT_ID),
            DATA_DIR_ID => Self::dir_attr(DATA_DIR_ID),
            id if id >= CREATED_DIR_START => {
                // Check if this is a created directory
                let created_dirs = self.created_dirs.lock().await;
                let exists = created_dirs.values().any(|&did| did == id);
                drop(created_dirs);
                if exists {
                    Self::dir_attr(id)
                } else {
                    return Err(nfsstat3::NFS3ERR_NOENT);
                }
            }
            DATA_CSV_ID => {
                // Call size() without holding the lock across await
                let db = self.db.clone();
                let view = CsvFileView::new(db);
                let size = match view.size().await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to get CSV size: {}", e);
                        0
                    }
                };
                Self::file_attr(DATA_CSV_ID, size)
            }
            id if (PARQUET_FILE_ID_START..CREATED_DIR_START).contains(&id) => {
                // Parquet file (Delta Lake mode)
                let parquet_files = self.parquet_files.lock().await;
                if let Some(file_path) = parquet_files.get(&id) {
                    // Get file size directly from filesystem
                    let full_path = self.db.base_path().join(file_path);
                    if let Ok(metadata) = std::fs::metadata(&full_path) {
                        Self::file_attr(id, metadata.len())
                    } else {
                        error!("File not found: {}", file_path);
                        return Err(nfsstat3::NFS3ERR_NOENT);
                    }
                } else {
                    return Err(nfsstat3::NFS3ERR_NOENT);
                }
            }
            id if id >= CREATED_FILE_START => {
                // Created file - use stored timestamps for stability
                let created_files = self.created_files.lock().await;
                if let Some(metadata) = created_files.values().find(|m| m.file_id == id) {
                    let size = metadata.content.len() as u64;
                    let attr = fattr3 {
                        ftype: ftype3::NF3REG,
                        mode: 0o666,
                        nlink: 1,
                        uid: 0, // Match Windows NFS client auth
                        gid: 0,
                        size,
                        used: size,
                        rdev: specdata3::default(),
                        fsid: 0,
                        fileid: id,
                        atime: metadata.atime,
                        mtime: metadata.mtime,
                        ctime: metadata.ctime,
                    };
                    drop(created_files);
                    attr
                } else {
                    return Err(nfsstat3::NFS3ERR_NOENT);
                }
            }
            _ => return Err(nfsstat3::NFS3ERR_NOENT),
        };

        // Store in cache
        self.attr_cache.set(id, attr).await;

        Ok(attr)
    }

    async fn setattr(&self, id: fileid3, setattr: sattr3) -> std::result::Result<fattr3, nfsstat3> {
        info!("NFS SETATTR: id={}, setattr={:?}", id, setattr);

        // For created files, acknowledge setattr but preserve stable timestamps
        if id >= CREATED_FILE_START {
            let created_files = self.created_files.lock().await;
            if let Some(metadata) = created_files.values().find(|m| m.file_id == id) {
                let size = metadata.content.len() as u64;
                // Return current attributes with stable timestamps
                let attr = fattr3 {
                    ftype: ftype3::NF3REG,
                    mode: 0o666, // Ignore mode changes for simplicity
                    nlink: 1,
                    uid: 0, // Match Windows NFS client auth
                    gid: 0,
                    size,
                    used: size,
                    rdev: specdata3::default(),
                    fsid: 0,
                    fileid: id,
                    atime: metadata.atime,
                    mtime: metadata.mtime,
                    ctime: metadata.ctime,
                };
                drop(created_files);

                // Update cache with stable attributes
                self.attr_cache.set(id, attr).await;

                info!("SETATTR succeeded for file ID {}", id);
                return Ok(attr);
            }
        }

        // For other files (data.csv, directories), not supported
        info!("SETATTR not supported for ID {}", id);
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> std::result::Result<(Vec<u8>, bool), nfsstat3> {
        info!("NFS READ: id={}, offset={}, count={}", id, offset, count);

        match id {
            DATA_CSV_ID => {
                // Try cache first if enabled
                if let Some(ref cache) = self.cache {
                    if let Ok(Some(cached_content)) = cache.get("csv:data").await {
                        info!("Cache HIT for data.csv");
                        let end = (offset + count as u64).min(cached_content.len() as u64) as usize;
                        let start = offset.min(cached_content.len() as u64) as usize;
                        let data = cached_content[start..end].to_vec();
                        let eof = end >= cached_content.len();
                        return Ok((data, eof));
                    }
                }

                // Cache miss or no cache - generate content without holding lock
                let db = self.db.clone();
                let view = CsvFileView::new(db);

                let data = view.read(offset, count).await.map_err(|e| {
                    error!("Read error: {}", e);
                    nfsstat3::NFS3ERR_IO
                })?;

                // Store in cache if enabled (only on first read, offset==0)
                if offset == 0 {
                    if let Some(ref cache) = self.cache {
                        if let Ok(full_content) = view.get_full_content().await {
                            let _ = cache.insert("csv:data".to_string(), full_content).await;
                        }
                    }
                }

                let size = view.size().await.unwrap_or(0);
                let eof = offset + data.len() as u64 >= size;
                Ok((data, eof))
            }
            id if id >= CREATED_FILE_START => {
                // Read created file
                let created_files = self.created_files.lock().await;
                if let Some(metadata) = created_files.values().find(|m| m.file_id == id) {
                    let offset_usize = offset as usize;
                    let count_usize = count as usize;
                    let end = (offset_usize + count_usize).min(metadata.content.len());
                    let start = offset_usize.min(metadata.content.len());
                    let data = if start < metadata.content.len() {
                        metadata.content[start..end].to_vec()
                    } else {
                        Vec::new()
                    };
                    let eof = end >= metadata.content.len();
                    drop(created_files);
                    Ok((data, eof))
                } else {
                    Err(nfsstat3::NFS3ERR_NOENT)
                }
            }
            id if (PARQUET_FILE_ID_START..CREATED_DIR_START).contains(&id) => {
                // Read individual Parquet file as CSV
                let file_path = {
                    let parquet_files = self.parquet_files.lock().await;
                    parquet_files
                        .get(&id)
                        .ok_or(nfsstat3::NFS3ERR_NOENT)?
                        .clone()
                };
                let cache_key = format!("csv:file:{}", file_path);

                // Try cache first if enabled
                if let Some(ref cache) = self.cache {
                    if let Ok(Some(cached_content)) = cache.get(&cache_key).await {
                        info!("Cache HIT for {}", file_path);
                        let end = (offset + count as u64).min(cached_content.len() as u64) as usize;
                        let start = offset.min(cached_content.len() as u64) as usize;
                        let data = cached_content[start..end].to_vec();
                        let eof = end >= cached_content.len();
                        return Ok((data, eof));
                    }
                }

                // Cache miss - generate content
                let file_view = CsvFileView::new_for_file(self.db.clone(), file_path.clone());
                let data = file_view.read(offset, count).await.map_err(|e| {
                    error!("Read error for Parquet file: {}", e);
                    nfsstat3::NFS3ERR_IO
                })?;

                // Store in cache if enabled (only on first read)
                if offset == 0 {
                    if let Some(ref cache) = self.cache {
                        if let Ok(full_content) = file_view.get_full_content().await {
                            let _ = cache.insert(cache_key, full_content).await;
                        }
                    }
                }

                let size = file_view.size().await.unwrap_or(0);
                let eof = offset + data.len() as u64 >= size;
                Ok((data, eof))
            }
            _ => Err(nfsstat3::NFS3ERR_ISDIR),
        }
    }

    async fn write(
        &self,
        id: fileid3,
        offset: u64,
        data: &[u8],
    ) -> std::result::Result<fattr3, nfsstat3> {
        info!("NFS WRITE: id={}, offset={}, data_len={}", id, offset, data.len());

        match id {
            DATA_CSV_ID => {
                // Fetch cached content BEFORE invalidating (for performance)
                let cached_content = if let Some(ref cache) = self.cache {
                    match cache.get("csv:data").await {
                        Ok(Some(content)) => {
                            info!("Using cached CSV for write diff ({} bytes)", content.len());
                            Some(content)
                        }
                        _ => {
                            debug!("No cached CSV available for write diff");
                            None
                        }
                    }
                } else {
                    None
                };

                // Don't hold lock across await - create temporary view
                let db = self.db.clone();
                let view = CsvFileView::new(db);

                // Handle partial writes (append) by combining with existing content
                let write_data = if offset > 0 {
                    // Get current content
                    let current = match &cached_content {
                        Some(c) => c.clone(),
                        None => view.generate_csv().await.map_err(|e| {
                            error!("Failed to get current content for append: {}", e);
                            nfsstat3::NFS3ERR_IO
                        })?,
                    };

                    let offset_usize = offset as usize;
                    if offset_usize <= current.len() {
                        // Combine: content before offset + new data
                        let mut combined = current[..offset_usize].to_vec();
                        combined.extend_from_slice(data);
                        info!("Append write: existing {} bytes + {} new bytes at offset {}",
                              current.len(), data.len(), offset);
                        combined
                    } else {
                        // Offset beyond current content - pad with zeros (unusual case)
                        let mut combined = current.clone();
                        combined.resize(offset_usize, 0);
                        combined.extend_from_slice(data);
                        combined
                    }
                } else {
                    // Offset 0 - treat as complete write
                    data.to_vec()
                };

                view.apply_write(&write_data, cached_content).await.map_err(|e| {
                    error!("Write error: {}", e);
                    nfsstat3::NFS3ERR_IO
                })?;

                // UPDATE content cache after write (don't invalidate!)
                // This keeps subsequent reads fast by avoiding CSV regeneration
                if let Some(ref cache) = self.cache {
                    // Generate fresh CSV content after write
                    let fresh_csv = view.generate_csv().await.map_err(|e| {
                        error!("Failed to generate CSV for cache: {}", e);
                        nfsstat3::NFS3ERR_IO
                    })?;
                    let fresh_size = fresh_csv.len();

                    // Update cache with new content
                    if let Err(e) = cache.insert("csv:data".to_string(), fresh_csv).await {
                        error!("Failed to update cache after write: {}", e);
                        // Don't fail the write if cache update fails
                    } else {
                        info!(
                            "Content cache UPDATED for data.csv after write ({} bytes)",
                            fresh_size
                        );
                    }
                }

                // IMPORTANT: Update attr_cache with NEW file size after write
                // We must return accurate file size or OS NFS clients will truncate reads!
                let size = view.size().await.unwrap_or(0);
                let attr = Self::file_attr(DATA_CSV_ID, size);
                self.attr_cache.set(DATA_CSV_ID, attr).await;
                info!(
                    "Write completed, attr cache updated with new size: {} bytes",
                    size
                );

                Ok(attr)
            }
            id if id >= CREATED_FILE_START => {
                // Write to created file - preserve timestamps
                let mut created_files = self.created_files.lock().await;
                if let Some(metadata) = created_files.values_mut().find(|m| m.file_id == id) {
                    // Replace content
                    metadata.content = data.to_vec();
                    let size = metadata.content.len() as u64;

                    // Update mtime only (preserve atime and ctime for stability)
                    metadata.mtime = Self::now();

                    // Create attributes with stable timestamps
                    let attr = fattr3 {
                        ftype: ftype3::NF3REG,
                        mode: 0o666,
                        nlink: 1,
                        uid: 0, // Match Windows NFS client auth
                        gid: 0,
                        size,
                        used: size,
                        rdev: specdata3::default(),
                        fsid: 0,
                        fileid: id,
                        atime: metadata.atime,
                        mtime: metadata.mtime,
                        ctime: metadata.ctime,
                    };
                    drop(created_files);

                    // Update attributes cache
                    self.attr_cache.set(id, attr).await;

                    info!(
                        "Write completed to created file ID {}, size: {} bytes",
                        id, size
                    );
                    Ok(attr)
                } else {
                    Err(nfsstat3::NFS3ERR_NOENT)
                }
            }
            _ => Err(nfsstat3::NFS3ERR_ROFS),
        }
    }

    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        _attr: sattr3,
    ) -> std::result::Result<(fileid3, fattr3), nfsstat3> {
        let name = String::from_utf8_lossy(filename.as_ref());
        info!("NFS CREATE: dir={}, filename={}", dirid, name);

        // Only allow creating files in root, data directory, or created directories
        if dirid != ROOT_ID && dirid != DATA_DIR_ID && dirid < CREATED_DIR_START {
            error!("create not allowed in directory {}", dirid);
            return Err(nfsstat3::NFS3ERR_NOTDIR);
        }

        // Don't allow creating data.csv (it's special)
        if dirid == DATA_DIR_ID && name == "data.csv" {
            error!("Cannot create data.csv - it's a special file");
            return Err(nfsstat3::NFS3ERR_EXIST);
        }

        // Check if file already exists
        let created_files = self.created_files.lock().await;
        let key = (dirid, name.to_string());
        if created_files.contains_key(&key) {
            error!("File {} already exists in parent {}", name, dirid);
            return Err(nfsstat3::NFS3ERR_EXIST);
        }
        drop(created_files);

        // Allocate new file ID
        let mut next_id = self.next_file_id.lock().await;
        let new_file_id = *next_id;
        *next_id += 1;
        drop(next_id);

        // Create stable timestamps for the new file
        let now = Self::now();
        let metadata = FileMetadata {
            file_id: new_file_id,
            content: Vec::new(),
            atime: now,
            mtime: now,
            ctime: now,
        };

        // Register the new file with metadata
        let mut created_files = self.created_files.lock().await;
        created_files.insert((dirid, name.to_string()), metadata.clone());
        drop(created_files);

        // Create file attributes using stored timestamps
        let attr = fattr3 {
            ftype: ftype3::NF3REG,
            mode: 0o666,
            nlink: 1,
            uid: 0, // Match Windows NFS client auth
            gid: 0,
            size: 0,
            used: 0,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: new_file_id,
            atime: metadata.atime,
            mtime: metadata.mtime,
            ctime: metadata.ctime,
        };

        // Cache attributes
        self.attr_cache.set(new_file_id, attr).await;

        info!(
            "Created file {} with ID {} in parent {}",
            name, new_file_id, dirid
        );
        Ok((new_file_id, attr))
    }

    async fn create_exclusive(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
    ) -> std::result::Result<fileid3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> std::result::Result<(fileid3, fattr3), nfsstat3> {
        let name = String::from_utf8_lossy(dirname.as_ref());
        info!("NFS MKDIR: dir={}, dirname={}", dirid, name);

        // Only allow creating directories in root or data directory
        if dirid != ROOT_ID && dirid != DATA_DIR_ID {
            error!("mkdir not allowed in directory {}", dirid);
            return Err(nfsstat3::NFS3ERR_NOTDIR);
        }

        // Check if directory already exists
        let created_dirs = self.created_dirs.lock().await;
        let key = (dirid, name.to_string());
        if created_dirs.contains_key(&key) {
            error!("Directory {} already exists in parent {}", name, dirid);
            return Err(nfsstat3::NFS3ERR_EXIST);
        }
        drop(created_dirs);

        // Allocate new directory ID
        let mut next_id = self.next_dir_id.lock().await;
        let new_dir_id = *next_id;
        *next_id += 1;
        drop(next_id);

        // Register the new directory
        let mut created_dirs = self.created_dirs.lock().await;
        created_dirs.insert((dirid, name.to_string()), new_dir_id);
        debug!(
            "NFS MKDIR [inst={}]: inserted ({}, {}) -> {}, created_dirs now has {} entries",
            self.instance_id, dirid, name, new_dir_id, created_dirs.len()
        );
        drop(created_dirs);

        // Create directory attributes
        let attr = Self::dir_attr(new_dir_id);

        // Cache attributes
        self.attr_cache.set(new_dir_id, attr).await;

        info!(
            "Created directory {} with ID {} in parent {}",
            name, new_dir_id, dirid
        );
        Ok((new_dir_id, attr))
    }

    async fn remove(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> std::result::Result<(), nfsstat3> {
        let filename_str = String::from_utf8_lossy(filename);
        info!("NFS REMOVE: dir={}, file={}", dirid, filename_str);

        // Check if it's data.csv file deletion (truncate table)
        if dirid == DATA_DIR_ID && filename_str == "data.csv" {
            info!("Deleting data.csv - truncating table");

            // Delete all rows using deletion vectors (efficient, no rewrite)
            let db = self.db.clone();
            db.delete_rows_where("1=1").await.map_err(|e| {
                error!("Failed to truncate table: {}", e);
                nfsstat3::NFS3ERR_IO
            })?;

            // Invalidate cache after deletion
            if let Some(ref cache) = self.cache {
                let _ = cache.remove("csv:data").await;
                info!("Content cache invalidated for data.csv after deletion");
            }

            info!("Table truncated successfully");
            return Ok(());
        }

        // Check if it's a directory removal (rmdir)
        let created_dirs = self.created_dirs.lock().await;
        let dir_key = (dirid, filename_str.to_string());
        if let Some(&target_dir_id) = created_dirs.get(&dir_key) {
            drop(created_dirs); // Drop lock IMMEDIATELY

            info!(
                "Removing directory {} (ID {}) from parent {}",
                filename_str, target_dir_id, dirid
            );

            // Check if directory is empty (no child directories)
            let created_dirs = self.created_dirs.lock().await;
            let has_child_dirs = created_dirs
                .keys()
                .any(|(parent_id, _)| *parent_id == target_dir_id);
            drop(created_dirs); // Drop lock IMMEDIATELY

            if has_child_dirs {
                error!(
                    "Directory {} is not empty - has child directories",
                    filename_str
                );
                return Err(nfsstat3::NFS3ERR_NOTEMPTY);
            }

            // Check if directory has child files
            let created_files = self.created_files.lock().await;
            let has_child_files = created_files
                .keys()
                .any(|(parent_id, _)| *parent_id == target_dir_id);
            drop(created_files); // Drop lock IMMEDIATELY

            if has_child_files {
                error!("Directory {} is not empty - has child files", filename_str);
                return Err(nfsstat3::NFS3ERR_NOTEMPTY);
            }

            // Directory is empty, remove it
            let mut created_dirs = self.created_dirs.lock().await;
            created_dirs.remove(&dir_key);
            drop(created_dirs); // Drop lock IMMEDIATELY

            info!("Directory {} removed successfully", filename_str);
            return Ok(());
        }
        drop(created_dirs); // Drop lock IMMEDIATELY

        // Try to remove a created file (not data.csv)
        let created_files = self.created_files.lock().await;
        let file_key = (dirid, filename_str.to_string());
        if created_files.contains_key(&file_key) {
            drop(created_files);

            // Remove user-created file
            let mut created_files = self.created_files.lock().await;
            created_files.remove(&file_key);
            drop(created_files); // Drop lock IMMEDIATELY

            info!("File {} removed successfully", filename_str);
            return Ok(());
        }
        drop(created_files);

        // Cannot remove built-in files/directories
        error!("Cannot remove built-in file/directory: {}", filename_str);
        Err(nfsstat3::NFS3ERR_ACCES)
    }

    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> std::result::Result<(), nfsstat3> {
        let from_name = String::from_utf8_lossy(from_filename.as_ref());
        let to_name = String::from_utf8_lossy(to_filename.as_ref());
        info!(
            "NFS RENAME: from_dir={}, from={}, to_dir={}, to={}",
            from_dirid, from_name, to_dirid, to_name
        );

        // Try to rename a created file
        let created_files = self.created_files.lock().await;
        let from_file_key = (from_dirid, from_name.to_string());
        if let Some(file_metadata) = created_files.get(&from_file_key).cloned() {
            drop(created_files); // Drop lock IMMEDIATELY

            info!(
                "Renaming file {} (ID {}) from dir {} to {} in dir {}",
                from_name, file_metadata.file_id, from_dirid, to_name, to_dirid
            );

            // Check if target file already exists
            let created_files = self.created_files.lock().await;
            let to_file_key = (to_dirid, to_name.to_string());
            if created_files.contains_key(&to_file_key) {
                drop(created_files);
                error!("Target file {} already exists in dir {}", to_name, to_dirid);
                return Err(nfsstat3::NFS3ERR_EXIST);
            }
            drop(created_files); // Drop lock IMMEDIATELY

            // Perform the rename: remove old entry, add new entry
            let mut created_files = self.created_files.lock().await;
            created_files.remove(&from_file_key);
            created_files.insert(to_file_key, file_metadata.clone());
            drop(created_files); // Drop lock IMMEDIATELY

            // Update attr_cache to prevent getattr failures
            self.attr_cache
                .set(
                    file_metadata.file_id,
                    fattr3 {
                        ftype: ftype3::NF3REG,
                        mode: 0o666,
                        nlink: 1,
                        uid: 0, // Match Windows NFS client auth
                        gid: 0,
                        size: file_metadata.content.len() as u64,
                        used: file_metadata.content.len() as u64,
                        rdev: specdata3::default(),
                        fsid: 0,
                        fileid: file_metadata.file_id,
                        atime: file_metadata.atime,
                        mtime: file_metadata.mtime,
                        ctime: file_metadata.ctime,
                    },
                )
                .await;

            info!("File renamed successfully: {} -> {}", from_name, to_name);
            return Ok(());
        }
        drop(created_files); // Drop lock IMMEDIATELY

        // Try to rename a created directory
        let created_dirs = self.created_dirs.lock().await;
        let from_dir_key = (from_dirid, from_name.to_string());
        if let Some(dir_id) = created_dirs.get(&from_dir_key).cloned() {
            drop(created_dirs); // Drop lock IMMEDIATELY

            info!(
                "Renaming directory {} (ID {}) from dir {} to {} in dir {}",
                from_name, dir_id, from_dirid, to_name, to_dirid
            );

            // Check if target directory already exists
            let created_dirs = self.created_dirs.lock().await;
            let to_dir_key = (to_dirid, to_name.to_string());
            if created_dirs.contains_key(&to_dir_key) {
                drop(created_dirs);
                error!(
                    "Target directory {} already exists in dir {}",
                    to_name, to_dirid
                );
                return Err(nfsstat3::NFS3ERR_EXIST);
            }
            drop(created_dirs); // Drop lock IMMEDIATELY

            // Perform the rename: remove old entry, add new entry
            let mut created_dirs = self.created_dirs.lock().await;
            created_dirs.remove(&from_dir_key);
            created_dirs.insert(to_dir_key, dir_id);
            drop(created_dirs); // Drop lock IMMEDIATELY

            // Update attr_cache to prevent getattr failures
            let attr = Self::dir_attr(dir_id);
            self.attr_cache.set(dir_id, attr).await;

            info!(
                "Directory renamed successfully: {} -> {}",
                from_name, to_name
            );
            return Ok(());
        }
        drop(created_dirs); // Drop lock IMMEDIATELY

        // Cannot rename built-in files/directories (data.csv, data/, etc.)
        error!("Cannot rename built-in file/directory: {}", from_name);
        Err(nfsstat3::NFS3ERR_ACCES)
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> std::result::Result<ReadDirResult, nfsstat3> {
        info!(
            "NFS READDIR: dir={}, start_after={}, max={}",
            dirid, start_after, max_entries
        );

        let mut entries = Vec::new();

        match dirid {
            ROOT_ID => {
                if start_after < DATA_DIR_ID {
                    entries.push(DirEntry {
                        fileid: DATA_DIR_ID,
                        name: "data".as_bytes().into(),
                        attr: Self::dir_attr(DATA_DIR_ID),
                    });
                }
                // Add created directories in root
                let created_dirs = self.created_dirs.lock().await;
                for ((parent_id, dir_name), &dir_id) in created_dirs.iter() {
                    if *parent_id == ROOT_ID && dir_id > start_after && entries.len() < max_entries
                    {
                        entries.push(DirEntry {
                            fileid: dir_id,
                            name: dir_name.as_bytes().into(),
                            attr: Self::dir_attr(dir_id),
                        });
                    }
                }
                drop(created_dirs);
                // Add created files in root
                let created_files = self.created_files.lock().await;
                for ((parent_id, file_name), metadata) in created_files.iter() {
                    if *parent_id == ROOT_ID
                        && metadata.file_id > start_after
                        && entries.len() < max_entries
                    {
                        entries.push(DirEntry {
                            fileid: metadata.file_id,
                            name: file_name.as_bytes().into(),
                            attr: fattr3 {
                                ftype: ftype3::NF3REG,
                                mode: 0o666,
                                nlink: 1,
                                uid: 0, // Match Windows NFS client auth
                                gid: 0,
                                size: metadata.content.len() as u64,
                                used: metadata.content.len() as u64,
                                rdev: specdata3::default(),
                                fsid: 0,
                                fileid: metadata.file_id,
                                atime: metadata.atime,
                                mtime: metadata.mtime,
                                ctime: metadata.ctime,
                            },
                        });
                    }
                }
            }
            DATA_DIR_ID => {
                // Always include data.csv
                if start_after < DATA_CSV_ID {
                    let db = self.db.clone();
                    let view = CsvFileView::new(db);
                    let size = match view.size().await {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Failed to get CSV size in readdir: {}", e);
                            0
                        }
                    };
                    entries.push(DirEntry {
                        fileid: DATA_CSV_ID,
                        name: "data.csv".as_bytes().into(),
                        attr: Self::file_attr(DATA_CSV_ID, size),
                    });
                }

                // Add Parquet files (Delta Lake mode)
                self.refresh_parquet_files().await?;
                let parquet_files = self.parquet_files.lock().await;

                for (id, file_path) in parquet_files.iter() {
                    if *id > start_after && entries.len() < max_entries {
                        let basename = std::path::Path::new(file_path)
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or(file_path)
                            .as_bytes()
                            .into();

                        // Get real file size from filesystem
                        let full_path = self.db.base_path().join(file_path);
                        let size = std::fs::metadata(&full_path)
                            .map(|m| m.len())
                            .unwrap_or(1024); // Fallback to 1024 if not found

                        entries.push(DirEntry {
                            fileid: *id,
                            name: basename,
                            attr: Self::file_attr(*id, size),
                        });
                    }
                }

                // Add created directories in /data
                let created_dirs = self.created_dirs.lock().await;
                for ((parent_id, dir_name), &dir_id) in created_dirs.iter() {
                    if *parent_id == DATA_DIR_ID
                        && dir_id > start_after
                        && entries.len() < max_entries
                    {
                        entries.push(DirEntry {
                            fileid: dir_id,
                            name: dir_name.as_bytes().into(),
                            attr: Self::dir_attr(dir_id),
                        });
                    }
                }
                drop(created_dirs);
                // Add created files in /data
                let created_files = self.created_files.lock().await;
                for ((parent_id, file_name), metadata) in created_files.iter() {
                    if *parent_id == DATA_DIR_ID
                        && metadata.file_id > start_after
                        && entries.len() < max_entries
                    {
                        entries.push(DirEntry {
                            fileid: metadata.file_id,
                            name: file_name.as_bytes().into(),
                            attr: fattr3 {
                                ftype: ftype3::NF3REG,
                                mode: 0o666,
                                nlink: 1,
                                uid: 0, // Match Windows NFS client auth
                                gid: 0,
                                size: metadata.content.len() as u64,
                                used: metadata.content.len() as u64,
                                rdev: specdata3::default(),
                                fsid: 0,
                                fileid: metadata.file_id,
                                atime: metadata.atime,
                                mtime: metadata.mtime,
                                ctime: metadata.ctime,
                            },
                        });
                    }
                }
            }
            id if id >= CREATED_DIR_START => {
                // List contents of created directory
                let created_dirs = self.created_dirs.lock().await;
                for ((parent_id, dir_name), &dir_id) in created_dirs.iter() {
                    if *parent_id == id && dir_id > start_after && entries.len() < max_entries {
                        entries.push(DirEntry {
                            fileid: dir_id,
                            name: dir_name.as_bytes().into(),
                            attr: Self::dir_attr(dir_id),
                        });
                    }
                }
                drop(created_dirs);
                // Add created files in this directory
                let created_files = self.created_files.lock().await;
                for ((parent_id, file_name), metadata) in created_files.iter() {
                    if *parent_id == id
                        && metadata.file_id > start_after
                        && entries.len() < max_entries
                    {
                        entries.push(DirEntry {
                            fileid: metadata.file_id,
                            name: file_name.as_bytes().into(),
                            attr: fattr3 {
                                ftype: ftype3::NF3REG,
                                mode: 0o666,
                                nlink: 1,
                                uid: 0, // Match Windows NFS client auth
                                gid: 0,
                                size: metadata.content.len() as u64,
                                used: metadata.content.len() as u64,
                                rdev: specdata3::default(),
                                fsid: 0,
                                fileid: metadata.file_id,
                                atime: metadata.atime,
                                mtime: metadata.mtime,
                                ctime: metadata.ctime,
                            },
                        });
                    }
                }
            }
            _ => return Err(nfsstat3::NFS3ERR_NOTDIR),
        }

        Ok(ReadDirResult { entries, end: true })
    }

    async fn symlink(
        &self,
        _dirid: fileid3,
        _linkname: &filename3,
        _symlink_data: &nfsserve::nfs::nfspath3,
        _attr: &sattr3,
    ) -> std::result::Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn readlink(
        &self,
        _id: fileid3,
    ) -> std::result::Result<nfsserve::nfs::nfspath3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn path_to_id(&self, path: &[u8]) -> std::result::Result<fileid3, nfsstat3> {
        let path_str = String::from_utf8_lossy(path);
        info!("NFS PATH_TO_ID: path={}", path_str);

        // Handle root path
        if path.is_empty() || path == b"/" {
            info!("NFS PATH_TO_ID: returning root");
            return Ok(self.root_dir());
        }

        // Walk the path from root
        let mut current_id = self.root_dir();
        let path_trimmed = path_str.trim_start_matches('/');

        for component in path_trimmed.split('/') {
            if component.is_empty() {
                continue;
            }
            info!("NFS PATH_TO_ID: looking up '{}' in dir {}", component, current_id);
            match self.lookup(current_id, &component.as_bytes().into()).await {
                Ok(id) => {
                    info!("NFS PATH_TO_ID: found '{}' -> id {}", component, id);
                    current_id = id;
                }
                Err(e) => {
                    error!("NFS PATH_TO_ID: lookup failed for '{}': {:?}", component, e);
                    return Err(e);
                }
            }
        }

        info!("NFS PATH_TO_ID: resolved path '{}' to id {}", path_str, current_id);
        Ok(current_id)
    }
}
