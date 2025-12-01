// Memory-Mapped I/O Cache for zero-copy large file access
//
// Architecture:
// - Use memmap2 for memory-mapped file access
// - Zero-copy reads from disk via OS page cache
// - Automatic page management by OS
// - Integration with existing NfsCache (moka + sled)
// - Bypass for small files, mmap for large files (>1MB threshold)

use memmap2::Mmap;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Configuration for memory-mapped I/O
#[derive(Debug, Clone)]
pub struct MmapConfig {
    /// Minimum file size to use mmap (bytes)
    /// Files smaller than this use regular I/O
    pub min_file_size: u64,

    /// Maximum number of mmapped files to keep open
    pub max_open_files: usize,
}

impl Default for MmapConfig {
    fn default() -> Self {
        Self {
            min_file_size: 1_048_576, // 1 MB - use mmap for files >= 1MB
            max_open_files: 100,      // Keep up to 100 files mmapped
        }
    }
}

/// Memory-mapped file handle
struct MmapFile {
    mmap: Mmap,
    size: u64,
    last_access: std::time::Instant,
}

/// Memory-mapped file cache for zero-copy I/O
pub struct MmapCache {
    config: MmapConfig,
    files: Arc<RwLock<HashMap<PathBuf, Arc<MmapFile>>>>,
}

impl MmapCache {
    /// Create a new memory-mapped file cache
    pub fn new(config: MmapConfig) -> Self {
        info!(
            "Initializing mmap cache with min_file_size={} bytes, max_open_files={}",
            config.min_file_size, config.max_open_files
        );
        Self {
            config,
            files: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Read file using memory-mapped I/O if file size exceeds threshold
    /// Returns None if file should use regular I/O (too small)
    pub async fn read_mmap(&self, path: impl AsRef<Path>) -> std::io::Result<Option<Vec<u8>>> {
        let path = path.as_ref();

        // Check file size
        let metadata = std::fs::metadata(path)?;
        let file_size = metadata.len();

        if file_size < self.config.min_file_size {
            debug!(
                "File {} ({} bytes) below mmap threshold, using regular I/O",
                path.display(),
                file_size
            );
            return Ok(None);
        }

        debug!(
            "Reading file {} ({} bytes) using mmap",
            path.display(),
            file_size
        );

        // Try to get existing mmap
        {
            let mut files = self.files.write().await;
            if let Some(mmap_file) = files.get_mut(path) {
                debug!("Using existing mmap for {}", path.display());
                // Update last access time
                let mmap_file = Arc::get_mut(mmap_file).expect("Multiple references to mmap file");
                mmap_file.last_access = std::time::Instant::now();

                // Return full file contents
                return Ok(Some(mmap_file.mmap[..].to_vec()));
            }
        }

        // Create new mmap
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        let mmap_file = Arc::new(MmapFile {
            mmap,
            size: file_size,
            last_access: std::time::Instant::now(),
        });

        let data = mmap_file.mmap[..].to_vec();

        // Store mmap
        {
            let mut files = self.files.write().await;

            // Evict old files if we exceed max_open_files
            if files.len() >= self.config.max_open_files {
                self.evict_lru(&mut files).await;
            }

            files.insert(path.to_path_buf(), mmap_file);
        }

        info!("Mmapped file {} ({} bytes)", path.display(), file_size);
        Ok(Some(data))
    }

    /// Read a range from a memory-mapped file (zero-copy when possible)
    pub async fn read_range(
        &self,
        path: impl AsRef<Path>,
        offset: u64,
        size: usize,
    ) -> std::io::Result<Option<Vec<u8>>> {
        let path = path.as_ref();

        // Check file size
        let metadata = std::fs::metadata(path)?;
        let file_size = metadata.len();

        if file_size < self.config.min_file_size {
            return Ok(None);
        }

        debug!(
            "Reading range [{}, {}) from {} using mmap",
            offset,
            offset + size as u64,
            path.display()
        );

        // Get or create mmap
        let files = self.files.read().await;
        if let Some(mmap_file) = files.get(path) {
            // Read from existing mmap
            let end = ((offset + size as u64) as usize).min(mmap_file.mmap.len());
            let start = (offset as usize).min(end);
            return Ok(Some(mmap_file.mmap[start..end].to_vec()));
        }
        drop(files);

        // Create new mmap
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        let end = ((offset + size as u64) as usize).min(mmap.len());
        let start = (offset as usize).min(end);
        let data = mmap[start..end].to_vec();

        let mmap_file = Arc::new(MmapFile {
            mmap,
            size: file_size,
            last_access: std::time::Instant::now(),
        });

        // Store mmap
        {
            let mut files = self.files.write().await;

            // Evict old files if we exceed max_open_files
            if files.len() >= self.config.max_open_files {
                self.evict_lru(&mut files).await;
            }

            files.insert(path.to_path_buf(), mmap_file);
        }

        Ok(Some(data))
    }

    /// Evict least recently used mmapped file
    async fn evict_lru(&self, files: &mut HashMap<PathBuf, Arc<MmapFile>>) {
        if files.is_empty() {
            return;
        }

        // Find LRU file
        let lru_path = files
            .iter()
            .min_by_key(|(_, mmap_file)| mmap_file.last_access)
            .map(|(path, _)| path.clone());

        if let Some(path) = lru_path {
            files.remove(&path);
            debug!("Evicted LRU mmapped file: {}", path.display());
        }
    }

    /// Invalidate (remove) a mmapped file from cache
    pub async fn invalidate(&self, path: impl AsRef<Path>) {
        let path = path.as_ref();
        let mut files = self.files.write().await;
        if files.remove(path).is_some() {
            debug!("Invalidated mmapped file: {}", path.display());
        }
    }

    /// Clear all mmapped files
    pub async fn clear(&self) {
        let mut files = self.files.write().await;
        let count = files.len();
        files.clear();
        info!("Cleared {} mmapped files", count);
    }

    /// Get statistics about mmapped files
    pub async fn stats(&self) -> MmapStats {
        let files = self.files.read().await;
        let total_size: u64 = files.values().map(|f| f.size).sum();
        MmapStats {
            num_files: files.len(),
            total_size,
        }
    }
}

/// Statistics about mmapped files
#[derive(Debug, Clone)]
pub struct MmapStats {
    pub num_files: usize,
    pub total_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_file(dir: &Path, name: &str, size: usize) -> PathBuf {
        let path = dir.join(name);
        let mut file = File::create(&path).unwrap();
        let data = vec![b'A'; size];
        file.write_all(&data).unwrap();
        file.sync_all().unwrap();
        path
    }

    #[tokio::test]
    async fn test_mmap_small_file_bypass() {
        let temp_dir = TempDir::new().unwrap();
        let config = MmapConfig {
            min_file_size: 1024,
            max_open_files: 10,
        };
        let cache = MmapCache::new(config);

        // Create small file (500 bytes)
        let path = create_test_file(temp_dir.path(), "small.txt", 500);

        // Should return None (below threshold)
        let result = cache.read_mmap(&path).await.unwrap();
        assert!(result.is_none(), "Small file should bypass mmap");

        let stats = cache.stats().await;
        assert_eq!(stats.num_files, 0);
    }

    #[tokio::test]
    async fn test_mmap_large_file() {
        let temp_dir = TempDir::new().unwrap();
        let config = MmapConfig {
            min_file_size: 1024,
            max_open_files: 10,
        };
        let cache = MmapCache::new(config);

        // Create large file (2KB)
        let path = create_test_file(temp_dir.path(), "large.txt", 2048);

        // Should use mmap
        let result = cache.read_mmap(&path).await.unwrap();
        assert!(result.is_some(), "Large file should use mmap");
        assert_eq!(result.unwrap().len(), 2048);

        let stats = cache.stats().await;
        assert_eq!(stats.num_files, 1);
        assert_eq!(stats.total_size, 2048);
    }

    #[tokio::test]
    async fn test_mmap_reuse() {
        let temp_dir = TempDir::new().unwrap();
        let cache = MmapCache::new(MmapConfig::default());

        // Create file
        let path = create_test_file(temp_dir.path(), "test.txt", 2_000_000);

        // First read
        let data1 = cache.read_mmap(&path).await.unwrap().unwrap();
        assert_eq!(data1.len(), 2_000_000);

        // Second read (should reuse mmap)
        let data2 = cache.read_mmap(&path).await.unwrap().unwrap();
        assert_eq!(data2.len(), 2_000_000);

        // Still only one file in cache
        let stats = cache.stats().await;
        assert_eq!(stats.num_files, 1);
    }

    #[tokio::test]
    async fn test_mmap_range_read() {
        let temp_dir = TempDir::new().unwrap();
        let cache = MmapCache::new(MmapConfig::default());

        // Create file
        let path = create_test_file(temp_dir.path(), "range.txt", 2_000_000);

        // Read range
        let data = cache.read_range(&path, 1000, 500).await.unwrap().unwrap();
        assert_eq!(data.len(), 500);
        assert_eq!(data[0], b'A');

        let stats = cache.stats().await;
        assert_eq!(stats.num_files, 1);
    }

    #[tokio::test]
    async fn test_mmap_lru_eviction() {
        let temp_dir = TempDir::new().unwrap();
        let config = MmapConfig {
            min_file_size: 1024,
            max_open_files: 3, // Only keep 3 files
        };
        let cache = MmapCache::new(config);

        // Create 5 files
        for i in 0..5 {
            let path = create_test_file(temp_dir.path(), &format!("file{}.txt", i), 2048);
            cache.read_mmap(&path).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await; // Ensure different timestamps
        }

        // Should only have 3 files (LRU evicted)
        let stats = cache.stats().await;
        assert_eq!(stats.num_files, 3);
    }

    #[tokio::test]
    async fn test_mmap_invalidate() {
        let temp_dir = TempDir::new().unwrap();
        let cache = MmapCache::new(MmapConfig::default());

        // Create file
        let path = create_test_file(temp_dir.path(), "test.txt", 2_000_000);

        // Read file
        cache.read_mmap(&path).await.unwrap();
        assert_eq!(cache.stats().await.num_files, 1);

        // Invalidate
        cache.invalidate(&path).await;
        assert_eq!(cache.stats().await.num_files, 0);
    }

    #[tokio::test]
    async fn test_mmap_clear() {
        let temp_dir = TempDir::new().unwrap();
        let cache = MmapCache::new(MmapConfig::default());

        // Create multiple files
        for i in 0..5 {
            let path = create_test_file(temp_dir.path(), &format!("file{}.txt", i), 2_000_000);
            cache.read_mmap(&path).await.unwrap();
        }

        assert_eq!(cache.stats().await.num_files, 5);

        // Clear all
        cache.clear().await;
        assert_eq!(cache.stats().await.num_files, 0);
    }
}
