// NFS Cache Module - Two-tier caching system (memory + disk)
//
// Architecture:
// - Memory cache (moka): Hot cache for frequently accessed data (microsecond access)
// - Disk cache (sled): Warm cache for persistent storage (millisecond access)
// - Database: Cold storage (10-100ms access)

use crate::error::Result;
use moka::future::Cache;
use sled::Db;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Two-tier cache for NFS server performance optimization
///
/// Provides fast access to query results and file metadata with:
/// - In-memory cache (moka) for hot data
/// - Disk cache (sled) for persistent warm data
/// - Automatic eviction based on LRU and TTL
pub struct NfsCache {
    /// Hot cache - query results in memory
    memory: Cache<String, Arc<Vec<u8>>>,

    /// Warm cache - persistent across restarts
    disk: Db,

    /// Maximum disk cache size in bytes
    max_disk_size: u64,
}

impl NfsCache {
    /// Create a new NFS cache with default settings
    pub async fn new(disk_path: impl AsRef<Path>) -> Result<Self> {
        Self::with_config(disk_path, CacheConfig::default()).await
    }

    /// Create a new NFS cache with custom configuration
    pub async fn with_config(disk_path: impl AsRef<Path>, config: CacheConfig) -> Result<Self> {
        let memory = Cache::builder()
            .max_capacity(config.max_memory_entries)
            .time_to_live(config.memory_ttl)
            .time_to_idle(config.memory_idle_timeout)
            .build();

        let disk = sled::open(disk_path.as_ref()).map_err(|e| {
            crate::error::Error::InvalidOperation(format!("Failed to open disk cache: {}", e))
        })?;

        Ok(Self {
            memory,
            disk,
            max_disk_size: config.max_disk_size,
        })
    }

    /// Get value from cache (checks memory first, then disk)
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // Try memory cache first (microsecond access)
        if let Some(value) = self.memory.get(key).await {
            tracing::debug!("Cache HIT (memory): {}", key);
            return Ok(Some((*value).clone()));
        }

        // Try disk cache (millisecond access)
        if let Some(value) = self.disk.get(key).map_err(|e| {
            crate::error::Error::InvalidOperation(format!("Disk cache read error: {}", e))
        })? {
            tracing::debug!("Cache HIT (disk): {}", key);
            let bytes = value.to_vec();

            // Promote to memory cache
            self.memory
                .insert(key.to_string(), Arc::new(bytes.clone()))
                .await;

            return Ok(Some(bytes));
        }

        tracing::debug!("Cache MISS: {}", key);
        Ok(None)
    }

    /// Insert value into both memory and disk caches
    pub async fn insert(&self, key: String, value: Vec<u8>) -> Result<()> {
        // Insert into memory cache
        self.memory
            .insert(key.clone(), Arc::new(value.clone()))
            .await;

        // Insert into disk cache
        self.disk
            .insert(key.as_bytes(), value.as_slice())
            .map_err(|e| {
                crate::error::Error::InvalidOperation(format!("Disk cache write error: {}", e))
            })?;

        // Flush to ensure test visibility (sled buffers writes)
        self.disk.flush().map_err(|e| {
            crate::error::Error::InvalidOperation(format!("Disk cache flush error: {}", e))
        })?;

        // Check disk cache size and evict if needed
        self.maybe_evict_disk().await?;

        Ok(())
    }

    /// Remove entry from both caches
    pub async fn remove(&self, key: &str) -> Result<()> {
        self.memory.invalidate(key).await;
        self.disk.remove(key.as_bytes()).map_err(|e| {
            crate::error::Error::InvalidOperation(format!("Disk cache remove error: {}", e))
        })?;
        Ok(())
    }

    /// Clear all entries from both caches
    pub async fn clear(&self) -> Result<()> {
        self.memory.invalidate_all();
        self.disk.clear().map_err(|e| {
            crate::error::Error::InvalidOperation(format!("Disk cache clear error: {}", e))
        })?;
        Ok(())
    }

    /// Get the number of entries in memory cache
    pub async fn memory_entry_count(&self) -> u64 {
        self.memory.entry_count()
    }

    /// Get the number of entries in disk cache
    pub fn disk_entry_count(&self) -> Result<u64> {
        Ok(self.disk.len() as u64)
    }

    /// Get approximate size of disk cache in bytes
    pub fn disk_size_bytes(&self) -> Result<u64> {
        self.disk.size_on_disk().map_err(|e| {
            crate::error::Error::InvalidOperation(format!("Failed to get disk size: {}", e))
        })
    }

    /// Evict old entries from disk cache if size exceeds limit
    async fn maybe_evict_disk(&self) -> Result<()> {
        let size = self.disk_size_bytes()?;

        if size > self.max_disk_size {
            tracing::info!(
                "Disk cache size ({} bytes) exceeds limit ({} bytes), clearing",
                size,
                self.max_disk_size
            );
            self.disk.clear().map_err(|e| {
                crate::error::Error::InvalidOperation(format!("Failed to clear disk cache: {}", e))
            })?;
        }

        Ok(())
    }
}

/// Configuration for NFS cache
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of entries in memory cache
    pub max_memory_entries: u64,

    /// Time-to-live for memory cache entries
    pub memory_ttl: Duration,

    /// Time-to-idle for memory cache entries (evict if unused)
    pub memory_idle_timeout: Duration,

    /// Maximum disk cache size in bytes
    pub max_disk_size: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_memory_entries: 100_000,                  // 100K entries
            memory_ttl: Duration::from_secs(300),         // 5 minutes
            memory_idle_timeout: Duration::from_secs(60), // 1 minute
            max_disk_size: 1_073_741_824,                 // 1 GB
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_cache() -> (NfsCache, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let cache = NfsCache::new(temp_dir.path().join("cache")).await.unwrap();
        (cache, temp_dir)
    }

    #[tokio::test]
    async fn test_cache_creation() {
        let (cache, _temp) = create_test_cache().await;

        assert_eq!(cache.memory_entry_count().await, 0);
        assert_eq!(cache.disk_entry_count().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_cache_insert_and_get() {
        let (cache, _temp) = create_test_cache().await;

        let key = "query:SELECT_FROM_data";
        let value = b"id,name,value\n1,Alice,100\n2,Bob,200\n".to_vec();

        // Insert into cache
        cache.insert(key.to_string(), value.clone()).await.unwrap();

        // Sync memory cache
        cache.memory.run_pending_tasks().await;

        // Verify it's in cache
        let cached = cache.get(key).await.unwrap();
        assert_eq!(cached, Some(value));

        // Verify entry count
        assert_eq!(cache.memory_entry_count().await, 1);
        assert_eq!(cache.disk_entry_count().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let (cache, _temp) = create_test_cache().await;

        let result = cache.get("nonexistent_key").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_cache_remove() {
        let (cache, _temp) = create_test_cache().await;

        let key = "test_key";
        let value = b"test_value".to_vec();

        // Insert
        cache.insert(key.to_string(), value.clone()).await.unwrap();
        assert!(cache.get(key).await.unwrap().is_some());

        // Remove
        cache.remove(key).await.unwrap();
        assert!(cache.get(key).await.unwrap().is_none());

        // Verify counts
        assert_eq!(cache.memory_entry_count().await, 0);
        assert_eq!(cache.disk_entry_count().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let (cache, _temp) = create_test_cache().await;

        // Insert multiple entries
        for i in 0..10 {
            cache
                .insert(format!("key_{}", i), format!("value_{}", i).into_bytes())
                .await
                .unwrap();
        }

        // Sync memory cache
        cache.memory.run_pending_tasks().await;

        assert_eq!(cache.memory_entry_count().await, 10);
        assert_eq!(cache.disk_entry_count().unwrap(), 10);

        // Clear all
        cache.clear().await.unwrap();

        // Sync after clear
        cache.memory.run_pending_tasks().await;

        assert_eq!(cache.memory_entry_count().await, 0);
        assert_eq!(cache.disk_entry_count().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_memory_promotion_from_disk() {
        let (cache, _temp) = create_test_cache().await;

        let key = "test_key";
        let value = b"test_value".to_vec();

        // Insert into cache
        cache.insert(key.to_string(), value.clone()).await.unwrap();
        cache.memory.run_pending_tasks().await;

        // Evict from memory only
        cache.memory.invalidate(key).await;
        cache.memory.run_pending_tasks().await;
        assert_eq!(cache.memory_entry_count().await, 0);

        // Get from cache (should promote from disk to memory)
        let cached = cache.get(key).await.unwrap();
        assert_eq!(cached, Some(value));

        // Sync after get
        cache.memory.run_pending_tasks().await;

        // Verify it's back in memory
        assert_eq!(cache.memory_entry_count().await, 1);
    }

    #[tokio::test]
    async fn test_large_values() {
        let (cache, _temp) = create_test_cache().await;

        // Create a 1MB value
        let large_value = vec![0u8; 1_048_576];

        cache
            .insert("large_key".to_string(), large_value.clone())
            .await
            .unwrap();

        let cached = cache.get("large_key").await.unwrap();
        assert_eq!(cached, Some(large_value));
    }

    #[tokio::test]
    async fn test_disk_size_tracking() {
        let (cache, _temp) = create_test_cache().await;

        // Insert some data
        for i in 0..100 {
            cache
                .insert(
                    format!("key_{}", i),
                    vec![0u8; 1000], // 1KB each
                )
                .await
                .unwrap();
        }

        let size = cache.disk_size_bytes().unwrap();
        assert!(size > 0, "Disk cache should have non-zero size");
        assert!(size < 1_000_000, "Disk cache should be reasonable size");
    }

    #[tokio::test]
    async fn test_custom_cache_config() {
        let temp_dir = TempDir::new().unwrap();

        let config = CacheConfig {
            max_memory_entries: 10,
            memory_ttl: Duration::from_secs(1),
            memory_idle_timeout: Duration::from_millis(500),
            max_disk_size: 1_000_000, // 1MB
        };

        let cache = NfsCache::with_config(temp_dir.path().join("cache"), config)
            .await
            .unwrap();

        cache
            .insert("test".to_string(), b"value".to_vec())
            .await
            .unwrap();

        let cached = cache.get("test").await.unwrap();
        assert_eq!(cached, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let (cache, _temp) = create_test_cache().await;
        let cache = Arc::new(cache);

        // Spawn multiple tasks that read/write concurrently
        let mut handles = vec![];

        for i in 0..10 {
            let cache_clone = cache.clone();
            let handle = tokio::spawn(async move {
                let key = format!("key_{}", i);
                let value = format!("value_{}", i).into_bytes();

                cache_clone
                    .insert(key.clone(), value.clone())
                    .await
                    .unwrap();
                let cached = cache_clone.get(&key).await.unwrap();
                assert_eq!(cached, Some(value));
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // Sync memory cache after all tasks complete
        cache.memory.run_pending_tasks().await;

        assert_eq!(cache.memory_entry_count().await, 10);
    }
}
