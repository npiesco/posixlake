//! NFS attribute cache to prevent mount disconnections during concurrent writes
//!
//! macOS NFSv3 client is sensitive to rapidly changing file attributes and will
//! disconnect mounts it perceives as "unstable". This cache provides stable
//! attributes for a short TTL window.

use nfsserve::nfs::fattr3;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

const ATTR_CACHE_TTL: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
struct CachedAttr {
    attr: fattr3,
    cached_at: Instant,
}

impl CachedAttr {
    fn new(attr: fattr3) -> Self {
        Self {
            attr,
            cached_at: Instant::now(),
        }
    }

    fn is_valid(&self) -> bool {
        self.cached_at.elapsed() < ATTR_CACHE_TTL
    }
}

/// Attribute cache for NFS file attributes
pub struct AttrCache {
    cache: Arc<RwLock<HashMap<u64, CachedAttr>>>,
}

impl Default for AttrCache {
    fn default() -> Self {
        Self::new()
    }
}

impl AttrCache {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get cached attributes if still valid
    pub async fn get(&self, file_id: u64) -> Option<fattr3> {
        let cache = self.cache.read().await;
        if let Some(cached) = cache.get(&file_id) {
            if cached.is_valid() {
                return Some(cached.attr);
            }
        }
        None
    }

    /// Store attributes in cache
    pub async fn set(&self, file_id: u64, attr: fattr3) {
        let mut cache = self.cache.write().await;
        cache.insert(file_id, CachedAttr::new(attr));
    }

    /// Invalidate cached attributes for a file (call after writes)
    pub async fn invalidate(&self, file_id: u64) {
        let mut cache = self.cache.write().await;
        cache.remove(&file_id);
    }

    /// Clear all cached attributes
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nfsserve::nfs::{ftype3, nfstime3, specdata3};

    fn test_attr(file_id: u64, size: u64) -> fattr3 {
        fattr3 {
            ftype: ftype3::NF3REG,
            mode: 420,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            size,
            used: size,
            rdev: specdata3 {
                specdata1: 0,
                specdata2: 0,
            },
            fsid: 0,
            fileid: file_id,
            atime: nfstime3 {
                seconds: 0,
                nseconds: 0,
            },
            mtime: nfstime3 {
                seconds: 0,
                nseconds: 0,
            },
            ctime: nfstime3 {
                seconds: 0,
                nseconds: 0,
            },
        }
    }

    #[tokio::test]
    async fn test_attr_cache_basic() {
        let cache = AttrCache::new();
        let attr = test_attr(123, 1000);

        // Cache miss initially
        assert!(cache.get(123).await.is_none());

        // Set and retrieve
        cache.set(123, attr).await;
        let cached = cache.get(123).await.unwrap();
        assert_eq!(cached.size, 1000);
        assert_eq!(cached.fileid, 123);
    }

    #[tokio::test]
    async fn test_attr_cache_invalidation() {
        let cache = AttrCache::new();
        let attr = test_attr(123, 1000);

        cache.set(123, attr).await;
        assert!(cache.get(123).await.is_some());

        cache.invalidate(123).await;
        assert!(cache.get(123).await.is_none());
    }

    #[tokio::test]
    async fn test_attr_cache_clear() {
        let cache = AttrCache::new();

        cache.set(1, test_attr(1, 100)).await;
        cache.set(2, test_attr(2, 200)).await;

        assert!(cache.get(1).await.is_some());
        assert!(cache.get(2).await.is_some());

        cache.clear().await;

        assert!(cache.get(1).await.is_none());
        assert!(cache.get(2).await.is_none());
    }

    #[tokio::test]
    async fn test_attr_cache_expiry() {
        let cache = AttrCache::new();
        let attr = test_attr(123, 1000);

        cache.set(123, attr).await;
        assert!(cache.get(123).await.is_some());

        // Wait for TTL to expire (2 seconds + margin)
        tokio::time::sleep(Duration::from_millis(2100)).await;

        // Should be expired now
        assert!(cache.get(123).await.is_none());
    }
}
