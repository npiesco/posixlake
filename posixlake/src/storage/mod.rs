//! Storage abstraction layer for cross-platform file I/O
//! Supports local filesystem, S3, and other ObjectStore backends

pub mod local;
pub mod parquet;
pub mod path;
pub mod s3;

use object_store::ObjectStore;
use std::sync::Arc;

/// Storage backend abstraction
pub enum StorageBackend {
    Local(Arc<dyn ObjectStore>),
    S3(Arc<dyn ObjectStore>),
}

impl StorageBackend {
    pub fn as_object_store(&self) -> &Arc<dyn ObjectStore> {
        match self {
            StorageBackend::Local(store) => store,
            StorageBackend::S3(store) => store,
        }
    }
}
