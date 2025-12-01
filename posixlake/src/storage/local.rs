//! Local filesystem storage backend implementation

use crate::Result;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::path::Path;
use std::sync::Arc;

/// Creates a local filesystem ObjectStore
pub fn create_local_store<P: AsRef<Path>>(base_path: P) -> Result<Arc<dyn ObjectStore>> {
    // Ensure the directory exists
    std::fs::create_dir_all(base_path.as_ref())?;

    let local_fs = LocalFileSystem::new_with_prefix(base_path.as_ref())?;
    Ok(Arc::new(local_fs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_local_store() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = create_local_store(temp_dir.path());
        assert!(store.is_ok());
    }
}
