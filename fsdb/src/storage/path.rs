//! Cross-platform path resolution
//! Handles Windows (C:\), Unix (/home/user), and S3 (s3://) paths

use crate::Result;
use std::path::PathBuf;

/// Path resolver for cross-platform compatibility
pub struct PathResolver;

impl PathResolver {
    /// Normalize a path to use forward slashes consistently
    pub fn normalize(path: &str) -> String {
        path.replace('\\', "/")
    }

    /// Resolve a logical path to a physical storage path
    pub fn resolve(path: &str) -> Result<ResolvedPath> {
        if path.starts_with("s3://") {
            Ok(ResolvedPath::S3(path.to_string()))
        } else {
            let normalized = Self::normalize(path);
            let path_buf = PathBuf::from(&normalized);
            Ok(ResolvedPath::Local(path_buf))
        }
    }
}

/// Resolved path types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedPath {
    Local(PathBuf),
    S3(String),
}

impl ResolvedPath {
    pub fn as_str(&self) -> String {
        match self {
            ResolvedPath::Local(p) => p.display().to_string(),
            ResolvedPath::S3(s) => s.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_windows_path() {
        let path = r"C:\data\db";
        let normalized = PathResolver::normalize(path);
        assert_eq!(normalized, "C:/data/db");
    }

    #[test]
    fn test_resolve_local_path() {
        let path = "/tmp/test_db";
        let resolved = PathResolver::resolve(path).unwrap();
        assert!(matches!(resolved, ResolvedPath::Local(_)));
    }

    #[test]
    fn test_resolve_s3_path() {
        let path = "s3://my-bucket/db";
        let resolved = PathResolver::resolve(path).unwrap();
        assert!(matches!(resolved, ResolvedPath::S3(_)));
    }
}
