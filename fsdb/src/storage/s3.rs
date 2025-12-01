//! S3/MinIO storage backend implementation for Delta Lake

use crate::Result;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

/// Parse S3 URI into bucket and prefix
/// Format: s3://bucket/prefix
pub fn parse_s3_uri(s3_path: &str) -> Result<(String, String)> {
    let url =
        Url::parse(s3_path).map_err(|e| crate::Error::Other(format!("Invalid S3 URI: {}", e)))?;

    if url.scheme() != "s3" {
        return Err(crate::Error::Other(format!(
            "Invalid S3 URI scheme: {}",
            url.scheme()
        )));
    }

    let bucket = url
        .host_str()
        .ok_or_else(|| crate::Error::Other("S3 URI missing bucket".to_string()))?
        .to_string();

    let prefix = url.path().trim_start_matches('/').to_string();

    Ok((bucket, prefix))
}

/// Create an S3/MinIO ObjectStore (legacy - use Delta Lake storage options instead)
pub fn create_s3_store(
    bucket: &str,
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<Arc<dyn ObjectStore + Send + Sync>> {
    let store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_endpoint(endpoint)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_allow_http(true) // Allow HTTP for local MinIO
        .build()
        .map_err(|e| crate::Error::Other(format!("Failed to create S3 store: {}", e)))?;

    Ok(Arc::new(store))
}

/// Create Delta Lake storage options for S3/MinIO
/// These options configure Delta Lake's native S3 backend
pub fn create_delta_storage_options(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
) -> HashMap<String, String> {
    let mut storage_options = HashMap::new();
    storage_options.insert("AWS_ENDPOINT_URL".to_string(), endpoint.to_string());
    storage_options.insert("AWS_ACCESS_KEY_ID".to_string(), access_key.to_string());
    storage_options.insert("AWS_SECRET_ACCESS_KEY".to_string(), secret_key.to_string());
    storage_options.insert("AWS_REGION".to_string(), "us-east-1".to_string());
    storage_options.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());
    storage_options
}

/// Parse S3 path and validate it as a URL
pub fn parse_s3_url(s3_path: &str) -> Result<Url> {
    Url::parse(s3_path).map_err(|e| crate::Error::Other(format!("Invalid S3 URL: {}", e)))
}

/// Generate local cache path for S3-backed database
pub fn get_s3_cache_path(s3_path: &str) -> std::path::PathBuf {
    let path_hash = format!("{:x}", md5::compute(s3_path.as_bytes()));
    std::env::temp_dir().join("fsdb_s3").join(&path_hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_uri() {
        let (bucket, prefix) = parse_s3_uri("s3://my-bucket/path/to/db").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "path/to/db");
    }

    #[test]
    fn test_parse_s3_uri_no_prefix() {
        let (bucket, prefix) = parse_s3_uri("s3://my-bucket").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_parse_s3_uri_invalid() {
        assert!(parse_s3_uri("http://bucket/path").is_err());
        assert!(parse_s3_uri("not-a-uri").is_err());
    }
}
