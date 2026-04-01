//! Azure Blob Storage / ADLS Gen2 backend implementation for Delta Lake

use crate::Result;
use std::collections::HashMap;
use url::Url;

/// Parse Azure storage URL and validate it
/// Supports az://container/path format (used by Delta Lake)
pub fn parse_azure_url(azure_path: &str) -> Result<Url> {
    Url::parse(azure_path).map_err(|e| crate::Error::Other(format!("Invalid Azure URL: {}", e)))
}

/// Create Delta Lake storage options for Azure Blob Storage
/// These options configure Delta Lake's native Azure backend
///
/// For Azurite (local emulator), set use_emulator=true and provide the endpoint.
/// For production Azure, omit endpoint and set use_emulator=false.
pub fn create_delta_storage_options(
    account_name: &str,
    account_key: &str,
    endpoint: Option<&str>,
) -> HashMap<String, String> {
    let mut storage_options = HashMap::new();
    storage_options.insert(
        "AZURE_STORAGE_ACCOUNT_NAME".to_string(),
        account_name.to_string(),
    );
    storage_options.insert(
        "AZURE_STORAGE_ACCOUNT_KEY".to_string(),
        account_key.to_string(),
    );

    if let Some(ep) = endpoint {
        // When an explicit endpoint is provided, we're targeting Azurite or a custom endpoint
        storage_options.insert("AZURE_STORAGE_USE_EMULATOR".to_string(), "true".to_string());
        storage_options.insert("AZURE_STORAGE_ENDPOINT".to_string(), ep.to_string());
    }

    storage_options
}

/// Generate local cache path for Azure-backed database
pub fn get_azure_cache_path(azure_path: &str) -> std::path::PathBuf {
    let path_hash = format!("{:x}", md5::compute(azure_path.as_bytes()));
    std::env::temp_dir().join("fsdb_azure").join(&path_hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_azure_url_az_scheme() {
        let url = parse_azure_url("az://my-container/path/to/db").unwrap();
        assert_eq!(url.scheme(), "az");
        assert_eq!(url.host_str(), Some("my-container"));
    }

    #[test]
    fn test_parse_azure_url_invalid() {
        assert!(parse_azure_url("not-a-uri").is_err());
    }

    #[test]
    fn test_create_delta_storage_options_with_endpoint() {
        let opts =
            create_delta_storage_options("myaccount", "mykey", Some("http://127.0.0.1:10000"));
        assert_eq!(opts.get("AZURE_STORAGE_ACCOUNT_NAME").unwrap(), "myaccount");
        assert_eq!(opts.get("AZURE_STORAGE_ACCOUNT_KEY").unwrap(), "mykey");
        assert_eq!(opts.get("AZURE_STORAGE_USE_EMULATOR").unwrap(), "true");
        assert_eq!(
            opts.get("AZURE_STORAGE_ENDPOINT").unwrap(),
            "http://127.0.0.1:10000"
        );
    }

    #[test]
    fn test_create_delta_storage_options_production() {
        let opts = create_delta_storage_options("prodaccount", "prodkey", None);
        assert_eq!(
            opts.get("AZURE_STORAGE_ACCOUNT_NAME").unwrap(),
            "prodaccount"
        );
        assert!(opts.get("AZURE_STORAGE_USE_EMULATOR").is_none());
        assert!(opts.get("AZURE_STORAGE_ENDPOINT").is_none());
    }

    #[test]
    fn test_get_azure_cache_path() {
        let path1 = get_azure_cache_path("az://container/db1");
        let path2 = get_azure_cache_path("az://container/db2");
        assert_ne!(path1, path2);
        assert!(path1.to_string_lossy().contains("fsdb_azure"));
    }
}
