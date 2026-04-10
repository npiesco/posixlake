//! Azure Blob Storage / ADLS Gen2 backend implementation for Delta Lake

use crate::Result;
use std::collections::HashMap;
use url::Url;

/// Parse Azure storage URL and validate it
/// Supports az://container/path format (used by Delta Lake)
pub fn parse_azure_url(azure_path: &str) -> Result<Url> {
    Url::parse(azure_path).map_err(|e| crate::Error::Other(format!("Invalid Azure URL: {}", e)))
}

/// Extract container name from az://container/path URL
pub fn parse_container_name(azure_path: &str) -> Result<String> {
    let url = parse_azure_url(azure_path)?;
    let container = url
        .host_str()
        .ok_or_else(|| crate::Error::Other("Azure URI missing container name".to_string()))?
        .to_string();
    Ok(container)
}

/// Ensure an Azure Blob container exists, creating it if necessary.
pub async fn ensure_container_exists(
    container: &str,
    account_name: &str,
    account_key: &str,
    endpoint: &str,
) -> Result<()> {
    // Directly create the container — if it already exists (409), that's fine
    let effective_endpoint = if endpoint.contains("127.0.0.1") || endpoint.contains("localhost") {
        format!("{}/{}", endpoint.trim_end_matches('/'), account_name)
    } else {
        endpoint.to_string()
    };

    create_container_via_rest(container, account_name, account_key, &effective_endpoint).await
}

/// Create an Azure Blob container via REST API with SharedKey auth
async fn create_container_via_rest(
    container: &str,
    account_name: &str,
    account_key: &str,
    effective_endpoint: &str,
) -> Result<()> {
    use base64::{engine::general_purpose::STANDARD, Engine};

    let now = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    let version = "2020-10-02";

    let canonicalized_headers = format!("x-ms-date:{}\nx-ms-version:{}", now, version);

    // For Azurite emulator with path-style URLs, the canonical resource needs
    // double account prefix: /<account>/<account>/<container>
    let canonicalized_resource = if effective_endpoint.contains(account_name) {
        format!(
            "/{}/{}/{}\nrestype:container",
            account_name, account_name, container
        )
    } else {
        format!("/{}/{}\nrestype:container", account_name, container)
    };

    let string_to_sign = format!(
        "PUT\n\n\n\n\n\n\n\n\n\n\n\n{}\n{}",
        canonicalized_headers, canonicalized_resource
    );

    tracing::debug!("Azure container create string_to_sign:\n{}", string_to_sign);

    let key_bytes = STANDARD
        .decode(account_key)
        .map_err(|e| crate::Error::Other(format!("Invalid Azure account key: {}", e)))?;

    let signature = {
        use hmac::{Hmac, KeyInit, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(&key_bytes)
            .map_err(|e| crate::Error::Other(format!("HMAC error: {}", e)))?;
        mac.update(string_to_sign.as_bytes());
        STANDARD.encode(mac.finalize().into_bytes())
    };

    let url = format!(
        "{}/{}?restype=container",
        effective_endpoint.trim_end_matches('/'),
        container,
    );

    tracing::debug!("Azure container create URL: {}", url);

    let client = reqwest::Client::new();
    let resp = client
        .put(&url)
        .header("x-ms-date", &now)
        .header("x-ms-version", version)
        .header(
            "Authorization",
            format!("SharedKey {}:{}", account_name, signature),
        )
        .send()
        .await
        .map_err(|e| crate::Error::Other(format!("Failed to create container: {}", e)))?;

    let status = resp.status();
    if status.is_success() || status.as_u16() == 409 {
        tracing::info!("Container '{}' ready (HTTP {})", container, status);
        Ok(())
    } else {
        let body = resp.text().await.unwrap_or_default();
        tracing::error!(
            "Container creation failed. URL: {}, StringToSign: {:?}",
            url,
            string_to_sign
        );
        Err(crate::Error::Other(format!(
            "Failed to create container '{}': HTTP {} - {}",
            container, status, body
        )))
    }
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
        // For Azurite/custom endpoints, set the endpoint with account name in the path.
        // Use azure_allow_http instead of USE_EMULATOR to avoid path-stripping behavior.
        let endpoint_with_account = if ep.contains("127.0.0.1") || ep.contains("localhost") {
            format!("{}/{}", ep.trim_end_matches('/'), account_name)
        } else {
            ep.to_string()
        };
        storage_options.insert("AZURE_STORAGE_ENDPOINT".to_string(), endpoint_with_account);
        storage_options.insert("azure_allow_http".to_string(), "true".to_string());
        storage_options.insert("azure_skip_signature".to_string(), "false".to_string());
    }

    storage_options
}

/// Generate local cache path for Azure-backed database
pub fn get_azure_cache_path(azure_path: &str) -> std::path::PathBuf {
    let path_hash = format!("{:x}", md5::compute(azure_path.as_bytes()));
    std::env::temp_dir().join("fsdb_azure").join(&path_hash)
}

/// Create Delta Lake storage options for OneLake / Fabric using Service Principal auth
/// OneLake exposes ADLS Gen2-compatible endpoints authenticated via Entra ID
pub fn create_delta_storage_options_sp(
    client_id: &str,
    client_secret: &str,
    tenant_id: &str,
) -> HashMap<String, String> {
    let mut storage_options = HashMap::new();
    storage_options.insert(
        "AZURE_STORAGE_ACCOUNT_NAME".to_string(),
        "onelake".to_string(),
    );
    storage_options.insert("AZURE_STORAGE_CLIENT_ID".to_string(), client_id.to_string());
    storage_options.insert(
        "AZURE_STORAGE_CLIENT_SECRET".to_string(),
        client_secret.to_string(),
    );
    storage_options.insert("AZURE_STORAGE_TENANT_ID".to_string(), tenant_id.to_string());
    storage_options.insert(
        "AZURE_STORAGE_AUTHORITY_HOST".to_string(),
        "https://login.microsoftonline.com".to_string(),
    );
    storage_options
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
        assert_eq!(
            opts.get("AZURE_STORAGE_ENDPOINT").unwrap(),
            "http://127.0.0.1:10000/myaccount"
        );
        assert_eq!(opts.get("azure_allow_http").unwrap(), "true");
    }

    #[test]
    fn test_create_delta_storage_options_production() {
        let opts = create_delta_storage_options("prodaccount", "prodkey", None);
        assert_eq!(
            opts.get("AZURE_STORAGE_ACCOUNT_NAME").unwrap(),
            "prodaccount"
        );
        assert!(!opts.contains_key("AZURE_STORAGE_ENDPOINT"));
        assert!(!opts.contains_key("AZURE_STORAGE_USE_EMULATOR"));
    }

    #[test]
    fn test_get_azure_cache_path() {
        let path1 = get_azure_cache_path("az://container/db1");
        let path2 = get_azure_cache_path("az://container/db2");
        assert_ne!(path1, path2);
        assert!(path1.to_string_lossy().contains("fsdb_azure"));
    }
}
