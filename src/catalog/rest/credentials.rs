//! Vended credential provider for REST catalogs
use crate::error::{Error, Result};
use crate::io::{VendedCredentialProvider, VendedCredentials};
use reqwest::Client;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use urlencoding::encode;

use super::types::LoadTableCredentialsResponse;

/// Credential provider that fetches vended credentials from Iceberg REST catalog
#[derive(Debug)]
pub(crate) struct RestCredentialProvider {
    pub(crate) endpoint: String,
    pub(crate) prefix: String,
    pub(crate) token: String,
    pub(crate) http_client: Client,
    pub(crate) s3_endpoint: Option<String>,
    /// Cache credentials by table location prefix
    pub(crate) credential_cache: Arc<RwLock<HashMap<String, VendedCredentials>>>,
}

/// Extract table location from a file path.
///
/// For R2 Data Catalog, paths follow pattern:
/// `s3://bucket/namespace.db/tablename/metadata/...`
/// `s3://bucket/namespace.db/tablename/data/...`
///
/// This function strips the Iceberg-specific directories (data, metadata) to get
/// the table location prefix.
///
/// # Arguments
/// * `path` - Full path to an Iceberg file (data or metadata)
///
/// # Returns
/// The table location prefix (e.g., `s3://bucket/namespace.db/tablename`)
///
/// # Errors
/// Returns `Error::IoError` if the path doesn't match expected Iceberg structure
fn extract_table_location(path: &str) -> Result<String> {
    // Find known Iceberg directories that mark the table boundary
    let iceberg_dirs = ["/data/", "/metadata/"];

    for dir in iceberg_dirs {
        if let Some(idx) = path.find(dir) {
            return Ok(path[..idx].to_string());
        }
    }

    // If no Iceberg directory found, try to handle paths that end with these dirs
    for dir_name in ["data", "metadata"] {
        let suffix = format!("/{}", dir_name);
        if path.ends_with(&suffix) {
            return Ok(path[..path.len() - suffix.len()].to_string());
        }
    }

    Err(Error::IoError(format!(
        "Path does not contain Iceberg directory structure (data/ or metadata/): {}",
        path
    )))
}

/// Parse table identifier (namespace, table_name) from a table location.
///
/// For R2 Data Catalog, table locations follow pattern:
/// `s3://bucket/namespace.db/tablename`
///
/// The namespace is extracted from the part before `.db`, and the table name
/// is the final path component.
///
/// # Arguments
/// * `location` - Table location (e.g., `s3://bucket/namespace.db/tablename`)
///
/// # Returns
/// Tuple of (namespace, table_name)
///
/// # Errors
/// Returns `Error::IoError` if the location doesn't match expected pattern
fn parse_table_identifier_from_location(location: &str) -> Result<(String, String)> {
    // Strip the s3:// or similar prefix and bucket
    let path = if let Some(rest) = location.strip_prefix("s3://") {
        // Skip the bucket name (first path segment)
        if let Some(idx) = rest.find('/') {
            &rest[idx + 1..]
        } else {
            return Err(Error::IoError(format!(
                "Table location missing path after bucket: {}",
                location
            )));
        }
    } else {
        return Err(Error::IoError(format!(
            "Table location must start with s3://: {}",
            location
        )));
    };

    // Split the remaining path by '/'
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    if segments.is_empty() {
        return Err(Error::IoError(format!(
            "Table location has no path segments: {}",
            location
        )));
    }

    // The last segment is the table name
    let table_name = segments.last().unwrap().to_string();

    // Look for namespace.db pattern in the path
    // The namespace is typically in a segment ending with .db
    for segment in &segments[..segments.len().saturating_sub(1)] {
        if let Some(ns) = segment.strip_suffix(".db") {
            return Ok((ns.to_string(), table_name));
        }
    }

    // Fallback: if no .db suffix found, use the segment before the table name as namespace
    // This handles paths like s3://bucket/warehouse/namespace/table
    if segments.len() >= 2 {
        let namespace = segments[segments.len() - 2].to_string();
        return Ok((namespace, table_name));
    }

    Err(Error::IoError(format!(
        "Could not extract namespace from table location: {}",
        location
    )))
}

impl RestCredentialProvider {
    /// Check if credentials are cached for the given table location.
    fn check_cache_by_location(&self, table_location: &str) -> Result<Option<VendedCredentials>> {
        let cache = self
            .credential_cache
            .read()
            .map_err(|e| Error::IoError(format!("Failed to acquire cache read lock: {}", e)))?;

        Ok(cache.get(table_location).cloned())
    }

    /// Cache credentials for a table location.
    fn cache_credentials(&self, table_location: &str, creds: VendedCredentials) -> Result<()> {
        let mut cache = self
            .credential_cache
            .write()
            .map_err(|e| Error::IoError(format!("Failed to acquire cache write lock: {}", e)))?;

        cache.insert(table_location.to_string(), creds);
        Ok(())
    }

    /// Fetch credentials from the REST catalog's /credentials endpoint.
    async fn fetch_credentials(
        &self,
        namespace: &str,
        table_name: &str,
    ) -> Result<LoadTableCredentialsResponse> {
        let url = format!(
            "{}/v1/{}/namespaces/{}/tables/{}/credentials",
            self.endpoint.trim_end_matches('/'),
            encode(&self.prefix),
            encode(namespace),
            encode(table_name)
        );

        let response = self
            .http_client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| Error::IoError(format!("Failed to fetch credentials: {}", e)))?;

        let status = response.status();
        if status.as_u16() == 404 {
            return Err(Error::NotFound {
                resource: format!("credentials for {}.{}", namespace, table_name),
            });
        }

        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Error::IoError(format!(
                "Credentials request failed with status {}: {}",
                status, body
            )));
        }

        let creds_response: LoadTableCredentialsResponse = response
            .json()
            .await
            .map_err(|e| Error::IoError(format!("Failed to parse credentials response: {}", e)))?;

        Ok(creds_response)
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl VendedCredentialProvider for RestCredentialProvider {
    async fn get_credentials(&self, path: &str) -> Result<VendedCredentials> {
        // 1. Parse table location from path
        let table_location = extract_table_location(path)?;

        // 2. Check cache first using the extracted table location
        if let Some(cached) = self.check_cache_by_location(&table_location)? {
            return Ok(cached);
        }

        // 3. Derive table identifier from location
        let (namespace, table_name) = parse_table_identifier_from_location(&table_location)?;

        // 4. Fetch credentials from REST endpoint
        let creds_response = self.fetch_credentials(&namespace, &table_name).await?;

        // 5. Find matching credential for this path
        let cred = creds_response
            .storage_credentials
            .iter()
            .find(|c| path.starts_with(&c.prefix))
            .ok_or_else(|| {
                Error::IoError(format!(
                    "No matching credential prefix for path: {}. Available prefixes: {:?}",
                    path,
                    creds_response
                        .storage_credentials
                        .iter()
                        .map(|c| &c.prefix)
                        .collect::<Vec<_>>()
                ))
            })?;

        // 6. Convert to VendedCredentials
        let access_key_id = cred.config.access_key_id.clone().ok_or_else(|| {
            Error::InvalidInput("Vended credentials missing access_key_id".to_string())
        })?;

        let secret_access_key = cred.config.secret_access_key.clone().ok_or_else(|| {
            Error::InvalidInput("Vended credentials missing secret_access_key".to_string())
        })?;

        let vended = VendedCredentials {
            access_key_id,
            secret_access_key,
            session_token: cred.config.session_token.clone(),
            endpoint: cred
                .config
                .endpoint
                .clone()
                .or_else(|| self.s3_endpoint.clone()),
            region: cred.config.region.clone(),
        };

        // 7. Cache by table location
        self.cache_credentials(&table_location, vended.clone())?;

        Ok(vended)
    }

    fn s3_endpoint(&self) -> Option<&str> {
        self.s3_endpoint.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_table_location_data_path() {
        let path = "s3://bucket/warehouse/default.db/logs/data/00001.parquet";
        let result = extract_table_location(path).unwrap();
        assert_eq!(result, "s3://bucket/warehouse/default.db/logs");
    }

    #[test]
    fn test_extract_table_location_metadata_path() {
        let path = "s3://bucket/warehouse/default.db/logs/metadata/v1.metadata.json";
        let result = extract_table_location(path).unwrap();
        assert_eq!(result, "s3://bucket/warehouse/default.db/logs");
    }

    #[test]
    fn test_extract_table_location_nested_data() {
        let path = "s3://bucket/ns.db/table/data/partition=a/file.parquet";
        let result = extract_table_location(path).unwrap();
        assert_eq!(result, "s3://bucket/ns.db/table");
    }

    #[test]
    fn test_extract_table_location_no_iceberg_dir() {
        let path = "s3://bucket/some/random/path.parquet";
        let result = extract_table_location(path);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("does not contain Iceberg directory structure"));
    }

    #[test]
    fn test_parse_table_identifier_with_db_suffix() {
        let location = "s3://bucket/warehouse/default.db/logs";
        let (namespace, table) = parse_table_identifier_from_location(location).unwrap();
        assert_eq!(namespace, "default");
        assert_eq!(table, "logs");
    }

    #[test]
    fn test_parse_table_identifier_nested_warehouse() {
        let location = "s3://bucket/some/path/myns.db/mytable";
        let (namespace, table) = parse_table_identifier_from_location(location).unwrap();
        assert_eq!(namespace, "myns");
        assert_eq!(table, "mytable");
    }

    #[test]
    fn test_parse_table_identifier_fallback_no_db_suffix() {
        // When there's no .db suffix, use segment before table name
        let location = "s3://bucket/warehouse/namespace/table";
        let (namespace, table) = parse_table_identifier_from_location(location).unwrap();
        assert_eq!(namespace, "namespace");
        assert_eq!(table, "table");
    }

    #[test]
    fn test_parse_table_identifier_missing_prefix() {
        let location = "http://bucket/path/ns.db/table";
        let result = parse_table_identifier_from_location(location);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must start with s3://"));
    }

    #[test]
    fn test_parse_table_identifier_no_path() {
        let location = "s3://bucket";
        let result = parse_table_identifier_from_location(location);
        assert!(result.is_err());
    }

    /// Create a test RestCredentialProvider with dummy values.
    /// Only the credential_cache is functional; HTTP calls will fail.
    fn create_test_provider() -> RestCredentialProvider {
        RestCredentialProvider {
            endpoint: "http://localhost:8080".to_string(),
            prefix: "test-prefix".to_string(),
            token: "test-token".to_string(),
            http_client: Client::new(),
            s3_endpoint: None,
            credential_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn sample_credentials(id: &str) -> VendedCredentials {
        VendedCredentials {
            access_key_id: format!("AKIATEST{}", id),
            secret_access_key: format!("secret-{}", id),
            session_token: Some(format!("token-{}", id)),
            endpoint: Some("https://s3.example.com".to_string()),
            region: Some("us-west-2".to_string()),
        }
    }

    #[test]
    fn test_credential_caching_cache_miss_returns_none() {
        let provider = create_test_provider();

        // Cache miss: uncached location returns None
        let result = provider
            .check_cache_by_location("s3://bucket/ns.db/table1")
            .unwrap();
        assert!(result.is_none(), "Uncached location should return None");
    }

    #[test]
    fn test_credential_caching_cache_hit_after_store() {
        let provider = create_test_provider();
        let location = "s3://bucket/ns.db/table1";
        let creds = sample_credentials("1");

        // Store credentials
        provider.cache_credentials(location, creds.clone()).unwrap();

        // Cache hit: should return the stored credentials
        let cached = provider
            .check_cache_by_location(location)
            .unwrap()
            .expect("Should find cached credentials");

        assert_eq!(cached.access_key_id, creds.access_key_id);
        assert_eq!(cached.secret_access_key, creds.secret_access_key);
        assert_eq!(cached.session_token, creds.session_token);
        assert_eq!(cached.endpoint, creds.endpoint);
        assert_eq!(cached.region, creds.region);
    }

    #[test]
    fn test_credential_caching_different_locations_get_different_entries() {
        let provider = create_test_provider();

        let location1 = "s3://bucket/ns.db/table1";
        let location2 = "s3://bucket/ns.db/table2";
        let creds1 = sample_credentials("1");
        let creds2 = sample_credentials("2");

        // Store credentials for both locations
        provider
            .cache_credentials(location1, creds1.clone())
            .unwrap();
        provider
            .cache_credentials(location2, creds2.clone())
            .unwrap();

        // Verify each location returns its own credentials
        let cached1 = provider
            .check_cache_by_location(location1)
            .unwrap()
            .expect("Should find cached credentials for table1");
        let cached2 = provider
            .check_cache_by_location(location2)
            .unwrap()
            .expect("Should find cached credentials for table2");

        assert_eq!(cached1.access_key_id, creds1.access_key_id);
        assert_eq!(cached2.access_key_id, creds2.access_key_id);
        assert_ne!(cached1.access_key_id, cached2.access_key_id);
    }

    #[test]
    fn test_credential_caching_overwrite_existing() {
        let provider = create_test_provider();
        let location = "s3://bucket/ns.db/table1";

        let creds_v1 = sample_credentials("v1");
        let creds_v2 = sample_credentials("v2");

        // Store initial credentials
        provider.cache_credentials(location, creds_v1).unwrap();

        // Overwrite with new credentials
        provider
            .cache_credentials(location, creds_v2.clone())
            .unwrap();

        // Should return the updated credentials
        let cached = provider
            .check_cache_by_location(location)
            .unwrap()
            .expect("Should find cached credentials");

        assert_eq!(cached.access_key_id, creds_v2.access_key_id);
        assert_eq!(cached.secret_access_key, creds_v2.secret_access_key);
    }

    #[test]
    fn test_credential_caching_cache_isolation() {
        // Each provider has its own cache
        let provider1 = create_test_provider();
        let provider2 = create_test_provider();

        let location = "s3://bucket/ns.db/shared_table";
        let creds = sample_credentials("shared");

        // Store in provider1's cache only
        provider1.cache_credentials(location, creds).unwrap();

        // provider1 should have the entry
        assert!(provider1
            .check_cache_by_location(location)
            .unwrap()
            .is_some());

        // provider2 should not have the entry (separate cache)
        assert!(provider2
            .check_cache_by_location(location)
            .unwrap()
            .is_none());
    }
}
