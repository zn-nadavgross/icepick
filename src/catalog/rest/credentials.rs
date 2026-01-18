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
    /// Map table location prefix -> (namespace, table_name) for UUID-based paths
    /// R2 Data Catalog uses UUID-based file paths that cannot be parsed to extract
    /// namespace/table name. This registry allows explicit registration of table
    /// identity for credential lookup.
    pub(crate) table_registry: Arc<RwLock<HashMap<String, (String, String)>>>,
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
    /// Register a table's identity for credential lookup.
    ///
    /// This allows the credential provider to fetch credentials using the table's
    /// actual namespace and name, rather than trying to parse them from file paths.
    /// This is essential for R2 Data Catalog which uses UUID-based paths like:
    /// `s3://bucket/019b9635-52b8-72b3-829b-de5900e5b195.019b9635-53e1-7732-b9f4-7b6b9ff240e7/data/file.parquet`
    ///
    /// # Arguments
    /// * `table_location` - The table's location prefix (e.g., `s3://bucket/uuid.uuid`)
    /// * `namespace` - The namespace name
    /// * `table_name` - The table name
    pub fn register_table(
        &self,
        table_location: &str,
        namespace: &str,
        table_name: &str,
    ) -> Result<()> {
        let mut registry = self.table_registry.write().map_err(|e| {
            Error::IoError(format!(
                "Failed to acquire table registry write lock: {}",
                e
            ))
        })?;
        registry.insert(
            table_location.to_string(),
            (namespace.to_string(), table_name.to_string()),
        );
        Ok(())
    }

    /// Look up a registered table identity by location.
    ///
    /// Returns `Some((namespace, table_name))` if the table was registered,
    /// or `None` if not found.
    fn lookup_registered_table(&self, table_location: &str) -> Result<Option<(String, String)>> {
        let registry = self.table_registry.read().map_err(|e| {
            Error::IoError(format!("Failed to acquire table registry read lock: {}", e))
        })?;
        Ok(registry.get(table_location).cloned())
    }

    /// Check if non-expired credentials are cached for the given table location.
    /// Returns None if credentials are not cached or have expired.
    fn check_cache_by_location(&self, table_location: &str) -> Result<Option<VendedCredentials>> {
        let cache = self
            .credential_cache
            .read()
            .map_err(|e| Error::IoError(format!("Failed to acquire cache read lock: {}", e)))?;

        match cache.get(table_location) {
            Some(creds) if !creds.is_expired() => Ok(Some(creds.clone())),
            Some(_) => Ok(None), // Expired credentials - treat as cache miss
            None => Ok(None),
        }
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
        // Check if we have a registered table identity for this location (for UUID-based paths)
        let (namespace, table_name) =
            if let Some((ns, tn)) = self.lookup_registered_table(&table_location)? {
                (ns, tn)
            } else {
                // Fall back to path parsing for backwards compatibility
                parse_table_identifier_from_location(&table_location)?
            };

        // 4. Fetch credentials from REST endpoint
        let creds_response = self.fetch_credentials(&namespace, &table_name).await?;

        // 5. Find matching credential for this path
        // R2 may return "/" as the prefix meaning "all paths", so we need flexible matching
        let cred = creds_response
            .storage_credentials
            .iter()
            .find(|c| {
                // "/" or empty prefix means "match all"
                if c.prefix == "/" || c.prefix.is_empty() {
                    return true;
                }
                // Try exact prefix match first
                if path.starts_with(&c.prefix) {
                    return true;
                }
                // Try matching just the path portion (after s3://bucket/)
                if let Some(path_portion) = path
                    .strip_prefix("s3://")
                    .and_then(|p| p.find('/').map(|i| &p[i..]))
                {
                    if path_portion.starts_with(&c.prefix) {
                        return true;
                    }
                }
                false
            })
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
            expires_at_ms: cred.config.expires_at_ms,
        };

        // 7. Cache by table location
        self.cache_credentials(&table_location, vended.clone())?;

        Ok(vended)
    }

    fn s3_endpoint(&self) -> Option<&str> {
        self.s3_endpoint.as_deref()
    }

    fn register_table(
        &self,
        table_location: &str,
        namespace: &str,
        table_name: &str,
    ) -> Result<()> {
        // Delegate to the struct's register_table method
        RestCredentialProvider::register_table(self, table_location, namespace, table_name)
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
            table_registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn sample_credentials(id: &str) -> VendedCredentials {
        VendedCredentials {
            access_key_id: format!("AKIATEST{}", id),
            secret_access_key: format!("secret-{}", id),
            session_token: Some(format!("token-{}", id)),
            endpoint: Some("https://s3.example.com".to_string()),
            region: Some("us-west-2".to_string()),
            expires_at_ms: None, // No expiration for test credentials
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

    #[test]
    fn test_table_registry_register_and_lookup() {
        let provider = create_test_provider();
        let location =
            "s3://bucket/019b9635-52b8-72b3-829b-de5900e5b195.019b9635-53e1-7732-b9f4-7b6b9ff240e7";

        // Initially not registered
        let result = provider.lookup_registered_table(location).unwrap();
        assert!(result.is_none());

        // Register the table
        provider
            .register_table(location, "my_namespace", "my_table")
            .unwrap();

        // Now it should be found
        let (namespace, table_name) = provider
            .lookup_registered_table(location)
            .unwrap()
            .expect("Should find registered table");
        assert_eq!(namespace, "my_namespace");
        assert_eq!(table_name, "my_table");
    }

    #[test]
    fn test_table_registry_overwrite() {
        let provider = create_test_provider();
        let location = "s3://bucket/uuid-path";

        // Register initial values
        provider.register_table(location, "ns1", "table1").unwrap();

        // Overwrite with new values
        provider.register_table(location, "ns2", "table2").unwrap();

        // Should return the updated values
        let (namespace, table_name) = provider
            .lookup_registered_table(location)
            .unwrap()
            .expect("Should find registered table");
        assert_eq!(namespace, "ns2");
        assert_eq!(table_name, "table2");
    }

    #[test]
    fn test_table_registry_multiple_tables() {
        let provider = create_test_provider();
        let location1 = "s3://bucket/uuid1";
        let location2 = "s3://bucket/uuid2";

        provider.register_table(location1, "ns1", "table1").unwrap();
        provider.register_table(location2, "ns2", "table2").unwrap();

        let (ns1, tn1) = provider
            .lookup_registered_table(location1)
            .unwrap()
            .expect("Should find table1");
        let (ns2, tn2) = provider
            .lookup_registered_table(location2)
            .unwrap()
            .expect("Should find table2");

        assert_eq!(ns1, "ns1");
        assert_eq!(tn1, "table1");
        assert_eq!(ns2, "ns2");
        assert_eq!(tn2, "table2");
    }

    #[test]
    fn test_expired_credentials_not_returned_from_cache() {
        let provider = create_test_provider();
        let location = "s3://bucket/ns.db/table1";

        // Create credentials that expired 1 hour ago
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let expired_creds = VendedCredentials {
            access_key_id: "AKIAEXPIRED".to_string(),
            secret_access_key: "expired-secret".to_string(),
            session_token: None,
            endpoint: Some("https://s3.example.com".to_string()),
            region: Some("us-west-2".to_string()),
            expires_at_ms: Some(now_ms - 3_600_000), // Expired 1 hour ago
        };

        // Store expired credentials
        provider.cache_credentials(location, expired_creds).unwrap();

        // Cache check should return None for expired credentials
        let result = provider.check_cache_by_location(location).unwrap();
        assert!(
            result.is_none(),
            "Expired credentials should not be returned from cache"
        );
    }

    #[test]
    fn test_valid_credentials_returned_from_cache() {
        let provider = create_test_provider();
        let location = "s3://bucket/ns.db/table1";

        // Create credentials that expire in 1 hour
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let valid_creds = VendedCredentials {
            access_key_id: "AKIAVALID".to_string(),
            secret_access_key: "valid-secret".to_string(),
            session_token: None,
            endpoint: Some("https://s3.example.com".to_string()),
            region: Some("us-west-2".to_string()),
            expires_at_ms: Some(now_ms + 3_600_000), // Expires in 1 hour
        };

        // Store valid credentials
        provider
            .cache_credentials(location, valid_creds.clone())
            .unwrap();

        // Cache check should return the credentials
        let result = provider.check_cache_by_location(location).unwrap();
        assert!(
            result.is_some(),
            "Valid credentials should be returned from cache"
        );
        assert_eq!(result.unwrap().access_key_id, "AKIAVALID");
    }

    #[test]
    fn test_credentials_near_expiry_not_returned() {
        let provider = create_test_provider();
        let location = "s3://bucket/ns.db/table1";

        // Create credentials that expire in 30 seconds (within 60s buffer)
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let near_expiry_creds = VendedCredentials {
            access_key_id: "AKIANEAREXPIRY".to_string(),
            secret_access_key: "near-expiry-secret".to_string(),
            session_token: None,
            endpoint: Some("https://s3.example.com".to_string()),
            region: Some("us-west-2".to_string()),
            expires_at_ms: Some(now_ms + 30_000), // Expires in 30 seconds
        };

        // Store credentials
        provider
            .cache_credentials(location, near_expiry_creds)
            .unwrap();

        // Cache check should return None (within 60s buffer)
        let result = provider.check_cache_by_location(location).unwrap();
        assert!(
            result.is_none(),
            "Credentials near expiry should not be returned from cache"
        );
    }
}
