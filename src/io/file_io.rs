//! FileIO implementation using OpenDAL
//! Compatible with both WASM and native targets

use crate::error::{Error, Result};
use opendal::Operator;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// AWS credentials for creating S3 operators dynamically
#[derive(Clone)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

/// Vended credentials returned by the catalog's /credentials endpoint
#[derive(Debug, Clone)]
pub struct VendedCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    /// Expiration time in milliseconds since Unix epoch (if provided by catalog)
    pub expires_at_ms: Option<i64>,
}

impl VendedCredentials {
    /// Check if these credentials have expired.
    /// Returns false if no expiration time is set (credentials don't expire).
    /// Uses a 60-second buffer to avoid using credentials that are about to expire.
    pub fn is_expired(&self) -> bool {
        const EXPIRY_BUFFER_MS: i64 = 60_000; // 60 seconds buffer

        match self.expires_at_ms {
            Some(expires_at) => {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                now_ms >= (expires_at - EXPIRY_BUFFER_MS)
            }
            None => false, // No expiration set, assume valid
        }
    }
}

/// Trait for providers that can fetch vended credentials from a catalog
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
pub trait VendedCredentialProvider: Send + Sync + std::fmt::Debug {
    /// Fetch credentials for accessing the given path
    async fn get_credentials(&self, path: &str) -> Result<VendedCredentials>;

    /// Get the S3-compatible endpoint for this provider (if known)
    fn s3_endpoint(&self) -> Option<&str>;

    /// Register a table's identity for credential lookup.
    ///
    /// This is used for catalogs like R2 Data Catalog that use UUID-based paths
    /// where the namespace and table name cannot be parsed from the file path.
    /// The default implementation does nothing (for providers that don't need this).
    ///
    /// # Arguments
    /// * `table_location` - The table's location prefix
    /// * `namespace` - The namespace name
    /// * `table_name` - The table name
    fn register_table(
        &self,
        _table_location: &str,
        _namespace: &str,
        _table_name: &str,
    ) -> Result<()> {
        Ok(()) // Default: no-op for providers that don't need table registration
    }
}

/// File I/O abstraction for reading/writing Iceberg files
///
/// Supports three modes:
/// - Single operator mode (R2): Uses pre-configured default_operator
/// - Multi-bucket mode (S3 Tables): Creates operators dynamically per bucket using credentials
/// - Vended credentials mode: Fetches credentials from catalog for each table
///
/// For S3 Tables, all buckets are in the same region, so we only cache by bucket name.
#[derive(Clone)]
pub struct FileIO {
    /// AWS credentials for creating operators (S3 Tables mode)
    credentials: Option<AwsCredentials>,
    /// Default region for all operators
    default_region: String,
    /// Cache of operators per bucket (all same region for S3 Tables)
    operator_cache: Arc<RwLock<HashMap<String, Operator>>>,
    /// Pre-configured operator (R2 mode)
    default_operator: Option<Operator>,
    /// Vended credential provider (REST catalog mode)
    vended_credential_provider: Option<Arc<dyn VendedCredentialProvider>>,
}

impl FileIO {
    /// Create a new FileIO with the given OpenDAL operator (R2 mode)
    ///
    /// This creates a FileIO that uses a single pre-configured operator for all operations.
    /// Suitable for R2 and other single-bucket scenarios.
    pub fn new(operator: Operator) -> Self {
        Self {
            credentials: None,
            default_region: String::new(),
            operator_cache: Arc::new(RwLock::new(HashMap::new())),
            default_operator: Some(operator),
            vended_credential_provider: None,
        }
    }

    /// Create a new FileIO from AWS credentials (S3 Tables mode)
    ///
    /// This creates a FileIO that dynamically creates and caches operators per bucket.
    /// Suitable for S3 Tables where data may be in different buckets/regions.
    pub fn from_aws_credentials(credentials: AwsCredentials, default_region: String) -> Self {
        Self {
            credentials: Some(credentials),
            default_region,
            operator_cache: Arc::new(RwLock::new(HashMap::new())),
            default_operator: None,
            vended_credential_provider: None,
        }
    }

    /// Create a new FileIO with vended credentials from a catalog
    ///
    /// This creates a FileIO that fetches credentials on-demand from the catalog's
    /// /credentials endpoint. The credentials are cached per bucket.
    pub fn with_vended_credentials(provider: Arc<dyn VendedCredentialProvider>) -> Self {
        Self {
            credentials: None,
            default_region: "auto".to_string(),
            operator_cache: Arc::new(RwLock::new(HashMap::new())),
            default_operator: None,
            vended_credential_provider: Some(provider),
        }
    }

    /// Create a FileIO with pre-fetched vended credentials
    ///
    /// Use this when you've already fetched credentials (e.g., from loading a table)
    /// and want to create a FileIO for that specific table's files.
    pub fn from_vended_credentials(creds: VendedCredentials, bucket: &str) -> Result<Self> {
        let endpoint = creds.endpoint.clone().ok_or_else(|| {
            Error::InvalidInput("Vended credentials missing endpoint".to_string())
        })?;

        let region = creds.region.clone().unwrap_or_else(|| "auto".to_string());

        use opendal::services::S3;
        let mut builder = S3::default()
            .bucket(bucket)
            .region(&region)
            .endpoint(&endpoint)
            .access_key_id(&creds.access_key_id)
            .secret_access_key(&creds.secret_access_key);

        if let Some(ref token) = creds.session_token {
            builder = builder.session_token(token);
        }

        let operator = Operator::new(builder)
            .map_err(|e| Error::IoError(format!("Failed to create S3 operator: {}", e)))?
            .finish();

        Ok(Self::new(operator))
    }

    /// Extract bucket name from S3 URI
    ///
    /// Converts "s3://bucket/path/to/file" to ("bucket", "path/to/file")
    fn extract_bucket_from_uri(&self, path: &str) -> Result<(String, String)> {
        for prefix in &["s3://", "gs://", "s3a://"] {
            if let Some(stripped) = path.strip_prefix(prefix) {
                if let Some(slash_pos) = stripped.find('/') {
                    let bucket = stripped[..slash_pos].to_string();
                    let path = stripped[slash_pos + 1..].to_string();
                    return Ok((bucket, path));
                } else {
                    return Ok((stripped.to_string(), String::new()));
                }
            }
        }
        Err(Error::InvalidInput(format!(
            "Path does not start with s3://, gs://, or s3a://: {}",
            path
        )))
    }

    /// Get operator for a given path using priority-based routing
    ///
    /// Priority:
    /// 1. If default_operator exists → use it (R2 case)
    /// 2. If credentials exist → create dynamic operator (S3 Tables case)
    /// 3. If vended credential provider exists → fetch and cache credentials
    /// 4. Error - no operator configured
    async fn get_operator_for_path(&self, path: &str) -> Result<Operator> {
        // Priority 1: Use default operator if available (R2 mode)
        if let Some(ref op) = self.default_operator {
            return Ok(op.clone());
        }

        // Priority 2: Create dynamic operator if credentials available (S3 Tables mode)
        if self.credentials.is_some() {
            let (bucket, _) = self.extract_bucket_from_uri(path)?;
            return self.get_or_create_operator(&bucket).await;
        }

        // Priority 3: Use vended credentials if provider available
        if let Some(ref provider) = self.vended_credential_provider {
            let (bucket, _) = self.extract_bucket_from_uri(path)?;

            // Check cache first
            {
                let cache = self
                    .operator_cache
                    .read()
                    .map_err(|e| Error::IoError(format!(
                        "Lock poisoned due to panic in another thread. This indicates a critical bug. Original error: {}",
                        e
                    )))?;
                if let Some(op) = cache.get(&bucket) {
                    return Ok(op.clone());
                }
            }

            // Fetch credentials from provider
            let creds = provider.get_credentials(path).await?;

            // Build operator with vended credentials
            let endpoint = creds
                .endpoint
                .clone()
                .or_else(|| provider.s3_endpoint().map(|s| s.to_string()))
                .ok_or_else(|| {
                    Error::InvalidInput(
                        "No S3 endpoint available for vended credentials".to_string(),
                    )
                })?;

            let region = creds.region.clone().unwrap_or_else(|| "auto".to_string());

            use opendal::services::S3;
            let mut builder = S3::default()
                .bucket(&bucket)
                .region(&region)
                .endpoint(&endpoint)
                .access_key_id(&creds.access_key_id)
                .secret_access_key(&creds.secret_access_key);

            if let Some(ref token) = creds.session_token {
                builder = builder.session_token(token);
            }

            let operator = Operator::new(builder)
                .map_err(|e| Error::IoError(format!("Failed to create S3 operator: {}", e)))?
                .finish();

            // Cache the operator
            let mut cache = self
                .operator_cache
                .write()
                .map_err(|e| Error::IoError(format!(
                    "Lock poisoned due to panic in another thread. This indicates a critical bug. Original error: {}",
                    e
                )))?;
            cache.insert(bucket, operator.clone());

            return Ok(operator);
        }

        // Priority 4: No operator configured
        Err(Error::InvalidInput(
            "FileIO not configured with operator or credentials".to_string(),
        ))
    }

    /// Get or create an operator for the given bucket, using cache
    ///
    /// Uses double-checked locking pattern for thread-safe caching.
    /// All buckets are in the same region (default_region), so we only cache by bucket name.
    async fn get_or_create_operator(&self, bucket: &str) -> Result<Operator> {
        // Fast path: read lock
        {
            let cache = self
                .operator_cache
                .read()
                .map_err(|e| Error::IoError(format!(
                    "Lock poisoned due to panic in another thread. This indicates a critical bug. Original error: {}",
                    e
                )))?;
            if let Some(op) = cache.get(bucket) {
                return Ok(op.clone());
            }
        }

        // Slow path: write lock
        let mut cache = self
            .operator_cache
            .write()
            .map_err(|e| Error::IoError(format!(
                "Lock poisoned due to panic in another thread. This indicates a critical bug. Original error: {}",
                e
            )))?;

        // Double-check pattern
        if let Some(op) = cache.get(bucket) {
            return Ok(op.clone());
        }

        let op = self.create_s3_operator(bucket, &self.default_region)?;
        cache.insert(bucket.to_string(), op.clone());
        Ok(op)
    }

    /// Create an S3 operator for the given bucket and region
    fn create_s3_operator(&self, bucket: &str, region: &str) -> Result<Operator> {
        use opendal::services::S3;

        let credentials = self.credentials.as_ref().ok_or_else(|| {
            Error::InvalidInput("No credentials available for S3 operator creation".to_string())
        })?;

        let builder = S3::default()
            .bucket(bucket)
            .region(region)
            .access_key_id(&credentials.access_key_id)
            .secret_access_key(&credentials.secret_access_key);

        let builder = if let Some(ref token) = credentials.session_token {
            builder.session_token(token)
        } else {
            builder
        };

        Ok(Operator::new(builder)
            .map_err(|e| Error::IoError(format!("Failed to create S3 operator: {}", e)))?
            .finish())
    }

    /// Normalize path by stripping S3 URI prefix if present
    /// Converts "s3://bucket/path/to/file" to "path/to/file"
    fn normalize_path<'a>(&self, path: &'a str) -> &'a str {
        for prefix in &["s3://", "gs://", "s3a://"] {
            if let Some(stripped) = path.strip_prefix(prefix) {
                if let Some(pos) = stripped.find('/') {
                    return &stripped[pos + 1..];
                }
            }
        }
        path
    }

    /// Read a file completely
    pub async fn read(&self, path: &str) -> Result<Vec<u8>> {
        let operator = self.get_operator_for_path(path).await?;
        let normalized = self.normalize_path(path);
        operator
            .read(normalized)
            .await
            .map(|b| b.to_vec())
            .map_err(|e| Error::IoError(format!("Failed to read {}: {}", path, e)))
    }

    /// Read a byte range from a file `[offset, offset + length)`
    pub async fn read_range(&self, path: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        let operator = self.get_operator_for_path(path).await?;
        let normalized = self.normalize_path(path);
        let start = offset;
        let end = offset.saturating_add(length);
        let range = start..end;
        operator
            .read_with(normalized)
            .range(range)
            .await
            .map(|b| b.to_vec())
            .map_err(|e| Error::IoError(format!("Failed to read range of {}: {}", path, e)))
    }

    /// Get file size in bytes
    pub async fn file_size(&self, path: &str) -> Result<u64> {
        let operator = self.get_operator_for_path(path).await?;
        let normalized = self.normalize_path(path);
        operator
            .stat(normalized)
            .await
            .map(|m| m.content_length())
            .map_err(|e| Error::IoError(format!("Failed to stat {}: {}", path, e)))
    }

    /// Write data to a file
    ///
    /// Accepts anything convertible into `bytes::Bytes` (including `Vec<u8>`)
    /// so callers that already hold a `Bytes` — e.g. compaction parsing the
    /// parquet footer in-memory before uploading — can avoid an extra copy.
    pub async fn write(&self, path: &str, data: impl Into<bytes::Bytes>) -> Result<()> {
        let operator = self.get_operator_for_path(path).await?;
        let normalized = self.normalize_path(path);
        operator
            .write(normalized, data.into())
            .await
            .map(|_| ()) // Discard Metadata return value
            .map_err(|e| Error::IoError(format!("Failed to write {}: {}", path, e)))
    }

    /// Check if a file exists
    pub async fn exists(&self, path: &str) -> Result<bool> {
        let operator = self.get_operator_for_path(path).await?;
        let normalized = self.normalize_path(path);
        match operator.exists(normalized).await {
            Ok(exists) => Ok(exists),
            Err(e) => Err(Error::IoError(format!(
                "Failed to check existence of {}: {}",
                path, e
            ))),
        }
    }

    /// Delete a file
    pub async fn delete(&self, path: &str) -> Result<()> {
        let operator = self.get_operator_for_path(path).await?;
        let normalized = self.normalize_path(path);
        operator
            .delete(normalized)
            .await
            .map_err(|e| Error::IoError(format!("Failed to delete {}: {}", path, e)))
    }

    /// Register a table's identity for credential lookup.
    ///
    /// This is used for catalogs like R2 Data Catalog that use UUID-based paths
    /// where the namespace and table name cannot be parsed from the file path.
    /// When vended credentials are used, this registers the table's identity
    /// so that credential fetching can use the actual namespace and table name.
    ///
    /// This is a no-op if no vended credential provider is configured.
    ///
    /// # Arguments
    /// * `table_location` - The table's location prefix
    /// * `namespace` - The namespace name
    /// * `table_name` - The table name
    pub fn register_table(
        &self,
        table_location: &str,
        namespace: &str,
        table_name: &str,
    ) -> Result<()> {
        if let Some(ref provider) = self.vended_credential_provider {
            provider.register_table(table_location, namespace, table_name)
        } else {
            Ok(()) // No-op if no vended credential provider
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_io_creation() {
        let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
        let _file_io = FileIO::new(op);
        // Just verify it compiles and constructs
    }
}
