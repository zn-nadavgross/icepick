//! Client constructor methods for IcebergRestCatalog
use super::commit_types::{CommitTableRequest, CommitTableResponse};
use super::credentials::RestCredentialProvider;
use super::types;
use super::IcebergRestCatalog;
use crate::catalog::{
    AuthProvider, CatalogError, CatalogOptions, HttpClientConfig, R2Config, Result,
};
use crate::io::FileIO;
use crate::spec::TableIdent;
use reqwest::Client;
use std::sync::Arc;

#[cfg(not(target_family = "wasm"))]
use super::arn::{parse_s3tables_arn, ARN_ENCODE_SET};

#[cfg(not(target_family = "wasm"))]
use aws_credential_types::provider::ProvideCredentials;

#[cfg(not(target_family = "wasm"))]
use percent_encoding::utf8_percent_encode;

/// Fetch catalog configuration from /v1/config endpoint
async fn fetch_config_response(
    http_client: &Client,
    auth: &dyn AuthProvider,
    endpoint: &str,
    warehouse: &str,
) -> Result<types::ConfigResponse> {
    let config_url = format!(
        "{}/v1/config?warehouse={}",
        endpoint.trim_end_matches('/'),
        urlencoding::encode(warehouse)
    );

    let req = http_client
        .get(&config_url)
        .build()
        .map_err(|e| CatalogError::HttpError(format!("Failed to build config request: {}", e)))?;

    let signed_req = auth.sign_request(req).await?;

    let response = http_client
        .execute(signed_req)
        .await
        .map_err(|e| CatalogError::HttpError(format!("Config request failed: {}", e)))?;

    let status = response.status();
    let body_text = response
        .text()
        .await
        .map_err(|e| CatalogError::HttpError(format!(
            "Failed to read response body from {}: {}. This may indicate a network interruption or invalid response encoding.",
            config_url, e
        )))?;

    if !status.is_success() {
        return Err(CatalogError::HttpError(format!(
            "Config request failed with status {}: {}",
            status, body_text
        )));
    }

    serde_json::from_str(&body_text)
        .map_err(|e| CatalogError::HttpError(format!("Failed to parse config response: {}", e)))
}

impl IcebergRestCatalog {
    /// Create a generic Iceberg REST catalog from preconfigured components.
    pub(crate) fn from_components(
        name: String,
        endpoint: impl Into<String>,
        prefix: impl Into<String>,
        auth_provider: Box<dyn AuthProvider>,
        file_io: FileIO,
        options: CatalogOptions,
    ) -> Result<Self> {
        let http_client = build_http_client(options.http())?;

        Ok(Self {
            endpoint: endpoint.into(),
            prefix: prefix.into(),
            http_client,
            auth_provider,
            file_io,
            name,
            options,
        })
    }

    /// Create catalog for Cloudflare R2 Data Catalog (shortcut)
    pub async fn from_r2(
        name: String,
        account_id: impl Into<String>,
        bucket_name: impl Into<String>,
        api_token: impl Into<String>,
    ) -> Result<Self> {
        Self::from_r2_with_options(
            name,
            account_id,
            bucket_name,
            api_token,
            CatalogOptions::default(),
        )
        .await
    }

    /// Create catalog for Cloudflare R2 Data Catalog with explicit options
    pub async fn from_r2_with_options(
        name: String,
        account_id: impl Into<String>,
        bucket_name: impl Into<String>,
        api_token: impl Into<String>,
        options: CatalogOptions,
    ) -> Result<Self> {
        let config = R2Config {
            account_id: account_id.into(),
            bucket_name: bucket_name.into(),
            api_token: api_token.into(),
            endpoint_override: None,
        };
        Self::from_r2_config_with_options(name, config, options).await
    }

    pub(crate) async fn from_r2_config_with_options(
        name: String,
        config: R2Config,
        options: CatalogOptions,
    ) -> Result<Self> {
        let endpoint = config.endpoint_override.unwrap_or_else(|| {
            format!(
                "https://catalog.cloudflarestorage.com/{}/{}",
                config.account_id, config.bucket_name
            )
        });

        let auth = Box::new(crate::catalog::BearerTokenAuthProvider::new(
            config.api_token,
        ));
        let http_client = build_http_client(options.http())?;

        // Construct warehouse name from account_id and bucket_name
        let warehouse = format!("{}_{}", config.account_id, config.bucket_name);

        // Fetch catalog configuration
        let config_response =
            fetch_config_response(&http_client, auth.as_ref(), &endpoint, &warehouse).await?;

        // Merge configuration: defaults < client properties < overrides
        let mut properties = config_response.defaults;
        properties.insert("warehouse".to_string(), warehouse.clone());
        properties.extend(config_response.overrides);

        // Extract prefix from server configuration (defaults to empty string)
        let prefix = properties.get("prefix").cloned().unwrap_or_default();

        // Configure FileIO for R2 S3-compatible storage using opendal
        let r2_endpoint = format!("https://{}.r2.cloudflarestorage.com", config.account_id);

        let mut s3_config_vec = vec![
            ("endpoint".to_string(), r2_endpoint),
            ("bucket".to_string(), config.bucket_name.clone()),
            ("region".to_string(), "auto".to_string()), // R2 always uses "auto" region
        ];

        // Apply properties from config response
        for (key, value) in &properties {
            if key.starts_with("s3.") {
                let opendal_key = key.strip_prefix("s3.").unwrap_or(key).to_string();
                s3_config_vec.push((opendal_key, value.clone()));
            }
        }

        let operator =
            opendal::Operator::via_iter(opendal::Scheme::S3, s3_config_vec).map_err(|e| {
                CatalogError::Unexpected(format!("Failed to create S3 operator: {}", e))
            })?;
        let file_io = FileIO::new(operator);

        Ok(Self {
            endpoint,
            prefix,
            http_client,
            auth_provider: auth,
            file_io,
            name,
            options,
        })
    }

    /// Create catalog for Cloudflare R2 with a pre-configured FileIO (for explicit credentials)
    pub(crate) async fn from_r2_with_file_io(
        name: String,
        config: R2Config,
        file_io: FileIO,
        options: CatalogOptions,
    ) -> Result<Self> {
        let endpoint = config.endpoint_override.unwrap_or_else(|| {
            format!(
                "https://catalog.cloudflarestorage.com/{}/{}",
                config.account_id, config.bucket_name
            )
        });

        let auth = Box::new(crate::catalog::BearerTokenAuthProvider::new(
            config.api_token,
        ));
        let http_client = build_http_client(options.http())?;

        // Construct warehouse name from account_id and bucket_name
        let warehouse = format!("{}_{}", config.account_id, config.bucket_name);

        // Fetch catalog configuration
        let config_response =
            fetch_config_response(&http_client, auth.as_ref(), &endpoint, &warehouse).await?;

        // Merge configuration: defaults < client properties < overrides
        let mut properties = config_response.defaults;
        properties.insert("warehouse".to_string(), warehouse.clone());
        properties.extend(config_response.overrides);

        // Extract prefix from server configuration (defaults to empty string)
        let prefix = properties.get("prefix").cloned().unwrap_or_default();

        // Use the provided FileIO instead of creating a new one
        Ok(Self {
            endpoint,
            prefix,
            http_client,
            auth_provider: auth,
            file_io,
            name,
            options,
        })
    }

    /// Create catalog from a catalog URL and bearer token (calls /v1/config, sets up vended credentials)
    pub async fn from_url(
        name: impl Into<String>,
        catalog_url: impl Into<String>,
        token: impl Into<String>,
        warehouse: Option<String>,
    ) -> Result<Self> {
        Self::from_url_with_options(
            name,
            catalog_url,
            token,
            warehouse,
            CatalogOptions::default(),
        )
        .await
    }

    /// Create catalog from a catalog URL and bearer token with custom options.
    pub async fn from_url_with_options(
        name: impl Into<String>,
        catalog_url: impl Into<String>,
        token: impl Into<String>,
        warehouse: Option<String>,
        options: CatalogOptions,
    ) -> Result<Self> {
        let name = name.into();
        let endpoint = catalog_url.into();
        let token = token.into();

        // Derive warehouse from URL if not provided
        // URL format: https://catalog.example.com/account/bucket -> account_bucket
        let warehouse = warehouse.unwrap_or_else(|| derive_warehouse_from_url(&endpoint));

        let auth = Box::new(crate::catalog::BearerTokenAuthProvider::new(token.clone()));
        let http_client = build_http_client(options.http())?;

        // Fetch catalog configuration
        let config_response =
            fetch_config_response(&http_client, auth.as_ref(), &endpoint, &warehouse).await?;

        // Merge configuration: defaults < overrides
        let mut properties = config_response.defaults;
        properties.extend(config_response.overrides);

        // Extract prefix from server configuration
        let prefix = properties.get("prefix").cloned().unwrap_or_default();

        // Extract S3 endpoint from config if available (for R2, this comes from properties)
        let s3_endpoint = properties.get("s3.endpoint").cloned();

        // Create credential provider for vended credentials
        let credential_provider = Arc::new(RestCredentialProvider {
            endpoint: endpoint.clone(),
            prefix: prefix.clone(),
            token: token.clone(),
            http_client: http_client.clone(),
            s3_endpoint,
            credential_cache: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            table_registry: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        });

        // Create FileIO with vended credential support
        let file_io = FileIO::with_vended_credentials(credential_provider);

        Ok(Self {
            endpoint,
            prefix,
            http_client,
            auth_provider: auth,
            file_io,
            name,
            options,
        })
    }

    /// Load credentials for a table from the catalog's /credentials endpoint
    pub async fn load_table_credentials(
        &self,
        identifier: &TableIdent,
    ) -> Result<types::LoadTableCredentialsResponse> {
        let namespace = identifier.namespace().as_ref().join("/");
        let table_name = identifier.name();

        let url = format!(
            "{}/v1/{}/namespaces/{}/tables/{}/credentials",
            self.endpoint.trim_end_matches('/'),
            self.prefix,
            namespace,
            table_name
        );

        let req = self.http_client.get(&url).build().map_err(|e| {
            CatalogError::HttpError(format!("Failed to build credentials request: {}", e))
        })?;

        let response = self.send_request(req).await?;
        let json_value = self.handle_response(response).await?;

        serde_json::from_value(json_value).map_err(|e| {
            CatalogError::HttpError(format!("Failed to parse credentials response: {}", e))
        })
    }

    /// Create catalog for AWS S3 Tables
    #[cfg(not(target_family = "wasm"))]
    pub async fn from_s3_tables_arn(name: String, arn: &str) -> Result<Self> {
        Self::from_s3_tables_arn_with_options(name, arn, CatalogOptions::default()).await
    }

    #[cfg(not(target_family = "wasm"))]
    pub async fn from_s3_tables_arn_with_options(
        name: String,
        arn: &str,
        options: CatalogOptions,
    ) -> Result<Self> {
        let (region, _bucket_name) = parse_s3tables_arn(arn)?;
        let endpoint = format!("https://s3tables.{}.amazonaws.com/iceberg", region);

        // URL-encode the ARN for use in path
        let warehouse_prefix = utf8_percent_encode(arn, ARN_ENCODE_SET).to_string();

        // Load AWS credentials
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let credentials = config
            .credentials_provider()
            .ok_or_else(|| CatalogError::AuthError("No credentials provider found".to_string()))?
            .provide_credentials()
            .await
            .map_err(|e| CatalogError::AuthError(format!("Failed to load credentials: {}", e)))?;

        let auth = Box::new(crate::catalog::SigV4AuthProvider::new(
            region.clone(),
            "s3tables".to_string(),
            credentials.clone(),
        ));

        let http_client = build_http_client(options.http())?;

        // Create FileIO with AWS credentials for multi-bucket support
        // S3 Tables stores data in AWS-managed buckets that may be in different regions
        let file_io_credentials = crate::io::AwsCredentials {
            access_key_id: credentials.access_key_id().to_string(),
            secret_access_key: credentials.secret_access_key().to_string(),
            session_token: credentials.session_token().map(|s| s.to_string()),
        };
        let file_io = FileIO::from_aws_credentials(file_io_credentials, region.clone());

        Ok(Self {
            endpoint,
            prefix: warehouse_prefix, // url() method already adds /v1/
            http_client,
            auth_provider: auth,
            file_io,
            name,
            options,
        })
    }

    /// Commit table changes
    pub async fn commit_table(
        &self,
        identifier: &TableIdent,
        request: CommitTableRequest,
    ) -> Result<CommitTableResponse> {
        let namespace = identifier.namespace().as_ref().join("/");
        let table_name = identifier.name();

        let url = self.table_url(&namespace, table_name, true);

        // Diagnostic logging for debugging
        let req = self
            .http_client
            .post(&url)
            .json(&request)
            .build()
            .map_err(|e| CatalogError::HttpError(format!("Failed to build request: {}", e)))?;

        let response = self.send_request(req).await?;

        if response.status().as_u16() == 409 {
            return Err(CatalogError::Conflict(
                "Concurrent modification detected".to_string(),
            ));
        }

        // Handle response using common handler (supports empty responses)
        let json_value = self.handle_response(response).await?;

        let commit_response: CommitTableResponse = serde_json::from_value(json_value)
            .map_err(|e| CatalogError::HttpError(format!("Failed to parse response: {}", e)))?;

        Ok(commit_response)
    }
}

#[cfg(not(target_family = "wasm"))]
fn build_http_client(config: &HttpClientConfig) -> Result<Client> {
    let mut builder = Client::builder();
    if let Some(timeout) = config.timeout() {
        builder = builder.timeout(timeout);
    }
    if let Some(connect_timeout) = config.connect_timeout() {
        builder = builder.connect_timeout(connect_timeout);
    }
    if let Some(user_agent) = config.user_agent() {
        builder = builder.user_agent(user_agent.to_string());
    }
    builder
        .build()
        .map_err(|e| CatalogError::HttpError(format!("Failed to build HTTP client: {}", e)))
}

#[cfg(target_family = "wasm")]
fn build_http_client(_config: &HttpClientConfig) -> Result<Client> {
    Client::builder()
        .build()
        .map_err(|e| CatalogError::HttpError(format!("Failed to build HTTP client: {}", e)))
}

/// Derive warehouse from URL (last two path segments joined with underscore)
fn derive_warehouse_from_url(url: &str) -> String {
    // Parse URL and extract path segments
    if let Ok(parsed) = url::Url::parse(url) {
        let segments: Vec<&str> = parsed
            .path_segments()
            .map(|s| s.collect())
            .unwrap_or_default();

        // Take last two non-empty segments
        let non_empty: Vec<&str> = segments.into_iter().filter(|s| !s.is_empty()).collect();
        if non_empty.len() >= 2 {
            return format!(
                "{}_{}",
                non_empty[non_empty.len() - 2],
                non_empty[non_empty.len() - 1]
            );
        } else if non_empty.len() == 1 {
            return non_empty[0].to_string();
        }
    }

    // Fallback: use the full URL as warehouse (will likely fail, but provides context)
    url.to_string()
}

#[cfg(test)]
mod url_tests {
    use super::*;

    #[test]
    fn test_derive_warehouse_from_url() {
        assert_eq!(
            derive_warehouse_from_url("https://catalog.example.com/account/bucket"),
            "account_bucket"
        );
        assert_eq!(
            derive_warehouse_from_url("https://catalog.cloudflarestorage.com/abc123/my-bucket"),
            "abc123_my-bucket"
        );
        assert_eq!(
            derive_warehouse_from_url("https://example.com/single"),
            "single"
        );
    }
}
