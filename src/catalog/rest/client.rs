//! Client constructor methods for IcebergRestCatalog

use super::arn::{parse_s3tables_arn, ARN_ENCODE_SET};
use super::types;
use super::IcebergRestCatalog;
use crate::catalog::{AuthProvider, CatalogError, R2Config, Result};
use iceberg::io::FileIO;
use percent_encoding::utf8_percent_encode;
use reqwest::Client;

#[cfg(not(target_family = "wasm"))]
use aws_credential_types::provider::ProvideCredentials;

impl IcebergRestCatalog {
    /// Create catalog for Cloudflare R2 Data Catalog (shortcut)
    pub async fn from_r2(
        name: String,
        account_id: impl Into<String>,
        bucket_name: impl Into<String>,
        api_token: impl Into<String>,
    ) -> Result<Self> {
        let config = R2Config {
            account_id: account_id.into(),
            bucket_name: bucket_name.into(),
            api_token: api_token.into(),
            endpoint_override: None,
        };
        Self::from_r2_config(name, config).await
    }

    /// Create catalog for Cloudflare R2 Data Catalog (with config)
    pub async fn from_r2_config(name: String, config: R2Config) -> Result<Self> {
        let endpoint = config.endpoint_override.unwrap_or_else(|| {
            format!(
                "https://api.cloudflare.com/client/v4/accounts/{}/r2/buckets/{}/data-catalog",
                config.account_id, config.bucket_name
            )
        });

        let auth = Box::new(crate::catalog::BearerTokenAuthProvider::new(
            config.api_token,
        ));
        let http_client = Client::new();

        // Create FileIO for S3 access
        let file_io = FileIO::from_path("s3://")
            .map_err(|e| CatalogError::Unexpected(format!("Failed to create FileIO: {}", e)))?
            .build()
            .map_err(|e| CatalogError::Unexpected(format!("Failed to build FileIO: {}", e)))?;

        Ok(Self {
            endpoint,
            prefix: "v1".to_string(),
            http_client,
            auth_provider: auth,
            file_io,
            name,
        })
    }

    /// Create catalog with direct catalog URI and warehouse name
    /// This is useful for Cloudflare R2 when you have the full catalog URI.
    /// Follows PyIceberg pattern: calls /v1/config first to get server configuration.
    pub async fn from_catalog_uri(
        name: String,
        catalog_uri: impl Into<String>,
        warehouse_name: impl Into<String>,
        api_token: impl Into<String>,
    ) -> Result<Self> {
        let endpoint = catalog_uri.into();
        let warehouse = warehouse_name.into();

        let auth = Box::new(crate::catalog::BearerTokenAuthProvider::new(
            api_token.into(),
        ));
        let http_client = Client::new();

        // Call /v1/config to get server configuration (per Iceberg REST spec)
        let config_url = format!("{}/v1/config?warehouse={}", endpoint, warehouse);
        eprintln!("DEBUG: Fetching config from URL: {}", config_url);

        let req = http_client.get(&config_url).build().map_err(|e| {
            CatalogError::HttpError(format!("Failed to build config request: {}", e))
        })?;

        // Sign the request with auth
        let signed_req = auth.sign_request(req).await?;
        eprintln!("DEBUG: Config request headers: {:?}", signed_req.headers());

        let response = http_client
            .execute(signed_req)
            .await
            .map_err(|e| CatalogError::HttpError(format!("Config request failed: {}", e)))?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .unwrap_or_else(|_| "Unable to read response".to_string());

        eprintln!(
            "DEBUG: Config response status: {}, body: {}",
            status, body_text
        );

        if !status.is_success() {
            return Err(CatalogError::HttpError(format!(
                "Config request failed with status {}: {}",
                status, body_text
            )));
        }

        let config: types::ConfigResponse = serde_json::from_str(&body_text).map_err(|e| {
            CatalogError::HttpError(format!("Failed to parse config response: {}", e))
        })?;

        // Merge configuration: defaults < client properties < overrides
        let mut properties = config.defaults;
        properties.insert("warehouse".to_string(), warehouse.clone());
        properties.extend(config.overrides);

        // Extract prefix from server configuration (defaults to empty string)
        let prefix = properties.get("prefix").cloned().unwrap_or_default();
        eprintln!("DEBUG: Using prefix from server: '{}'", prefix);
        eprintln!("DEBUG: Config properties: {:?}", properties);

        // Extract account ID from endpoint for R2 S3 endpoint
        // endpoint format: https://catalog.cloudflarestorage.com/{account_id}/{bucket}
        let account_id = endpoint.split('/').nth(3).ok_or_else(|| {
            CatalogError::InvalidConfig("Cannot extract account ID from catalog URI".to_string())
        })?;

        // Configure FileIO for R2 S3-compatible storage
        let mut file_io_builder = FileIO::from_path("s3://")
            .map_err(|e| CatalogError::Unexpected(format!("Failed to create FileIO: {}", e)))?;

        // Set R2's S3-compatible endpoint
        let r2_endpoint = format!("https://{}.r2.cloudflarestorage.com", account_id);
        eprintln!("DEBUG: Setting S3 endpoint to: {}", r2_endpoint);
        file_io_builder = file_io_builder.with_prop("s3.endpoint", &r2_endpoint);

        // Apply all properties from config response to FileIO
        for (key, value) in &properties {
            if key.starts_with("s3.") {
                eprintln!("DEBUG: Setting FileIO property: {}={}", key, value);
                file_io_builder = file_io_builder.with_prop(key, value);
            }
        }

        let file_io = file_io_builder
            .build()
            .map_err(|e| CatalogError::Unexpected(format!("Failed to build FileIO: {}", e)))?;

        Ok(Self {
            endpoint,
            prefix,
            http_client,
            auth_provider: auth,
            file_io,
            name,
        })
    }

    /// Create catalog for AWS S3 Tables
    #[cfg(not(target_family = "wasm"))]
    pub async fn from_s3_tables_arn(name: String, arn: &str) -> Result<Self> {
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
            region,
            "s3tables".to_string(),
            credentials,
        ));

        let http_client = Client::new();

        // Create FileIO for S3 access
        let file_io = FileIO::from_path("s3://")
            .map_err(|e| CatalogError::Unexpected(format!("Failed to create FileIO: {}", e)))?
            .build()
            .map_err(|e| CatalogError::Unexpected(format!("Failed to build FileIO: {}", e)))?;

        Ok(Self {
            endpoint,
            prefix: warehouse_prefix, // url() method already adds /v1/
            http_client,
            auth_provider: auth,
            file_io,
            name,
        })
    }
}
