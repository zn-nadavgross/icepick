//! Client constructor methods for IcebergRestCatalog

use super::commit_types::{CommitTableRequest, CommitTableResponse};
use super::types;
use super::IcebergRestCatalog;
use crate::catalog::{AuthProvider, CatalogError, R2Config, Result};
use crate::io::FileIO;
use crate::spec::TableIdent;
use reqwest::Client;

#[cfg(not(target_family = "wasm"))]
use super::arn::{parse_s3tables_arn, ARN_ENCODE_SET};

#[cfg(not(target_family = "wasm"))]
use aws_credential_types::provider::ProvideCredentials;

#[cfg(not(target_family = "wasm"))]
use percent_encoding::utf8_percent_encode;

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
                "https://catalog.cloudflarestorage.com/{}/{}",
                config.account_id, config.bucket_name
            )
        });

        let auth = Box::new(crate::catalog::BearerTokenAuthProvider::new(
            config.api_token,
        ));
        let http_client = Client::new();

        // Construct warehouse name from account_id and bucket_name
        let warehouse = format!("{}_{}", config.account_id, config.bucket_name);

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

        let config_response: types::ConfigResponse =
            serde_json::from_str(&body_text).map_err(|e| {
                CatalogError::HttpError(format!("Failed to parse config response: {}", e))
            })?;

        // Merge configuration: defaults < client properties < overrides
        let mut properties = config_response.defaults;
        properties.insert("warehouse".to_string(), warehouse.clone());
        properties.extend(config_response.overrides);

        // Extract prefix from server configuration (defaults to empty string)
        let prefix = properties.get("prefix").cloned().unwrap_or_default();
        eprintln!("DEBUG: Using prefix from server: '{}'", prefix);
        eprintln!("DEBUG: Config properties: {:?}", properties);

        // Configure FileIO for R2 S3-compatible storage using opendal
        let r2_endpoint = format!("https://{}.r2.cloudflarestorage.com", config.account_id);
        eprintln!("DEBUG: Setting S3 endpoint to: {}", r2_endpoint);

        let mut s3_config_vec = vec![
            ("endpoint".to_string(), r2_endpoint),
            ("bucket".to_string(), config.bucket_name.clone()),
        ];

        // Apply properties from config response
        for (key, value) in &properties {
            if key.starts_with("s3.") {
                eprintln!("DEBUG: FileIO property: {}={}", key, value);
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
            region.clone(),
            "s3tables".to_string(),
            credentials.clone(),
        ));

        let http_client = Client::new();

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

        let url = self.url(&format!("namespaces/{}/tables/{}", namespace, table_name));

        // Diagnostic logging for debugging
        eprintln!("DEBUG: Commit table URL: {}", url);
        eprintln!(
            "DEBUG: Commit request: {}",
            serde_json::to_string_pretty(&request)
                .unwrap_or_else(|_| "Failed to serialize".to_string())
        );

        let req = self
            .http_client
            .post(&url)
            .json(&request)
            .build()
            .map_err(|e| CatalogError::HttpError(format!("Failed to build request: {}", e)))?;

        let response = self.send_request(req).await?;

        eprintln!("DEBUG: Commit response status: {}", response.status());

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
