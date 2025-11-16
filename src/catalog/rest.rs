use crate::catalog::{AuthProvider, CatalogError, R2Config, Result};
use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{Schema, TableMetadata};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error as IcebergError, ErrorKind, Namespace, NamespaceIdent,
    Result as IcebergResult, TableCommit, TableCreation, TableIdent, TableRequirement, TableUpdate,
};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[cfg(not(target_family = "wasm"))]
use aws_credential_types::provider::ProvideCredentials;

// Request/Response types for Iceberg REST API
#[derive(Serialize)]
struct CreateNamespaceRequest {
    namespace: Vec<String>,
    properties: HashMap<String, String>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct CreateNamespaceResponse {
    namespace: Vec<String>,
    properties: HashMap<String, String>,
}

#[derive(Serialize)]
struct CreateTableRequest {
    name: String,
    schema: Schema,
    location: Option<String>,
    #[serde(rename = "partition-spec")]
    partition_spec: serde_json::Value,
    #[serde(rename = "write-order")]
    write_order: serde_json::Value,
    properties: HashMap<String, String>,
    #[serde(rename = "stage-create")]
    stage_create: bool,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct CreateTableResponse {
    metadata: TableMetadata,
    #[serde(rename = "metadata-location")]
    metadata_location: String,
}

type LoadTableResponse = CreateTableResponse;

#[derive(Serialize)]
struct UpdateTableRequest {
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

type UpdateTableResponse = CreateTableResponse;

// Define encoding set for ARN in path
const ARN_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// Parse S3 Tables ARN and extract region and bucket name
/// ARN format: arn:aws:s3tables:region:account:bucket/name
fn parse_s3tables_arn(arn: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = arn.split(':').collect();

    if parts.len() != 6 {
        return Err(CatalogError::InvalidArn(format!(
            "Expected 6 parts, got {}",
            parts.len()
        )));
    }

    if parts[0] != "arn" {
        return Err(CatalogError::InvalidArn(
            "Must start with 'arn'".to_string(),
        ));
    }

    if parts[2] != "s3tables" {
        return Err(CatalogError::InvalidArn(format!(
            "Not an S3 Tables ARN: {}",
            parts[2]
        )));
    }

    let region = parts[3].to_string();
    let bucket_name = parts[5]
        .strip_prefix("bucket/")
        .ok_or_else(|| CatalogError::InvalidArn("Missing 'bucket/' prefix".to_string()))?
        .to_string();

    Ok((region, bucket_name))
}

/// Shared Iceberg REST catalog implementation
pub struct IcebergRestCatalog {
    endpoint: String,
    prefix: String,
    http_client: Client,
    auth_provider: Box<dyn AuthProvider>,
    file_io: FileIO,
    name: String,
}

impl std::fmt::Debug for IcebergRestCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergRestCatalog")
            .field("endpoint", &self.endpoint)
            .field("prefix", &self.prefix)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

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

        let auth = Box::new(crate::catalog::BearerTokenAuthProvider::new(config.api_token));
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
            prefix: format!("v1/{}", warehouse_prefix),
            http_client,
            auth_provider: auth,
            file_io,
            name,
        })
    }

    async fn send_request(&self, req: reqwest::Request) -> Result<Response> {
        let signed_req = self.auth_provider.sign_request(req).await?;

        let response = self
            .http_client
            .execute(signed_req)
            .await
            .map_err(|e| CatalogError::HttpError(format!("Request failed: {}", e)))?;

        Ok(response)
    }

    async fn handle_response(&self, response: Response) -> Result<serde_json::Value> {
        let status = response.status();

        match status.as_u16() {
            200..=299 => response.json().await.map_err(|e| {
                CatalogError::HttpError(format!("Failed to parse JSON response: {}", e))
            }),

            403 => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unable to read response".to_string());
                Err(CatalogError::AuthError(format!(
                    "Authentication failed: {}",
                    body
                )))
            }

            404 => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Resource not found".to_string());
                Err(CatalogError::NotFound(body))
            }

            409 => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Conflict".to_string());
                Err(CatalogError::Conflict(format!(
                    "Requirements not met: {}",
                    body
                )))
            }

            400 => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Bad request".to_string());
                Err(CatalogError::InvalidRequest(body))
            }

            _ => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                Err(CatalogError::Unexpected(format!(
                    "HTTP {}: {}",
                    status, body
                )))
            }
        }
    }
}
