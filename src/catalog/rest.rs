use crate::catalog::{AuthProvider, CatalogError, R2Config, Result};
use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{Schema, TableMetadata};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error as IcebergError, ErrorKind, Namespace, NamespaceIdent,
    Result as IcebergResult, TableCommit, TableCreation, TableIdent, TableRequirement, TableUpdate,
};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
}
