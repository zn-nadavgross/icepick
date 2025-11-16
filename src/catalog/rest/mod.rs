mod arn;
mod helpers;
mod types;

use self::arn::{parse_s3tables_arn, ARN_ENCODE_SET};
use self::helpers::to_iceberg_error;
use self::types::*;
use crate::catalog::{AuthProvider, CatalogError, R2Config, Result};
use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error as IcebergError, ErrorKind, Namespace, NamespaceIdent, Result as IcebergResult,
    TableCommit, TableCreation, TableIdent,
};
use percent_encoding::utf8_percent_encode;
use reqwest::{Client, Response};
use std::collections::HashMap;

#[cfg(not(target_family = "wasm"))]
use aws_credential_types::provider::ProvideCredentials;

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

#[async_trait]
impl Catalog for IcebergRestCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Listing namespaces is not supported",
        ))
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        let namespace_name = namespace.to_string();
        let url = format!("{}/{}/namespaces", self.endpoint, self.prefix);

        let body = CreateNamespaceRequest {
            namespace: vec![namespace_name],
            properties: properties.clone(),
        };

        let req = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .build()
            .map_err(|e| {
                IcebergError::new(
                    ErrorKind::Unexpected,
                    format!("Failed to build request: {}", e),
                )
            })?;

        let response = self.send_request(req).await.map_err(to_iceberg_error)?;
        let _json_value = self
            .handle_response(response)
            .await
            .map_err(to_iceberg_error)?;

        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Getting namespace properties is not supported",
        ))
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> IcebergResult<bool> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Checking namespace existence is not supported",
        ))
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Updating namespaces is not supported",
        ))
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<()> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Dropping namespaces is not supported",
        ))
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Listing tables is not supported",
        ))
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        let namespace_name = namespace.to_string();
        let url = format!(
            "{}/{}/namespaces/{}/tables",
            self.endpoint, self.prefix, namespace_name
        );

        let body = CreateTableRequest {
            name: creation.name.clone(),
            schema: creation.schema,
            location: None,
            partition_spec: serde_json::json!({
                "spec-id": 0,
                "fields": []
            }),
            write_order: serde_json::json!({
                "order-id": 0,
                "fields": []
            }),
            properties: HashMap::new(),
            stage_create: false,
        };

        let req = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .build()
            .map_err(|e| {
                IcebergError::new(
                    ErrorKind::Unexpected,
                    format!("Failed to build request: {}", e),
                )
            })?;

        let response = self.send_request(req).await.map_err(to_iceberg_error)?;
        let json_value = self
            .handle_response(response)
            .await
            .map_err(to_iceberg_error)?;

        let table_response: CreateTableResponse =
            serde_json::from_value(json_value).map_err(|e| {
                IcebergError::new(
                    ErrorKind::Unexpected,
                    format!("Failed to parse table response: {}", e),
                )
            })?;

        let table_ident = TableIdent::new(namespace.clone(), creation.name);
        helpers::build_table(table_ident, table_response.metadata, self.file_io.clone())
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        let namespace_name = table.namespace.to_string();
        let url = format!(
            "{}/{}/namespaces/{}/tables/{}",
            self.endpoint, self.prefix, namespace_name, table.name
        );

        let req = self
            .http_client
            .get(&url)
            .header("Accept", "application/json")
            .build()
            .map_err(|e| {
                IcebergError::new(
                    ErrorKind::Unexpected,
                    format!("Failed to build request: {}", e),
                )
            })?;

        let response = self.send_request(req).await.map_err(to_iceberg_error)?;
        let json_value = self
            .handle_response(response)
            .await
            .map_err(to_iceberg_error)?;

        let table_response: LoadTableResponse =
            serde_json::from_value(json_value).map_err(|e| {
                IcebergError::new(
                    ErrorKind::Unexpected,
                    format!("Failed to parse table response: {}", e),
                )
            })?;

        helpers::build_table(table.clone(), table_response.metadata, self.file_io.clone())
    }

    async fn drop_table(&self, _table: &TableIdent) -> IcebergResult<()> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Dropping tables is not supported",
        ))
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        match self.load_table(table).await {
            Ok(_) => Ok(true),
            Err(e) if e.to_string().contains("not found") => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Renaming tables is not supported",
        ))
    }

    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> IcebergResult<Table> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "Registering tables is not supported",
        ))
    }

    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<Table> {
        let namespace_name = commit.identifier().namespace.to_string();
        let table_name = commit.identifier().name.clone();
        let table_ident = commit.identifier().clone();

        let url = format!(
            "{}/{}/namespaces/{}/tables/{}",
            self.endpoint, self.prefix, namespace_name, table_name
        );

        let requirements = commit.take_requirements();
        let updates = commit.take_updates();

        let body = UpdateTableRequest {
            requirements,
            updates,
        };

        let req = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .build()
            .map_err(|e| {
                IcebergError::new(
                    ErrorKind::Unexpected,
                    format!("Failed to build request: {}", e),
                )
            })?;

        let response = self.send_request(req).await.map_err(to_iceberg_error)?;
        let json_value = self
            .handle_response(response)
            .await
            .map_err(to_iceberg_error)?;

        let table_response: UpdateTableResponse =
            serde_json::from_value(json_value).map_err(|e| {
                IcebergError::new(
                    ErrorKind::Unexpected,
                    format!("Failed to parse table response: {}", e),
                )
            })?;

        helpers::build_table(table_ident, table_response.metadata, self.file_io.clone())
    }
}
