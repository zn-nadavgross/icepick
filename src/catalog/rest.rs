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
