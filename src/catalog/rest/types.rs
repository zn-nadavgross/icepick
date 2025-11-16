use iceberg::spec::{Schema, TableMetadata};
use iceberg::{TableRequirement, TableUpdate};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Request/Response types for Iceberg REST API
#[derive(Serialize)]
pub struct CreateNamespaceRequest {
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct CreateNamespaceResponse {
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Serialize)]
pub struct CreateTableRequest {
    pub name: String,
    pub schema: Schema,
    pub location: Option<String>,
    #[serde(rename = "partition-spec")]
    pub partition_spec: serde_json::Value,
    #[serde(rename = "write-order")]
    pub write_order: serde_json::Value,
    pub properties: HashMap<String, String>,
    #[serde(rename = "stage-create")]
    pub stage_create: bool,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct CreateTableResponse {
    pub metadata: TableMetadata,
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
}

pub type LoadTableResponse = CreateTableResponse;

#[derive(Serialize)]
pub struct UpdateTableRequest {
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
}

pub type UpdateTableResponse = CreateTableResponse;

#[derive(Deserialize, Debug)]
pub struct ConfigResponse {
    #[serde(default)]
    pub defaults: HashMap<String, String>,
    #[serde(default)]
    pub overrides: HashMap<String, String>,
}
