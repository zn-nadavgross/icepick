use super::commit_types::{TableRequirement, TableUpdate};
use crate::spec::{Schema, TableMetadata};
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(rename = "partition-spec", skip_serializing_if = "Option::is_none")]
    pub partition_spec: Option<serde_json::Value>,
    #[serde(rename = "write-order", skip_serializing_if = "Option::is_none")]
    pub write_order: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
    #[serde(rename = "stage-create", skip_serializing_if = "Option::is_none")]
    pub stage_create: Option<bool>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct CreateTableResponse {
    pub metadata: TableMetadata,
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
}

pub type LoadTableResponse = CreateTableResponse;

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct ListTablesResponse {
    pub identifiers: Vec<TableIdentifier>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct TableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}

#[allow(dead_code)]
#[derive(Serialize)]
pub struct UpdateTableRequest {
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
}

#[allow(dead_code)]
pub type UpdateTableResponse = CreateTableResponse;

#[derive(Deserialize, Debug)]
pub struct ConfigResponse {
    #[serde(default)]
    pub defaults: HashMap<String, String>,
    #[serde(default)]
    pub overrides: HashMap<String, String>,
}
