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
pub struct ListNamespacesResponse {
    pub namespaces: Vec<Vec<String>>,
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

/// Response from the /credentials endpoint (vended credentials)
#[derive(Deserialize, Debug, Clone)]
pub struct LoadTableCredentialsResponse {
    #[serde(rename = "storage-credentials")]
    pub storage_credentials: Vec<StorageCredential>,
}

/// Individual storage credential from vended credentials response
#[derive(Deserialize, Debug, Clone)]
pub struct StorageCredential {
    pub prefix: String,
    pub config: StorageCredentialConfig,
}

/// Configuration within a storage credential
#[derive(Deserialize, Debug, Clone)]
pub struct StorageCredentialConfig {
    #[serde(rename = "s3.access-key-id")]
    pub access_key_id: Option<String>,
    #[serde(rename = "s3.secret-access-key")]
    pub secret_access_key: Option<String>,
    #[serde(rename = "s3.session-token")]
    pub session_token: Option<String>,
    #[serde(rename = "s3.endpoint")]
    pub endpoint: Option<String>,
    #[serde(rename = "s3.region")]
    pub region: Option<String>,
    /// Credential expiration time in milliseconds since Unix epoch
    #[serde(rename = "expires-at-ms")]
    pub expires_at_ms: Option<i64>,
}
