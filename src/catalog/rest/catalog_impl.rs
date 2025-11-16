//! Catalog trait implementation for IcebergRestCatalog

use super::commit_types::{CommitTableRequest, TableRequirement, TableUpdate};
use super::helpers::{self, to_iceberg_error};
use super::types::*;
use super::IcebergRestCatalog;
use crate::catalog::Result as CatalogResult;
use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error as IcebergError, ErrorKind, Namespace, NamespaceIdent, Result as IcebergResult,
    TableCommit, TableCreation, TableIdent,
};
use std::collections::HashMap;

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
        let url = self.url("namespaces");
        eprintln!("DEBUG: Creating namespace at URL: {}", url);

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
        let url = self.url(&format!("namespaces/{}/tables", namespace_name));
        eprintln!("DEBUG: Creating table at URL: {}", url);

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
        let url = self.url(&format!(
            "namespaces/{}/tables/{}",
            namespace_name, table.name
        ));
        eprintln!("DEBUG: Loading table from URL: {}", url);

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

        let url = self.url(&format!(
            "namespaces/{}/tables/{}",
            namespace_name, table_name
        ));

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

// Internal methods for icepick
impl IcebergRestCatalog {
    /// Update table metadata atomically (for commit orchestrator)
    #[allow(dead_code)]
    pub async fn update_table_metadata(
        &self,
        identifier: &crate::spec::TableIdent,
        old_metadata_location: &str,
        new_metadata_location: &str,
    ) -> CatalogResult<()> {
        // 1. Load current metadata to get current snapshot ID
        let current_metadata_bytes = self
            .file_io
            .new_input(old_metadata_location)
            .map_err(|e| {
                crate::catalog::CatalogError::Unexpected(format!(
                    "Failed to create input for old metadata: {}",
                    e
                ))
            })?
            .read()
            .await
            .map_err(|e| {
                crate::catalog::CatalogError::Unexpected(format!(
                    "Failed to read old metadata: {}",
                    e
                ))
            })?;

        let current_metadata: crate::spec::TableMetadata =
            serde_json::from_slice(&current_metadata_bytes).map_err(|e| {
                crate::catalog::CatalogError::Unexpected(format!(
                    "Failed to parse current metadata: {}",
                    e
                ))
            })?;
        let current_snapshot_id = current_metadata.current_snapshot_id();

        // 2. Load new metadata to get new snapshot ID
        let new_metadata_bytes = self
            .file_io
            .new_input(new_metadata_location)
            .map_err(|e| {
                crate::catalog::CatalogError::Unexpected(format!(
                    "Failed to create input for new metadata: {}",
                    e
                ))
            })?
            .read()
            .await
            .map_err(|e| {
                crate::catalog::CatalogError::Unexpected(format!(
                    "Failed to read new metadata: {}",
                    e
                ))
            })?;

        let new_metadata: crate::spec::TableMetadata = serde_json::from_slice(&new_metadata_bytes)
            .map_err(|e| {
                crate::catalog::CatalogError::Unexpected(format!(
                    "Failed to parse new metadata: {}",
                    e
                ))
            })?;
        let new_snapshot_id = new_metadata.current_snapshot_id().ok_or_else(|| {
            crate::catalog::CatalogError::InvalidRequest("New metadata has no snapshot".to_string())
        })?;

        // 3. Build commit request
        let request = CommitTableRequest {
            requirements: vec![TableRequirement::AssertCurrentSnapshotId {
                snapshot_id: current_snapshot_id,
            }],
            updates: vec![TableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                snapshot_id: new_snapshot_id,
                ref_type: "branch".to_string(),
            }],
        };

        // 4. Send to REST endpoint
        self.commit_table(identifier, request).await?;

        Ok(())
    }
}
