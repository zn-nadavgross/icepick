//! Catalog trait implementation for IcebergRestCatalog

use super::commit_types::{CommitTableRequest, TableRequirement, TableUpdate};
use super::helpers;
use super::types::*;
use super::IcebergRestCatalog;
use async_trait::async_trait;

// Implementation of icepick Catalog trait
#[async_trait]
impl crate::catalog::Catalog for IcebergRestCatalog {
    async fn create_namespace(
        &self,
        namespace: &crate::spec::NamespaceIdent,
        properties: std::collections::HashMap<String, String>,
    ) -> crate::error::Result<()> {
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
                crate::error::Error::io_error(format!("Failed to build request: {}", e))
            })?;

        let response = self
            .send_request(req)
            .await
            .map_err(helpers::from_catalog_error)?;
        let _json_value = self
            .handle_response(response)
            .await
            .map_err(helpers::from_catalog_error)?;

        Ok(())
    }

    async fn namespace_exists(
        &self,
        _namespace: &crate::spec::NamespaceIdent,
    ) -> crate::error::Result<bool> {
        Err(crate::error::Error::unexpected(
            "Checking namespace existence is not supported",
        ))
    }

    async fn list_tables(
        &self,
        _namespace: &crate::spec::NamespaceIdent,
    ) -> crate::error::Result<Vec<crate::spec::TableIdent>> {
        Err(crate::error::Error::unexpected(
            "Listing tables is not supported",
        ))
    }

    async fn table_exists(&self, table: &crate::spec::TableIdent) -> crate::error::Result<bool> {
        match self.load_table(table).await {
            Ok(_) => Ok(true),
            Err(crate::error::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn create_table(
        &self,
        namespace: &crate::spec::NamespaceIdent,
        creation: crate::spec::TableCreation,
    ) -> crate::error::Result<crate::table::Table> {
        let namespace_name = namespace.to_string();
        let url = self.url(&format!("namespaces/{}/tables", namespace_name));
        eprintln!("DEBUG: Creating table at URL: {}", url);

        let body = CreateTableRequest {
            name: creation.name().to_string(),
            schema: creation.schema().clone(),
            location: creation.location().map(String::from),
            partition_spec: serde_json::json!({
                "spec-id": 0,
                "fields": []
            }),
            write_order: serde_json::json!({
                "order-id": 0,
                "fields": []
            }),
            properties: creation.properties().clone(),
            stage_create: false,
        };

        let req = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .build()
            .map_err(|e| {
                crate::error::Error::io_error(format!("Failed to build request: {}", e))
            })?;

        let response = self
            .send_request(req)
            .await
            .map_err(helpers::from_catalog_error)?;
        let json_value = self
            .handle_response(response)
            .await
            .map_err(helpers::from_catalog_error)?;

        let table_response: CreateTableResponse =
            serde_json::from_value(json_value).map_err(|e| {
                crate::error::Error::invalid_input(format!("Failed to parse table response: {}", e))
            })?;

        let table_ident =
            crate::spec::TableIdent::new(namespace.clone(), creation.name().to_string());
        helpers::build_table(table_ident, table_response.metadata, self.file_io.clone())
    }

    async fn load_table(
        &self,
        table: &crate::spec::TableIdent,
    ) -> crate::error::Result<crate::table::Table> {
        let namespace_name = table.namespace().to_string();
        let url = self.url(&format!(
            "namespaces/{}/tables/{}",
            namespace_name,
            table.name()
        ));
        eprintln!("DEBUG: Loading table from URL: {}", url);

        let req = self
            .http_client
            .get(&url)
            .header("Accept", "application/json")
            .build()
            .map_err(|e| {
                crate::error::Error::io_error(format!("Failed to build request: {}", e))
            })?;

        let response = self
            .send_request(req)
            .await
            .map_err(helpers::from_catalog_error)?;
        let json_value = self
            .handle_response(response)
            .await
            .map_err(helpers::from_catalog_error)?;

        let table_response: LoadTableResponse =
            serde_json::from_value(json_value).map_err(|e| {
                crate::error::Error::invalid_input(format!("Failed to parse table response: {}", e))
            })?;

        helpers::build_table(table.clone(), table_response.metadata, self.file_io.clone())
    }

    async fn drop_table(&self, _table: &crate::spec::TableIdent) -> crate::error::Result<()> {
        Err(crate::error::Error::unexpected(
            "Dropping tables is not supported",
        ))
    }

    async fn update_table_metadata(
        &self,
        identifier: &crate::spec::TableIdent,
        old_metadata_location: &str,
        new_metadata_location: &str,
    ) -> crate::error::Result<()> {
        // 1. Load current metadata to get current snapshot ID
        let current_metadata_bytes =
            self.file_io
                .read(old_metadata_location)
                .await
                .map_err(|e| {
                    crate::error::Error::io_error(format!("Failed to read old metadata: {}", e))
                })?;

        let current_metadata: crate::spec::TableMetadata =
            serde_json::from_slice(&current_metadata_bytes).map_err(|e| {
                crate::error::Error::invalid_input(format!(
                    "Failed to parse current metadata: {}",
                    e
                ))
            })?;
        let current_snapshot_id = current_metadata.current_snapshot_id();

        // 2. Load new metadata to get new snapshot ID
        let new_metadata_bytes = self
            .file_io
            .read(new_metadata_location)
            .await
            .map_err(|e| {
                crate::error::Error::io_error(format!("Failed to read new metadata: {}", e))
            })?;

        let new_metadata: crate::spec::TableMetadata = serde_json::from_slice(&new_metadata_bytes)
            .map_err(|e| {
                crate::error::Error::invalid_input(format!("Failed to parse new metadata: {}", e))
            })?;
        let new_snapshot_id = new_metadata.current_snapshot_id().ok_or_else(|| {
            crate::error::Error::invalid_input("New metadata has no snapshot".to_string())
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
        self.commit_table(identifier, request)
            .await
            .map_err(helpers::from_catalog_error)?;

        Ok(())
    }
}
