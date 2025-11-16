//! Catalog trait implementation for IcebergRestCatalog

use super::commit_types::{CommitTableRequest, TableRequirement, TableUpdate};
use super::helpers;
use super::types::*;
use super::IcebergRestCatalog;
use async_trait::async_trait;

// Private helper functions containing the actual implementation
// These are shared between native and WASM implementations

impl IcebergRestCatalog {
    async fn create_namespace_impl(
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

    async fn namespace_exists_impl(
        &self,
        _namespace: &crate::spec::NamespaceIdent,
    ) -> crate::error::Result<bool> {
        Err(crate::error::Error::unexpected(
            "Checking namespace existence is not supported",
        ))
    }

    async fn list_tables_impl(
        &self,
        _namespace: &crate::spec::NamespaceIdent,
    ) -> crate::error::Result<Vec<crate::spec::TableIdent>> {
        Err(crate::error::Error::unexpected(
            "Listing tables is not supported",
        ))
    }

    async fn table_exists_impl(
        &self,
        table: &crate::spec::TableIdent,
    ) -> crate::error::Result<bool> {
        match self.load_table_impl(table).await {
            Ok(_) => Ok(true),
            Err(crate::error::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn create_table_impl(
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
            partition_spec: None, // Will use server defaults
            write_order: None,    // Will use server defaults
            properties: if creation.properties().is_empty() {
                None
            } else {
                Some(creation.properties().clone())
            },
            stage_create: Some(false),
        };

        eprintln!(
            "DEBUG: Create table request body: {}",
            serde_json::to_string_pretty(&body)
                .unwrap_or_else(|_| "Failed to serialize".to_string())
        );

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
        helpers::build_table(
            table_ident,
            table_response.metadata,
            table_response.metadata_location,
            self.file_io.clone(),
        )
    }

    async fn load_table_impl(
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

        helpers::build_table(
            table.clone(),
            table_response.metadata,
            table_response.metadata_location,
            self.file_io.clone(),
        )
    }

    async fn drop_table_impl(&self, _table: &crate::spec::TableIdent) -> crate::error::Result<()> {
        Err(crate::error::Error::unexpected(
            "Dropping tables is not supported",
        ))
    }

    /// Helper function to read and optionally decompress metadata files
    fn read_metadata_bytes(path: &str, bytes: Vec<u8>) -> crate::error::Result<Vec<u8>> {
        // Check if the file is gzipped (R2 uses .gz.metadata.json)
        if path.ends_with(".gz.metadata.json") || path.ends_with(".metadata.json.gz") {
            use std::io::Read;
            let mut decoder = flate2::read::GzDecoder::new(&bytes[..]);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed).map_err(|e| {
                crate::error::Error::io_error(format!(
                    "Failed to decompress gzipped metadata: {}",
                    e
                ))
            })?;
            Ok(decompressed)
        } else {
            Ok(bytes)
        }
    }

    async fn update_table_metadata_impl(
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

        let current_metadata_bytes =
            Self::read_metadata_bytes(old_metadata_location, current_metadata_bytes)?;

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

        let new_metadata_bytes =
            Self::read_metadata_bytes(new_metadata_location, new_metadata_bytes)?;

        let new_metadata: crate::spec::TableMetadata = serde_json::from_slice(&new_metadata_bytes)
            .map_err(|e| {
                crate::error::Error::invalid_input(format!("Failed to parse new metadata: {}", e))
            })?;
        // Get the new snapshot that was added
        let new_snapshot = new_metadata
            .snapshots()
            .last()
            .ok_or_else(|| {
                crate::error::Error::invalid_input("New metadata has no snapshots".to_string())
            })?
            .clone();
        let new_snapshot_id = new_snapshot.snapshot_id();

        eprintln!("DEBUG: New snapshot details:");
        eprintln!("  snapshot_id: {}", new_snapshot.snapshot_id());
        eprintln!(
            "  parent_snapshot_id: {:?}",
            new_snapshot.parent_snapshot_id()
        );
        eprintln!("  schema_id: {:?}", new_snapshot.schema_id());

        // 3. Build commit request with both AddSnapshot and SetSnapshotRef updates
        // Note: -1 means "no snapshot", which should be represented as null in REST API
        // Use assert-ref-snapshot-id with the "main" branch reference
        let snapshot_id_requirement = if current_snapshot_id == Some(-1) {
            None
        } else {
            current_snapshot_id
        };
        let request = CommitTableRequest {
            requirements: vec![TableRequirement::AssertRefSnapshotId {
                r#ref: "main".to_string(),
                snapshot_id: snapshot_id_requirement,
            }],
            updates: vec![
                // First, add the new snapshot to the table
                TableUpdate::AddSnapshot {
                    snapshot: new_snapshot,
                },
                // Then, update the main branch reference to point to it
                TableUpdate::SetSnapshotRef {
                    ref_name: "main".to_string(),
                    snapshot_id: new_snapshot_id,
                    ref_type: "branch".to_string(),
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            ],
        };

        // 4. Send to REST endpoint
        self.commit_table(identifier, request)
            .await
            .map_err(helpers::from_catalog_error)?;

        Ok(())
    }
}

// Native platform implementation
#[cfg(not(target_family = "wasm"))]
#[async_trait]
impl crate::catalog::Catalog for IcebergRestCatalog {
    async fn create_namespace(
        &self,
        namespace: &crate::spec::NamespaceIdent,
        properties: std::collections::HashMap<String, String>,
    ) -> crate::error::Result<()> {
        self.create_namespace_impl(namespace, properties).await
    }

    async fn namespace_exists(
        &self,
        namespace: &crate::spec::NamespaceIdent,
    ) -> crate::error::Result<bool> {
        self.namespace_exists_impl(namespace).await
    }

    async fn list_tables(
        &self,
        namespace: &crate::spec::NamespaceIdent,
    ) -> crate::error::Result<Vec<crate::spec::TableIdent>> {
        self.list_tables_impl(namespace).await
    }

    async fn table_exists(&self, table: &crate::spec::TableIdent) -> crate::error::Result<bool> {
        self.table_exists_impl(table).await
    }

    async fn create_table(
        &self,
        namespace: &crate::spec::NamespaceIdent,
        creation: crate::spec::TableCreation,
    ) -> crate::error::Result<crate::table::Table> {
        self.create_table_impl(namespace, creation).await
    }

    async fn load_table(
        &self,
        table: &crate::spec::TableIdent,
    ) -> crate::error::Result<crate::table::Table> {
        self.load_table_impl(table).await
    }

    async fn drop_table(&self, table: &crate::spec::TableIdent) -> crate::error::Result<()> {
        self.drop_table_impl(table).await
    }

    async fn update_table_metadata(
        &self,
        identifier: &crate::spec::TableIdent,
        old_metadata_location: &str,
        new_metadata_location: &str,
    ) -> crate::error::Result<()> {
        self.update_table_metadata_impl(identifier, old_metadata_location, new_metadata_location)
            .await
    }
}

// WASM platform implementation
#[cfg(target_family = "wasm")]
#[async_trait(?Send)]
impl crate::catalog::Catalog for IcebergRestCatalog {
    async fn create_namespace(
        &self,
        namespace: &crate::spec::NamespaceIdent,
        properties: std::collections::HashMap<String, String>,
    ) -> crate::error::Result<()> {
        self.create_namespace_impl(namespace, properties).await
    }

    async fn namespace_exists(
        &self,
        namespace: &crate::spec::NamespaceIdent,
    ) -> crate::error::Result<bool> {
        self.namespace_exists_impl(namespace).await
    }

    async fn list_tables(
        &self,
        namespace: &crate::spec::NamespaceIdent,
    ) -> crate::error::Result<Vec<crate::spec::TableIdent>> {
        self.list_tables_impl(namespace).await
    }

    async fn table_exists(&self, table: &crate::spec::TableIdent) -> crate::error::Result<bool> {
        self.table_exists_impl(table).await
    }

    async fn create_table(
        &self,
        namespace: &crate::spec::NamespaceIdent,
        creation: crate::spec::TableCreation,
    ) -> crate::error::Result<crate::table::Table> {
        self.create_table_impl(namespace, creation).await
    }

    async fn load_table(
        &self,
        table: &crate::spec::TableIdent,
    ) -> crate::error::Result<crate::table::Table> {
        self.load_table_impl(table).await
    }

    async fn drop_table(&self, table: &crate::spec::TableIdent) -> crate::error::Result<()> {
        self.drop_table_impl(table).await
    }

    async fn update_table_metadata(
        &self,
        identifier: &crate::spec::TableIdent,
        old_metadata_location: &str,
        new_metadata_location: &str,
    ) -> crate::error::Result<()> {
        self.update_table_metadata_impl(identifier, old_metadata_location, new_metadata_location)
            .await
    }
}
