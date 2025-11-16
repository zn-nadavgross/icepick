//! Core Catalog trait for Iceberg catalogs
//! New design - simpler than iceberg-rust

use async_trait::async_trait;
use std::collections::HashMap;

use crate::error::Result;
use crate::spec::{NamespaceIdent, TableIdent};

/// Core catalog operations for Iceberg tables
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Create a namespace (idempotent - returns Ok if already exists)
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()>;

    /// Check if a namespace exists
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool>;

    /// List all tables in a namespace
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>>;

    /// Check if a table exists
    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool>;

    /// Delete a table
    async fn drop_table(&self, identifier: &TableIdent) -> Result<()>;

    /// Update table metadata location atomically
    ///
    /// This method atomically updates the catalog's pointer to the table metadata.
    /// If the current metadata location doesn't match `old_metadata_location`,
    /// returns a ConcurrentModification error.
    ///
    /// # Arguments
    /// * `identifier` - The table identifier
    /// * `old_metadata_location` - Expected current metadata location (for optimistic locking)
    /// * `new_metadata_location` - New metadata location to set
    async fn update_table_metadata(
        &self,
        identifier: &TableIdent,
        old_metadata_location: &str,
        new_metadata_location: &str,
    ) -> Result<()>;
}
