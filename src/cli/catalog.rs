//! Catalog connection utilities

use crate::catalog::rest::IcebergRestCatalog;
use crate::catalog::Catalog;
use std::sync::Arc;

/// Configuration for connecting to a catalog
///
/// The simplest way to connect to any Iceberg REST catalog is with just two parameters:
/// - `catalog_url`: The base URL of the catalog (e.g., `https://catalog.cloudflarestorage.com/account/bucket`)
/// - `token`: Bearer token for authentication
#[derive(Debug, Clone)]
pub struct CatalogConfig {
    /// Iceberg REST catalog URL
    pub catalog_url: Option<String>,
    /// API Token for catalog authentication
    pub token: Option<String>,
}

impl CatalogConfig {
    /// Create a catalog from the configuration
    pub async fn create_catalog(&self) -> Result<Arc<dyn Catalog>, String> {
        let url = self.catalog_url.as_ref().ok_or_else(|| {
            "Catalog URL required. Use --catalog-url or ICEPICK_CATALOG_URL".to_string()
        })?;

        let token = self
            .token
            .as_ref()
            .ok_or_else(|| "Token required. Use --token or ICEPICK_TOKEN".to_string())?;

        let catalog = IcebergRestCatalog::from_url("icepick", url, token, None)
            .await
            .map_err(|e| format!("Failed to create catalog: {}", e))?;

        Ok(Arc::new(RestCatalogWrapper(catalog)))
    }

    /// Get a description of the catalog type
    pub fn catalog_type(&self) -> &'static str {
        if self.catalog_url.is_some() {
            "REST Catalog"
        } else {
            "Unknown"
        }
    }
}

/// Wrapper to implement Catalog trait for IcebergRestCatalog
struct RestCatalogWrapper(IcebergRestCatalog);

#[async_trait::async_trait]
impl Catalog for RestCatalogWrapper {
    async fn create_namespace(
        &self,
        namespace: &crate::spec::NamespaceIdent,
        properties: std::collections::HashMap<String, String>,
    ) -> crate::error::Result<()> {
        self.0.create_namespace(namespace, properties).await
    }

    async fn namespace_exists(
        &self,
        namespace: &crate::spec::NamespaceIdent,
    ) -> crate::error::Result<bool> {
        self.0.namespace_exists(namespace).await
    }

    async fn list_tables(
        &self,
        namespace: &crate::spec::NamespaceIdent,
    ) -> crate::error::Result<Vec<crate::spec::TableIdent>> {
        self.0.list_tables(namespace).await
    }

    async fn table_exists(
        &self,
        identifier: &crate::spec::TableIdent,
    ) -> crate::error::Result<bool> {
        self.0.table_exists(identifier).await
    }

    async fn create_table(
        &self,
        namespace: &crate::spec::NamespaceIdent,
        creation: crate::spec::TableCreation,
    ) -> crate::error::Result<crate::table::Table> {
        self.0.create_table(namespace, creation).await
    }

    async fn load_table(
        &self,
        identifier: &crate::spec::TableIdent,
    ) -> crate::error::Result<crate::table::Table> {
        self.0.load_table(identifier).await
    }

    async fn drop_table(&self, identifier: &crate::spec::TableIdent) -> crate::error::Result<()> {
        self.0.drop_table(identifier).await
    }

    async fn update_table_metadata(
        &self,
        identifier: &crate::spec::TableIdent,
        old_metadata_location: &str,
        new_metadata_location: &str,
    ) -> crate::error::Result<()> {
        self.0
            .update_table_metadata(identifier, old_metadata_location, new_metadata_location)
            .await
    }
}
