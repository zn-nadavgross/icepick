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

        // IcebergRestCatalog implements Catalog directly, no wrapper needed
        Ok(Arc::new(catalog))
    }

    /// Get a description of the catalog type
    pub fn catalog_type(&self) -> &'static str {
        "REST Catalog"
    }
}
