//! Cloudflare R2 Data Catalog implementation
//!
//! Provides a production-ready implementation of the Iceberg catalog trait for Cloudflare R2.
//! This catalog uses bearer token authentication and supports both native and WASM platforms.

use crate::catalog::rest::IcebergRestCatalog;
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::spec::{NamespaceIdent, TableCreation, TableIdent};
use crate::table::Table;
use async_trait::async_trait;
use std::collections::HashMap;

/// Cloudflare R2 Data Catalog
///
/// This catalog provides access to Apache Iceberg tables stored in Cloudflare R2.
/// It uses bearer token authentication and works on all platforms including WASM.
///
/// # Platform Support
///
/// Unlike S3TablesCatalog, R2Catalog supports both native platforms and WASM
/// (wasm32-unknown-unknown), making it suitable for browser and Cloudflare Workers use cases.
///
/// # Example
///
/// ```no_run
/// use icepick::R2Catalog;
/// use iceberg::Catalog;
/// use iceberg::TableIdent;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create catalog for R2
/// let catalog = R2Catalog::new(
///     "my-catalog",
///     "account-id",
///     "bucket-name",
///     "api-token"
/// ).await?;
///
/// // Use the catalog
/// let table_id = TableIdent::from_strs(["my_namespace", "my_table"])?;
/// let table = catalog.load_table(&table_id).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Authentication
///
/// The catalog uses Cloudflare API tokens for authentication. To create an API token:
///
/// 1. Log into the Cloudflare dashboard
/// 2. Go to "My Profile" → "API Tokens"
/// 3. Create a token with R2 read/write permissions
/// 4. Use the token when constructing the catalog
#[derive(Debug)]
pub struct R2Catalog {
    inner: IcebergRestCatalog,
}

impl R2Catalog {
    /// Create a new R2 catalog
    ///
    /// # Arguments
    ///
    /// * `name` - Catalog name for identification
    /// * `account_id` - Cloudflare account ID
    /// * `bucket_name` - R2 bucket name
    /// * `api_token` - Cloudflare API token with R2 permissions
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The account ID or bucket name is invalid
    /// - The API token is invalid or lacks permissions
    /// - The R2 service is unreachable
    ///
    /// # Example
    ///
    /// ```no_run
    /// use icepick::R2Catalog;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let catalog = R2Catalog::new(
    ///     "production",
    ///     "abc123",
    ///     "my-bucket",
    ///     "cloudflare-api-token-here"
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(
        name: impl Into<String>,
        account_id: impl Into<String>,
        bucket_name: impl Into<String>,
        api_token: impl Into<String>,
    ) -> Result<Self> {
        let name = name.into();
        let account_id = account_id.into();
        let bucket_name = bucket_name.into();
        let api_token = api_token.into();

        let inner = IcebergRestCatalog::from_r2(name, account_id, bucket_name, api_token)
            .await
            .map_err(|e| match e {
                crate::catalog::CatalogError::InvalidArn(msg) => Error::invalid_arn(msg),
                crate::catalog::CatalogError::AuthError(msg) => Error::unauthorized(msg),
                crate::catalog::CatalogError::HttpError(msg) => Error::unexpected(msg),
                crate::catalog::CatalogError::NotFound(msg) => Error::not_found(msg),
                crate::catalog::CatalogError::Conflict(msg) => Error::conflict(msg),
                crate::catalog::CatalogError::InvalidRequest(msg) => Error::invalid_request(msg),
                crate::catalog::CatalogError::Unexpected(msg) => Error::unexpected(msg),
            })?;

        Ok(Self { inner })
    }
}

// Implement Catalog trait by delegating to inner IcebergRestCatalog
#[async_trait]
impl Catalog for R2Catalog {
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool> {
        self.inner.table_exists(identifier).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
        self.inner.load_table(identifier).await
    }

    async fn drop_table(&self, identifier: &TableIdent) -> Result<()> {
        self.inner.drop_table(identifier).await
    }

    async fn update_table_metadata(
        &self,
        identifier: &TableIdent,
        old_metadata_location: &str,
        new_metadata_location: &str,
    ) -> Result<()> {
        self.inner
            .update_table_metadata(identifier, old_metadata_location, new_metadata_location)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_r2_catalog_debug() {
        // Just verify the type exists and Debug is implemented
        // We can't construct without valid credentials
        let _type_check: fn(R2Catalog) = |c| {
            let _ = format!("{:?}", c);
        };
    }
}
