//! AWS S3 Tables catalog implementation
//!
//! Provides a production-ready implementation of the Iceberg catalog trait for AWS S3 Tables.
//! This catalog uses AWS SigV4 authentication and is only available on non-WASM platforms.

use crate::catalog::rest::IcebergRestCatalog;
use crate::error::{Error, Result};
use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};
use std::collections::HashMap;

/// AWS S3 Tables catalog
///
/// This catalog provides access to Apache Iceberg tables stored in AWS S3 Tables.
/// It uses AWS SigV4 authentication via the default credential provider chain.
///
/// # Platform Support
///
/// This catalog is only available on native platforms (Linux, macOS, Windows).
/// It cannot be compiled to WASM due to AWS SDK dependencies.
///
/// # Example
///
/// ```no_run
/// use icepick::S3TablesCatalog;
/// use iceberg::Catalog;
/// use iceberg::TableIdent;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create catalog from S3 Tables ARN
/// let catalog = S3TablesCatalog::from_arn(
///     "my-catalog",
///     "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
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
/// The catalog uses the AWS default credential provider chain, which checks:
/// - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
/// - AWS credentials file (`~/.aws/credentials`)
/// - IAM instance profile (when running on EC2)
/// - ECS task role (when running on ECS)
///
/// Ensure your AWS credentials have appropriate permissions for S3 Tables operations.
#[derive(Debug)]
pub struct S3TablesCatalog {
    inner: IcebergRestCatalog,
}

impl S3TablesCatalog {
    /// Create a new S3 Tables catalog from an ARN
    ///
    /// # Arguments
    ///
    /// * `name` - Catalog name for identification
    /// * `arn` - S3 Tables bucket ARN (format: `arn:aws:s3tables:region:account:bucket/bucket-name`)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The ARN format is invalid
    /// - AWS credentials cannot be loaded
    /// - The S3 Tables service is unreachable
    ///
    /// # Example
    ///
    /// ```no_run
    /// use icepick::S3TablesCatalog;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let catalog = S3TablesCatalog::from_arn(
    ///     "production",
    ///     "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn from_arn(name: impl Into<String>, arn: impl AsRef<str>) -> Result<Self> {
        let name = name.into();
        let arn = arn.as_ref();

        let inner = IcebergRestCatalog::from_s3_tables_arn(name, arn)
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
impl Catalog for S3TablesCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.inner.update_table(commit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_tables_catalog_debug() {
        // Just verify the type exists and Debug is implemented
        // We can't construct without AWS credentials
        let _type_check: fn(S3TablesCatalog) = |c| {
            let _ = format!("{:?}", c);
        };
    }
}
