//! AWS S3 Tables catalog implementation
//!
//! Provides a production-ready implementation of the Iceberg catalog trait for AWS S3 Tables.
//! This catalog uses AWS SigV4 authentication and is only available on non-WASM platforms.

#![cfg(not(target_family = "wasm"))]

use crate::catalog::rest::IcebergRestCatalog;
use crate::catalog::{map_catalog_error, Catalog, CatalogOptions};
use crate::error::Result;
use crate::spec::{NamespaceIdent, TableCreation, TableIdent};
use crate::table::Table;
use async_trait::async_trait;
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
/// use icepick::catalog::Catalog;
/// use icepick::spec::{TableIdent, NamespaceIdent};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create catalog from S3 Tables ARN
/// let catalog = S3TablesCatalog::from_arn(
///     "my-catalog",
///     "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
/// ).await?;
///
/// // Use the catalog
/// let namespace = NamespaceIdent::new(vec!["my_namespace".to_string()]);
/// let table_id = TableIdent::new(namespace, "my_table".to_string());
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
///
/// Use [`S3TablesCatalog::from_arn_with_options`] to customize HTTP behaviour or
/// operate on references other than `main`.
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
            .map_err(map_catalog_error)?;

        Ok(Self { inner })
    }

    /// Create a new catalog with explicit options such as HTTP configuration and reference.
    ///
    /// * `name` / `arn` - Same as [`S3TablesCatalog::from_arn`]
    /// * `options` - Additional configuration (HTTP timeouts, retries, default branch)
    pub async fn from_arn_with_options(
        name: impl Into<String>,
        arn: impl AsRef<str>,
        options: CatalogOptions,
    ) -> Result<Self> {
        let name = name.into();
        let arn = arn.as_ref();

        let inner = IcebergRestCatalog::from_s3_tables_arn_with_options(name, arn, options)
            .await
            .map_err(map_catalog_error)?;

        Ok(Self { inner })
    }
}

// Implement Catalog trait by delegating to inner IcebergRestCatalog
#[cfg(not(target_family = "wasm"))]
#[async_trait]
impl Catalog for S3TablesCatalog {
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
    fn test_s3_tables_catalog_debug() {
        // Just verify the type exists and Debug is implemented
        // We can't construct without AWS credentials
        let _type_check: fn(S3TablesCatalog) = |c| {
            let _ = format!("{:?}", c);
        };
    }
}
