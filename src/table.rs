//! Iceberg table representation

use crate::error::Result;
use crate::io::FileIO;
#[cfg(feature = "maintenance")]
use crate::maintenance::{
    expire_snapshots, CatalogMaintenance, ExpireSnapshotsOptions, ExpireSnapshotsResult,
};
use crate::reader::{DataFileEntry, ManifestListReader, ManifestReader};
use crate::scan::TableScanBuilder;
use crate::spec::{Schema, Snapshot, TableIdent, TableMetadata};
use crate::transaction::Transaction;

/// An Iceberg table with integrated storage
#[derive(Clone)]
pub struct Table {
    identifier: TableIdent,
    metadata: TableMetadata,
    metadata_location: String,
    #[allow(dead_code)]
    file_io: FileIO,
}

impl Table {
    /// Create a new table instance
    pub fn new(
        identifier: TableIdent,
        metadata: TableMetadata,
        metadata_location: String,
        file_io: FileIO,
    ) -> Self {
        Self {
            identifier,
            metadata,
            metadata_location,
            file_io,
        }
    }

    /// Get the table identifier
    pub fn identifier(&self) -> &TableIdent {
        &self.identifier
    }

    /// Get the table metadata
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Get the current schema
    pub fn schema(&self) -> Result<&Schema> {
        self.metadata.current_schema()
    }

    /// Get the table location
    pub fn location(&self) -> &str {
        self.metadata.location()
    }

    /// Get the metadata file location
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }

    /// Get the FileIO
    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Get current snapshot
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        self.metadata.current_snapshot()
    }

    /// Start a new transaction for writing data
    pub fn transaction(&self) -> Transaction {
        Transaction::new(self.clone())
    }

    /// List all data files in the current snapshot
    ///
    /// Returns a list of data file entries discovered from the manifest files.
    /// This is a simplified version that reads all data files without filtering.
    pub async fn files(&self) -> Result<Vec<DataFileEntry>> {
        // Get current snapshot
        let snapshot = self
            .current_snapshot()
            .ok_or_else(|| crate::error::Error::invalid_input("Table has no current snapshot"))?;

        // Read manifest list to get manifest file paths
        let manifest_paths =
            ManifestListReader::read(&self.file_io, snapshot.manifest_list()).await?;

        // Read each manifest and collect data files
        let mut all_files = Vec::new();
        for manifest_path in manifest_paths {
            let files = ManifestReader::read(&self.file_io, &manifest_path).await?;
            all_files.extend(files);
        }

        Ok(all_files)
    }

    /// Create a table scan builder for reading data
    ///
    /// Returns a builder that can be used to configure and execute a scan.
    /// For the MVP, this provides basic sequential reading without filtering.
    pub fn scan(&self) -> TableScanBuilder<'_> {
        TableScanBuilder::new(self)
    }

    /// Expire old snapshots and optionally clean up orphaned files.
    #[cfg(feature = "maintenance")]
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog does not support snapshot removal
    /// or if the expiration options are invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use icepick::maintenance::ExpireSnapshotsOptions;
    /// use icepick::catalog::Catalog;
    /// use icepick::R2Catalog;
    /// use icepick::spec::TableIdent;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let catalog = R2Catalog::new("catalog", "account", "bucket", "token").await?;
    /// let table_id = TableIdent::from_strs(&["namespace"], "table");
    /// let table = catalog.load_table(&table_id).await?;
    /// let options = ExpireSnapshotsOptions {
    ///     older_than_ms: Some(chrono::Utc::now().timestamp_millis() - 86_400_000),
    ///     retain_last: Some(1),
    ///     ..Default::default()
    /// };
    ///
    /// let result = table.expire_snapshots(&catalog, options).await?;
    /// println!("Expired snapshots: {}", result.expired_snapshot_ids.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn expire_snapshots(
        &self,
        catalog: &dyn CatalogMaintenance,
        options: ExpireSnapshotsOptions,
    ) -> Result<ExpireSnapshotsResult> {
        expire_snapshots(self, catalog, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{NamespaceIdent, NestedField, PrimitiveType, Type};
    use opendal::Operator;

    #[test]
    fn test_table_creation() {
        let schema = crate::spec::Schema::builder()
            .with_fields(vec![NestedField::required_field(
                1,
                "id".to_string(),
                Type::Primitive(PrimitiveType::Long),
            )])
            .build()
            .unwrap();

        let metadata = TableMetadata::builder()
            .with_location("s3://test/table")
            .with_current_schema(schema)
            .build()
            .unwrap();

        let ident = TableIdent::new(
            NamespaceIdent::new(vec!["default".to_string()]),
            "test".to_string(),
        );

        let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
        let file_io = FileIO::new(op);

        let table = Table::new(
            ident,
            metadata,
            "s3://test/metadata.json".to_string(),
            file_io,
        );
        assert_eq!(table.location(), "s3://test/table");
    }
}
