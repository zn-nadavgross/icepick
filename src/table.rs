//! Iceberg table representation

use crate::io::FileIO;
use crate::spec::{Schema, TableIdent, TableMetadata};

/// An Iceberg table with integrated storage
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
    pub fn schema(&self) -> &Schema {
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

    /// Get the FileIO (for internal use)
    #[allow(dead_code)]
    pub(crate) fn file_io(&self) -> &FileIO {
        &self.file_io
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
