//! Iceberg table metadata
//! Vendored and simplified from iceberg-rust v0.7.0

use crate::error::{Error, Result};
use crate::spec::{Schema, Snapshot};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metadata for an Iceberg table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableMetadata {
    #[serde(rename = "format-version")]
    format_version: i32,
    #[serde(rename = "table-uuid")]
    table_uuid: String,
    location: String,
    #[serde(rename = "last-updated-ms")]
    last_updated_ms: i64,
    #[serde(rename = "last-column-id")]
    last_column_id: i32,
    schemas: Vec<Schema>,
    #[serde(rename = "current-schema-id")]
    current_schema_id: i32,
    #[serde(default)]
    snapshots: Vec<Snapshot>,
    #[serde(
        rename = "current-snapshot-id",
        skip_serializing_if = "Option::is_none"
    )]
    current_snapshot_id: Option<i64>,
    #[serde(default)]
    properties: HashMap<String, String>,
}

impl TableMetadata {
    /// Create a metadata builder
    pub fn builder() -> TableMetadataBuilder {
        TableMetadataBuilder::default()
    }

    /// Get format version
    pub fn format_version(&self) -> i32 {
        self.format_version
    }

    /// Get table UUID
    pub fn table_uuid(&self) -> &str {
        &self.table_uuid
    }

    /// Get table location
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Get last updated timestamp
    pub fn last_updated_ms(&self) -> i64 {
        self.last_updated_ms
    }

    /// Get all schemas
    pub fn schemas(&self) -> &[Schema] {
        &self.schemas
    }

    /// Get current schema
    pub fn current_schema(&self) -> Result<&Schema> {
        self.schemas
            .iter()
            .find(|s| s.schema_id() == self.current_schema_id)
            .ok_or_else(|| {
                Error::InvalidInput(format!(
                    "Current schema ID {} not found in metadata",
                    self.current_schema_id
                ))
            })
    }

    /// Get all snapshots
    pub fn snapshots(&self) -> &[Snapshot] {
        &self.snapshots
    }

    /// Get current snapshot ID
    pub fn current_snapshot_id(&self) -> Option<i64> {
        self.current_snapshot_id
    }

    /// Get current snapshot
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        self.current_snapshot_id
            .and_then(|id| self.snapshots.iter().find(|s| s.snapshot_id() == id))
    }

    /// Get table properties
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    /// Create a new TableMetadata with an added snapshot
    pub fn add_snapshot(&self, snapshot: Snapshot) -> Self {
        let mut snapshots = self.snapshots.clone();
        snapshots.push(snapshot.clone());

        Self {
            format_version: self.format_version,
            table_uuid: self.table_uuid.clone(),
            location: self.location.clone(),
            last_updated_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            last_column_id: self.last_column_id,
            schemas: self.schemas.clone(),
            current_schema_id: self.current_schema_id,
            snapshots,
            current_snapshot_id: Some(snapshot.snapshot_id()),
            properties: self.properties.clone(),
        }
    }
}

/// Builder for TableMetadata
#[derive(Default)]
pub struct TableMetadataBuilder {
    format_version: Option<i32>,
    table_uuid: Option<String>,
    location: Option<String>,
    last_updated_ms: Option<i64>,
    schemas: Vec<Schema>,
    current_schema_id: Option<i32>,
    snapshots: Vec<Snapshot>,
    current_snapshot_id: Option<i64>,
    properties: HashMap<String, String>,
}

impl TableMetadataBuilder {
    pub fn with_format_version(mut self, version: i32) -> Self {
        self.format_version = Some(version);
        self
    }

    pub fn with_table_uuid(mut self, uuid: String) -> Self {
        self.table_uuid = Some(uuid);
        self
    }

    pub fn with_location(mut self, location: &str) -> Self {
        self.location = Some(location.to_string());
        self
    }

    pub fn with_last_updated_ms(mut self, timestamp: i64) -> Self {
        self.last_updated_ms = Some(timestamp);
        self
    }

    pub fn with_current_schema(mut self, schema: Schema) -> Self {
        let schema_id = schema.schema_id();
        self.current_schema_id = Some(schema_id);
        self.schemas.push(schema);
        self
    }

    pub fn with_current_snapshot(mut self, snapshot: Snapshot) -> Self {
        let snapshot_id = snapshot.snapshot_id();
        self.current_snapshot_id = Some(snapshot_id);
        self.snapshots.push(snapshot);
        self
    }

    pub fn with_property(mut self, key: String, value: String) -> Self {
        self.properties.insert(key, value);
        self
    }

    pub fn build(self) -> Result<TableMetadata> {
        let location = self
            .location
            .ok_or_else(|| Error::InvalidInput("TableMetadata must have location".to_string()))?;

        let schemas = if self.schemas.is_empty() {
            return Err(Error::InvalidInput(
                "TableMetadata must have at least one schema".to_string(),
            ));
        } else {
            self.schemas
        };

        // Find max field ID across all schemas
        let last_column_id = schemas
            .iter()
            .flat_map(|s| s.fields())
            .map(|f| f.id())
            .max()
            .unwrap_or(0);

        Ok(TableMetadata {
            format_version: self.format_version.unwrap_or(2),
            table_uuid: self
                .table_uuid
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            location,
            last_updated_ms: self.last_updated_ms.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64
            }),
            last_column_id,
            schemas,
            current_schema_id: self.current_schema_id.unwrap_or(0),
            snapshots: self.snapshots,
            current_snapshot_id: self.current_snapshot_id,
            properties: self.properties,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{NestedField, PrimitiveType, Type};

    #[test]
    fn test_current_snapshot_id() {
        let schema = Schema::builder()
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

        assert_eq!(metadata.current_snapshot_id(), None);
    }

    #[test]
    fn test_add_snapshot_to_metadata() {
        let schema = Schema::builder()
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

        let snapshot = crate::spec::Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(1000)
            .with_manifest_list("s3://test/metadata/snap-1.avro")
            .build()
            .unwrap();

        let updated = metadata.add_snapshot(snapshot);

        assert_eq!(updated.current_snapshot_id(), Some(1));
        assert_eq!(updated.snapshots().len(), 1);
    }
}
