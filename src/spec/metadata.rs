//! Iceberg table metadata
//! Vendored and simplified from iceberg-rust v0.7.0

use crate::error::{Error, Result};
use crate::spec::{Schema, Snapshot};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

mod types;

pub use types::{
    MetadataLogEntry, PartitionField, PartitionSpec, SnapshotLogEntry, SnapshotReference,
    SortField, SortOrder,
};

fn is_zero(value: &i64) -> bool {
    *value == 0
}

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
    #[serde(
        rename = "partition-specs",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    partition_specs: Vec<PartitionSpec>,
    #[serde(rename = "sort-orders", default, skip_serializing_if = "Vec::is_empty")]
    sort_orders: Vec<SortOrder>,
    #[serde(rename = "refs", default, skip_serializing_if = "HashMap::is_empty")]
    refs: HashMap<String, SnapshotReference>,
    #[serde(
        rename = "snapshot-log",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    snapshot_log: Vec<SnapshotLogEntry>,
    #[serde(
        rename = "metadata-log",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    metadata_log: Vec<MetadataLogEntry>,
    #[serde(
        rename = "last-sequence-number",
        default,
        skip_serializing_if = "is_zero"
    )]
    last_sequence_number: i64,
    #[serde(
        rename = "table-features",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    table_features: Vec<String>,
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

    /// Get partition specs
    pub fn partition_specs(&self) -> &[PartitionSpec] {
        &self.partition_specs
    }

    /// Get sort orders
    pub fn sort_orders(&self) -> &[SortOrder] {
        &self.sort_orders
    }

    /// Get snapshot references
    pub fn refs(&self) -> &HashMap<String, SnapshotReference> {
        &self.refs
    }

    /// Get snapshot log
    pub fn snapshot_log(&self) -> &[SnapshotLogEntry] {
        &self.snapshot_log
    }

    /// Get metadata log
    pub fn metadata_log(&self) -> &[MetadataLogEntry] {
        &self.metadata_log
    }

    /// Get last committed sequence number
    pub fn last_sequence_number(&self) -> i64 {
        self.last_sequence_number
    }

    /// Get advertised table features
    pub fn table_features(&self) -> &[String] {
        &self.table_features
    }

    /// Create a new TableMetadata with an added snapshot
    pub fn add_snapshot(&self, snapshot: Snapshot, timestamp_ms: i64) -> Self {
        let mut updated = self.clone();
        updated.snapshots.push(snapshot.clone());

        updated.current_snapshot_id = Some(snapshot.snapshot_id());
        updated.last_updated_ms = timestamp_ms;
        if let Some(sequence_number) = snapshot.sequence_number() {
            updated.last_sequence_number = sequence_number;
        }
        updated.snapshot_log.push(SnapshotLogEntry::new(
            snapshot.timestamp_ms(),
            snapshot.snapshot_id(),
        ));

        match updated.refs.get_mut("main") {
            Some(reference) => {
                reference.set_snapshot_id(snapshot.snapshot_id());
            }
            None => {
                updated.refs.insert(
                    "main".to_string(),
                    SnapshotReference::branch(snapshot.snapshot_id()),
                );
            }
        }

        updated
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
    partition_specs: Vec<PartitionSpec>,
    sort_orders: Vec<SortOrder>,
    refs: HashMap<String, SnapshotReference>,
    snapshot_log: Vec<SnapshotLogEntry>,
    metadata_log: Vec<MetadataLogEntry>,
    last_sequence_number: Option<i64>,
    table_features: Vec<String>,
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
        if let Some(sequence_number) = snapshot.sequence_number() {
            self.last_sequence_number = Some(sequence_number);
        }
        self.snapshot_log
            .push(SnapshotLogEntry::new(snapshot.timestamp_ms(), snapshot_id));
        self.refs
            .entry("main".to_string())
            .and_modify(|reference| {
                reference.set_snapshot_id(snapshot_id);
            })
            .or_insert_with(|| SnapshotReference::branch(snapshot_id));
        self.snapshots.push(snapshot);
        self
    }

    pub fn with_property(mut self, key: String, value: String) -> Self {
        self.properties.insert(key, value);
        self
    }

    pub fn with_partition_specs(mut self, specs: Vec<PartitionSpec>) -> Self {
        self.partition_specs = specs;
        self
    }

    pub fn with_sort_orders(mut self, orders: Vec<SortOrder>) -> Self {
        self.sort_orders = orders;
        self
    }

    pub fn with_refs(mut self, refs: HashMap<String, SnapshotReference>) -> Self {
        self.refs = refs;
        self
    }

    pub fn with_snapshot_log(mut self, entries: Vec<SnapshotLogEntry>) -> Self {
        self.snapshot_log = entries;
        self
    }

    pub fn with_metadata_log(mut self, entries: Vec<MetadataLogEntry>) -> Self {
        self.metadata_log = entries;
        self
    }

    pub fn with_last_sequence_number(mut self, seq: i64) -> Self {
        self.last_sequence_number = Some(seq);
        self
    }

    pub fn with_table_features(mut self, features: Vec<String>) -> Self {
        self.table_features = features;
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
            partition_specs: self.partition_specs,
            sort_orders: self.sort_orders,
            refs: self.refs,
            snapshot_log: self.snapshot_log,
            metadata_log: self.metadata_log,
            last_sequence_number: self.last_sequence_number.unwrap_or(0),
            table_features: self.table_features,
        })
    }
}

#[cfg(test)]
mod tests;
