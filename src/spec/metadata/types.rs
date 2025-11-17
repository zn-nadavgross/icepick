use serde::{Deserialize, Serialize};

/// Partition specification entry
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionSpec {
    #[serde(rename = "spec-id")]
    spec_id: i32,
    fields: Vec<PartitionField>,
}

impl PartitionSpec {
    /// Partition spec identifier
    pub fn spec_id(&self) -> i32 {
        self.spec_id
    }

    /// Fields that make up the partition spec
    pub fn fields(&self) -> &[PartitionField] {
        &self.fields
    }
}

/// Partition field definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionField {
    #[serde(rename = "field-id")]
    field_id: i32,
    #[serde(rename = "source-id")]
    source_id: i32,
    transform: String,
    name: String,
}

impl PartitionField {
    pub fn field_id(&self) -> i32 {
        self.field_id
    }

    pub fn source_id(&self) -> i32 {
        self.source_id
    }

    pub fn transform(&self) -> &str {
        &self.transform
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Sort order configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortOrder {
    #[serde(rename = "order-id")]
    order_id: i32,
    fields: Vec<SortField>,
}

impl SortOrder {
    pub fn order_id(&self) -> i32 {
        self.order_id
    }

    pub fn fields(&self) -> &[SortField] {
        &self.fields
    }
}

/// Sort order field information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortField {
    #[serde(rename = "source-id")]
    source_id: i32,
    transform: String,
    direction: String,
    #[serde(rename = "null-order")]
    null_order: String,
}

impl SortField {
    pub fn source_id(&self) -> i32 {
        self.source_id
    }

    pub fn transform(&self) -> &str {
        &self.transform
    }

    pub fn direction(&self) -> &str {
        &self.direction
    }

    pub fn null_order(&self) -> &str {
        &self.null_order
    }
}

/// Snapshot reference (branch/tag)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotReference {
    #[serde(rename = "type")]
    reference_type: String,
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
    #[serde(
        rename = "min-snapshots-to-keep",
        skip_serializing_if = "Option::is_none"
    )]
    min_snapshots_to_keep: Option<i32>,
    #[serde(
        rename = "max-snapshot-age-ms",
        skip_serializing_if = "Option::is_none"
    )]
    max_snapshot_age_ms: Option<i64>,
    #[serde(rename = "max-ref-age-ms", skip_serializing_if = "Option::is_none")]
    max_ref_age_ms: Option<i64>,
}

impl SnapshotReference {
    pub fn branch(snapshot_id: i64) -> Self {
        Self {
            reference_type: "branch".to_string(),
            snapshot_id,
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        }
    }

    pub fn reference_type(&self) -> &str {
        &self.reference_type
    }

    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    pub fn min_snapshots_to_keep(&self) -> Option<i32> {
        self.min_snapshots_to_keep
    }

    pub fn max_snapshot_age_ms(&self) -> Option<i64> {
        self.max_snapshot_age_ms
    }

    pub fn max_ref_age_ms(&self) -> Option<i64> {
        self.max_ref_age_ms
    }

    pub fn set_snapshot_id(&mut self, snapshot_id: i64) {
        self.snapshot_id = snapshot_id;
    }
}

/// Snapshot log entry
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotLogEntry {
    #[serde(rename = "timestamp-ms")]
    timestamp_ms: i64,
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
}

impl SnapshotLogEntry {
    pub fn new(timestamp_ms: i64, snapshot_id: i64) -> Self {
        Self {
            timestamp_ms,
            snapshot_id,
        }
    }

    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }
}

/// Metadata log entry
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetadataLogEntry {
    #[serde(rename = "timestamp-ms")]
    timestamp_ms: i64,
    #[serde(rename = "metadata-file")]
    metadata_file: String,
}

impl MetadataLogEntry {
    pub fn new(timestamp_ms: i64, metadata_file: impl Into<String>) -> Self {
        Self {
            timestamp_ms,
            metadata_file: metadata_file.into(),
        }
    }

    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    pub fn metadata_file(&self) -> &str {
        &self.metadata_file
    }
}
