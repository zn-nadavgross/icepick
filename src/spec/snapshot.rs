//! Iceberg snapshots
//! Vendored from iceberg-rust v0.7.0

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Summary of a snapshot
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    operation: String,
    #[serde(flatten)]
    additional_properties: HashMap<String, String>,
}

impl Summary {
    /// Create a summary builder
    pub fn builder() -> SummaryBuilder {
        SummaryBuilder::default()
    }

    /// Get the operation type
    pub fn operation(&self) -> &str {
        &self.operation
    }

    /// Get a property by key
    pub fn get(&self, key: &str) -> Option<&String> {
        if key == "operation" {
            Some(&self.operation)
        } else {
            self.additional_properties.get(key)
        }
    }

    /// Get all properties
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.additional_properties
    }
}

/// Builder for Summary
#[derive(Default)]
pub struct SummaryBuilder {
    operation: Option<String>,
    properties: HashMap<String, String>,
}

impl SummaryBuilder {
    /// Set a property
    pub fn set(mut self, key: &str, value: &str) -> Self {
        if key == "operation" {
            self.operation = Some(value.to_string());
        } else {
            self.properties.insert(key.to_string(), value.to_string());
        }
        self
    }

    /// Build the summary
    pub fn build(self) -> Summary {
        Summary {
            operation: self.operation.unwrap_or_else(|| "append".to_string()),
            additional_properties: self.properties,
        }
    }
}

/// A snapshot of a table at a point in time
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
    #[serde(rename = "parent-snapshot-id", skip_serializing_if = "Option::is_none")]
    parent_snapshot_id: Option<i64>,
    #[serde(rename = "sequence-number", skip_serializing_if = "Option::is_none")]
    sequence_number: Option<i64>,
    #[serde(rename = "timestamp-ms")]
    timestamp_ms: i64,
    #[serde(rename = "manifest-list")]
    manifest_list: String,
    summary: Summary,
    #[serde(rename = "schema-id", skip_serializing_if = "Option::is_none")]
    schema_id: Option<i32>,
}

impl Snapshot {
    /// Create a snapshot builder
    pub fn builder() -> SnapshotBuilder {
        SnapshotBuilder::default()
    }

    /// Get snapshot ID
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Get parent snapshot ID
    pub fn parent_snapshot_id(&self) -> Option<i64> {
        self.parent_snapshot_id
    }

    /// Get sequence number
    pub fn sequence_number(&self) -> Option<i64> {
        self.sequence_number
    }

    /// Get timestamp in milliseconds
    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    /// Get manifest list location
    pub fn manifest_list(&self) -> &str {
        &self.manifest_list
    }

    /// Get summary
    pub fn summary(&self) -> &Summary {
        &self.summary
    }

    /// Get schema ID
    pub fn schema_id(&self) -> Option<i32> {
        self.schema_id
    }
}

/// Builder for Snapshot
#[derive(Default)]
pub struct SnapshotBuilder {
    snapshot_id: Option<i64>,
    parent_snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    timestamp_ms: Option<i64>,
    manifest_list: Option<String>,
    summary: Option<Summary>,
    schema_id: Option<i32>,
}

impl SnapshotBuilder {
    pub fn with_snapshot_id(mut self, id: i64) -> Self {
        self.snapshot_id = Some(id);
        self
    }

    pub fn with_parent_snapshot_id(mut self, id: i64) -> Self {
        self.parent_snapshot_id = Some(id);
        self
    }

    pub fn with_sequence_number(mut self, seq: i64) -> Self {
        self.sequence_number = Some(seq);
        self
    }

    pub fn with_timestamp_ms(mut self, timestamp: i64) -> Self {
        self.timestamp_ms = Some(timestamp);
        self
    }

    pub fn with_manifest_list(mut self, location: &str) -> Self {
        self.manifest_list = Some(location.to_string());
        self
    }

    pub fn with_summary(mut self, summary: Summary) -> Self {
        self.summary = Some(summary);
        self
    }

    pub fn with_schema_id(mut self, id: i32) -> Self {
        self.schema_id = Some(id);
        self
    }

    pub fn build(self) -> Result<Snapshot> {
        Ok(Snapshot {
            snapshot_id: self
                .snapshot_id
                .ok_or_else(|| Error::InvalidInput("Snapshot must have ID".to_string()))?,
            parent_snapshot_id: self.parent_snapshot_id,
            sequence_number: self.sequence_number,
            timestamp_ms: self
                .timestamp_ms
                .ok_or_else(|| Error::InvalidInput("Snapshot must have timestamp".to_string()))?,
            manifest_list: self.manifest_list.ok_or_else(|| {
                Error::InvalidInput("Snapshot must have manifest list".to_string())
            })?,
            summary: self.summary.unwrap_or_else(|| Summary::builder().build()),
            schema_id: self.schema_id,
        })
    }
}
