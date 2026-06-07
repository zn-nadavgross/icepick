//! Data file metadata
//! Vendored from iceberg-rust v0.7.0

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Content type of a data file
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataContentType {
    /// Regular data
    #[default]
    Data,
    /// Position deletes
    PositionDeletes,
    /// Equality deletes
    EqualityDeletes,
}

/// Metadata about a data file in an Iceberg table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFile {
    #[serde(rename = "content")]
    content_type: DataContentType,
    #[serde(rename = "file-path")]
    file_path: String,
    #[serde(rename = "file-format")]
    file_format: String,
    #[serde(rename = "partition", default)]
    partition: HashMap<String, String>,
    #[serde(rename = "record-count")]
    record_count: i64,
    #[serde(rename = "file-size-in-bytes")]
    file_size_in_bytes: i64,
    #[serde(rename = "column-sizes", skip_serializing_if = "Option::is_none")]
    column_sizes: Option<HashMap<i32, i64>>,
    #[serde(rename = "value-counts", skip_serializing_if = "Option::is_none")]
    value_counts: Option<HashMap<i32, i64>>,
    #[serde(rename = "null-value-counts", skip_serializing_if = "Option::is_none")]
    null_value_counts: Option<HashMap<i32, i64>>,
    #[serde(rename = "split-offsets", skip_serializing_if = "Option::is_none")]
    split_offsets: Option<Vec<i64>>,
    #[serde(rename = "key-metadata", skip_serializing_if = "Option::is_none")]
    key_metadata: Option<Vec<u8>>,
    #[serde(rename = "equality-ids", skip_serializing_if = "Option::is_none")]
    equality_ids: Option<Vec<i32>>,
    #[serde(rename = "lower-bounds", skip_serializing_if = "Option::is_none")]
    lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    #[serde(rename = "upper-bounds", skip_serializing_if = "Option::is_none")]
    upper_bounds: Option<HashMap<i32, Vec<u8>>>,
    /// Manifest-entry-level snapshot_id from the entry that originally added
    /// this file. Iceberg requires DELETE manifest entries to reference the
    /// adding snapshot's id so readers can match tombstones to live entries;
    /// without this, Trino sees the DELETE as unrelated and keeps the file
    /// live (orphan cleanup then refuses to remove it). Not part of the
    /// Iceberg DataFile JSON spec — skipped from REST catalog wire format.
    #[serde(skip)]
    manifest_snapshot_id: Option<i64>,
    /// Manifest-entry-level file_sequence_number, paired with
    /// `manifest_snapshot_id` for the same reason.
    #[serde(skip)]
    manifest_file_sequence_number: Option<i64>,
}

impl DataFile {
    /// Create a data file builder
    pub fn builder() -> DataFileBuilder {
        DataFileBuilder::default()
    }

    /// Get content type
    pub fn content_type(&self) -> DataContentType {
        self.content_type
    }

    /// Get file path
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Get file format
    pub fn file_format(&self) -> &str {
        &self.file_format
    }

    /// Get record count
    pub fn record_count(&self) -> i64 {
        self.record_count
    }

    /// Get partition data
    pub fn partition(&self) -> &HashMap<String, String> {
        &self.partition
    }

    /// Replace partition data. Used by compaction to backfill partition values
    /// on files loaded from `DataFileEntry` (which doesn't carry them) before
    /// re-emitting them in a manifest entry.
    pub fn set_partition(&mut self, partition: HashMap<String, String>) {
        self.partition = partition;
    }

    /// Get file size in bytes
    pub fn file_size_in_bytes(&self) -> i64 {
        self.file_size_in_bytes
    }

    /// Get column sizes
    pub fn column_sizes(&self) -> Option<&HashMap<i32, i64>> {
        self.column_sizes.as_ref()
    }

    /// Get value counts
    pub fn value_counts(&self) -> Option<&HashMap<i32, i64>> {
        self.value_counts.as_ref()
    }

    /// Get null value counts
    pub fn null_value_counts(&self) -> Option<&HashMap<i32, i64>> {
        self.null_value_counts.as_ref()
    }

    /// Get split offsets
    pub fn split_offsets(&self) -> Option<&[i64]> {
        self.split_offsets.as_deref()
    }

    /// Get key metadata (encryption)
    pub fn key_metadata(&self) -> Option<&[u8]> {
        self.key_metadata.as_deref()
    }

    /// Get equality IDs
    pub fn equality_ids(&self) -> Option<&[i32]> {
        self.equality_ids.as_deref()
    }

    /// Get lower bounds
    pub fn lower_bounds(&self) -> Option<&HashMap<i32, Vec<u8>>> {
        self.lower_bounds.as_ref()
    }

    /// Get upper bounds
    pub fn upper_bounds(&self) -> Option<&HashMap<i32, Vec<u8>>> {
        self.upper_bounds.as_ref()
    }

    /// Snapshot id of the manifest entry that originally added this file.
    pub fn manifest_snapshot_id(&self) -> Option<i64> {
        self.manifest_snapshot_id
    }

    /// File sequence number from the manifest entry that originally added this file.
    pub fn manifest_file_sequence_number(&self) -> Option<i64> {
        self.manifest_file_sequence_number
    }

    /// Stamp the originating manifest entry's snapshot_id and
    /// file_sequence_number on this DataFile so a later DELETE entry can
    /// reference them.
    pub fn set_manifest_provenance(
        &mut self,
        snapshot_id: Option<i64>,
        file_sequence_number: Option<i64>,
    ) {
        self.manifest_snapshot_id = snapshot_id;
        self.manifest_file_sequence_number = file_sequence_number;
    }
}

/// Builder for DataFile
#[derive(Default)]
pub struct DataFileBuilder {
    content_type: Option<DataContentType>,
    file_path: Option<String>,
    file_format: Option<String>,
    partition: Option<HashMap<String, String>>,
    record_count: Option<i64>,
    file_size_in_bytes: Option<i64>,
    column_sizes: Option<HashMap<i32, i64>>,
    value_counts: Option<HashMap<i32, i64>>,
    null_value_counts: Option<HashMap<i32, i64>>,
    split_offsets: Option<Vec<i64>>,
    key_metadata: Option<Vec<u8>>,
    equality_ids: Option<Vec<i32>>,
    lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    upper_bounds: Option<HashMap<i32, Vec<u8>>>,
}

impl DataFileBuilder {
    pub fn with_content_type(mut self, content_type: DataContentType) -> Self {
        self.content_type = Some(content_type);
        self
    }

    pub fn with_file_path(mut self, path: &str) -> Self {
        self.file_path = Some(path.to_string());
        self
    }

    pub fn with_file_format(mut self, format: &str) -> Self {
        self.file_format = Some(format.to_string());
        self
    }

    pub fn with_partition(mut self, partition: HashMap<String, String>) -> Self {
        self.partition = Some(partition);
        self
    }

    pub fn with_record_count(mut self, count: i64) -> Self {
        self.record_count = Some(count);
        self
    }

    pub fn with_file_size_in_bytes(mut self, size: i64) -> Self {
        self.file_size_in_bytes = Some(size);
        self
    }

    pub fn with_column_sizes(mut self, sizes: HashMap<i32, i64>) -> Self {
        self.column_sizes = Some(sizes);
        self
    }

    pub fn with_value_counts(mut self, counts: HashMap<i32, i64>) -> Self {
        self.value_counts = Some(counts);
        self
    }

    pub fn with_null_value_counts(mut self, counts: HashMap<i32, i64>) -> Self {
        self.null_value_counts = Some(counts);
        self
    }

    pub fn with_split_offsets(mut self, offsets: Vec<i64>) -> Self {
        self.split_offsets = Some(offsets);
        self
    }

    pub fn with_key_metadata(mut self, metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(metadata);
        self
    }

    pub fn with_equality_ids(mut self, ids: Vec<i32>) -> Self {
        self.equality_ids = Some(ids);
        self
    }

    pub fn with_lower_bounds(mut self, bounds: HashMap<i32, Vec<u8>>) -> Self {
        self.lower_bounds = Some(bounds);
        self
    }

    pub fn with_upper_bounds(mut self, bounds: HashMap<i32, Vec<u8>>) -> Self {
        self.upper_bounds = Some(bounds);
        self
    }

    pub fn build(self) -> Result<DataFile> {
        Ok(DataFile {
            content_type: self.content_type.unwrap_or_default(),
            file_path: self
                .file_path
                .ok_or_else(|| Error::InvalidInput("DataFile must have file path".to_string()))?,
            file_format: self
                .file_format
                .ok_or_else(|| Error::InvalidInput("DataFile must have file format".to_string()))?,
            partition: self.partition.unwrap_or_default(),
            record_count: self.record_count.ok_or_else(|| {
                Error::InvalidInput("DataFile must have record count".to_string())
            })?,
            file_size_in_bytes: self
                .file_size_in_bytes
                .ok_or_else(|| Error::InvalidInput("DataFile must have file size".to_string()))?,
            column_sizes: self.column_sizes,
            value_counts: self.value_counts,
            null_value_counts: self.null_value_counts,
            split_offsets: self.split_offsets,
            key_metadata: self.key_metadata,
            equality_ids: self.equality_ids,
            lower_bounds: self.lower_bounds,
            upper_bounds: self.upper_bounds,
            manifest_snapshot_id: None,
            manifest_file_sequence_number: None,
        })
    }
}
