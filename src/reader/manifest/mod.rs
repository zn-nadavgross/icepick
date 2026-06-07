//! Reading Iceberg manifest files

use std::collections::HashMap;

use crate::manifest::FieldSummary;

mod extract;
mod file;
mod list;
mod parse;

pub use file::ManifestReader;
pub use list::ManifestListReader;

/// Information about a data file discovered from manifests
#[derive(Debug, Clone)]
pub struct DataFileEntry {
    /// Path to the data file
    pub file_path: String,
    /// Number of records in the file
    pub record_count: i64,
    /// Size of the file in bytes
    pub file_size_in_bytes: i64,
    /// File format (e.g., "PARQUET")
    pub file_format: String,
}

/// Enhanced data file entry with partition and statistics info for pruning
#[derive(Debug, Clone)]
pub struct DataFileStats {
    /// Path to the data file
    pub file_path: String,
    /// Number of records in the file
    pub record_count: i64,
    /// Size of the file in bytes
    pub file_size_in_bytes: i64,
    /// File format (e.g., "PARQUET")
    pub file_format: String,
    /// Partition values (field_id -> raw bytes)
    pub partition: HashMap<i32, Vec<u8>>,
    /// Lower bounds per column (field_id -> raw bytes)
    pub lower_bounds: HashMap<i32, Vec<u8>>,
    /// Upper bounds per column (field_id -> raw bytes)
    pub upper_bounds: HashMap<i32, Vec<u8>>,
    /// Null value counts per column (field_id -> count)
    pub null_value_counts: HashMap<i32, i64>,
    /// Value counts per column (field_id -> count, non-null values)
    pub value_counts: HashMap<i32, i64>,
}

/// Information about a manifest file entry in a manifest list
#[derive(Debug, Clone, Default)]
pub struct ManifestFileInfo {
    /// Path to the manifest file
    pub manifest_path: String,
    /// Size of the manifest file in bytes
    pub manifest_length: i64,
    /// Partition spec ID
    pub partition_spec_id: i32,
    /// Content type (0 = DATA, 1 = DELETES)
    pub content: i32,
    /// Sequence number
    pub sequence_number: i64,
    /// Minimum sequence number
    pub min_sequence_number: i64,
    /// Snapshot ID that added this manifest
    pub added_snapshot_id: i64,
    /// Number of files added
    pub added_files_count: i32,
    /// Number of existing files
    pub existing_files_count: i32,
    /// Number of deleted files
    pub deleted_files_count: i32,
    /// Number of rows added
    pub added_rows_count: i64,
    /// Number of existing rows
    pub existing_rows_count: i64,
    /// Number of deleted rows
    pub deleted_rows_count: i64,
    /// Per-partition-field summaries from the manifest list entry. Must be
    /// carried forward verbatim when rewriting manifest lists so partition spec
    /// readers (Trino, etc.) see the correct number of field entries.
    pub partitions: Vec<FieldSummary>,
}
