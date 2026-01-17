//! Write manifest and manifest list files

use crate::error::Result;
use crate::io::FileIO;
use crate::manifest::avro::data_file_to_avro;
use crate::manifest::schema::{manifest_entry_schema_v2, manifest_list_schema_v2};
use crate::spec::DataFile;
use apache_avro::types::Value;
use apache_avro::Writer;

/// Status of a manifest entry
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ManifestEntryStatus {
    /// File exists from a previous snapshot
    Existing = 0,
    /// File was added in this snapshot
    Added = 1,
    /// File was deleted in this snapshot
    Deleted = 2,
}

impl From<ManifestEntryStatus> for i32 {
    fn from(status: ManifestEntryStatus) -> Self {
        status as i32
    }
}

/// A data file with its manifest entry status
#[derive(Debug, Clone)]
pub struct ManifestEntry {
    /// The data file
    pub data_file: DataFile,
    /// The status of this entry
    pub status: ManifestEntryStatus,
}

/// Represents an entry in a manifest list
#[derive(Debug, Clone)]
pub struct ManifestListEntry {
    /// Path to the manifest file
    pub manifest_path: String,
    /// Size of the manifest file in bytes
    pub manifest_length: i64,
    /// Partition spec ID (0 for unpartitioned)
    pub partition_spec_id: i32,
    /// Content type (0 = DATA, 1 = DELETES)
    pub content: i32,
    /// Sequence number for this manifest
    pub sequence_number: i64,
    /// Minimum sequence number in this manifest
    pub min_sequence_number: i64,
    /// Snapshot ID that added this manifest
    pub added_snapshot_id: i64,
    /// Number of files added in this manifest
    pub added_files_count: i32,
    /// Number of existing files in this manifest
    pub existing_files_count: i32,
    /// Number of deleted files in this manifest
    pub deleted_files_count: i32,
    /// Number of rows added
    pub added_rows_count: i64,
    /// Number of existing rows
    pub existing_rows_count: i64,
    /// Number of deleted rows
    pub deleted_rows_count: i64,
}

/// Write a manifest file containing data file entries
///
/// Returns the number of bytes written
pub async fn write_manifest(
    file_io: &FileIO,
    path: &str,
    data_files: &[DataFile],
    snapshot_id: i64,
    sequence_number: i64,
) -> Result<i64> {
    // Convert to entries with Added status
    let entries: Vec<ManifestEntry> = data_files
        .iter()
        .map(|df| ManifestEntry {
            data_file: df.clone(),
            status: ManifestEntryStatus::Added,
        })
        .collect();

    write_manifest_with_entries(file_io, path, &entries, snapshot_id, sequence_number).await
}

/// Write a manifest file containing data file entries with explicit status
///
/// This function allows specifying the status for each entry (Existing, Added, or Deleted).
/// Returns the number of bytes written.
pub async fn write_manifest_with_entries(
    file_io: &FileIO,
    path: &str,
    entries: &[ManifestEntry],
    snapshot_id: i64,
    sequence_number: i64,
) -> Result<i64> {
    let schema = manifest_entry_schema_v2()
        .map_err(|e| crate::error::Error::InvalidInput(format!("Invalid Avro schema: {}", e)))?;

    let mut writer = Writer::new(&schema, Vec::new());

    for entry in entries {
        let data_file_value = data_file_to_avro(&entry.data_file)?;
        let status_value: i32 = entry.status.into();

        let avro_entry = Value::Record(vec![
            ("status".to_string(), Value::Int(status_value)),
            (
                "snapshot_id".to_string(),
                Value::Union(1, Box::new(Value::Long(snapshot_id))),
            ),
            (
                "sequence_number".to_string(),
                Value::Union(1, Box::new(Value::Long(sequence_number))),
            ),
            (
                "file_sequence_number".to_string(),
                Value::Union(1, Box::new(Value::Long(sequence_number))),
            ),
            ("data_file".to_string(), data_file_value),
        ]);

        writer.append(avro_entry).map_err(|e| {
            crate::error::Error::InvalidInput(format!("Failed to append to Avro writer: {}", e))
        })?;
    }

    let avro_bytes = writer.into_inner().map_err(|e| {
        crate::error::Error::InvalidInput(format!("Failed to finalize Avro writer: {}", e))
    })?;

    let bytes_written = avro_bytes.len() as i64;
    file_io.write(path, avro_bytes).await?;

    Ok(bytes_written)
}

/// Write a manifest list file containing manifest file metadata
///
/// Accepts multiple manifest entries to support carrying forward manifests from parent snapshots
pub async fn write_manifest_list(
    file_io: &FileIO,
    path: &str,
    entries: Vec<ManifestListEntry>,
) -> Result<()> {
    let schema = manifest_list_schema_v2()
        .map_err(|e| crate::error::Error::InvalidInput(format!("Invalid Avro schema: {}", e)))?;

    let mut writer = Writer::new(&schema, Vec::new());

    for entry in entries {
        let avro_entry = Value::Record(vec![
            (
                "manifest_path".to_string(),
                Value::String(entry.manifest_path),
            ),
            (
                "manifest_length".to_string(),
                Value::Long(entry.manifest_length),
            ),
            (
                "partition_spec_id".to_string(),
                Value::Int(entry.partition_spec_id),
            ),
            ("content".to_string(), Value::Int(entry.content)),
            (
                "sequence_number".to_string(),
                Value::Long(entry.sequence_number),
            ),
            (
                "min_sequence_number".to_string(),
                Value::Long(entry.min_sequence_number),
            ),
            (
                "added_snapshot_id".to_string(),
                Value::Long(entry.added_snapshot_id),
            ),
            (
                "added_files_count".to_string(),
                Value::Int(entry.added_files_count),
            ),
            (
                "existing_files_count".to_string(),
                Value::Int(entry.existing_files_count),
            ),
            (
                "deleted_files_count".to_string(),
                Value::Int(entry.deleted_files_count),
            ),
            (
                "added_rows_count".to_string(),
                Value::Long(entry.added_rows_count),
            ),
            (
                "existing_rows_count".to_string(),
                Value::Long(entry.existing_rows_count),
            ),
            (
                "deleted_rows_count".to_string(),
                Value::Long(entry.deleted_rows_count),
            ),
            // TODO: Support partitioned tables
            // Currently icepick only writes unpartitioned data (partition_spec_id=0),
            // so partitions is always an empty array. When partition support is added,
            // this should contain field summaries (min/max values for each partition field).
            // Using empty array instead of null is required for DuckDB compatibility.
            (
                "partitions".to_string(),
                Value::Union(1, Box::new(Value::Array(vec![]))),
            ),
            (
                "key_metadata".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
        ]);

        writer.append(avro_entry).map_err(|e| {
            crate::error::Error::InvalidInput(format!("Failed to append to Avro writer: {}", e))
        })?;
    }

    let avro_bytes = writer.into_inner().map_err(|e| {
        crate::error::Error::InvalidInput(format!("Failed to finalize Avro writer: {}", e))
    })?;

    file_io.write(path, avro_bytes).await?;

    Ok(())
}
