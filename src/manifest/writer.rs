//! Write manifest and manifest list files

use crate::error::Result;
use crate::io::FileIO;
use crate::manifest::avro::data_file_to_avro;
use crate::manifest::schema::{manifest_entry_schema_v2, manifest_list_schema_v2};
use crate::spec::{DataFile, PartitionSpec, Schema as IcebergSchema};
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

/// Per-partition-field summary written into manifest list entries. One entry
/// per field in the partition spec referenced by `partition_spec_id`; the
/// length must match the spec or readers like Trino throw OOB.
#[derive(Debug, Clone, Default)]
pub struct FieldSummary {
    pub contains_null: bool,
    pub contains_nan: Option<bool>,
    pub lower_bound: Option<Vec<u8>>,
    pub upper_bound: Option<Vec<u8>>,
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
    /// Per-field partition summaries; must have one entry per field in the
    /// partition spec referenced by `partition_spec_id`.
    pub partitions: Vec<FieldSummary>,
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
    partition_spec: &PartitionSpec,
    iceberg_schema: &IcebergSchema,
) -> Result<i64> {
    // Convert to entries with Added status
    let entries: Vec<ManifestEntry> = data_files
        .iter()
        .map(|df| ManifestEntry {
            data_file: df.clone(),
            status: ManifestEntryStatus::Added,
        })
        .collect();

    write_manifest_with_entries(
        file_io,
        path,
        &entries,
        snapshot_id,
        sequence_number,
        partition_spec,
        iceberg_schema,
    )
    .await
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
    partition_spec: &PartitionSpec,
    iceberg_schema: &IcebergSchema,
) -> Result<i64> {
    let schema = manifest_entry_schema_v2(partition_spec, iceberg_schema)?;

    let mut writer = Writer::new(&schema, Vec::new());

    // Iceberg's ManifestReader pulls table schema and partition spec from the
    // Avro file's user metadata; without these keys Trino's iceberg lib NPEs
    // in SchemaParser.fromJson when listing the table's $files.
    let schema_json = serde_json::to_string(iceberg_schema).map_err(|e| {
        crate::error::Error::InvalidInput(format!("Failed to serialize schema: {}", e))
    })?;
    let partition_spec_fields_json = serde_json::to_string(partition_spec.fields())
        .map_err(|e| {
            crate::error::Error::InvalidInput(format!(
                "Failed to serialize partition spec fields: {}",
                e
            ))
        })?;
    let add_meta = |w: &mut Writer<Vec<u8>>, name: &str, value: String| -> Result<()> {
        w.add_user_metadata(name.to_string(), value.as_bytes())
            .map_err(|e| {
                crate::error::Error::InvalidInput(format!(
                    "Failed to add Avro user metadata '{}': {}",
                    name, e
                ))
            })?;
        Ok(())
    };
    add_meta(&mut writer, "schema", schema_json)?;
    add_meta(
        &mut writer,
        "partition-spec",
        partition_spec_fields_json,
    )?;
    add_meta(
        &mut writer,
        "partition-spec-id",
        partition_spec.spec_id().to_string(),
    )?;
    add_meta(&mut writer, "format-version", "2".to_string())?;
    add_meta(&mut writer, "content", "data".to_string())?;

    for entry in entries {
        let data_file_value =
            data_file_to_avro(&entry.data_file, partition_spec, iceberg_schema)?;
        let status_value: i32 = entry.status.into();

        // DELETE entries must reference the *original* snapshot_id and
        // file_sequence_number of the entry that added the file, otherwise
        // Iceberg readers can't tombstone the file and orphan cleanup keeps
        // it on disk. ADD entries get the current commit's values.
        let (entry_snapshot_id, entry_file_seq) = match entry.status {
            ManifestEntryStatus::Deleted => (
                entry
                    .data_file
                    .manifest_snapshot_id()
                    .unwrap_or(snapshot_id),
                entry
                    .data_file
                    .manifest_file_sequence_number()
                    .unwrap_or(sequence_number),
            ),
            _ => (snapshot_id, sequence_number),
        };

        let avro_entry = Value::Record(vec![
            ("status".to_string(), Value::Int(status_value)),
            (
                "snapshot_id".to_string(),
                Value::Union(1, Box::new(Value::Long(entry_snapshot_id))),
            ),
            (
                "sequence_number".to_string(),
                Value::Union(1, Box::new(Value::Long(sequence_number))),
            ),
            (
                "file_sequence_number".to_string(),
                Value::Union(1, Box::new(Value::Long(entry_file_seq))),
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
            // For partitioned tables this must contain one field summary per
            // partition spec field, or Trino throws ArrayIndexOutOfBoundsException
            // when iterating field indices. Using empty array instead of null is
            // required for DuckDB compatibility.
            (
                "partitions".to_string(),
                Value::Union(1, Box::new(Value::Array(
                    entry.partitions.iter().map(field_summary_to_avro).collect(),
                ))),
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

fn field_summary_to_avro(summary: &FieldSummary) -> Value {
    let contains_nan = match summary.contains_nan {
        Some(b) => Value::Union(1, Box::new(Value::Boolean(b))),
        None => Value::Union(0, Box::new(Value::Null)),
    };
    let lower_bound = match &summary.lower_bound {
        Some(bytes) => Value::Union(1, Box::new(Value::Bytes(bytes.clone()))),
        None => Value::Union(0, Box::new(Value::Null)),
    };
    let upper_bound = match &summary.upper_bound {
        Some(bytes) => Value::Union(1, Box::new(Value::Bytes(bytes.clone()))),
        None => Value::Union(0, Box::new(Value::Null)),
    };
    Value::Record(vec![
        (
            "contains_null".to_string(),
            Value::Boolean(summary.contains_null),
        ),
        ("contains_nan".to_string(), contains_nan),
        ("lower_bound".to_string(), lower_bound),
        ("upper_bound".to_string(), upper_bound),
    ])
}
