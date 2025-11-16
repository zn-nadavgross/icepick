//! Write manifest and manifest list files

use crate::error::Result;
use crate::io::FileIO;
use crate::manifest::avro::data_file_to_avro;
use crate::manifest::schema::{manifest_entry_schema_v2, manifest_list_schema_v2};
use crate::spec::DataFile;
use apache_avro::types::Value;
use apache_avro::Writer;

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
    let schema = manifest_entry_schema_v2()
        .map_err(|e| crate::error::Error::InvalidInput(format!("Invalid Avro schema: {}", e)))?;

    let mut writer = Writer::new(&schema, Vec::new());

    for data_file in data_files {
        let data_file_value = data_file_to_avro(data_file)?;

        let entry = Value::Record(vec![
            ("status".to_string(), Value::Int(1)), // 1 = ADDED
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

        writer.append(entry).map_err(|e| {
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
#[allow(clippy::too_many_arguments)]
pub async fn write_manifest_list(
    file_io: &FileIO,
    path: &str,
    manifest_path: &str,
    manifest_length: i64,
    snapshot_id: i64,
    sequence_number: i64,
    added_files_count: i32,
    added_rows_count: i64,
) -> Result<()> {
    let schema = manifest_list_schema_v2()
        .map_err(|e| crate::error::Error::InvalidInput(format!("Invalid Avro schema: {}", e)))?;

    let mut writer = Writer::new(&schema, Vec::new());

    let entry = Value::Record(vec![
        (
            "manifest_path".to_string(),
            Value::String(manifest_path.to_string()),
        ),
        ("manifest_length".to_string(), Value::Long(manifest_length)),
        ("partition_spec_id".to_string(), Value::Int(0)), // Unpartitioned
        ("content".to_string(), Value::Int(0)),           // 0 = DATA
        ("sequence_number".to_string(), Value::Long(sequence_number)),
        (
            "min_sequence_number".to_string(),
            Value::Long(sequence_number),
        ),
        ("added_snapshot_id".to_string(), Value::Long(snapshot_id)),
        (
            "added_files_count".to_string(),
            Value::Int(added_files_count),
        ),
        ("existing_files_count".to_string(), Value::Int(0)),
        ("deleted_files_count".to_string(), Value::Int(0)),
        (
            "added_rows_count".to_string(),
            Value::Long(added_rows_count),
        ),
        ("existing_rows_count".to_string(), Value::Long(0)),
        ("deleted_rows_count".to_string(), Value::Long(0)),
        (
            "partitions".to_string(),
            Value::Union(0, Box::new(Value::Null)),
        ),
        (
            "key_metadata".to_string(),
            Value::Union(0, Box::new(Value::Null)),
        ),
    ]);

    writer.append(entry).map_err(|e| {
        crate::error::Error::InvalidInput(format!("Failed to append to Avro writer: {}", e))
    })?;

    let avro_bytes = writer.into_inner().map_err(|e| {
        crate::error::Error::InvalidInput(format!("Failed to finalize Avro writer: {}", e))
    })?;

    file_io.write(path, avro_bytes).await?;

    Ok(())
}
