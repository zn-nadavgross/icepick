//! Write manifest and manifest list files

use crate::error::Result;
use crate::io::FileIO;
use crate::manifest::avro::data_file_to_avro;
use crate::manifest::schema::manifest_entry_schema_v2;
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
