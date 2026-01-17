//! Reading Iceberg manifest files

use crate::error::{Error, Result};
use crate::io::FileIO;
use apache_avro::types::Value;
use apache_avro::Reader as AvroReader;
use std::collections::HashMap;

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
}

/// Reads manifest list files
pub struct ManifestListReader;

fn extract_int(value: &Value) -> Option<i32> {
    match value {
        Value::Int(n) => Some(*n),
        Value::Union(_, boxed) => extract_int(boxed),
        _ => None,
    }
}

fn extract_long(value: &Value) -> Option<i64> {
    match value {
        Value::Long(n) => Some(*n),
        Value::Union(_, boxed) => extract_long(boxed),
        _ => None,
    }
}

/// Extract string from Avro value
fn extract_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Union(_, boxed) => extract_string(boxed),
        _ => None,
    }
}

/// Parse a manifest file info record from Avro fields
fn parse_manifest_file_info(fields: Vec<(String, Value)>) -> ManifestFileInfo {
    let mut info = ManifestFileInfo::default();
    for (name, field_value) in fields {
        match name.as_str() {
            "manifest_path" => {
                info.manifest_path = extract_string(&field_value).unwrap_or_default()
            }
            "manifest_length" => info.manifest_length = extract_long(&field_value).unwrap_or(0),
            "partition_spec_id" => info.partition_spec_id = extract_int(&field_value).unwrap_or(0),
            "content" => info.content = extract_int(&field_value).unwrap_or(0),
            "sequence_number" => info.sequence_number = extract_long(&field_value).unwrap_or(0),
            "min_sequence_number" => {
                info.min_sequence_number = extract_long(&field_value).unwrap_or(0)
            }
            "added_snapshot_id" => info.added_snapshot_id = extract_long(&field_value).unwrap_or(0),
            "added_files_count" => info.added_files_count = extract_int(&field_value).unwrap_or(0),
            "existing_files_count" => {
                info.existing_files_count = extract_int(&field_value).unwrap_or(0)
            }
            "deleted_files_count" => {
                info.deleted_files_count = extract_int(&field_value).unwrap_or(0)
            }
            "added_rows_count" => info.added_rows_count = extract_long(&field_value).unwrap_or(0),
            "existing_rows_count" => {
                info.existing_rows_count = extract_long(&field_value).unwrap_or(0)
            }
            "deleted_rows_count" => {
                info.deleted_rows_count = extract_long(&field_value).unwrap_or(0)
            }
            _ => {}
        }
    }
    info
}

impl ManifestListReader {
    /// Read a manifest list and return the paths to manifest files
    pub async fn read(file_io: &FileIO, manifest_list_path: &str) -> Result<Vec<String>> {
        let bytes = file_io.read(manifest_list_path).await?;

        let reader = AvroReader::new(&bytes[..])
            .map_err(|e| Error::invalid_input(format!("Failed to read manifest list: {}", e)))?;

        Ok(reader
            .filter_map(|value| {
                let apache_avro::types::Value::Record(fields) = value.ok()? else {
                    return None;
                };
                fields.into_iter().find_map(|(name, value)| {
                    (name == "manifest_path").then_some(extract_string(&value))?
                })
            })
            .collect())
    }

    /// Read a manifest list and return detailed manifest file information
    pub async fn read_entries(
        file_io: &FileIO,
        manifest_list_path: &str,
    ) -> Result<Vec<ManifestFileInfo>> {
        let bytes = file_io.read(manifest_list_path).await?;

        let reader = AvroReader::new(&bytes[..])
            .map_err(|e| Error::invalid_input(format!("Failed to read manifest list: {}", e)))?;

        let mut entries = Vec::new();

        for value in reader {
            let value = value.map_err(|e| {
                Error::invalid_input(format!("Failed to parse manifest list entry: {}", e))
            })?;

            if let Value::Record(fields) = value {
                entries.push(parse_manifest_file_info(fields));
            }
        }

        Ok(entries)
    }
}

/// Extract status and data_file from manifest entry fields
fn extract_manifest_entry_parts(fields: Vec<(String, Value)>) -> (Option<i32>, Option<Value>) {
    let mut status = None;
    let mut data_file_value = None;
    for (name, field_value) in fields {
        match name.as_str() {
            "status" => status = extract_int(&field_value),
            "data_file" => data_file_value = Some(field_value),
            _ => {}
        }
    }
    (status, data_file_value)
}

/// Parse manifest entry with full stats, skipping deleted entries
fn parse_manifest_entry_with_stats(fields: Vec<(String, Value)>) -> Option<DataFileStats> {
    let (status, data_file_value) = extract_manifest_entry_parts(fields);
    if status == Some(2) {
        return None;
    }
    if let Some(Value::Record(data_file_fields)) = data_file_value {
        parse_data_file_stats(data_file_fields)
    } else {
        None
    }
}

/// Parse a manifest entry and extract data file entry if not deleted
fn parse_manifest_entry(fields: Vec<(String, Value)>) -> Option<DataFileEntry> {
    let (status, data_file_value) = extract_manifest_entry_parts(fields);
    if status == Some(2) {
        return None;
    }

    if let Some(Value::Record(data_file_fields)) = data_file_value {
        let mut file_path = None;
        let mut file_format = None;
        let mut record_count = None;
        let mut file_size = None;

        for (name, field_value) in data_file_fields {
            match name.as_str() {
                "file_path" => file_path = extract_string(&field_value),
                "file_format" => file_format = extract_string(&field_value),
                "record_count" => record_count = extract_long(&field_value),
                "file_size_in_bytes" => file_size = extract_long(&field_value),
                _ => {}
            }
        }

        Some(DataFileEntry {
            file_path: file_path?,
            file_format: file_format?,
            record_count: record_count?,
            file_size_in_bytes: file_size?,
        })
    } else {
        None
    }
}

/// Reads manifest files
pub struct ManifestReader;

impl ManifestReader {
    /// Read a manifest and return data file entries (excluding deleted files)
    pub async fn read(file_io: &FileIO, manifest_path: &str) -> Result<Vec<DataFileEntry>> {
        let bytes = file_io.read(manifest_path).await?;

        let reader = AvroReader::new(&bytes[..])
            .map_err(|e| Error::invalid_input(format!("Failed to read manifest: {}", e)))?;

        let mut data_files = Vec::new();

        for value in reader {
            let value = value.map_err(|e| {
                Error::invalid_input(format!("Failed to parse manifest entry: {}", e))
            })?;

            if let Value::Record(fields) = value {
                if let Some(entry) = parse_manifest_entry(fields) {
                    data_files.push(entry);
                }
            }
        }

        Ok(data_files)
    }

    /// Read a manifest and return data file entries with full statistics for pruning
    pub async fn read_with_stats(
        file_io: &FileIO,
        manifest_path: &str,
    ) -> Result<Vec<DataFileStats>> {
        let bytes = file_io.read(manifest_path).await?;

        let reader = AvroReader::new(&bytes[..])
            .map_err(|e| Error::invalid_input(format!("Failed to read manifest: {}", e)))?;

        let mut data_files = Vec::new();

        for value in reader {
            let value = value.map_err(|e| {
                Error::invalid_input(format!("Failed to parse manifest entry: {}", e))
            })?;

            if let Value::Record(fields) = value {
                if let Some(entry) = parse_manifest_entry_with_stats(fields) {
                    data_files.push(entry);
                }
            }
        }

        Ok(data_files)
    }
}

/// Parse a data_file record into DataFileStats
fn parse_data_file_stats(fields: Vec<(String, Value)>) -> Option<DataFileStats> {
    let mut file_path = None;
    let mut file_format = None;
    let mut record_count = None;
    let mut file_size = None;
    let mut partition = HashMap::new();
    let mut lower_bounds = HashMap::new();
    let mut upper_bounds = HashMap::new();
    let mut null_value_counts = HashMap::new();
    let mut value_counts = HashMap::new();

    for (name, field_value) in fields {
        match name.as_str() {
            "file_path" => file_path = extract_string(&field_value),
            "file_format" => file_format = extract_string(&field_value),
            "record_count" => record_count = extract_long(&field_value),
            "file_size_in_bytes" => file_size = extract_long(&field_value),
            "partition" => partition = extract_partition_values(&field_value),
            "lower_bounds" => lower_bounds = extract_bounds_map(&field_value),
            "upper_bounds" => upper_bounds = extract_bounds_map(&field_value),
            "null_value_counts" => null_value_counts = extract_count_map(&field_value),
            "value_counts" => value_counts = extract_count_map(&field_value),
            _ => {}
        }
    }

    Some(DataFileStats {
        file_path: file_path?,
        file_format: file_format?,
        record_count: record_count?,
        file_size_in_bytes: file_size?,
        partition,
        lower_bounds,
        upper_bounds,
        null_value_counts,
        value_counts,
    })
}

/// Extract partition values from the partition field
/// Partition is a struct where each field corresponds to a partition field ID
fn extract_partition_values(value: &Value) -> HashMap<i32, Vec<u8>> {
    let inner = match value {
        Value::Union(_, boxed) => boxed.as_ref(),
        other => other,
    };

    if let Value::Record(fields) = inner {
        fields
            .iter()
            .filter_map(|(field_name, field_value)| {
                let field_id = field_name.parse::<i32>().ok()?;
                let bytes = value_to_bytes(field_value)?;
                Some((field_id, bytes))
            })
            .collect()
    } else {
        HashMap::new()
    }
}

/// Generic extraction helper for map<int, V> fields
fn extract_map<V, F>(value: &Value, extractor: F) -> HashMap<i32, V>
where
    F: Fn(&Value) -> Option<V>,
{
    let inner = match value {
        Value::Union(_, boxed) => boxed.as_ref(),
        other => other,
    };

    match inner {
        Value::Map(map) => map
            .iter()
            .filter_map(|(key, val)| {
                let field_id = key.parse::<i32>().ok()?;
                let v = extractor(val)?;
                Some((field_id, v))
            })
            .collect(),
        Value::Array(items) => items
            .iter()
            .filter_map(|item| {
                let Value::Record(fields) = item else {
                    return None;
                };
                let mut key = None;
                let mut val = None;
                for (name, field_val) in fields {
                    match name.as_str() {
                        "key" => key = extract_int(field_val),
                        "value" => val = extractor(field_val),
                        _ => {}
                    }
                }
                Some((key?, val?))
            })
            .collect(),
        _ => HashMap::new(),
    }
}

/// Extract bounds map (field_id -> bytes)
fn extract_bounds_map(value: &Value) -> HashMap<i32, Vec<u8>> {
    extract_map(value, |v| match v {
        Value::Bytes(bytes) => Some(bytes.clone()),
        _ => None,
    })
}

/// Extract count map (field_id -> count)
fn extract_count_map(value: &Value) -> HashMap<i32, i64> {
    extract_map(value, extract_long)
}

/// Convert an Avro value to bytes for storage
fn value_to_bytes(value: &Value) -> Option<Vec<u8>> {
    let inner = match value {
        Value::Union(_, boxed) => boxed.as_ref(),
        Value::Null => return None,
        other => other,
    };

    Some(match inner {
        Value::Null => return None,
        Value::Boolean(b) => vec![if *b { 1 } else { 0 }],
        Value::Int(n) => n.to_le_bytes().to_vec(),
        Value::Long(n) => n.to_le_bytes().to_vec(),
        Value::Float(n) => n.to_le_bytes().to_vec(),
        Value::Double(n) => n.to_le_bytes().to_vec(),
        Value::Bytes(b) | Value::Fixed(_, b) => b.clone(),
        Value::String(s) => s.as_bytes().to_vec(),
        _ => return None,
    })
}
