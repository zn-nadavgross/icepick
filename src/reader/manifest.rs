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
#[derive(Debug, Clone)]
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

impl ManifestListReader {
    /// Read a manifest list and return the paths to manifest files
    pub async fn read(file_io: &FileIO, manifest_list_path: &str) -> Result<Vec<String>> {
        let bytes = file_io.read(manifest_list_path).await?;

        let reader = AvroReader::new(&bytes[..])
            .map_err(|e| Error::invalid_input(format!("Failed to read manifest list: {}", e)))?;

        let mut manifest_paths = Vec::new();

        for value in reader {
            let value = value.map_err(|e| {
                Error::invalid_input(format!("Failed to parse manifest list entry: {}", e))
            })?;

            // Extract manifest_path from the Avro record
            if let apache_avro::types::Value::Record(fields) = value {
                for (name, field_value) in fields {
                    if name == "manifest_path" {
                        if let apache_avro::types::Value::String(path) = field_value {
                            manifest_paths.push(path);
                        }
                    }
                }
            }
        }

        Ok(manifest_paths)
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
                let mut info = ManifestFileInfo {
                    manifest_path: String::new(),
                    manifest_length: 0,
                    partition_spec_id: 0,
                    content: 0,
                    sequence_number: 0,
                    min_sequence_number: 0,
                    added_snapshot_id: 0,
                    added_files_count: 0,
                    existing_files_count: 0,
                    deleted_files_count: 0,
                    added_rows_count: 0,
                    existing_rows_count: 0,
                    deleted_rows_count: 0,
                };

                for (name, field_value) in fields {
                    match name.as_str() {
                        "manifest_path" => {
                            if let Value::String(s) = field_value {
                                info.manifest_path = s;
                            }
                        }
                        "manifest_length" => {
                            if let Value::Long(n) = field_value {
                                info.manifest_length = n;
                            }
                        }
                        "partition_spec_id" => {
                            if let Value::Int(n) = field_value {
                                info.partition_spec_id = n;
                            }
                        }
                        "content" => {
                            if let Value::Int(n) = field_value {
                                info.content = n;
                            }
                        }
                        "sequence_number" => {
                            if let Value::Long(n) = field_value {
                                info.sequence_number = n;
                            }
                        }
                        "min_sequence_number" => {
                            if let Value::Long(n) = field_value {
                                info.min_sequence_number = n;
                            }
                        }
                        "added_snapshot_id" => {
                            if let Value::Long(n) = field_value {
                                info.added_snapshot_id = n;
                            }
                        }
                        "added_files_count" => {
                            if let Some(n) = extract_int(&field_value) {
                                info.added_files_count = n;
                            }
                        }
                        "existing_files_count" => {
                            if let Some(n) = extract_int(&field_value) {
                                info.existing_files_count = n;
                            }
                        }
                        "deleted_files_count" => {
                            if let Some(n) = extract_int(&field_value) {
                                info.deleted_files_count = n;
                            }
                        }
                        "added_rows_count" => {
                            if let Some(n) = extract_long(&field_value) {
                                info.added_rows_count = n;
                            }
                        }
                        "existing_rows_count" => {
                            if let Some(n) = extract_long(&field_value) {
                                info.existing_rows_count = n;
                            }
                        }
                        "deleted_rows_count" => {
                            if let Some(n) = extract_long(&field_value) {
                                info.deleted_rows_count = n;
                            }
                        }
                        _ => {}
                    }
                }

                entries.push(info);
            }
        }

        Ok(entries)
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

            // Parse the manifest entry
            if let apache_avro::types::Value::Record(fields) = value {
                let mut status: Option<i32> = None;
                let mut data_file_value: Option<apache_avro::types::Value> = None;

                for (name, field_value) in fields {
                    match name.as_str() {
                        "status" => {
                            if let apache_avro::types::Value::Int(s) = field_value {
                                status = Some(s);
                            }
                        }
                        "data_file" => {
                            data_file_value = Some(field_value);
                        }
                        _ => {}
                    }
                }

                // Skip deleted entries (status = 2)
                if let Some(s) = status {
                    if s == 2 {
                        continue;
                    }
                }

                // Parse data_file record
                if let Some(apache_avro::types::Value::Record(data_file_fields)) = data_file_value {
                    let mut file_path: Option<String> = None;
                    let mut file_format: Option<String> = None;
                    let mut record_count: Option<i64> = None;
                    let mut file_size: Option<i64> = None;

                    for (name, field_value) in data_file_fields {
                        match name.as_str() {
                            "file_path" => {
                                if let apache_avro::types::Value::String(s) = field_value {
                                    file_path = Some(s);
                                }
                            }
                            "file_format" => {
                                if let apache_avro::types::Value::String(s) = field_value {
                                    file_format = Some(s);
                                }
                            }
                            "record_count" => {
                                if let apache_avro::types::Value::Long(n) = field_value {
                                    record_count = Some(n);
                                }
                            }
                            "file_size_in_bytes" => {
                                if let apache_avro::types::Value::Long(n) = field_value {
                                    file_size = Some(n);
                                }
                            }
                            _ => {}
                        }
                    }

                    if let (Some(path), Some(format), Some(count), Some(size)) =
                        (file_path, file_format, record_count, file_size)
                    {
                        data_files.push(DataFileEntry {
                            file_path: path,
                            file_format: format,
                            record_count: count,
                            file_size_in_bytes: size,
                        });
                    }
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

            // Parse the manifest entry
            if let Value::Record(fields) = value {
                let mut status: Option<i32> = None;
                let mut data_file_value: Option<Value> = None;

                for (name, field_value) in fields {
                    match name.as_str() {
                        "status" => {
                            if let Value::Int(s) = field_value {
                                status = Some(s);
                            }
                        }
                        "data_file" => {
                            data_file_value = Some(field_value);
                        }
                        _ => {}
                    }
                }

                // Skip deleted entries (status = 2)
                if let Some(s) = status {
                    if s == 2 {
                        continue;
                    }
                }

                // Parse data_file record with all stats
                if let Some(Value::Record(data_file_fields)) = data_file_value {
                    if let Some(stats) = parse_data_file_stats(data_file_fields) {
                        data_files.push(stats);
                    }
                }
            }
        }

        Ok(data_files)
    }
}

/// Parse a data_file record into DataFileStats
fn parse_data_file_stats(fields: Vec<(String, Value)>) -> Option<DataFileStats> {
    let mut file_path: Option<String> = None;
    let mut file_format: Option<String> = None;
    let mut record_count: Option<i64> = None;
    let mut file_size: Option<i64> = None;
    let mut partition = HashMap::new();
    let mut lower_bounds = HashMap::new();
    let mut upper_bounds = HashMap::new();
    let mut null_value_counts = HashMap::new();
    let mut value_counts = HashMap::new();

    for (name, field_value) in fields {
        match name.as_str() {
            "file_path" => {
                if let Value::String(s) = field_value {
                    file_path = Some(s);
                }
            }
            "file_format" => {
                if let Value::String(s) = field_value {
                    file_format = Some(s);
                }
            }
            "record_count" => {
                if let Value::Long(n) = field_value {
                    record_count = Some(n);
                }
            }
            "file_size_in_bytes" => {
                if let Value::Long(n) = field_value {
                    file_size = Some(n);
                }
            }
            "partition" => {
                partition = extract_partition_values(&field_value);
            }
            "lower_bounds" => {
                lower_bounds = extract_bounds_map(&field_value);
            }
            "upper_bounds" => {
                upper_bounds = extract_bounds_map(&field_value);
            }
            "null_value_counts" => {
                null_value_counts = extract_count_map(&field_value);
            }
            "value_counts" => {
                value_counts = extract_count_map(&field_value);
            }
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
    let mut result = HashMap::new();

    // Handle union wrapper
    let inner = match value {
        Value::Union(_, boxed) => boxed.as_ref(),
        other => other,
    };

    if let Value::Record(fields) = inner {
        for (field_name, field_value) in fields {
            // Field names in partition struct are the partition field IDs
            if let Ok(field_id) = field_name.parse::<i32>() {
                if let Some(bytes) = value_to_bytes(field_value) {
                    result.insert(field_id, bytes);
                }
            }
        }
    }

    result
}

/// Extract bounds map (field_id -> bytes)
/// Bounds are stored as Avro map<int, bytes>
fn extract_bounds_map(value: &Value) -> HashMap<i32, Vec<u8>> {
    let mut result = HashMap::new();

    // Handle union wrapper
    let inner = match value {
        Value::Union(_, boxed) => boxed.as_ref(),
        other => other,
    };

    // Iceberg stores bounds as array of {key, value} records (Avro map)
    if let Value::Map(map) = inner {
        for (key, val) in map {
            if let Ok(field_id) = key.parse::<i32>() {
                if let Value::Bytes(bytes) = val {
                    result.insert(field_id, bytes.clone());
                }
            }
        }
    } else if let Value::Array(items) = inner {
        // Some Avro implementations use array of key-value pairs
        for item in items {
            if let Value::Record(fields) = item {
                let mut key: Option<i32> = None;
                let mut val: Option<Vec<u8>> = None;

                for (name, field_val) in fields {
                    match name.as_str() {
                        "key" => {
                            if let Value::Int(k) = field_val {
                                key = Some(*k);
                            }
                        }
                        "value" => {
                            if let Value::Bytes(v) = field_val {
                                val = Some(v.clone());
                            }
                        }
                        _ => {}
                    }
                }

                if let (Some(k), Some(v)) = (key, val) {
                    result.insert(k, v);
                }
            }
        }
    }

    result
}

/// Extract count map (field_id -> count)
fn extract_count_map(value: &Value) -> HashMap<i32, i64> {
    let mut result = HashMap::new();

    // Handle union wrapper
    let inner = match value {
        Value::Union(_, boxed) => boxed.as_ref(),
        other => other,
    };

    if let Value::Map(map) = inner {
        for (key, val) in map {
            if let Ok(field_id) = key.parse::<i32>() {
                if let Some(count) = extract_long(val) {
                    result.insert(field_id, count);
                }
            }
        }
    } else if let Value::Array(items) = inner {
        for item in items {
            if let Value::Record(fields) = item {
                let mut key: Option<i32> = None;
                let mut val: Option<i64> = None;

                for (name, field_val) in fields {
                    match name.as_str() {
                        "key" => {
                            if let Value::Int(k) = field_val {
                                key = Some(*k);
                            }
                        }
                        "value" => {
                            val = extract_long(field_val);
                        }
                        _ => {}
                    }
                }

                if let (Some(k), Some(v)) = (key, val) {
                    result.insert(k, v);
                }
            }
        }
    }

    result
}

/// Convert an Avro value to bytes for storage
fn value_to_bytes(value: &Value) -> Option<Vec<u8>> {
    // Handle union wrapper
    let inner = match value {
        Value::Union(_, boxed) => boxed.as_ref(),
        Value::Null => return None,
        other => other,
    };

    match inner {
        Value::Null => None,
        Value::Boolean(b) => Some(vec![if *b { 1 } else { 0 }]),
        Value::Int(n) => Some(n.to_le_bytes().to_vec()),
        Value::Long(n) => Some(n.to_le_bytes().to_vec()),
        Value::Float(n) => Some(n.to_le_bytes().to_vec()),
        Value::Double(n) => Some(n.to_le_bytes().to_vec()),
        Value::Bytes(b) => Some(b.clone()),
        Value::String(s) => Some(s.as_bytes().to_vec()),
        Value::Fixed(_, b) => Some(b.clone()),
        _ => None,
    }
}
