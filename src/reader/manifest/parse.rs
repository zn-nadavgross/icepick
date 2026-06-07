//! Manifest entry parsing

use super::extract::*;
use super::{DataFileEntry, DataFileStats, ManifestFileInfo};
use crate::error::{Error, Result};
use crate::manifest::FieldSummary;
use apache_avro::types::Value;
use std::collections::HashMap;

/// Parse a manifest file info record from Avro fields
pub(super) fn parse_manifest_file_info(fields: Vec<(String, Value)>) -> Result<ManifestFileInfo> {
    let mut info = ManifestFileInfo::default();
    for (name, field_value) in fields {
        match name.as_str() {
            "manifest_path" => {
                info.manifest_path = extract_required_string(&field_value, "manifest_path")?
            }
            "manifest_length" => {
                info.manifest_length = extract_required_long(&field_value, "manifest_length")?
            }
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
            "partitions" => info.partitions = parse_field_summaries(&field_value),
            _ => {}
        }
    }

    // Validate required fields
    if info.manifest_path.is_empty() {
        return Err(Error::invalid_input(
            "manifest_path is required but missing or empty".to_string(),
        ));
    }

    Ok(info)
}

/// Extract status and data_file from manifest entry fields
pub(super) fn extract_manifest_entry_parts(
    fields: Vec<(String, Value)>,
) -> (Option<i32>, Option<Value>) {
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

/// Parse data file basic fields from Avro record
pub(super) fn parse_data_file_basic(fields: Vec<(String, Value)>) -> Result<DataFileEntry> {
    let mut file_path = None;
    let mut file_format = None;
    let mut record_count = None;
    let mut file_size = None;

    for (name, field_value) in fields {
        match name.as_str() {
            "file_path" => file_path = extract_string(&field_value),
            "file_format" => file_format = extract_string(&field_value),
            "record_count" => record_count = extract_long(&field_value),
            "file_size_in_bytes" => file_size = extract_long(&field_value),
            _ => {}
        }
    }

    Ok(DataFileEntry {
        file_path: file_path.ok_or_else(|| missing_field_error("file_path"))?,
        file_format: file_format.ok_or_else(|| missing_field_error("file_format"))?,
        record_count: record_count.ok_or_else(|| missing_field_error("record_count"))?,
        file_size_in_bytes: file_size.ok_or_else(|| missing_field_error("file_size_in_bytes"))?,
    })
}

/// Parse manifest entry with full stats, skipping deleted entries
pub(super) fn parse_manifest_entry_with_stats(
    fields: Vec<(String, Value)>,
) -> Result<Option<DataFileStats>> {
    let (status, data_file_value) = extract_manifest_entry_parts(fields);
    if status == Some(2) {
        return Ok(None);
    }
    if let Some(Value::Record(data_file_fields)) = data_file_value {
        Ok(Some(parse_data_file_stats(data_file_fields)?))
    } else {
        Err(missing_field_error("data_file"))
    }
}

/// Parse a manifest entry and extract data file entry if not deleted
pub(super) fn parse_manifest_entry(fields: Vec<(String, Value)>) -> Result<Option<DataFileEntry>> {
    let (status, data_file_value) = extract_manifest_entry_parts(fields);
    if status == Some(2) {
        return Ok(None);
    }

    if let Some(Value::Record(data_file_fields)) = data_file_value {
        Ok(Some(parse_data_file_basic(data_file_fields)?))
    } else {
        Err(missing_field_error("data_file"))
    }
}

/// Parse a data_file record into DataFileStats
pub(super) fn parse_data_file_stats(fields: Vec<(String, Value)>) -> Result<DataFileStats> {
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

    Ok(DataFileStats {
        file_path: file_path.ok_or_else(|| missing_field_error("file_path"))?,
        file_format: file_format.ok_or_else(|| missing_field_error("file_format"))?,
        record_count: record_count.ok_or_else(|| missing_field_error("record_count"))?,
        file_size_in_bytes: file_size.ok_or_else(|| missing_field_error("file_size_in_bytes"))?,
        partition,
        lower_bounds,
        upper_bounds,
        null_value_counts,
        value_counts,
    })
}

/// Parse the manifest list `partitions` field (array of field_summary records).
pub(super) fn parse_field_summaries(value: &Value) -> Vec<FieldSummary> {
    let inner = match value {
        Value::Union(_, boxed) => boxed.as_ref(),
        other => other,
    };
    let Value::Array(items) = inner else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            let Value::Record(fields) = item else {
                return None;
            };
            let mut summary = FieldSummary::default();
            for (name, val) in fields {
                match name.as_str() {
                    "contains_null" => {
                        if let Value::Boolean(b) = val {
                            summary.contains_null = *b;
                        }
                    }
                    "contains_nan" => {
                        if let Value::Union(_, boxed) = val {
                            if let Value::Boolean(b) = boxed.as_ref() {
                                summary.contains_nan = Some(*b);
                            }
                        }
                    }
                    "lower_bound" => {
                        if let Value::Union(_, boxed) = val {
                            if let Value::Bytes(b) = boxed.as_ref() {
                                summary.lower_bound = Some(b.clone());
                            }
                        }
                    }
                    "upper_bound" => {
                        if let Value::Union(_, boxed) = val {
                            if let Value::Bytes(b) = boxed.as_ref() {
                                summary.upper_bound = Some(b.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
            Some(summary)
        })
        .collect()
}
