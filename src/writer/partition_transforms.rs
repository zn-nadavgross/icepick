//! Partition transform implementations
//!
//! This module contains the individual transform functions for Iceberg partition values.

use crate::error::{Error, Result};
use crate::spec::types::Type;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;

/// Apply a partition transform to extract a string value from an array
pub(super) fn apply_transform(
    array: &ArrayRef,
    transform: &str,
    field_type: &Type,
) -> Result<String> {
    if transform == "identity" {
        extract_identity(array, field_type)
    } else if transform == "year" {
        extract_year(array)
    } else if transform == "month" {
        extract_month(array)
    } else if transform == "day" {
        extract_day(array)
    } else if transform == "hour" {
        extract_hour(array)
    } else if let Some(bucket_str) = transform.strip_prefix("bucket[") {
        let n = bucket_str
            .strip_suffix(']')
            .and_then(|s| s.parse::<i32>().ok())
            .ok_or_else(|| {
                Error::invalid_input(format!("Invalid bucket transform: {}", transform))
            })?;
        extract_bucket(array, n, field_type)
    } else if let Some(truncate_str) = transform.strip_prefix("truncate[") {
        let width = truncate_str
            .strip_suffix(']')
            .and_then(|s| s.parse::<i32>().ok())
            .ok_or_else(|| {
                Error::invalid_input(format!("Invalid truncate transform: {}", transform))
            })?;
        extract_truncate(array, width, field_type)
    } else {
        Err(Error::invalid_input(format!(
            "Unsupported partition transform: {}",
            transform
        )))
    }
}

/// Extract identity transform - raw value from first non-null row
fn extract_identity(array: &ArrayRef, field_type: &Type) -> Result<String> {
    find_first_non_null(array, |i| array_value_to_string(array, i, field_type))
}

/// Extract year from timestamp
fn extract_year(array: &ArrayRef) -> Result<String> {
    extract_temporal(array, "%Y", "Year")
}

/// Extract month from timestamp
fn extract_month(array: &ArrayRef) -> Result<String> {
    extract_temporal(array, "%Y-%m", "Month")
}

/// Extract day from timestamp
fn extract_day(array: &ArrayRef) -> Result<String> {
    extract_temporal(array, "%Y-%m-%d", "Day")
}

/// Extract hour from timestamp
fn extract_hour(array: &ArrayRef) -> Result<String> {
    extract_temporal(array, "%Y-%m-%d-%H", "Hour")
}

/// Helper function for temporal transforms
fn extract_temporal(array: &ArrayRef, format: &str, transform_name: &str) -> Result<String> {
    use arrow::array::TimestampMicrosecondArray;
    use chrono::{DateTime, Utc};

    find_first_non_null(array, |i| match array.data_type() {
        DataType::Timestamp(_, _) => {
            let ts_array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| Error::invalid_input("Expected TimestampMicrosecondArray"))?;
            let micros = ts_array.value(i);
            let dt = DateTime::<Utc>::from_timestamp(
                micros / 1_000_000,
                ((micros % 1_000_000) * 1000) as u32,
            )
            .ok_or_else(|| Error::invalid_input("Invalid timestamp value"))?;
            Ok(dt.format(format).to_string())
        }
        _ => Err(Error::invalid_input(format!(
            "{} transform requires timestamp type, got {:?}",
            transform_name,
            array.data_type()
        ))),
    })
}

/// Extract bucket hash
fn extract_bucket(array: &ArrayRef, n: i32, field_type: &Type) -> Result<String> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    find_first_non_null(array, |i| {
        let value_str = array_value_to_string(array, i, field_type)?;
        let mut hasher = DefaultHasher::new();
        value_str.hash(&mut hasher);
        let hash = hasher.finish();
        let bucket = (hash % n as u64) as i32;
        Ok(bucket.to_string())
    })
}

/// Extract truncated value
fn extract_truncate(array: &ArrayRef, width: i32, _field_type: &Type) -> Result<String> {
    use arrow::array::{Int32Array, Int64Array, StringArray};

    find_first_non_null(array, |i| match array.data_type() {
        DataType::Utf8 => {
            let str_array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::invalid_input("Expected StringArray"))?;
            let value = str_array.value(i);
            let truncated = if value.len() > width as usize {
                &value[..width as usize]
            } else {
                value
            };
            Ok(truncated.to_string())
        }
        DataType::Int32 => {
            let int_array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| Error::invalid_input("Expected Int32Array"))?;
            let value = int_array.value(i);
            let truncated = value - (value % width);
            Ok(truncated.to_string())
        }
        DataType::Int64 => {
            let int_array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| Error::invalid_input("Expected Int64Array"))?;
            let value = int_array.value(i);
            let truncated = value - (value % width as i64);
            Ok(truncated.to_string())
        }
        _ => Err(Error::invalid_input(format!(
            "Truncate transform not supported for type {:?}",
            array.data_type()
        ))),
    })
}

/// Find first non-null value in array and apply a function
fn find_first_non_null<F>(array: &ArrayRef, f: F) -> Result<String>
where
    F: Fn(usize) -> Result<String>,
{
    if array.is_empty() {
        return Err(Error::invalid_input(
            "Cannot extract partition value from empty array",
        ));
    }

    for i in 0..array.len() {
        if !array.is_null(i) {
            return f(i);
        }
    }

    Err(Error::invalid_input(
        "Cannot extract partition value from array with all null values",
    ))
}

/// Convert an array value at index to string representation
fn array_value_to_string(array: &ArrayRef, index: usize, _field_type: &Type) -> Result<String> {
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };

    match array.data_type() {
        DataType::Int32 => {
            let int_array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| Error::invalid_input("Expected Int32Array"))?;
            Ok(int_array.value(index).to_string())
        }
        DataType::Int64 => {
            let int_array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| Error::invalid_input("Expected Int64Array"))?;
            Ok(int_array.value(index).to_string())
        }
        DataType::Float32 => {
            let float_array = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| Error::invalid_input("Expected Float32Array"))?;
            Ok(float_array.value(index).to_string())
        }
        DataType::Float64 => {
            let float_array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| Error::invalid_input("Expected Float64Array"))?;
            Ok(float_array.value(index).to_string())
        }
        DataType::Utf8 => {
            let str_array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::invalid_input("Expected StringArray"))?;
            Ok(str_array.value(index).to_string())
        }
        DataType::Boolean => {
            let bool_array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| Error::invalid_input("Expected BooleanArray"))?;
            Ok(bool_array.value(index).to_string())
        }
        DataType::Timestamp(_, _) => {
            let ts_array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| Error::invalid_input("Expected TimestampMicrosecondArray"))?;
            Ok(ts_array.value(index).to_string())
        }
        _ => Err(Error::invalid_input(format!(
            "Unsupported type for partition value: {:?}",
            array.data_type()
        ))),
    }
}
