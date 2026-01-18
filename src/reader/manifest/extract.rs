//! Avro value extraction helpers

use crate::error::{Error, Result};
use apache_avro::types::Value;
use std::collections::HashMap;

pub(super) fn extract_int(value: &Value) -> Option<i32> {
    match value {
        Value::Int(n) => Some(*n),
        Value::Union(_, boxed) => extract_int(boxed),
        _ => None,
    }
}

pub(super) fn extract_long(value: &Value) -> Option<i64> {
    match value {
        Value::Long(n) => Some(*n),
        Value::Union(_, boxed) => extract_long(boxed),
        _ => None,
    }
}

pub(super) fn extract_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Union(_, boxed) => extract_string(boxed),
        _ => None,
    }
}

pub(super) fn extract_required_string(value: &Value, field_name: &str) -> Result<String> {
    extract_string(value).ok_or_else(|| {
        Error::invalid_input(format!(
            "{} field has wrong type or is missing: {:?}",
            field_name, value
        ))
    })
}

pub(super) fn extract_required_long(value: &Value, field_name: &str) -> Result<i64> {
    extract_long(value).ok_or_else(|| {
        Error::invalid_input(format!("{} field has wrong type: {:?}", field_name, value))
    })
}

pub(super) fn missing_field_error(field_name: &str) -> Error {
    Error::invalid_input(format!("{} field is missing or has wrong type", field_name))
}

/// Generic extraction helper for map<int, V> fields
pub(super) fn extract_map<V, F>(value: &Value, extractor: F) -> HashMap<i32, V>
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

pub(super) fn extract_bounds_map(value: &Value) -> HashMap<i32, Vec<u8>> {
    extract_map(value, |v| match v {
        Value::Bytes(bytes) => Some(bytes.clone()),
        _ => None,
    })
}

pub(super) fn extract_count_map(value: &Value) -> HashMap<i32, i64> {
    extract_map(value, extract_long)
}

pub(super) fn extract_partition_values(value: &Value) -> HashMap<i32, Vec<u8>> {
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

pub(super) fn value_to_bytes(value: &Value) -> Option<Vec<u8>> {
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
