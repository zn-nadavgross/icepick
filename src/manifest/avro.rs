//! Convert Iceberg types to Avro values

use crate::error::Result;
use crate::spec::DataFile;
use apache_avro::types::Value;

/// Convert a DataFile to an Avro Record value for manifest entry
pub fn data_file_to_avro(data_file: &DataFile) -> Result<Value> {
    use std::collections::HashMap;

    let mut fields = vec![
        ("content".to_string(), Value::Int(0)), // 0 = DATA
        (
            "file_path".to_string(),
            Value::String(data_file.file_path().to_string()),
        ),
        (
            "file_format".to_string(),
            Value::String(data_file.file_format().to_string()),
        ),
        ("partition".to_string(), Value::Map(HashMap::new())), // Empty for unpartitioned
        (
            "record_count".to_string(),
            Value::Long(data_file.record_count()),
        ),
        (
            "file_size_in_bytes".to_string(),
            Value::Long(data_file.file_size_in_bytes()),
        ),
    ];

    // Optional column_sizes
    let column_sizes = if let Some(sizes) = data_file.column_sizes() {
        let map: HashMap<String, Value> = sizes
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Long(*v)))
            .collect();
        Value::Union(1, Box::new(Value::Map(map)))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("column_sizes".to_string(), column_sizes));

    // Optional value_counts
    let value_counts = if let Some(counts) = data_file.value_counts() {
        let map: HashMap<String, Value> = counts
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Long(*v)))
            .collect();
        Value::Union(1, Box::new(Value::Map(map)))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("value_counts".to_string(), value_counts));

    // Optional null_value_counts
    let null_value_counts = if let Some(counts) = data_file.null_value_counts() {
        let map: HashMap<String, Value> = counts
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Long(*v)))
            .collect();
        Value::Union(1, Box::new(Value::Map(map)))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("null_value_counts".to_string(), null_value_counts));

    // Optional lower_bounds
    fields.push((
        "lower_bounds".to_string(),
        Value::Union(0, Box::new(Value::Null)),
    ));

    // Optional upper_bounds
    fields.push((
        "upper_bounds".to_string(),
        Value::Union(0, Box::new(Value::Null)),
    ));

    // Optional fields set to null for MVP
    fields.push((
        "key_metadata".to_string(),
        Value::Union(0, Box::new(Value::Null)),
    ));
    fields.push((
        "split_offsets".to_string(),
        Value::Union(0, Box::new(Value::Null)),
    ));
    fields.push((
        "equality_ids".to_string(),
        Value::Union(0, Box::new(Value::Null)),
    ));
    fields.push((
        "sort_order_id".to_string(),
        Value::Union(0, Box::new(Value::Null)),
    ));

    Ok(Value::Record(fields))
}
