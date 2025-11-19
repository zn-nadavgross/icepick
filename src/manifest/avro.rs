//! Convert Iceberg types to Avro values

use crate::error::Result;
use crate::spec::DataFile;
use apache_avro::types::Value;
use std::collections::HashMap;

/// Convert a DataFile to an Avro Record value for manifest entry
pub fn data_file_to_avro(data_file: &DataFile) -> Result<Value> {
    // Partition is a struct/record in Iceberg spec (field 102)
    // For unpartitioned tables (partition_spec_id=0), it's an empty record
    // TODO: When partition support is added, populate fields based on partition spec
    let partition_record = Value::Record(vec![]);

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
        ("partition".to_string(), partition_record),
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
    let lower_bounds = if let Some(bounds) = data_file.lower_bounds() {
        let map: HashMap<String, Value> = bounds
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Bytes(v.clone())))
            .collect();
        Value::Union(1, Box::new(Value::Map(map)))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("lower_bounds".to_string(), lower_bounds));

    // Optional upper_bounds
    let upper_bounds = if let Some(bounds) = data_file.upper_bounds() {
        let map: HashMap<String, Value> = bounds
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Bytes(v.clone())))
            .collect();
        Value::Union(1, Box::new(Value::Map(map)))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("upper_bounds".to_string(), upper_bounds));

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
        if let Some(ids) = data_file.equality_ids() {
            let values = ids.iter().map(|id| Value::Int(*id)).collect();
            Value::Union(1, Box::new(Value::Array(values)))
        } else {
            Value::Union(0, Box::new(Value::Null))
        },
    ));
    fields.push((
        "sort_order_id".to_string(),
        Value::Union(0, Box::new(Value::Null)),
    ));

    Ok(Value::Record(fields))
}
