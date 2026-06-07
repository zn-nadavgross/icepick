//! Convert Iceberg types to Avro values

use crate::error::{Error, Result};
use crate::spec::{DataFile, PartitionField, PartitionSpec, PrimitiveType, Schema, Type};
use apache_avro::types::Value;

/// Convert a DataFile to an Avro Record value for manifest entry.
///
/// `partition_spec` must be the spec the manifest is written against; its
/// fields determine both the shape of the embedded partition record (field id
/// 102) and how each string partition value on `data_file` is encoded.
pub fn data_file_to_avro(
    data_file: &DataFile,
    partition_spec: &PartitionSpec,
    schema: &Schema,
) -> Result<Value> {
    let partition_record = build_partition_record(data_file, partition_spec, schema)?;

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

    // Optional column_sizes (array of key-value records)
    let column_sizes = if let Some(sizes) = data_file.column_sizes() {
        let array: Vec<Value> = sizes
            .iter()
            .map(|(k, v)| {
                Value::Record(vec![
                    ("key".to_string(), Value::Int(*k)),
                    ("value".to_string(), Value::Long(*v)),
                ])
            })
            .collect();
        Value::Union(1, Box::new(Value::Array(array)))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("column_sizes".to_string(), column_sizes));

    // Optional value_counts (array of key-value records)
    let value_counts = if let Some(counts) = data_file.value_counts() {
        let array: Vec<Value> = counts
            .iter()
            .map(|(k, v)| {
                Value::Record(vec![
                    ("key".to_string(), Value::Int(*k)),
                    ("value".to_string(), Value::Long(*v)),
                ])
            })
            .collect();
        Value::Union(1, Box::new(Value::Array(array)))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("value_counts".to_string(), value_counts));

    // Optional null_value_counts (array of key-value records)
    let null_value_counts = if let Some(counts) = data_file.null_value_counts() {
        let array: Vec<Value> = counts
            .iter()
            .map(|(k, v)| {
                Value::Record(vec![
                    ("key".to_string(), Value::Int(*k)),
                    ("value".to_string(), Value::Long(*v)),
                ])
            })
            .collect();
        Value::Union(1, Box::new(Value::Array(array)))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("null_value_counts".to_string(), null_value_counts));

    // Optional split offsets
    let split_offsets = if let Some(offsets) = data_file.split_offsets() {
        let array: Vec<Value> = offsets.iter().map(|offset| Value::Long(*offset)).collect();
        Value::Union(1, Box::new(Value::Array(array)))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("split_offsets".to_string(), split_offsets));

    // Optional key metadata (encryption)
    let key_metadata = if let Some(bytes) = data_file.key_metadata() {
        Value::Union(1, Box::new(Value::Bytes(bytes.to_vec())))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("key_metadata".to_string(), key_metadata));

    // Optional lower_bounds (array of key-value records)
    let lower_bounds = if let Some(bounds) = data_file.lower_bounds() {
        let array: Vec<Value> = bounds
            .iter()
            .map(|(k, v)| {
                Value::Record(vec![
                    ("key".to_string(), Value::Int(*k)),
                    ("value".to_string(), Value::Bytes(v.clone())),
                ])
            })
            .collect();
        Value::Union(1, Box::new(Value::Array(array)))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("lower_bounds".to_string(), lower_bounds));

    // Optional upper_bounds (array of key-value records)
    let upper_bounds = if let Some(bounds) = data_file.upper_bounds() {
        let array: Vec<Value> = bounds
            .iter()
            .map(|(k, v)| {
                Value::Record(vec![
                    ("key".to_string(), Value::Int(*k)),
                    ("value".to_string(), Value::Bytes(v.clone())),
                ])
            })
            .collect();
        Value::Union(1, Box::new(Value::Array(array)))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("upper_bounds".to_string(), upper_bounds));

    // Optional fields set to null for MVP
    fields.push((
        "equality_ids".to_string(),
        if let Some(ids) = data_file.equality_ids() {
            let values = ids.iter().map(|id| Value::Int(*id)).collect();
            Value::Union(1, Box::new(Value::Array(values)))
        } else {
            Value::Union(0, Box::new(Value::Null))
        },
    ));
    // Iceberg v2 manifest readers (Trino's $entries) treat sort_order_id as a
    // non-null int when present; emit 0 (unsorted) rather than null.
    fields.push((
        "sort_order_id".to_string(),
        Value::Union(1, Box::new(Value::Int(0))),
    ));

    Ok(Value::Record(fields))
}

/// Build the Avro record for `data_file.partition` (field id 102). Fields are
/// named by the partition field id so the reader can map them back to spec
/// fields without consulting the spec; their types come from each partition
/// field's transform applied to the source schema type.
fn build_partition_record(
    data_file: &DataFile,
    partition_spec: &PartitionSpec,
    schema: &Schema,
) -> Result<Value> {
    let mut fields: Vec<(String, Value)> = Vec::with_capacity(partition_spec.fields().len());
    for field in partition_spec.fields() {
        let result_type = partition_field_result_type(field, schema)?;
        let value = match data_file.partition().get(field.name()) {
            Some(raw) => {
                let typed = partition_string_to_avro(raw, &result_type)?;
                Value::Union(1, Box::new(typed))
            }
            None => Value::Union(0, Box::new(Value::Null)),
        };
        // Avro record field names cannot start with a digit, so use the
        // partition field's spec name. The Iceberg field id is carried as Avro
        // schema metadata, not as the record's field name.
        fields.push((field.name().to_string(), value));
    }
    Ok(Value::Record(fields))
}

/// Iceberg result type of a partition field — the transform applied to the
/// source field's primitive type.
pub(crate) fn partition_field_result_type(
    field: &PartitionField,
    schema: &Schema,
) -> Result<PrimitiveType> {
    let transform = field.transform().to_ascii_lowercase();
    match transform.as_str() {
        "year" | "month" | "hour" => Ok(PrimitiveType::Int),
        "day" => Ok(PrimitiveType::Date),
        "identity" | "void" => source_primitive_type(schema, field.source_id()),
        t if t.starts_with("bucket[") => Ok(PrimitiveType::Int),
        t if t.starts_with("truncate[") => source_primitive_type(schema, field.source_id()),
        other => Err(Error::invalid_input(format!(
            "Unsupported partition transform '{}'",
            other
        ))),
    }
}

fn source_primitive_type(schema: &Schema, source_id: i32) -> Result<PrimitiveType> {
    let f = schema
        .fields()
        .iter()
        .find(|f| f.id() == source_id)
        .ok_or_else(|| {
            Error::invalid_input(format!("source field id {} not found in schema", source_id))
        })?;
    match f.field_type() {
        Type::Primitive(p) => Ok(p.clone()),
        _ => Err(Error::invalid_input(format!(
            "source field id {} is not a primitive type",
            source_id
        ))),
    }
}

/// Parse a Hive-style partition value string into a typed Avro `Value`
/// matching the partition field's result primitive type.
pub(crate) fn partition_string_to_avro(raw: &str, result_type: &PrimitiveType) -> Result<Value> {
    Ok(match result_type {
        PrimitiveType::Boolean => Value::Boolean(
            raw.parse::<bool>()
                .map_err(|e| Error::invalid_input(format!("bool '{}': {}", raw, e)))?,
        ),
        PrimitiveType::Int => Value::Int(
            raw.parse::<i32>()
                .map_err(|e| Error::invalid_input(format!("int '{}': {}", raw, e)))?,
        ),
        PrimitiveType::Long => Value::Long(
            raw.parse::<i64>()
                .map_err(|e| Error::invalid_input(format!("long '{}': {}", raw, e)))?,
        ),
        PrimitiveType::Float => Value::Float(
            raw.parse::<f32>()
                .map_err(|e| Error::invalid_input(format!("float '{}': {}", raw, e)))?,
        ),
        PrimitiveType::Double => Value::Double(
            raw.parse::<f64>()
                .map_err(|e| Error::invalid_input(format!("double '{}': {}", raw, e)))?,
        ),
        PrimitiveType::Date => Value::Int(parse_date_days(raw)?),
        PrimitiveType::String | PrimitiveType::Uuid => Value::String(raw.to_string()),
        other => {
            return Err(Error::invalid_input(format!(
                "Hive partition encoding not implemented for {:?}",
                other
            )));
        }
    })
}

fn parse_date_days(raw: &str) -> Result<i32> {
    // Hive partition paths use ISO YYYY-MM-DD; storage is days from 1970-01-01.
    // Fall back to a raw integer if the writer chose to skip the formatting step.
    if let Ok(d) = chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
            .ok_or_else(|| Error::invalid_input("invalid epoch date"))?;
        return Ok(d.signed_duration_since(epoch).num_days() as i32);
    }
    raw.parse::<i32>()
        .map_err(|e| Error::invalid_input(format!("date '{}': {}", raw, e)))
}

/// Avro primitive type name (for embedding into the partition record schema)
/// for a given Iceberg partition result type.
pub(crate) fn partition_result_avro_name(result_type: &PrimitiveType) -> Result<&'static str> {
    Ok(match result_type {
        PrimitiveType::Boolean => "boolean",
        PrimitiveType::Int | PrimitiveType::Date => "int",
        PrimitiveType::Long
        | PrimitiveType::Time
        | PrimitiveType::Timestamp
        | PrimitiveType::Timestamptz => "long",
        PrimitiveType::Float => "float",
        PrimitiveType::Double => "double",
        PrimitiveType::String | PrimitiveType::Uuid => "string",
        PrimitiveType::Binary | PrimitiveType::Fixed(_) => "bytes",
        other => {
            return Err(Error::invalid_input(format!(
                "Avro mapping not implemented for partition result type {:?}",
                other
            )));
        }
    })
}
