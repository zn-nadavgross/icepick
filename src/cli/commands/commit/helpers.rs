//! Helper functions for the commit command

use std::collections::HashMap;
use std::path::Path;
use uuid::Uuid;

use crate::catalog::register::{convert_partition_values, PartitionValue};
use crate::io::{create_local_file_io, get_filename};
use crate::spec::{PartitionField, PartitionSpec, PrimitiveType, Schema, Type};

/// Parse a type string into a PrimitiveType
pub fn parse_type_str(type_str: &str) -> Result<PrimitiveType, String> {
    match type_str.to_lowercase().as_str() {
        "boolean" | "bool" => Ok(PrimitiveType::Boolean),
        "int" | "integer" => Ok(PrimitiveType::Int),
        "long" | "bigint" => Ok(PrimitiveType::Long),
        "float" => Ok(PrimitiveType::Float),
        "double" => Ok(PrimitiveType::Double),
        "date" => Ok(PrimitiveType::Date),
        "time" => Ok(PrimitiveType::Time),
        "timestamp" => Ok(PrimitiveType::Timestamp),
        "timestamptz" => Ok(PrimitiveType::Timestamptz),
        "string" => Ok(PrimitiveType::String),
        "uuid" => Ok(PrimitiveType::Uuid),
        "binary" => Ok(PrimitiveType::Binary),
        _ => Err(format!(
            "Unknown type '{}'. Valid types: boolean, int, long, float, double, date, time, timestamp, timestamptz, string, uuid, binary",
            type_str
        )),
    }
}

/// Parse partition spec like "year:int,month:int" into vec of (name, type)
pub fn parse_partition_spec(spec: &str) -> Result<Vec<(String, PrimitiveType)>, String> {
    spec.split(',')
        .map(|part| {
            let part = part.trim();
            let (name, type_str) = part.split_once(':').ok_or_else(|| {
                format!(
                    "Invalid partition spec '{}'. Expected format: name:type",
                    part
                )
            })?;
            let parsed_type = parse_type_str(type_str)?;
            Ok((name.to_string(), parsed_type))
        })
        .collect()
}

/// Parse partition values like "year=2024,month=01" into HashMap
pub fn parse_partition_values_arg(values: &str) -> Result<HashMap<String, String>, String> {
    values
        .split(',')
        .map(|part| {
            let part = part.trim();
            let (name, value) = part.split_once('=').ok_or_else(|| {
                format!(
                    "Invalid partition value '{}'. Expected format: name=value",
                    part
                )
            })?;
            Ok((name.to_string(), value.to_string()))
        })
        .collect()
}

/// Expand glob pattern to list of file paths
pub fn expand_glob(pattern: &str) -> Result<Vec<String>, String> {
    let paths: Result<Vec<_>, _> = glob::glob(pattern)
        .map_err(|e| format!("Invalid glob pattern '{}': {}", pattern, e))?
        .collect();

    let paths = paths.map_err(|e| format!("Error reading files matching '{}': {}", pattern, e))?;

    let parquet_files: Vec<String> = paths
        .into_iter()
        .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
        .map(|p| p.to_string_lossy().to_string())
        .collect();

    if parquet_files.is_empty() {
        return Err(format!(
            "No Parquet files found matching pattern: {}",
            pattern
        ));
    }

    Ok(parquet_files)
}

/// Build a partition spec from a spec string and schema
pub fn build_partition_spec(spec_str: &str, schema: &Schema) -> Result<PartitionSpec, String> {
    let parts = parse_partition_spec(spec_str)?;

    let fields: Vec<PartitionField> = parts
        .iter()
        .enumerate()
        .map(|(idx, (name, expected_type))| {
            let field = schema
                .fields()
                .iter()
                .find(|f| f.name() == name)
                .ok_or_else(|| format!("Partition column '{}' not found in schema", name))?;

            match field.field_type() {
                Type::Primitive(actual_type) => {
                    if actual_type != expected_type {
                        return Err(format!(
                            "Partition column '{}' type mismatch: specified {:?} but schema has {:?}",
                            name, expected_type, actual_type
                        ));
                    }
                }
                other => {
                    return Err(format!(
                        "Partition column '{}' must be a primitive type, got {:?}",
                        name, other
                    ));
                }
            }

            Ok(PartitionField::new(
                1000 + idx as i32,
                field.id(),
                "identity",
                name.clone(),
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(PartitionSpec::new(0, fields))
}

/// Check if two schemas are compatible for registration.
pub fn check_schema_compatibility(expected: &Schema, actual: &Schema) -> Result<(), String> {
    if expected.fields().len() != actual.fields().len() {
        return Err(format!(
            "field count mismatch: expected {} fields, got {}",
            expected.fields().len(),
            actual.fields().len()
        ));
    }

    for (e, a) in expected.fields().iter().zip(actual.fields().iter()) {
        if e.name() != a.name() {
            return Err(format!(
                "field name mismatch at position: expected '{}', got '{}'",
                e.name(),
                a.name()
            ));
        }
        if e.field_type() != a.field_type() {
            return Err(format!(
                "field '{}' type mismatch: expected {:?}, got {:?}",
                e.name(),
                e.field_type(),
                a.field_type()
            ));
        }
    }

    Ok(())
}

/// Determine partition values for a file
pub fn determine_partition_values(
    file_path: &str,
    explicit_values: &Option<HashMap<String, String>>,
    partition_spec: Option<&PartitionSpec>,
    schema: &Schema,
) -> Result<HashMap<String, PartitionValue>, String> {
    use crate::catalog::register::parse_hive_partition_values;

    if let Some(explicit) = explicit_values {
        return convert_partition_values(explicit, schema)
            .map_err(|e| format!("Invalid partition values: {}", e));
    }

    let hive_values = parse_hive_partition_values(file_path);

    if let Some(spec) = partition_spec {
        for field in spec.fields() {
            if !hive_values.contains_key(field.name()) {
                return Err(format!(
                    "Missing partition value for '{}' in path '{}'. Use --partition-values to specify.",
                    field.name(),
                    file_path
                ));
            }
        }
    }

    if hive_values.is_empty() {
        return Ok(HashMap::new());
    }

    convert_partition_values(&hive_values, schema)
        .map_err(|e| format!("Invalid partition values from path: {}", e))
}

/// Format partition values as a key string for grouping
pub fn format_partition_key(values: &HashMap<String, PartitionValue>) -> String {
    if values.is_empty() {
        return String::new();
    }

    let mut parts: Vec<String> = values
        .iter()
        .map(|(k, v)| format!("{}={}", k, v.to_value_string()))
        .collect();
    parts.sort();
    parts.join("/")
}

/// Generate a remote upload path for a local file
pub fn generate_upload_path(table_location: &str, local_path: &str) -> String {
    let uuid = Uuid::new_v4();
    let filename = Path::new(local_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("data");
    format!(
        "{}/data/{}_{}.parquet",
        table_location.trim_end_matches('/'),
        filename,
        uuid
    )
}

/// Upload a local file to remote storage
pub async fn upload_local_file(
    local_path: &str,
    remote_path: &str,
    remote_file_io: &crate::io::FileIO,
) -> Result<(), String> {
    let local_file_io = create_local_file_io(local_path)
        .map_err(|e| format!("Failed to create local file IO: {}", e))?;
    let filename = get_filename(local_path);
    let data = local_file_io
        .read(filename)
        .await
        .map_err(|e| format!("Failed to read local file {}: {}", local_path, e))?;
    remote_file_io
        .write(remote_path, data)
        .await
        .map_err(|e| format!("Failed to upload to {}: {}", remote_path, e))?;
    Ok(())
}

/// Introspect a Parquet file (local or remote)
pub async fn introspect_file(
    path: &str,
    file_io: &crate::io::FileIO,
) -> Result<crate::catalog::register::ParquetIntrospection, String> {
    use crate::catalog::register::{introspect_local_parquet_file, introspect_parquet_file};
    use crate::io::is_local_path;

    if is_local_path(path) {
        introspect_local_parquet_file(path, None)
            .await
            .map_err(|e| format!("Failed to read {}: {}", path, e))
    } else {
        introspect_parquet_file(file_io, path, None)
            .await
            .map_err(|e| format!("Failed to read {}: {}", path, e))
    }
}
