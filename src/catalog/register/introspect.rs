use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema as ArrowSchema};
use bytes::{Buf, Bytes};
use parquet::arrow::parquet_to_arrow_schema;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{ChunkReader, Length};
use parquet::schema::types::Type;

use crate::arrow_convert::arrow_schema_to_iceberg;
use crate::error::{Error, Result};
use crate::io::FileIO;
use crate::spec::{DataContentType, PartitionField, PartitionSpec, Schema};

use super::types::{DataFileFormat, DataFileInput, FileMetrics, PartitionValue};

/// Inspect a Parquet file and produce a `DataFileInput` plus schema.
///
/// Uses footer-only reads to avoid buffering the entire file.
/// Hive-style partitions are inferred strictly: missing or malformed `key=value`
/// segments for a provided `PartitionSpec` return a partition validation error.
pub async fn introspect_parquet_file(
    file_io: &FileIO,
    path: &str,
    partition_spec: Option<&PartitionSpec>,
) -> Result<ParquetIntrospection> {
    let file_size = file_io.file_size(path).await?;
    if file_size < 8 {
        return Err(Error::invalid_input(format!(
            "Parquet file {} too small to contain footer",
            path
        )));
    }

    let tail = file_io
        .read_range(path, file_size - 8, 8)
        .await
        .map(Bytes::from)?;

    if &tail[4..] != b"PAR1" {
        return Err(Error::invalid_input(format!(
            "Parquet file {} missing magic trailer",
            path
        )));
    }

    let metadata_len = i32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]) as u64;
    let footer_len = metadata_len
        .checked_add(8)
        .ok_or_else(|| Error::invalid_input("Parquet metadata length overflow"))?;
    if footer_len > file_size {
        return Err(Error::invalid_input(format!(
            "Invalid Parquet metadata length {} for file of size {}",
            metadata_len, file_size
        )));
    }

    let footer_start = file_size - footer_len;
    let mut footer_bytes = file_io.read_range(path, footer_start, metadata_len).await?;
    footer_bytes.extend_from_slice(&tail);
    let footer_bytes = Bytes::from(footer_bytes);

    let reader = SuffixChunkReader::new(footer_bytes, file_size, footer_start);

    let mut metadata_reader = parquet::file::metadata::ParquetMetaDataReader::new();
    metadata_reader
        .try_parse_sized(&reader, file_size)
        .map_err(|e| Error::invalid_input(format!("Failed to parse Parquet metadata: {e}")))?;
    let metadata = metadata_reader
        .finish()
        .map_err(|e| Error::invalid_input(format!("Failed to finish Parquet metadata: {e}")))?;

    let schema_descr = metadata.file_metadata().schema_descr();
    let record_count = metadata.file_metadata().num_rows();
    let arrow_schema = parquet_to_arrow_schema(schema_descr, None)
        .map_err(|e| Error::invalid_input(format!("Failed to convert Parquet schema: {e}")))?;
    let parquet_fields = schema_descr.root_schema().get_fields();
    let arrow_fields: Vec<Field> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            if let Some(p_field) = parquet_fields.get(idx) {
                apply_field_ids(p_field.as_ref(), field)
            } else {
                field.as_ref().clone()
            }
        })
        .collect();
    let arrow_schema = ArrowSchema::new(arrow_fields);
    let schema = arrow_schema_to_iceberg(&arrow_schema)?;

    let metrics = build_metrics(&metadata);
    let split_offsets = collect_split_offsets(&metadata);

    let partition_values = if let Some(spec) = partition_spec {
        infer_partition_values_from_path(spec, &schema, path)?
    } else {
        HashMap::new()
    };

    let partition_values_for_data_file = partition_values.clone();

    let data_file = DataFileInput {
        file_path: path.to_string(),
        file_format: DataFileFormat::Parquet,
        file_size_in_bytes: file_size as i64,
        record_count,
        partition_values: partition_values_for_data_file,
        metrics: Some(metrics),
        content_type: DataContentType::Data,
        split_offsets: Some(split_offsets),
        encryption: None,
        source_schema: Some(schema.clone()),
    };

    Ok(ParquetIntrospection {
        data_file,
        schema,
        partition_values: Some(partition_values),
    })
}

/// Parquet metadata used to seed DataFileInput.
pub struct ParquetIntrospection {
    pub data_file: DataFileInput,
    pub schema: Schema,
    pub partition_values: Option<HashMap<String, PartitionValue>>,
}

/// Infer partition values from a path like `col1=value1/col2=5/part-000.parquet`.
///
/// This is intentionally strict when a partition spec is provided:
/// - Missing required `key=value` segments trigger a `partition_validation` error
/// - Malformed or type-mismatched values also error
///
/// This keeps register/introspection flows from silently dropping partition pruning.
pub fn infer_partition_values_from_path(
    partition_spec: &PartitionSpec,
    schema: &Schema,
    path: &str,
) -> Result<HashMap<String, PartitionValue>> {
    let mut values = HashMap::new();
    let hive_segments = parse_hive_partition_values(path);

    for field in partition_spec.fields() {
        let expected_name = field.name();
        let raw_value = hive_segments.get(expected_name).ok_or_else(|| {
            Error::partition_validation(format!(
                "Missing partition segment '{}' in path '{}'",
                expected_name, path
            ))
        })?;

        let value = parse_partition_value(schema, field, raw_value).map_err(|err| {
            Error::partition_validation(format!(
                "Failed to parse partition value for '{}': {} (path: {})",
                expected_name, err, path
            ))
        })?;

        values.insert(expected_name.to_string(), value);
    }

    Ok(values)
}

/// Extract Hive-style `key=value` segments from a path.
fn parse_hive_partition_values(path: &str) -> HashMap<String, String> {
    path.rsplit_once('/')
        .map(|(dirs, file)| (dirs, Some(file)))
        .unwrap_or((path, None))
        .0
        .split('/')
        .filter_map(|segment| {
            let mut parts = segment.splitn(2, '=');
            match (parts.next(), parts.next()) {
                (Some(key), Some(value)) if !key.is_empty() => {
                    Some((key.to_string(), value.to_string()))
                }
                _ => None,
            }
        })
        .collect()
}

fn parse_partition_value(
    schema: &Schema,
    field: &PartitionField,
    raw: &str,
) -> std::result::Result<PartitionValue, String> {
    let transform = field.transform().to_ascii_lowercase();
    match transform.as_str() {
        "year" | "month" | "day" | "hour" => {
            return raw
                .parse::<i32>()
                .map(PartitionValue::Int)
                .map_err(|err| format!("expected integer for {} transform: {err}", transform));
        }
        "identity" => { /* fall through to source type parsing */ }
        _ => {
            if transform.starts_with("bucket[") || transform.starts_with("truncate[") {
                if let Ok(value) = raw.parse::<i32>() {
                    return Ok(PartitionValue::Int(value));
                }
            }
        }
    }

    parse_partition_value_by_source_type(schema, field.source_id(), raw)
}

fn parse_partition_value_by_source_type(
    schema: &Schema,
    source_id: i32,
    raw: &str,
) -> std::result::Result<PartitionValue, String> {
    use crate::spec::PrimitiveType;

    let field = schema
        .fields()
        .iter()
        .find(|f| f.id() == source_id)
        .ok_or_else(|| format!("source field id {} not found in schema", source_id))?;

    match field.field_type() {
        crate::spec::Type::Primitive(PrimitiveType::Boolean) => raw
            .parse::<bool>()
            .map(PartitionValue::Bool)
            .map_err(|err| format!("expected boolean: {err}")),
        crate::spec::Type::Primitive(PrimitiveType::Int)
        | crate::spec::Type::Primitive(PrimitiveType::Date) => raw
            .parse::<i32>()
            .map(PartitionValue::Int)
            .map_err(|err| format!("expected int: {err}")),
        crate::spec::Type::Primitive(PrimitiveType::Long)
        | crate::spec::Type::Primitive(PrimitiveType::Time)
        | crate::spec::Type::Primitive(PrimitiveType::Timestamp)
        | crate::spec::Type::Primitive(PrimitiveType::Timestamptz) => raw
            .parse::<i64>()
            .map(PartitionValue::Long)
            .map_err(|err| format!("expected long: {err}")),
        crate::spec::Type::Primitive(PrimitiveType::String)
        | crate::spec::Type::Primitive(PrimitiveType::Uuid) => {
            Ok(PartitionValue::String(raw.to_string()))
        }
        _ => Ok(PartitionValue::String(raw.to_string())),
    }
}

fn apply_field_ids(parquet_type: &Type, arrow_field: &Field) -> Field {
    let mut metadata = arrow_field.metadata().clone();
    if parquet_type.get_basic_info().has_id() {
        metadata.insert(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            parquet_type.get_basic_info().id().to_string(),
        );
    }

    let data_type = arrow_field.data_type().clone();
    let updated_data_type = match (parquet_type, data_type.clone()) {
        (
            Type::GroupType {
                fields: parquet_children,
                ..
            },
            arrow::datatypes::DataType::Struct(children),
        ) => {
            let updated_children: Vec<Arc<Field>> = children
                .iter()
                .enumerate()
                .map(|(idx, child)| {
                    let updated = parquet_children
                        .get(idx)
                        .map(|p_child| apply_field_ids(p_child.as_ref(), child))
                        .unwrap_or_else(|| child.as_ref().clone());
                    Arc::new(updated)
                })
                .collect();
            arrow::datatypes::DataType::Struct(updated_children.into())
        }
        (
            Type::GroupType {
                fields: parquet_children,
                ..
            },
            arrow::datatypes::DataType::List(inner),
        ) => {
            if let Some(parquet_child) = parquet_children.first() {
                let updated_child = apply_field_ids(parquet_child.as_ref(), inner.as_ref());
                arrow::datatypes::DataType::List(Arc::new(updated_child))
            } else {
                arrow::datatypes::DataType::List(inner)
            }
        }
        _ => data_type,
    };
    Field::new(
        arrow_field.name(),
        updated_data_type,
        arrow_field.is_nullable(),
    )
    .with_metadata(metadata)
}

struct SuffixChunkReader {
    data: Bytes,
    file_size: u64,
    start: u64,
}

impl SuffixChunkReader {
    fn new(data: Bytes, file_size: u64, start: u64) -> Self {
        Self {
            data,
            file_size,
            start,
        }
    }
}

impl Length for SuffixChunkReader {
    fn len(&self) -> u64 {
        self.file_size
    }
}

impl ChunkReader for SuffixChunkReader {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start < self.start || start > self.file_size {
            return Err(ParquetError::General(format!(
                "start {} outside available range {}..{}",
                start, self.start, self.file_size
            )));
        }
        let relative = (start - self.start) as usize;
        Ok(self.data.slice(relative..).reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        if start < self.start || start > self.file_size {
            return Err(ParquetError::General(format!(
                "start {} outside available range {}..{}",
                start, self.start, self.file_size
            )));
        }
        let relative = (start - self.start) as usize;
        let end = relative
            .checked_add(length)
            .ok_or_else(|| ParquetError::General("range overflow".to_string()))?;
        if end > self.data.len() {
            return Err(ParquetError::General(format!(
                "requested {} bytes at {} but only have {} available",
                length,
                start,
                self.data.len()
            )));
        }
        Ok(self.data.slice(relative..end))
    }
}

fn build_metrics(metadata: &ParquetMetaData) -> FileMetrics {
    let mut metrics = FileMetrics::default();

    for row_group in metadata.row_groups() {
        for column in row_group.columns() {
            let self_type = column.column_descr().self_type();
            if !self_type.get_basic_info().has_id() {
                continue;
            }
            let field_id = self_type.get_basic_info().id();

            metrics
                .column_sizes
                .entry(field_id)
                .and_modify(|v| *v += column.uncompressed_size())
                .or_insert(column.uncompressed_size());

            if let Some(stats) = column.statistics() {
                if let Some(nulls) = stats.null_count_opt() {
                    metrics
                        .null_value_counts
                        .entry(field_id)
                        .and_modify(|v| *v += nulls as i64)
                        .or_insert(nulls as i64);
                }

                let non_null = column.num_values()
                    - stats.null_count_opt().map(|n| n as i64).unwrap_or_default();
                metrics
                    .value_counts
                    .entry(field_id)
                    .and_modify(|v| *v += non_null)
                    .or_insert(non_null);

                if let Some(min) = stats.min_bytes_opt() {
                    metrics
                        .lower_bounds
                        .entry(field_id)
                        .or_insert_with(|| min.to_vec());
                }

                if let Some(max) = stats.max_bytes_opt() {
                    metrics
                        .upper_bounds
                        .entry(field_id)
                        .or_insert_with(|| max.to_vec());
                }
            }
        }
    }

    metrics
}

fn collect_split_offsets(metadata: &ParquetMetaData) -> Vec<i64> {
    metadata
        .row_groups()
        .iter()
        .filter_map(|rg| rg.file_offset())
        .collect()
}

#[cfg(test)]
mod tests;
