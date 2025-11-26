//! Partition value extraction from RecordBatch data

use super::partition_transforms;
use crate::error::{Error, Result};
use crate::spec::metadata::PartitionSpec;
use crate::spec::schema::Schema;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

/// Extract partition values from a RecordBatch using the partition spec
pub fn extract_partition_values(
    batch: &RecordBatch,
    partition_spec: &PartitionSpec,
    schema: &Schema,
) -> Result<HashMap<String, String>> {
    let mut partition_values = HashMap::new();

    for partition_field in partition_spec.fields() {
        // Find source column in batch by field ID
        let source_field = schema
            .as_struct()
            .field_by_id(partition_field.source_id())
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "Partition source field ID {} not found in schema",
                    partition_field.source_id()
                ))
            })?;

        let column_name = source_field.name();
        let array = batch.column_by_name(column_name).ok_or_else(|| {
            Error::invalid_input(format!(
                "Partition column '{}' not found in batch",
                column_name
            ))
        })?;

        // Apply transform to extract partition value
        let value = partition_transforms::apply_transform(
            array,
            partition_field.transform(),
            source_field.field_type(),
        )?;

        partition_values.insert(partition_field.name().to_string(), value);
    }

    Ok(partition_values)
}

/// Validate that all rows in the batch belong to the same partition
pub fn validate_single_partition(
    batch: &RecordBatch,
    partition_spec: &PartitionSpec,
    schema: &Schema,
) -> Result<()> {
    if batch.num_rows() <= 1 {
        return Ok(());
    }

    // Extract partition values from first and last row
    let first_values = extract_partition_values_from_row(batch, partition_spec, schema, 0)?;
    let last_values =
        extract_partition_values_from_row(batch, partition_spec, schema, batch.num_rows() - 1)?;

    if first_values != last_values {
        return Err(Error::invalid_input(
            "Batch contains multiple partition values. Please split the batch by partition before appending.",
        ));
    }

    Ok(())
}

/// Extract partition values from a specific row in the batch
fn extract_partition_values_from_row(
    batch: &RecordBatch,
    partition_spec: &PartitionSpec,
    schema: &Schema,
    row_index: usize,
) -> Result<HashMap<String, String>> {
    let mut partition_values = HashMap::new();

    for partition_field in partition_spec.fields() {
        let source_field = schema
            .as_struct()
            .field_by_id(partition_field.source_id())
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "Partition source field ID {} not found in schema",
                    partition_field.source_id()
                ))
            })?;

        let column_name = source_field.name();
        let array = batch.column_by_name(column_name).ok_or_else(|| {
            Error::invalid_input(format!(
                "Partition column '{}' not found in batch",
                column_name
            ))
        })?;

        // Create a single-row slice for this row
        let sliced = array.slice(row_index, 1);

        let value = partition_transforms::apply_transform(
            &sliced,
            partition_field.transform(),
            source_field.field_type(),
        )?;

        partition_values.insert(partition_field.name().to_string(), value);
    }

    Ok(partition_values)
}
