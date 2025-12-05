use std::collections::HashMap;

use crate::error::Result;
use crate::spec::types::NestedField;
use crate::spec::{PartitionSpec, Schema};
use crate::table::Table;
use crate::writer::SchemaEvolutionPolicy;

use super::types::DataFileInput;
use super::PartitionValue;

pub fn validate_partitions(
    provided: &HashMap<String, PartitionValue>,
    partition_spec: Option<&PartitionSpec>,
    schema: &Schema,
) -> Result<()> {
    let Some(spec) = partition_spec else {
        return Ok(());
    };

    if spec.fields().is_empty() {
        return Ok(());
    }

    for field in spec.fields() {
        if !provided.contains_key(field.name()) {
            return Err(crate::error::Error::partition_validation(format!(
                "Missing partition value for '{}'",
                field.name()
            )));
        }

        let value = provided.get(field.name()).unwrap();
        let source_field = schema
            .fields()
            .iter()
            .find(|f| f.id() == field.source_id())
            .ok_or_else(|| {
                crate::error::Error::partition_validation(format!(
                    "Partition field '{}' references unknown source id {}",
                    field.name(),
                    field.source_id()
                ))
            })?;

        validate_partition_value_type(value, source_field)?;
    }

    Ok(())
}

fn validate_partition_value_type(value: &PartitionValue, source_field: &NestedField) -> Result<()> {
    use crate::spec::PrimitiveType;

    let field_type = source_field.field_type();
    match (field_type, value) {
        (crate::spec::Type::Primitive(PrimitiveType::Boolean), PartitionValue::Bool(_)) => Ok(()),
        (crate::spec::Type::Primitive(PrimitiveType::Int), PartitionValue::Int(_)) => Ok(()),
        (
            crate::spec::Type::Primitive(PrimitiveType::Long),
            PartitionValue::Long(_) | PartitionValue::Int(_),
        ) => Ok(()),
        (crate::spec::Type::Primitive(PrimitiveType::String), PartitionValue::String(_)) => Ok(()),
        // Allow string values for any type to avoid false negatives with transforms; caller is responsible for correctness.
        (_, PartitionValue::String(_)) => Ok(()),
        _ => Err(crate::error::Error::partition_validation(format!(
            "Partition value for '{}' does not match source field type {:?}",
            source_field.name(),
            field_type
        ))),
    }
}

pub fn validate_schema(
    table: &Table,
    options: &super::RegisterOptions,
    inputs: &[DataFileInput],
) -> Result<()> {
    let table_schema = table.schema()?;
    if options.schema_evolution == SchemaEvolutionPolicy::AddFields {
        return Ok(());
    }

    for input in inputs {
        if let Some(ref schema) = input.source_schema {
            if schema != table_schema {
                return Err(crate::error::Error::schema_mismatch(
                    "Input file schema does not match table schema",
                ));
            }
        }
    }
    Ok(())
}
