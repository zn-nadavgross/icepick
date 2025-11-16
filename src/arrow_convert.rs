//! Convert between Iceberg and Arrow types

use crate::error::{Error, Result};
use crate::spec::{PrimitiveType, Schema, Type};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use std::sync::Arc;

/// Convert Iceberg schema to Arrow schema
pub fn schema_to_arrow(schema: &Schema) -> Result<ArrowSchema> {
    let fields: Result<Vec<Field>> = schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = type_to_arrow(field.field_type())?;
            Ok(Field::new(field.name(), data_type, !field.is_required()))
        })
        .collect();

    Ok(ArrowSchema::new(fields?))
}

/// Convert Iceberg type to Arrow data type
fn type_to_arrow(iceberg_type: &Type) -> Result<DataType> {
    match iceberg_type {
        Type::Primitive(prim) => match prim {
            PrimitiveType::Boolean => Ok(DataType::Boolean),
            PrimitiveType::Int => Ok(DataType::Int32),
            PrimitiveType::Long => Ok(DataType::Int64),
            PrimitiveType::Float => Ok(DataType::Float32),
            PrimitiveType::Double => Ok(DataType::Float64),
            PrimitiveType::String => Ok(DataType::Utf8),
            PrimitiveType::Binary => Ok(DataType::Binary),
            PrimitiveType::Date => Ok(DataType::Date32),
            PrimitiveType::Time => Ok(DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)),
            PrimitiveType::Timestamp => Ok(DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None,
            )),
            PrimitiveType::Timestamptz => Ok(DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some(Arc::from("UTC")),
            )),
            PrimitiveType::Decimal { precision, scale } => {
                Ok(DataType::Decimal128(*precision as u8, *scale as i8))
            }
            _ => Err(Error::invalid_input(format!(
                "Unsupported primitive type: {:?}",
                prim
            ))),
        },
        Type::Struct(struct_type) => {
            let fields: Result<Vec<Field>> = struct_type
                .fields()
                .iter()
                .map(|field| {
                    let data_type = type_to_arrow(field.field_type())?;
                    Ok(Field::new(field.name(), data_type, !field.is_required()))
                })
                .collect();
            Ok(DataType::Struct(fields?.into()))
        }
        Type::List(_list_type) => {
            // Simplified list handling for MVP
            Err(Error::invalid_input("List type not yet fully supported"))
        }
        Type::Map(_) => Err(Error::invalid_input("Map type not yet supported")),
    }
}
