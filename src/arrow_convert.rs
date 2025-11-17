//! Convert between Iceberg and Arrow types

use crate::error::{Error, Result};
use crate::spec::{PrimitiveType, Schema, Type};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use std::convert::TryFrom;
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
                let precision_i32 = i32::try_from(*precision).map_err(|_| {
                    Error::invalid_input(format!("Decimal precision {} too large", precision))
                })?;
                let scale_i32 = i32::try_from(*scale).map_err(|_| {
                    Error::invalid_input(format!("Decimal scale {} too large", scale))
                })?;
                let precision_u8 = u8::try_from(precision_i32).map_err(|_| {
                    Error::invalid_input(format!(
                        "Decimal precision {} out of range",
                        precision_i32
                    ))
                })?;
                let scale_i8 = i8::try_from(scale_i32).map_err(|_| {
                    Error::invalid_input(format!("Decimal scale {} out of range", scale_i32))
                })?;
                Ok(DataType::Decimal128(precision_u8, scale_i8))
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
        Type::List(list_type) => {
            let element_type = type_to_arrow(list_type.element_type())?;
            let element_field = Field::new("element", element_type, !list_type.element_required());
            Ok(DataType::List(Arc::new(element_field)))
        }
        Type::Map(map_type) => {
            let key_type = type_to_arrow(map_type.key_type())?;
            let value_type = type_to_arrow(map_type.value_type())?;
            let struct_fields = vec![
                Field::new("key", key_type, false),
                Field::new("value", value_type, !map_type.value_required()),
            ];
            let entries_field =
                Field::new("entries", DataType::Struct(struct_fields.into()), false);
            Ok(DataType::Map(Arc::new(entries_field), false))
        }
    }
}
