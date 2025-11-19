//! Convert between Iceberg and Arrow types

use crate::error::{Error, Result};
use crate::spec::types::{ListType, MapType, NestedField, StructType};
use crate::spec::{PrimitiveType, Schema, Type};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use std::convert::TryFrom;
use std::sync::Arc;

/// Metadata key used by Arrow to persist Iceberg field IDs
pub const PARQUET_FIELD_ID_METADATA_KEY: &str = "PARQUET:field_id";

/// Convert Iceberg schema to Arrow schema, embedding `PARQUET:field_id` metadata.
pub fn schema_to_arrow(schema: &Schema) -> Result<ArrowSchema> {
    let fields: Result<Vec<Field>> = schema.fields().iter().map(iceberg_field_to_arrow).collect();
    Ok(ArrowSchema::new(fields?))
}

/// Convert an Arrow schema (with `PARQUET:field_id` metadata) back into an Iceberg schema.
pub fn arrow_schema_to_iceberg(schema: &ArrowSchema) -> Result<Schema> {
    let fields: Result<Vec<NestedField>> = schema
        .fields()
        .iter()
        .map(|field| arrow_field_to_nested(field))
        .collect();

    Schema::builder().with_fields(fields?).build()
}

fn iceberg_field_to_arrow(field: &NestedField) -> Result<Field> {
    let data_type = type_to_arrow(field.field_type())?;
    let arrow_field = Field::new(field.name(), data_type, !field.is_required());
    Ok(apply_field_id_metadata(arrow_field, field.id()))
}

/// Convert Iceberg type to Arrow data type
fn type_to_arrow(iceberg_type: &Type) -> Result<DataType> {
    match iceberg_type {
        Type::Primitive(prim) => primitive_to_arrow(prim),
        Type::Struct(struct_type) => {
            let fields: Result<Vec<Field>> = struct_type
                .fields()
                .iter()
                .map(iceberg_field_to_arrow)
                .collect();
            Ok(DataType::Struct(fields?.into()))
        }
        Type::List(list_type) => Ok(DataType::List(Arc::new(build_list_field(list_type)?))),
        Type::Map(map_type) => Ok(DataType::Map(Arc::new(build_map_field(map_type)?), false)),
    }
}

fn primitive_to_arrow(prim: &PrimitiveType) -> Result<DataType> {
    match prim {
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
            let scale_i32 = i32::try_from(*scale)
                .map_err(|_| Error::invalid_input(format!("Decimal scale {} too large", scale)))?;
            let precision_u8 = u8::try_from(precision_i32).map_err(|_| {
                Error::invalid_input(format!("Decimal precision {} out of range", precision_i32))
            })?;
            let scale_i8 = i8::try_from(scale_i32).map_err(|_| {
                Error::invalid_input(format!("Decimal scale {} out of range", scale_i32))
            })?;
            Ok(DataType::Decimal128(precision_u8, scale_i8))
        }
        PrimitiveType::Uuid => Ok(DataType::FixedSizeBinary(16)),
        PrimitiveType::Fixed(length) => {
            let length_i32 = i32::try_from(*length)
                .map_err(|_| Error::invalid_input(format!("Fixed length {} too large", length)))?;
            Ok(DataType::FixedSizeBinary(length_i32))
        }
    }
}

fn build_list_field(list_type: &ListType) -> Result<Field> {
    let element_type = type_to_arrow(list_type.element_type())?;
    let element_field = Field::new(
        Field::LIST_FIELD_DEFAULT_NAME,
        element_type,
        !list_type.element_required(),
    );
    Ok(apply_field_id_metadata(
        element_field,
        list_type.element_id(),
    ))
}

fn build_map_field(map_type: &MapType) -> Result<Field> {
    let key_type = type_to_arrow(map_type.key_type())?;
    let mut key_field = Field::new("key", key_type, false);
    key_field = apply_field_id_metadata(key_field, map_type.key_id());

    let value_type = type_to_arrow(map_type.value_type())?;
    let mut value_field = Field::new("value", value_type, !map_type.value_required());
    value_field = apply_field_id_metadata(value_field, map_type.value_id());

    let struct_fields = vec![key_field, value_field];
    Ok(Field::new(
        "entries",
        DataType::Struct(struct_fields.into()),
        false,
    ))
}

fn apply_field_id_metadata(mut field: Field, field_id: i32) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(
        PARQUET_FIELD_ID_METADATA_KEY.to_string(),
        field_id.to_string(),
    );
    field.set_metadata(metadata);
    field
}

fn arrow_field_to_nested(field: &Field) -> Result<NestedField> {
    let field_id = parse_parquet_field_id(field)?;
    let field_type = arrow_type_to_iceberg(field.data_type())?;
    Ok(NestedField::new(
        field_id,
        field.name().clone(),
        field_type,
        !field.is_nullable(),
        None,
    ))
}

fn arrow_type_to_iceberg(data_type: &DataType) -> Result<Type> {
    match data_type {
        DataType::Struct(fields) => {
            let nested_fields: Result<Vec<NestedField>> = fields
                .iter()
                .map(|field| arrow_field_to_nested(field))
                .collect();
            Ok(Type::Struct(StructType::new(nested_fields?)))
        }
        DataType::List(field) | DataType::LargeList(field) => {
            let nested_field = arrow_field_to_nested(field)?;
            Ok(Type::List(ListType::new(
                nested_field.id(),
                nested_field.is_required(),
                nested_field.field_type().clone(),
            )))
        }
        DataType::Map(entries_field, _) => build_map_type(entries_field),
        _ => Ok(Type::Primitive(arrow_primitive_to_iceberg(data_type)?)),
    }
}

fn build_map_type(entries_field: &Field) -> Result<Type> {
    let entries_type = entries_field.data_type();
    let struct_fields = match entries_type {
        DataType::Struct(fields) => fields,
        other => {
            return Err(Error::invalid_input(format!(
                "Arrow Map entries must be struct, got {other:?}"
            )))
        }
    };

    if struct_fields.len() != 2 {
        return Err(Error::invalid_input(format!(
            "Arrow Map entries must have key and value fields, found {}",
            struct_fields.len()
        )));
    }

    let key_field = &struct_fields[0];
    if key_field.is_nullable() {
        return Err(Error::invalid_input(
            "Arrow Map key field cannot be nullable",
        ));
    }
    let value_field = &struct_fields[1];

    let key_type = arrow_primitive_to_iceberg(key_field.data_type())?;
    let value_type = arrow_type_to_iceberg(value_field.data_type())?;

    let key_id = parse_parquet_field_id(key_field)?;
    let value_id = parse_parquet_field_id(value_field)?;

    Ok(Type::Map(MapType::new(
        key_id,
        Type::Primitive(key_type),
        value_id,
        !value_field.is_nullable(),
        value_type,
    )))
}

fn arrow_primitive_to_iceberg(data_type: &DataType) -> Result<PrimitiveType> {
    match data_type {
        DataType::Boolean => Ok(PrimitiveType::Boolean),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => Ok(PrimitiveType::Int),
        DataType::Int64 | DataType::UInt64 => Ok(PrimitiveType::Long),
        DataType::Float16 => Err(Error::invalid_input(
            "Float16 is not supported by Iceberg schemas",
        )),
        DataType::Float32 => Ok(PrimitiveType::Float),
        DataType::Float64 => Ok(PrimitiveType::Double),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(PrimitiveType::String),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            Ok(PrimitiveType::Binary)
        }
        DataType::FixedSizeBinary(len) => Ok(PrimitiveType::Fixed(*len as u64)),
        DataType::Decimal128(precision, scale) => Ok(PrimitiveType::Decimal {
            precision: u32::from(*precision),
            scale: decimal_scale_to_u32(*scale)?,
        }),
        DataType::Decimal256(precision, scale) => Ok(PrimitiveType::Decimal {
            precision: u32::from(*precision),
            scale: decimal_scale_to_u32(*scale)?,
        }),
        DataType::Date32 | DataType::Date64 => Ok(PrimitiveType::Date),
        DataType::Time32(unit) => match unit {
            arrow::datatypes::TimeUnit::Millisecond => Ok(PrimitiveType::Time),
            other => Err(Error::invalid_input(format!(
                "Time32 with unit {other:?} not supported for Iceberg"
            ))),
        },
        DataType::Time64(unit) => match unit {
            arrow::datatypes::TimeUnit::Microsecond => Ok(PrimitiveType::Time),
            other => Err(Error::invalid_input(format!(
                "Time64 with unit {other:?} not supported for Iceberg"
            ))),
        },
        DataType::Timestamp(unit, tz) => {
            if *unit != arrow::datatypes::TimeUnit::Microsecond {
                return Err(Error::invalid_input(format!(
                    "Timestamp unit {:?} not supported, expected microsecond",
                    unit
                )));
            }
            if tz.is_some() {
                Ok(PrimitiveType::Timestamptz)
            } else {
                Ok(PrimitiveType::Timestamp)
            }
        }
        DataType::Duration(_) => Err(Error::invalid_input(
            "Duration types are not supported in Iceberg schemas",
        )),
        DataType::Interval(_) => Err(Error::invalid_input(
            "Interval types are not supported in Iceberg schemas",
        )),
        other => Err(Error::invalid_input(format!(
            "Unsupported Arrow data type for Iceberg conversion: {other:?}"
        ))),
    }
}

fn decimal_scale_to_u32(scale: i8) -> Result<u32> {
    if scale < 0 {
        return Err(Error::invalid_input(format!(
            "Negative decimal scale {scale} is not supported in Iceberg"
        )));
    }
    Ok(scale as u32)
}

pub(crate) fn parse_parquet_field_id(field: &Field) -> Result<i32> {
    let metadata = field.metadata();
    let field_id_str = metadata.get(PARQUET_FIELD_ID_METADATA_KEY).ok_or_else(|| {
        Error::invalid_input(format!(
            "Arrow field '{}' is missing {PARQUET_FIELD_ID_METADATA_KEY} metadata",
            field.name()
        ))
    })?;

    field_id_str.parse::<i32>().map_err(|err| {
        Error::invalid_input(format!(
            "Invalid field ID '{}' for field '{}': {}",
            field_id_str,
            field.name(),
            err
        ))
    })
}
