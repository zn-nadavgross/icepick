use icepick::arrow_convert::schema_to_arrow;
use icepick::spec::types::{ListType, MapType, StructType};
use icepick::spec::{NestedField, PrimitiveType, Schema, Type};

use arrow::datatypes::DataType;

#[test]
fn test_schema_to_arrow_simple() {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required_field(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
            NestedField::optional_field(
                2,
                "name".to_string(),
                Type::Primitive(PrimitiveType::String),
            ),
        ])
        .build()
        .unwrap();

    let arrow_schema = schema_to_arrow(&schema).unwrap();

    assert_eq!(arrow_schema.fields().len(), 2);
    assert_eq!(arrow_schema.field(0).name(), "id");
    assert_eq!(arrow_schema.field(1).name(), "name");
    assert!(!arrow_schema.field(0).is_nullable());
    assert!(arrow_schema.field(1).is_nullable());
}

#[test]
fn test_schema_to_arrow_nested_types() {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional_field(
                1,
                "items".to_string(),
                Type::List(ListType::new(4, false, Type::Primitive(PrimitiveType::Int))),
            ),
            NestedField::required_field(
                2,
                "attrs".to_string(),
                Type::Map(MapType::new(
                    5,
                    Type::Primitive(PrimitiveType::String),
                    6,
                    false,
                    Type::Primitive(PrimitiveType::String),
                )),
            ),
            NestedField::required_field(
                3,
                "price".to_string(),
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 38,
                    scale: 4,
                }),
            ),
            NestedField::required_field(
                4,
                "inner".to_string(),
                Type::Struct(StructType::new(vec![NestedField::required_field(
                    5,
                    "value".to_string(),
                    Type::Primitive(PrimitiveType::Long),
                )])),
            ),
        ])
        .build()
        .unwrap();

    let arrow_schema = schema_to_arrow(&schema).unwrap();
    assert!(matches!(
        arrow_schema.field(0).data_type(),
        DataType::List(_)
    ));
    assert!(matches!(
        arrow_schema.field(1).data_type(),
        DataType::Map(_, _)
    ));
    assert_eq!(
        arrow_schema.field(2).data_type(),
        &DataType::Decimal128(38, 4)
    );
    assert!(matches!(
        arrow_schema.field(3).data_type(),
        DataType::Struct(_)
    ));
}
