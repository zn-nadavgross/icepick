use icepick::spec::{NestedField, PrimitiveType, Schema, Type};

#[test]
fn test_schema_simple() {
    let fields = vec![
        NestedField::required_field(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
        NestedField::required_field(
            2,
            "name".to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::optional_field(
            3,
            "email".to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
    ];

    let schema = Schema::builder()
        .with_fields(fields.clone())
        .build()
        .unwrap();

    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.fields()[0].id(), 1);
    assert_eq!(schema.fields()[1].id(), 2);
}

#[test]
fn test_schema_with_struct_type() {
    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    assert_eq!(schema.schema_id(), 1);
    let struct_type = schema.as_struct();
    assert_eq!(struct_type.fields().len(), 1);
}
