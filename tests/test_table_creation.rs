use icepick::spec::{NestedField, PrimitiveType, Schema, TableCreation, Type};

#[test]
fn test_table_creation_builder_minimal() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let creation = TableCreation::builder()
        .with_name("test_table")
        .with_schema(schema.clone())
        .build()
        .unwrap();

    assert_eq!(creation.name(), "test_table");
    assert_eq!(creation.schema().fields().len(), 1);
    assert!(creation.location().is_none());
    assert!(creation.properties().is_empty());
}

#[test]
fn test_table_creation_with_location() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let creation = TableCreation::builder()
        .with_name("test_table")
        .with_schema(schema)
        .with_location("s3://bucket/warehouse/table")
        .build()
        .unwrap();

    assert_eq!(creation.location(), Some("s3://bucket/warehouse/table"));
}
