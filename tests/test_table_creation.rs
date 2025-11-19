use icepick::spec::{
    NestedField, PartitionField, PartitionSpec, PrimitiveType, Schema, TableCreation, Type,
};

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

#[test]
fn test_table_creation_with_partition_spec() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            10,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let partition_spec = PartitionSpec::new(
        0,
        vec![PartitionField::new(1000, 10, "identity", "id_partition")],
    );

    let creation = TableCreation::builder()
        .with_name("test_table")
        .with_schema(schema)
        .with_partition_spec(partition_spec.clone())
        .build()
        .unwrap();

    assert!(creation.partition_spec().is_some());
    assert_eq!(creation.partition_spec().unwrap().spec_id(), 0);
    assert_eq!(
        creation.partition_spec().unwrap().fields()[0].name(),
        "id_partition"
    );
}
