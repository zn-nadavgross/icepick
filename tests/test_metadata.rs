use icepick::spec::{NestedField, PrimitiveType, Schema, Snapshot, Summary, TableMetadata, Type};

#[test]
fn test_table_metadata_basic() {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://bucket/warehouse/db/table")
        .with_current_schema(schema.clone())
        .build()
        .unwrap();

    assert_eq!(metadata.location(), "s3://bucket/warehouse/db/table");
    assert_eq!(metadata.current_schema().schema_id(), 0);
}

#[test]
fn test_table_metadata_with_snapshot() {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let summary = Summary::builder().set("operation", "append").build();

    let snapshot = Snapshot::builder()
        .with_snapshot_id(1)
        .with_timestamp_ms(1234567890000)
        .with_manifest_list("s3://bucket/metadata/snap-1.avro")
        .with_summary(summary)
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://bucket/warehouse/db/table")
        .with_current_schema(schema)
        .with_current_snapshot(snapshot.clone())
        .build()
        .unwrap();

    assert!(metadata.current_snapshot().is_some());
    assert_eq!(metadata.current_snapshot().unwrap().snapshot_id(), 1);
}
