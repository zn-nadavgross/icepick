use super::*;
use crate::spec::{NestedField, PrimitiveType, Schema, Snapshot, Type};

#[test]
fn test_current_snapshot_id() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://test/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    assert_eq!(metadata.current_snapshot_id(), None);
}

#[test]
fn test_add_snapshot_to_metadata() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://test/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let snapshot = Snapshot::builder()
        .with_snapshot_id(1)
        .with_timestamp_ms(1000)
        .with_manifest_list("s3://test/metadata/snap-1.avro")
        .build()
        .unwrap();

    let timestamp_ms = 1234567890;
    let updated = metadata.add_snapshot(snapshot, timestamp_ms);

    assert_eq!(updated.current_snapshot_id(), Some(1));
    assert_eq!(updated.snapshots().len(), 1);
    assert_eq!(updated.last_updated_ms(), timestamp_ms);
}
