use icepick::spec::{Snapshot, Summary};

#[test]
fn test_snapshot_creation() {
    let summary = Summary::builder()
        .set("operation", "append")
        .set("added-files", "1")
        .build();

    let snapshot = Snapshot::builder()
        .with_snapshot_id(1)
        .with_timestamp_ms(1234567890000)
        .with_manifest_list("s3://bucket/metadata/snap-1.avro")
        .with_summary(summary)
        .build()
        .unwrap();

    assert_eq!(snapshot.snapshot_id(), 1);
    assert_eq!(snapshot.timestamp_ms(), 1234567890000);
    assert_eq!(snapshot.manifest_list(), "s3://bucket/metadata/snap-1.avro");
}

#[test]
fn test_snapshot_summary() {
    let summary = Summary::builder()
        .set("operation", "append")
        .set("added-records", "100")
        .build();

    let snapshot = Snapshot::builder()
        .with_snapshot_id(1)
        .with_timestamp_ms(1234567890000)
        .with_manifest_list("s3://bucket/metadata/snap-1.avro")
        .with_summary(summary.clone())
        .build()
        .unwrap();

    assert_eq!(
        snapshot.summary().get("operation"),
        Some(&"append".to_string())
    );
    assert_eq!(
        snapshot.summary().get("added-records"),
        Some(&"100".to_string())
    );
}
