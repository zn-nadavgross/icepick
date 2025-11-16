use icepick::io::FileIO;
use icepick::spec::{
    DataFile, NamespaceIdent, NestedField, PrimitiveType, Schema, TableIdent, TableMetadata, Type,
};
use icepick::table::Table;
use opendal::Operator;
use std::collections::HashMap;

#[tokio::test]
async fn test_end_to_end_commit_with_stats() {
    // Setup
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

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

    let metadata = TableMetadata::builder()
        .with_location("memory://warehouse/db/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["db".to_string()]),
        "table".to_string(),
    );

    let table = Table::new(
        ident,
        metadata,
        "memory://warehouse/db/table/metadata/v0.metadata.json".to_string(),
        file_io.clone(),
    );

    // Create data file with stats
    let mut value_counts = HashMap::new();
    value_counts.insert(1, 1000);
    value_counts.insert(2, 950);

    let mut null_counts = HashMap::new();
    null_counts.insert(2, 50);

    let data_file = DataFile::builder()
        .with_file_path("memory://warehouse/db/table/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(1000)
        .with_file_size_in_bytes(50_000)
        .with_value_counts(value_counts)
        .with_null_value_counts(null_counts)
        .build()
        .unwrap();

    // Commit
    table
        .transaction()
        .append(vec![data_file])
        .commit()
        .await
        .unwrap();

    // Verify files exist
    let manifest_exists = op
        .list("memory://warehouse/db/table/metadata/")
        .await
        .unwrap()
        .into_iter()
        .any(|entry| entry.path().contains("-m0.avro"));
    assert!(manifest_exists, "Manifest file should exist");

    let manifest_list_exists = op
        .list("memory://warehouse/db/table/metadata/")
        .await
        .unwrap()
        .into_iter()
        .any(|entry| entry.path().contains("snap-1-1-"));
    assert!(manifest_list_exists, "Manifest list should exist");

    let metadata_exists = op
        .exists("memory://warehouse/db/table/metadata/v1.metadata.json")
        .await
        .unwrap();
    assert!(metadata_exists, "New metadata file should exist");

    // Read and verify metadata
    let metadata_bytes = op
        .read("memory://warehouse/db/table/metadata/v1.metadata.json")
        .await
        .unwrap();
    let new_metadata: TableMetadata = serde_json::from_slice(&metadata_bytes.to_vec()).unwrap();

    assert_eq!(new_metadata.current_snapshot_id(), Some(1));
    assert_eq!(new_metadata.snapshots().len(), 1);

    let snapshot = new_metadata.current_snapshot().unwrap();
    assert_eq!(
        snapshot.summary().get("operation"),
        Some(&"append".to_string())
    );
    assert_eq!(
        snapshot.summary().get("added-data-files"),
        Some(&"1".to_string())
    );
    assert_eq!(
        snapshot.summary().get("added-records"),
        Some(&"1000".to_string())
    );
}
