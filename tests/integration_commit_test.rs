use icepick::catalog::Catalog;
use icepick::io::FileIO;
use icepick::spec::{
    DataFile, NamespaceIdent, NestedField, PrimitiveType, Schema, TableCreation, TableIdent,
    TableMetadata, Type,
};
use icepick::table::Table;
use opendal::Operator;
use std::collections::HashMap;

// Simple in-memory catalog for testing
struct TestCatalog;

#[async_trait::async_trait]
impl Catalog for TestCatalog {
    fn file_io(&self) -> &FileIO {
        panic!("TestCatalog::file_io() is not supported in tests")
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> icepick::error::Result<()> {
        Ok(())
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> icepick::error::Result<bool> {
        Ok(true)
    }

    async fn list_tables(
        &self,
        _namespace: &NamespaceIdent,
    ) -> icepick::error::Result<Vec<TableIdent>> {
        Ok(vec![])
    }

    async fn table_exists(&self, _identifier: &TableIdent) -> icepick::error::Result<bool> {
        Ok(true)
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> icepick::error::Result<Table> {
        unimplemented!("TestCatalog::create_table")
    }

    async fn load_table(&self, _identifier: &TableIdent) -> icepick::error::Result<Table> {
        unimplemented!("TestCatalog::load_table")
    }

    async fn drop_table(&self, _identifier: &TableIdent) -> icepick::error::Result<()> {
        Ok(())
    }

    async fn update_table_metadata(
        &self,
        _identifier: &TableIdent,
        _old_metadata_location: &str,
        _new_metadata_location: &str,
    ) -> icepick::error::Result<()> {
        // For tests, we just no-op - the files are already written
        Ok(())
    }
}

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
    let catalog = TestCatalog;
    let timestamp_ms = 1234567890;
    table
        .transaction()
        .append(vec![data_file])
        .commit(&catalog, timestamp_ms)
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
        .any(|entry| entry.path().contains("snap-") && entry.path().contains("-1-"));
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

    // Verify snapshot exists and is a positive ID (UUID-based)
    let snapshot_id = new_metadata
        .current_snapshot_id()
        .expect("Should have current snapshot");
    assert!(snapshot_id > 0, "Snapshot ID should be positive");
    assert_eq!(new_metadata.snapshots().len(), 1);

    let snapshot = new_metadata.current_snapshot().unwrap();
    assert_eq!(snapshot.snapshot_id(), snapshot_id);
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

#[tokio::test]
async fn test_multiple_sequential_commits() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("memory://warehouse/test/multi")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["test".to_string()]),
        "multi".to_string(),
    );

    let mut table = Table::new(
        ident.clone(),
        metadata,
        "memory://warehouse/test/multi/metadata/v0.metadata.json".to_string(),
        file_io.clone(),
    );

    // First commit
    let catalog = TestCatalog;
    let data_file1 = DataFile::builder()
        .with_file_path("memory://warehouse/test/multi/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .build()
        .unwrap();

    let timestamp_ms_1 = 1234567890;
    table
        .transaction()
        .append(vec![data_file1])
        .commit(&catalog, timestamp_ms_1)
        .await
        .unwrap();

    // Read updated metadata for second commit
    let metadata_bytes = op
        .read("memory://warehouse/test/multi/metadata/v1.metadata.json")
        .await
        .unwrap();
    let updated_metadata: TableMetadata = serde_json::from_slice(&metadata_bytes.to_vec()).unwrap();

    table = Table::new(
        ident.clone(),
        updated_metadata,
        "memory://warehouse/test/multi/metadata/v1.metadata.json".to_string(),
        file_io.clone(),
    );

    // Second commit
    let data_file2 = DataFile::builder()
        .with_file_path("memory://warehouse/test/multi/data/file2.parquet")
        .with_file_format("PARQUET")
        .with_record_count(200)
        .with_file_size_in_bytes(10000)
        .build()
        .unwrap();

    let timestamp_ms_2 = 1234567900;
    table
        .transaction()
        .append(vec![data_file2])
        .commit(&catalog, timestamp_ms_2)
        .await
        .unwrap();

    // Verify
    let final_metadata_bytes = op
        .read("memory://warehouse/test/multi/metadata/v2.metadata.json")
        .await
        .unwrap();
    let final_metadata: TableMetadata =
        serde_json::from_slice(&final_metadata_bytes.to_vec()).unwrap();

    // Verify we have 2 snapshots with valid IDs
    assert_eq!(final_metadata.snapshots().len(), 2);
    let current_snapshot_id = final_metadata
        .current_snapshot_id()
        .expect("Should have current snapshot");
    assert!(
        current_snapshot_id > 0,
        "Current snapshot ID should be positive"
    );

    // Verify the current snapshot is the second one (most recent)
    let snapshots = final_metadata.snapshots();
    assert_eq!(snapshots[1].snapshot_id(), current_snapshot_id);

    // Verify the second snapshot has the first as its parent
    let first_snapshot_id = snapshots[0].snapshot_id();
    assert_eq!(snapshots[1].parent_snapshot_id(), Some(first_snapshot_id));

    // NEW TEST: Verify we can read data files from BOTH commits
    let reloaded_table = Table::new(
        ident,
        final_metadata,
        "memory://warehouse/test/multi/metadata/v2.metadata.json".to_string(),
        file_io,
    );

    let files = reloaded_table.files().await.unwrap();
    assert_eq!(files.len(), 2, "Should have 2 data files from both commits");

    // Verify both files are present
    let file_paths: Vec<String> = files.iter().map(|f| f.file_path.clone()).collect();
    assert!(
        file_paths.contains(&"memory://warehouse/test/multi/data/file1.parquet".to_string()),
        "Should include file1 from first commit"
    );
    assert!(
        file_paths.contains(&"memory://warehouse/test/multi/data/file2.parquet".to_string()),
        "Should include file2 from second commit"
    );

    // Verify total record count
    let total_records: i64 = files.iter().map(|f| f.record_count).sum();
    assert_eq!(
        total_records, 300,
        "Should have 100 + 200 = 300 total records"
    );
}
