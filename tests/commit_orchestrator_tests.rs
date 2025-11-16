mod common;

use common::TestCatalog;
use icepick::commit::try_commit;
use icepick::io::FileIO;
use icepick::spec::{
    DataFile, NamespaceIdent, NestedField, PrimitiveType, Schema, TableIdent, TableMetadata, Type,
};
use icepick::table::Table;
use opendal::Operator;

fn test_table(op: &Operator) -> Table {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
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

    let file_io = FileIO::new(op.clone());

    Table::new(
        ident,
        metadata,
        "memory://warehouse/db/table/metadata/v0.metadata.json".to_string(),
        file_io,
    )
}

#[tokio::test]
async fn try_commit_records_update_and_writes_files() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let table = test_table(&op);
    let catalog = TestCatalog::new(table.clone());

    let data_file = DataFile::builder()
        .with_file_path("memory://warehouse/db/table/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(10)
        .with_file_size_in_bytes(1024)
        .build()
        .unwrap();

    let transaction = table.transaction().append(vec![data_file]);
    try_commit(&transaction, &catalog).await.unwrap();

    let updates = catalog.recorded_updates().await;
    assert_eq!(updates.len(), 1);
    assert_eq!(
        updates[0].0,
        "memory://warehouse/db/table/metadata/v0.metadata.json"
    );
    assert!(updates[0].1.ends_with(".metadata.json"));

    let manifest_objects = op
        .list("memory://warehouse/db/table/metadata/")
        .await
        .unwrap();
    assert!(
        manifest_objects
            .iter()
            .any(|entry| entry.path().ends_with(".avro")),
        "Expected manifest files to be written"
    );
}
