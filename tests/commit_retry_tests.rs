mod common;

use common::TestCatalog;
use icepick::io::FileIO;
use icepick::spec::{
    DataFile, NamespaceIdent, NestedField, PrimitiveType, Schema, TableIdent, TableMetadata, Type,
};
use icepick::table::Table;
use opendal::Operator;

fn create_table(op: &Operator) -> Table {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("memory://warehouse/retry/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["retry".to_string()]),
        "table".to_string(),
    );
    let file_io = FileIO::new(op.clone());

    Table::new(
        ident,
        metadata,
        "memory://warehouse/retry/table/metadata/v0.metadata.json".to_string(),
        file_io,
    )
}

#[tokio::test]
async fn commit_transaction_retries_after_conflict() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let table = create_table(&op);
    let catalog = TestCatalog::new(table.clone());
    catalog.fail_next_update();

    let data_file = DataFile::builder()
        .with_file_path("memory://warehouse/retry/table/data/file.parquet")
        .with_file_format("PARQUET")
        .with_record_count(5)
        .with_file_size_in_bytes(2048)
        .build()
        .unwrap();

    table
        .transaction()
        .append(vec![data_file])
        .commit(&catalog)
        .await
        .expect("commit should succeed after retry");

    assert_eq!(catalog.load_call_count(), 1, "should reload table once");
    let updates = catalog.recorded_updates().await;
    assert_eq!(updates.len(), 2, "one failed update and one success");
}
