use icepick::{
    DataFile, FileIO, NamespaceIdent, NestedField, PrimitiveType, Schema, Table, TableIdent,
    TableMetadata, Type,
};
use opendal::Operator;

#[tokio::test]
async fn test_transaction_append() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://bucket/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["db".to_string()]),
        "table".to_string(),
    );

    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);

    let table = Table::new(
        ident,
        metadata,
        "s3://bucket/metadata.json".to_string(),
        file_io,
    );

    // Create data file
    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .build()
        .unwrap();

    // Create transaction with append
    let tx = table.transaction().append(vec![data_file]);

    // Verify we can build the transaction (commit will be tested later)
    assert!(tx.has_operations());
}
