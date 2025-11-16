use icepick::io::FileIO;
use icepick::spec::{
    DataFile, NamespaceIdent, NestedField, PrimitiveType, Schema, TableIdent, TableMetadata, Type,
};
use icepick::table::Table;
use opendal::Operator;

#[tokio::test]
async fn test_try_commit_first_snapshot() {
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
        .with_location("s3://bucket/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["default".to_string()]),
        "test".to_string(),
    );

    let table = Table::new(
        ident,
        metadata,
        "s3://bucket/metadata/v0.metadata.json".to_string(),
        file_io.clone(),
    );

    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .build()
        .unwrap();

    let transaction = table.transaction().append(vec![data_file]);

    // This will fail until we implement try_commit
    let result = icepick::commit::orchestrator::try_commit(&transaction).await;
    assert!(result.is_ok());
}
