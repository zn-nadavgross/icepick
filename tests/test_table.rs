use icepick::{
    FileIO, NamespaceIdent, NestedField, PrimitiveType, Schema, Table, TableIdent, TableMetadata,
    Type,
};
use opendal::Operator;

#[tokio::test]
async fn test_table_accessors() {
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
        ident.clone(),
        metadata.clone(),
        "s3://bucket/warehouse/db/table/metadata/v1.json".to_string(),
        file_io,
    );

    assert_eq!(table.identifier().to_string(), "db.table");
    assert_eq!(table.location(), "s3://bucket/warehouse/db/table");
    assert_eq!(table.schema().unwrap().schema_id(), 0);
    assert_eq!(
        table.metadata_location(),
        "s3://bucket/warehouse/db/table/metadata/v1.json"
    );
}

#[tokio::test]
async fn test_table_transaction() {
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

    // Create transaction
    let _tx = table.transaction();
    // Just verify it compiles for now
}
