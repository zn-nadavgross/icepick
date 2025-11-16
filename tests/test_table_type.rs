use icepick::io::FileIO;
use icepick::spec::{
    NamespaceIdent, NestedField, PrimitiveType, Schema, TableIdent, TableMetadata, Type,
};
use icepick::table::Table;
use opendal::Operator;

#[test]
fn test_table_new() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);

    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://bucket/warehouse/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["default".to_string()]),
        "test_table".to_string(),
    );

    let table = Table::new(
        ident.clone(),
        metadata,
        "s3://bucket/warehouse/table/metadata/v1.metadata.json".to_string(),
        file_io,
    );

    assert_eq!(table.identifier(), &ident);
    assert_eq!(table.location(), "s3://bucket/warehouse/table");
    assert_eq!(
        table.metadata_location(),
        "s3://bucket/warehouse/table/metadata/v1.metadata.json"
    );
}

#[test]
fn test_table_current_snapshot() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);

    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://bucket/warehouse/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["default".to_string()]),
        "test_table".to_string(),
    );

    let table = Table::new(ident, metadata, "meta.json".to_string(), file_io);

    assert!(table.current_snapshot().is_none());
}
