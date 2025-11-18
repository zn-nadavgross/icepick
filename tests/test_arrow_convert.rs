use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use icepick::arrow_convert::{
    arrow_schema_to_iceberg, schema_to_arrow, PARQUET_FIELD_ID_METADATA_KEY,
};
use icepick::spec::{types::ListType, NestedField, PrimitiveType, Schema, Type};
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn test_arrow_schema_to_iceberg_round_trip() {
    let iceberg_schema = Schema::builder()
        .with_fields(vec![
            NestedField::required_field(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
            NestedField::optional_field(
                2,
                "tags".to_string(),
                Type::List(ListType::new(
                    3,
                    false,
                    Type::Primitive(PrimitiveType::String),
                )),
            ),
        ])
        .build()
        .unwrap();

    let arrow_schema = schema_to_arrow(&iceberg_schema).unwrap();
    let converted = arrow_schema_to_iceberg(&arrow_schema).unwrap();

    assert_eq!(converted.fields(), iceberg_schema.fields());
}

#[test]
fn test_arrow_schema_to_iceberg_missing_metadata() {
    let arrow_schema = ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]);
    let err = arrow_schema_to_iceberg(&arrow_schema).unwrap_err();
    assert!(
        err.to_string().contains(PARQUET_FIELD_ID_METADATA_KEY),
        "expected error mentioning missing field ID metadata, got: {err}"
    );
}

#[test]
fn test_arrow_schema_to_iceberg_map_type() {
    let fields = vec![
        Field::new("key", DataType::Int32, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_METADATA_KEY.to_string(),
            "4".to_string(),
        )])),
        Field::new("value", DataType::Utf8, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_METADATA_KEY.to_string(),
            "5".to_string(),
        )])),
    ];
    let entries_field = Field::new("entries", DataType::Struct(fields.into()), false);
    let map_field = Field::new(
        "properties",
        DataType::Map(Arc::new(entries_field), false),
        true,
    )
    .with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_METADATA_KEY.to_string(),
        "6".to_string(),
    )]));

    let schema = ArrowSchema::new(vec![map_field]);
    let iceberg_schema = arrow_schema_to_iceberg(&schema).unwrap();
    assert_eq!(iceberg_schema.fields()[0].name(), "properties");
}
