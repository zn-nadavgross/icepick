use super::*;
use crate::io::FileIO;
use crate::spec::types::NestedField;
use crate::spec::{PartitionField, PrimitiveType, Schema, Type};
use crate::writer::ParquetWriter;
use arrow::array::TimestampMicrosecondArray;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use opendal::Operator;
use std::sync::Arc;

#[test]
fn parse_hive_partition_values_extracts_pairs() {
    let path = "prefix/year=2025/month=01/day=15/hour=10/file.parquet";
    let values = parse_hive_partition_values(path);

    assert_eq!(values.get("year"), Some(&"2025".to_string()));
    assert_eq!(values.get("month"), Some(&"01".to_string()));
    assert_eq!(values.get("day"), Some(&"15".to_string()));
    assert_eq!(values.get("hour"), Some(&"10".to_string()));
    assert!(!values.contains_key("prefix"));
}

#[test]
fn infer_partition_values_respects_temporal_transforms() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            5,
            "ts".to_string(),
            Type::Primitive(PrimitiveType::Timestamp),
        )])
        .build()
        .unwrap();

    let spec = PartitionSpec::new(
        0,
        vec![
            PartitionField::new(1, 5, "year", "year"),
            PartitionField::new(2, 5, "month", "month"),
            PartitionField::new(3, 5, "day", "day"),
            PartitionField::new(4, 5, "hour", "hour"),
        ],
    );

    let path = "logs/year=2025/month=12/day=06/hour=15/part.parquet";
    let values = infer_partition_values_from_path(&spec, &schema, path).unwrap();

    assert_eq!(values.get("year"), Some(&PartitionValue::Int(2025)));
    assert_eq!(values.get("month"), Some(&PartitionValue::Int(12)));
    assert_eq!(values.get("day"), Some(&PartitionValue::Int(6)));
    assert_eq!(values.get("hour"), Some(&PartitionValue::Int(15)));
}

#[test]
fn infer_partition_values_defaults_to_source_type() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            7,
            "user_id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let spec = PartitionSpec::new(0, vec![PartitionField::new(1, 7, "identity", "user_id")]);

    let path = "data/user_id=184467440737095516/file.parquet";
    let values = infer_partition_values_from_path(&spec, &schema, path).unwrap();

    assert_eq!(
        values.get("user_id"),
        Some(&PartitionValue::Long(184_467_440_737_095_516))
    );
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn introspect_parquet_file_populates_partition_values() {
    let iceberg_schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "ts".to_string(),
            Type::Primitive(PrimitiveType::Timestamp),
        )])
        .build()
        .unwrap();

    let arrow_schema = ArrowSchema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    )]);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(TimestampMicrosecondArray::from(vec![
            1_735_000_000_000_000i64,
        ]))],
    )
    .unwrap();

    let mut writer = ParquetWriter::new(iceberg_schema.clone()).unwrap();
    writer.write_batch(&batch).unwrap();

    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);
    // Use an absolute URI to satisfy DataFileInput validation
    let path = "memory://logs/year=2025/month=12/day=06/hour=15/file.parquet";
    writer.finish(&file_io, path.to_string()).await.unwrap();

    let spec = PartitionSpec::new(
        0,
        vec![
            PartitionField::new(10, 1, "year", "year"),
            PartitionField::new(11, 1, "month", "month"),
            PartitionField::new(12, 1, "day", "day"),
            PartitionField::new(13, 1, "hour", "hour"),
        ],
    );

    let result = introspect_parquet_file(&file_io, path, Some(&spec))
        .await
        .unwrap();
    let partitions = result.partition_values.unwrap();

    assert_eq!(partitions.get("year"), Some(&PartitionValue::Int(2025)));
    assert_eq!(partitions.get("month"), Some(&PartitionValue::Int(12)));
    assert_eq!(partitions.get("day"), Some(&PartitionValue::Int(6)));
    assert_eq!(partitions.get("hour"), Some(&PartitionValue::Int(15)));

    let data_file = result
        .data_file
        .clone()
        .into_data_file(Some(&spec), &result.schema)
        .unwrap();
    let string_partitions = data_file.partition();
    assert_eq!(string_partitions.get("year"), Some(&"2025".to_string()));
    assert_eq!(string_partitions.get("month"), Some(&"12".to_string()));
    assert_eq!(string_partitions.get("day"), Some(&"6".to_string()));
    assert_eq!(string_partitions.get("hour"), Some(&"15".to_string()));
}

#[test]
fn missing_partition_segment_errors() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            5,
            "ts".to_string(),
            Type::Primitive(PrimitiveType::Timestamp),
        )])
        .build()
        .unwrap();

    let spec = PartitionSpec::new(0, vec![PartitionField::new(1, 5, "hour", "hour")]);

    let err = infer_partition_values_from_path(&spec, &schema, "logs/year=2025/file.parquet")
        .unwrap_err();
    assert!(
        format!("{err}").contains("Missing partition segment 'hour'"),
        "unexpected error: {err}"
    );
}

#[test]
fn malformed_partition_value_errors() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            2,
            "ts".to_string(),
            Type::Primitive(PrimitiveType::Timestamp),
        )])
        .build()
        .unwrap();

    let spec = PartitionSpec::new(0, vec![PartitionField::new(1, 2, "hour", "hour")]);

    let err = infer_partition_values_from_path(&spec, &schema, "logs/hour=notanumber/file.parquet")
        .unwrap_err();
    assert!(
        format!("{err}").contains("expected integer for hour transform"),
        "unexpected error: {err}"
    );
}
