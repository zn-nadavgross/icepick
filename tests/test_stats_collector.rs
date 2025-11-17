use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use icepick::spec::{NestedField, PrimitiveType, Schema, Type};
use icepick::writer::stats::StatsCollector;
use std::convert::TryInto;
use std::sync::Arc;

#[test]
fn test_stats_collector_basic() {
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
    let mut collector = StatsCollector::new(&schema);

    let arrow_schema = ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap();

    collector.collect(&batch).unwrap();

    let stats = collector.finalize();
    assert_eq!(stats.record_count, 3);
    assert_eq!(stats.value_counts.get(&1), Some(&3));
    assert_eq!(stats.value_counts.get(&2), Some(&2)); // 2 non-null
    assert_eq!(stats.null_value_counts.get(&2), Some(&1)); // 1 null

    let id_lower = stats.lower_bounds.get(&1).unwrap();
    let id_upper = stats.upper_bounds.get(&1).unwrap();
    let lower_bytes: [u8; 8] = id_lower.as_slice().try_into().unwrap();
    let upper_bytes: [u8; 8] = id_upper.as_slice().try_into().unwrap();
    assert_eq!(i64::from_le_bytes(lower_bytes), 1);
    assert_eq!(i64::from_le_bytes(upper_bytes), 3);
    assert!(stats.column_sizes.get(&1).unwrap() > &0);
}

#[test]
fn test_stats_collector_multiple_batches() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            10,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();
    let mut collector = StatsCollector::new(&schema);

    let arrow_schema = ArrowSchema::new(vec![ArrowField::new("id", ArrowDataType::Int64, false)]);

    let batch1 = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(Int64Array::from(vec![3, 4, 5]))],
    )
    .unwrap();

    collector.collect(&batch1).unwrap();
    collector.collect(&batch2).unwrap();

    let stats = collector.finalize();
    assert_eq!(stats.record_count, 5);
    assert_eq!(stats.value_counts.get(&10), Some(&5));
}
