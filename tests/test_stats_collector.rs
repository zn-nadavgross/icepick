use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use icepick::writer::stats::StatsCollector;
use std::sync::Arc;

#[test]
fn test_stats_collector_basic() {
    let mut collector = StatsCollector::new();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap();

    collector.collect(&batch).unwrap();

    let stats = collector.finalize();
    assert_eq!(stats.record_count, 3);
    assert_eq!(stats.value_counts.get(&0), Some(&3));
    assert_eq!(stats.value_counts.get(&1), Some(&2)); // 2 non-null
    assert_eq!(stats.null_value_counts.get(&1), Some(&1)); // 1 null
}

#[test]
fn test_stats_collector_multiple_batches() {
    let mut collector = StatsCollector::new();

    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int64Array::from(vec![3, 4, 5]))],
    )
    .unwrap();

    collector.collect(&batch1).unwrap();
    collector.collect(&batch2).unwrap();

    let stats = collector.finalize();
    assert_eq!(stats.record_count, 5);
}
