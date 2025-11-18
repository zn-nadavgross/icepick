use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use icepick::arrow_convert::PARQUET_FIELD_ID_METADATA_KEY;
use icepick::{arrow_to_parquet, FileIO};
use parquet::basic::Compression;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_arrow_to_parquet_basic() {
    // Create in-memory FileIO
    let op = opendal::Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);

    // Create sample Arrow data
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    // Write with default compression
    let result = arrow_to_parquet(&batch, "test_data.parquet", &file_io).await;
    assert!(
        result.is_ok(),
        "Failed to write parquet: {:?}",
        result.err()
    );

    // Verify file was written
    let exists = file_io.exists("test_data.parquet").await.unwrap();
    assert!(exists, "Parquet file should exist");

    // Read back and verify it's valid Parquet
    let data = file_io.read("test_data.parquet").await.unwrap();
    assert!(!data.is_empty(), "Parquet file should not be empty");

    // Verify it's a valid Parquet file by checking magic bytes
    assert_eq!(&data[0..4], b"PAR1", "Should have Parquet magic bytes");
}

#[tokio::test]
async fn test_arrow_to_parquet_with_compression() {
    let op = opendal::Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);

    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .unwrap();

    // Test different compression codecs
    let compressions = [
        Compression::UNCOMPRESSED,
        Compression::SNAPPY,
        Compression::GZIP(parquet::basic::GzipLevel::default()),
        Compression::ZSTD(parquet::basic::ZstdLevel::default()),
    ];

    for (i, compression) in compressions.iter().enumerate() {
        let path = format!("test_compression_{}.parquet", i);
        let result = arrow_to_parquet(&batch, &path, &file_io)
            .with_compression(*compression)
            .await;

        assert!(
            result.is_ok(),
            "Failed to write with compression {:?}: {:?}",
            compression,
            result.err()
        );

        let exists = file_io.exists(&path).await.unwrap();
        assert!(
            exists,
            "File with compression {:?} should exist",
            compression
        );
    }
}

#[tokio::test]
async fn test_arrow_to_parquet_empty_batch() {
    let op = opendal::Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);

    // Create empty batch (0 rows)
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
    )
    .unwrap();

    // Should write valid Parquet file with schema but no data
    let result = arrow_to_parquet(&batch, "empty.parquet", &file_io).await;
    assert!(
        result.is_ok(),
        "Should handle empty batch: {:?}",
        result.err()
    );

    let data = file_io.read("empty.parquet").await.unwrap();
    assert!(
        !data.is_empty(),
        "Empty batch should still create valid Parquet file"
    );
    assert_eq!(&data[0..4], b"PAR1", "Should have Parquet magic bytes");
}

#[tokio::test]
async fn test_arrow_to_parquet_direct_await() {
    let op = opendal::Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);

    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "x",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![42]))]).unwrap();

    // Test that IntoFuture works - can await directly without .finish()
    arrow_to_parquet(&batch, "direct_await.parquet", &file_io)
        .await
        .unwrap();

    assert!(file_io.exists("direct_await.parquet").await.unwrap());
}

#[tokio::test]
async fn test_arrow_to_parquet_with_finish() {
    let op = opendal::Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);

    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "x",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![42]))]).unwrap();

    // Test that .finish() also works
    arrow_to_parquet(&batch, "with_finish.parquet", &file_io)
        .finish()
        .await
        .unwrap();

    assert!(file_io.exists("with_finish.parquet").await.unwrap());
}

#[tokio::test]
async fn test_arrow_to_parquet_finish_data_file() {
    let op = opendal::Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("name", DataType::Utf8, true, 2),
    ]));

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap();

    let data_file = arrow_to_parquet(&batch, "stats.parquet", &file_io)
        .finish_data_file()
        .await
        .unwrap();

    assert_eq!(data_file.record_count(), 3);
    assert!(data_file.column_sizes().is_some());
    assert!(data_file.value_counts().is_some());
    assert!(data_file.null_value_counts().is_some());
    assert!(data_file.lower_bounds().is_some());
    assert!(data_file.upper_bounds().is_some());

    assert!(op.exists("stats.parquet").await.unwrap());
}

fn field_with_id(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
    let mut field = Field::new(name, data_type, nullable);
    field.set_metadata(HashMap::from([(
        PARQUET_FIELD_ID_METADATA_KEY.to_string(),
        id.to_string(),
    )]));
    field
}
