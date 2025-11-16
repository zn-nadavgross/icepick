use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use icepick::io::FileIO;
use icepick::spec::{NestedField, PrimitiveType, Schema, Type};
use icepick::writer::ParquetWriter;
use opendal::Operator;
use std::sync::Arc;

#[tokio::test]
async fn test_parquet_writer_simple() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let mut writer = ParquetWriter::new(schema).unwrap();

    let arrow_schema = ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    writer.write_batch(&batch).unwrap();

    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

    let data_file = writer
        .finish(&file_io, "test.parquet".to_string())
        .await
        .unwrap();

    assert_eq!(data_file.file_path(), "test.parquet");
    assert_eq!(data_file.file_format(), "PARQUET");
    assert_eq!(data_file.record_count(), 3);
    assert!(data_file.file_size_in_bytes() > 0);

    // Verify file was written
    let exists = op.exists("test.parquet").await.unwrap();
    assert!(exists);
}
