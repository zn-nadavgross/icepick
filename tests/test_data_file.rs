use icepick::spec::{DataContentType, DataFile};

#[test]
fn test_data_file_builder() {
    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .build()
        .unwrap();

    assert_eq!(data_file.file_path(), "s3://bucket/data/file1.parquet");
    assert_eq!(data_file.file_format(), "PARQUET");
    assert_eq!(data_file.record_count(), 100);
    assert_eq!(data_file.file_size_in_bytes(), 5000);
}

#[test]
fn test_data_file_content_type() {
    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .with_content_type(DataContentType::Data)
        .build()
        .unwrap();

    assert_eq!(data_file.content_type(), DataContentType::Data);
}
