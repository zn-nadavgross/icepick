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
    assert!(data_file.partition().is_empty());
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

#[test]
fn test_data_file_with_stats_and_partition() {
    let mut partition = std::collections::HashMap::new();
    partition.insert("bucket".to_string(), "2024".to_string());
    let mut lower = std::collections::HashMap::new();
    lower.insert(1, vec![0, 0, 0, 0, 0, 0, 0, 1]);
    let mut upper = std::collections::HashMap::new();
    upper.insert(1, vec![0, 0, 0, 0, 0, 0, 0, 5]);

    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file2.parquet")
        .with_file_format("PARQUET")
        .with_record_count(10)
        .with_file_size_in_bytes(100)
        .with_partition(partition)
        .with_lower_bounds(lower.clone())
        .with_upper_bounds(upper.clone())
        .with_equality_ids(vec![1, 2])
        .build()
        .unwrap();

    assert_eq!(data_file.partition().get("bucket").unwrap(), "2024");
    assert_eq!(data_file.lower_bounds().unwrap().get(&1), lower.get(&1));
    assert_eq!(data_file.upper_bounds().unwrap().get(&1), upper.get(&1));
    assert_eq!(data_file.equality_ids(), Some(&[1, 2][..]));
}
