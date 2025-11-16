use icepick::manifest::avro::data_file_to_avro;
use icepick::spec::DataFile;
use std::collections::HashMap;

#[test]
fn test_data_file_to_avro_minimal() {
    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .build()
        .unwrap();

    let avro_value = data_file_to_avro(&data_file).unwrap();

    // Verify it's a Record
    if let apache_avro::types::Value::Record(fields) = avro_value {
        let file_path = fields.iter().find(|(k, _)| k == "file_path");
        assert!(file_path.is_some());
    } else {
        panic!("Expected Record value");
    }
}

#[test]
fn test_data_file_to_avro_with_stats() {
    let mut value_counts = HashMap::new();
    value_counts.insert(1, 100);

    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file2.parquet")
        .with_file_format("PARQUET")
        .with_record_count(200)
        .with_file_size_in_bytes(10000)
        .with_value_counts(value_counts)
        .build()
        .unwrap();

    let avro_value = data_file_to_avro(&data_file).unwrap();

    // Verify it's a Record with stats
    assert!(matches!(avro_value, apache_avro::types::Value::Record(_)));
}
