use icepick::manifest::avro::data_file_to_avro;
use icepick::spec::{DataFile, PartitionSpec, Schema};
use std::collections::HashMap;

fn empty_spec() -> PartitionSpec {
    PartitionSpec::new(0, Vec::new())
}

fn empty_schema() -> Schema {
    Schema::builder().with_fields(Vec::new()).build().unwrap()
}

#[test]
fn test_data_file_to_avro_minimal() {
    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .with_partition(HashMap::new())
        .build()
        .unwrap();

    let avro_value = data_file_to_avro(&data_file, &empty_spec(), &empty_schema()).unwrap();

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
    let mut lower = HashMap::new();
    lower.insert(1, vec![0, 0, 0, 0, 0, 0, 0, 1]);
    let mut upper = HashMap::new();
    upper.insert(1, vec![0, 0, 0, 0, 0, 0, 0, 2]);

    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file2.parquet")
        .with_file_format("PARQUET")
        .with_record_count(200)
        .with_file_size_in_bytes(10000)
        .with_value_counts(value_counts)
        .with_lower_bounds(lower.clone())
        .with_upper_bounds(upper.clone())
        .with_partition(HashMap::new())
        .build()
        .unwrap();

    let avro_value = data_file_to_avro(&data_file, &empty_spec(), &empty_schema()).unwrap();

    if let apache_avro::types::Value::Record(fields) = avro_value {
        let lower_bounds = fields.iter().find(|(k, _)| k == "lower_bounds");
        assert!(lower_bounds.is_some());
    } else {
        panic!("Expected Record value");
    }
}
