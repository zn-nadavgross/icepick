use apache_avro::types::Value;
use apache_avro::Writer;
use icepick::io::FileIO;
use icepick::manifest::schema::manifest_list_schema_v2;
use icepick::manifest::writer::{write_manifest, write_manifest_list, ManifestListEntry};
use icepick::reader::ManifestListReader;
use icepick::spec::DataFile;
use opendal::Operator;

#[tokio::test]
async fn test_write_manifest() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .build()
        .unwrap();

    let path = "metadata/test-m0.avro";
    let bytes_written = write_manifest(&file_io, path, &[data_file], 1, 1)
        .await
        .unwrap();

    assert!(bytes_written > 0);

    // Verify file exists
    let exists = op.exists(path).await.unwrap();
    assert!(exists);
}

#[tokio::test]
async fn test_write_manifest_list() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

    let manifest_path = "metadata/test-m0.avro";
    let manifest_length = 1000;
    let added_files_count = 5;
    let added_rows_count = 500;

    let list_path = "metadata/snap-1-1-test.avro";

    let entry = ManifestListEntry {
        manifest_path: manifest_path.to_string(),
        manifest_length,
        partition_spec_id: 0,
        content: 0,
        sequence_number: 1,
        min_sequence_number: 1,
        added_snapshot_id: 1,
        added_files_count,
        existing_files_count: 0,
        deleted_files_count: 0,
        added_rows_count,
        existing_rows_count: 0,
        deleted_rows_count: 0,
    };

    write_manifest_list(&file_io, list_path, vec![entry.clone()])
        .await
        .unwrap();

    // Verify file exists
    let exists = op.exists(list_path).await.unwrap();
    assert!(exists);

    // Ensure counts survive round-trip and are non-null unions
    let entries = ManifestListReader::read_entries(&file_io, list_path)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    let parsed = &entries[0];
    assert_eq!(parsed.manifest_path, entry.manifest_path);
    assert_eq!(parsed.added_files_count, entry.added_files_count);
    assert_eq!(parsed.existing_files_count, entry.existing_files_count);
    assert_eq!(parsed.deleted_files_count, entry.deleted_files_count);
    assert_eq!(parsed.added_rows_count, entry.added_rows_count);
    assert_eq!(parsed.existing_rows_count, entry.existing_rows_count);
    assert_eq!(parsed.deleted_rows_count, entry.deleted_rows_count);
}

#[tokio::test]
async fn manifest_list_reader_handles_null_counts() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

    let schema = manifest_list_schema_v2().unwrap();
    let mut writer = Writer::new(&schema, Vec::new());

    writer
        .append(Value::Record(vec![
            (
                "manifest_path".to_string(),
                Value::String("memory://list-null.avro".to_string()),
            ),
            ("manifest_length".to_string(), Value::Long(10)),
            ("partition_spec_id".to_string(), Value::Int(0)),
            ("content".to_string(), Value::Int(0)),
            ("sequence_number".to_string(), Value::Long(1)),
            ("min_sequence_number".to_string(), Value::Long(1)),
            ("added_snapshot_id".to_string(), Value::Long(1)),
            (
                "added_files_count".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "existing_files_count".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "deleted_files_count".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "added_rows_count".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "existing_rows_count".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "deleted_rows_count".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "partitions".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "key_metadata".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
        ]))
        .unwrap();

    let bytes = writer.into_inner().unwrap();
    let path = "metadata/null-counts.avro";
    op.write(path, bytes).await.unwrap();

    let entries = ManifestListReader::read_entries(&file_io, path)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    let parsed = &entries[0];

    assert_eq!(parsed.added_files_count, 0);
    assert_eq!(parsed.existing_files_count, 0);
    assert_eq!(parsed.deleted_files_count, 0);
    assert_eq!(parsed.added_rows_count, 0);
    assert_eq!(parsed.existing_rows_count, 0);
    assert_eq!(parsed.deleted_rows_count, 0);
}
