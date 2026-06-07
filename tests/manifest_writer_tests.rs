use apache_avro::types::Value;
use apache_avro::{Schema, Writer};
use icepick::io::FileIO;
use icepick::manifest::writer::{write_manifest, write_manifest_list, ManifestListEntry};
use icepick::reader::ManifestListReader;
use icepick::spec::{DataFile, PartitionSpec, Schema as IcebergSchema};
use opendal::Operator;

fn empty_spec() -> PartitionSpec {
    PartitionSpec::new(0, Vec::new())
}

fn empty_schema() -> IcebergSchema {
    IcebergSchema::builder()
        .with_fields(Vec::new())
        .build()
        .unwrap()
}

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
    let bytes_written = write_manifest(
        &file_io,
        path,
        &[data_file],
        1,
        1,
        &empty_spec(),
        &empty_schema(),
    )
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
        partitions: Vec::new(),
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

    // Use the legacy schema so we can write nullable count fields and ensure the reader still
    // tolerates them.
    let schema = legacy_manifest_list_schema_with_nullable_counts();
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

#[tokio::test]
async fn test_manifest_list_partitions_field_is_empty_array_for_unpartitioned_tables() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

    let entry = ManifestListEntry {
        manifest_path: "metadata/test-m0.avro".to_string(),
        manifest_length: 1000,
        partition_spec_id: 0, // unpartitioned table
        content: 0,
        sequence_number: 1,
        min_sequence_number: 1,
        added_snapshot_id: 1,
        added_files_count: 5,
        existing_files_count: 0,
        deleted_files_count: 0,
        added_rows_count: 500,
        existing_rows_count: 0,
        deleted_rows_count: 0,
        partitions: Vec::new(),
    };

    let list_path = "metadata/snap-1-partitions-test.avro";
    write_manifest_list(&file_io, list_path, vec![entry])
        .await
        .unwrap();

    // Read the raw Avro and verify partitions field is an empty array, not null
    let bytes = op.read(list_path).await.unwrap();
    let bytes_slice = bytes.to_vec();
    let reader = apache_avro::Reader::new(&bytes_slice[..]).unwrap();

    for record_result in reader {
        let record = record_result.unwrap();

        if let Value::Record(fields) = record {
            let partitions_field = fields
                .iter()
                .find(|(name, _)| name == "partitions")
                .map(|(_, value)| value);

            // For unpartitioned tables (partition_spec_id=0), partitions should be
            // an empty array, NOT null. This is required for DuckDB compatibility.
            // DuckDB crashes when partitions is null because it tries to access
            // integer fields within the partition summary structure.
            match partitions_field {
                Some(Value::Union(1, boxed_value)) => {
                    // Union variant 1 is the array
                    match &**boxed_value {
                        Value::Array(arr) => {
                            assert!(
                                arr.is_empty(),
                                "partitions array should be empty for unpartitioned tables"
                            );
                        }
                        _ => panic!("partitions union variant 1 should contain an Array"),
                    }
                }
                Some(Value::Union(0, boxed_value)) => {
                    panic!(
                        "partitions should be an empty array, not null (variant 0). Found: {:?}",
                        boxed_value
                    );
                }
                _ => panic!("partitions field not found or has unexpected structure"),
            }
        }
    }
}

fn legacy_manifest_list_schema_with_nullable_counts() -> Schema {
    Schema::parse_str(
        r#"{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "manifest_length", "type": "long"},
    {"name": "partition_spec_id", "type": "int"},
    {"name": "content", "type": "int"},
    {"name": "sequence_number", "type": "long"},
    {"name": "min_sequence_number", "type": "long"},
    {"name": "added_snapshot_id", "type": "long"},
    {"name": "added_files_count", "type": ["null", "int"], "default": null},
    {"name": "existing_files_count", "type": ["null", "int"], "default": null},
    {"name": "deleted_files_count", "type": ["null", "int"], "default": null},
    {"name": "added_rows_count", "type": ["null", "long"], "default": null},
    {"name": "existing_rows_count", "type": ["null", "long"], "default": null},
    {"name": "deleted_rows_count", "type": ["null", "long"], "default": null},
    {"name": "partitions", "type": ["null", {"type": "array", "items": "int"}], "default": null},
    {"name": "key_metadata", "type": ["null", "bytes"], "default": null}
  ]
}"#,
    )
    .expect("legacy manifest schema should parse")
}
