use icepick::io::FileIO;
use icepick::manifest::writer::{write_manifest, write_manifest_list, ManifestListEntry};
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

    write_manifest_list(&file_io, list_path, vec![entry])
        .await
        .unwrap();

    // Verify file exists
    let exists = op.exists(list_path).await.unwrap();
    assert!(exists);
}
