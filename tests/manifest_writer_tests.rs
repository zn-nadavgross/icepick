use icepick::io::FileIO;
use icepick::manifest::writer::write_manifest;
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
