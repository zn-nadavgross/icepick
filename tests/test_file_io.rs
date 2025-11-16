use icepick::io::FileIO;
use opendal::Operator;

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn test_file_io_write_read() {
    // Use memory backend for testing
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();

    let file_io = FileIO::new(op);

    // Write data
    let data = b"Hello, Iceberg!";
    file_io.write("test.txt", data.to_vec()).await.unwrap();

    // Read data back
    let read_data = file_io.read("test.txt").await.unwrap();
    assert_eq!(read_data, data);
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn test_file_io_exists() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();

    let file_io = FileIO::new(op);

    // File doesn't exist initially
    assert!(!file_io.exists("missing.txt").await.unwrap());

    // Write file
    file_io.write("exists.txt", b"data".to_vec()).await.unwrap();

    // Now it exists
    assert!(file_io.exists("exists.txt").await.unwrap());
}
