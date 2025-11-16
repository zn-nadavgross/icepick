use icepick::commit::paths::{manifest_list_path, manifest_path, metadata_path};

#[test]
fn test_manifest_path() {
    let uuid = "a1b2c3d4";
    let path = manifest_path("s3://bucket/table", uuid, 0);
    assert_eq!(path, "s3://bucket/table/metadata/a1b2c3d4-m0.avro");
}

#[test]
fn test_manifest_list_path() {
    let uuid = "e5f6g7h8";
    let path = manifest_list_path("s3://bucket/table", 1, uuid);
    assert_eq!(path, "s3://bucket/table/metadata/snap-1-1-e5f6g7h8.avro");
}

#[test]
fn test_metadata_path() {
    let path = metadata_path("s3://bucket/table", 2);
    assert_eq!(path, "s3://bucket/table/metadata/v2.metadata.json");
}
