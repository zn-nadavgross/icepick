//! File path generation for Iceberg metadata files

/// Generate manifest file path: {table}/metadata/{uuid}-m{n}.avro
pub fn manifest_path(table_location: &str, commit_uuid: &str, manifest_num: usize) -> String {
    format!(
        "{}/metadata/{}-m{}.avro",
        table_location.trim_end_matches('/'),
        commit_uuid,
        manifest_num
    )
}

/// Generate manifest list path: {table}/metadata/snap-{id}-1-{uuid}.avro
pub fn manifest_list_path(table_location: &str, snapshot_id: i64, commit_uuid: &str) -> String {
    format!(
        "{}/metadata/snap-{}-1-{}.avro",
        table_location.trim_end_matches('/'),
        snapshot_id,
        commit_uuid
    )
}

/// Generate metadata file path: {table}/metadata/v{n}.metadata.json
pub fn metadata_path(table_location: &str, version: usize) -> String {
    format!(
        "{}/metadata/v{}.metadata.json",
        table_location.trim_end_matches('/'),
        version
    )
}
