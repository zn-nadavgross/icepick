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

/// Generate the next metadata path using the existing metadata file name or a UUID fallback
pub fn next_metadata_path(
    table_location: &str,
    current_metadata_location: &str,
    commit_uuid: &str,
) -> String {
    if let Some(version) = metadata_version_from_path(current_metadata_location) {
        metadata_path(table_location, version + 1)
    } else {
        format!(
            "{}/metadata/{}.metadata.json",
            table_location.trim_end_matches('/'),
            commit_uuid
        )
    }
}

fn metadata_version_from_path(path: &str) -> Option<usize> {
    let filename = path.rsplit('/').next().unwrap_or(path);
    let bytes = filename.as_bytes();
    let mut idx = 0;

    while idx < bytes.len() {
        if bytes[idx] == b'v' {
            let mut end = idx + 1;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end > idx + 1 {
                if let Ok(digits) = std::str::from_utf8(&bytes[idx + 1..end]) {
                    if let Ok(num) = digits.parse::<usize>() {
                        return Some(num);
                    }
                }
            }
        }
        idx += 1;
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_next_metadata_path_from_versioned_file() {
        let path = next_metadata_path(
            "s3://bucket/table",
            "s3://bucket/table/metadata/v2.metadata.json",
            "deadbeef",
        );
        assert_eq!(
            path,
            "s3://bucket/table/metadata/v3.metadata.json".to_string()
        );
    }

    #[test]
    fn handles_gz_metadata_files() {
        let path = next_metadata_path(
            "s3://bucket/table",
            "s3://bucket/table/metadata/v10.metadata.json.gz",
            "deadbeef",
        );
        assert_eq!(
            path,
            "s3://bucket/table/metadata/v11.metadata.json".to_string()
        );

        let gz_alt = next_metadata_path(
            "s3://bucket/table",
            "s3://bucket/table/metadata/v11.gz.metadata.json",
            "deadbeef",
        );
        assert_eq!(
            gz_alt,
            "s3://bucket/table/metadata/v12.metadata.json".to_string()
        );
    }

    #[test]
    fn falls_back_to_uuid_when_version_missing() {
        let path = next_metadata_path(
            "s3://bucket/table",
            "s3://bucket/table/metadata/metadata.json",
            "deadbeef",
        );
        assert_eq!(
            path,
            "s3://bucket/table/metadata/deadbeef.metadata.json".to_string()
        );
    }
}
