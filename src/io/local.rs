//! Local filesystem utilities for the CLI
//!
//! This module provides helpers for working with local Parquet files,
//! including detection of local paths and creating FileIO instances
//! for local filesystem operations.

use crate::error::{Error, Result};
use crate::io::FileIO;
use opendal::Operator;

/// Check if a path is a local filesystem path (has no URI scheme)
pub fn is_local_path(path: &str) -> bool {
    !path.contains("://")
}

/// Create a FileIO for local filesystem operations
///
/// Creates an OpenDAL Fs operator rooted at the parent directory of the given path.
/// This allows reading the file using just its filename.
///
/// # Arguments
/// * `path` - Absolute path to a local file
///
/// # Returns
/// A FileIO configured for local filesystem access
pub fn create_local_file_io(path: &str) -> Result<FileIO> {
    use opendal::services::Fs;
    use std::path::Path;

    let file_path = Path::new(path);
    let root = file_path.parent().and_then(|p| p.to_str()).unwrap_or("/");

    let builder = Fs::default().root(root);
    let operator = Operator::new(builder)
        .map_err(|e| Error::IoError(format!("Failed to create local operator: {}", e)))?
        .finish();

    Ok(FileIO::new(operator))
}

/// Get the filename portion of a path
///
/// # Arguments
/// * `path` - A file path (local or remote)
///
/// # Returns
/// The filename component of the path
pub fn get_filename(path: &str) -> &str {
    std::path::Path::new(path)
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_local_path() {
        assert!(is_local_path("/path/to/file.parquet"));
        assert!(is_local_path("./relative/file.parquet"));
        assert!(is_local_path("file.parquet"));
        assert!(!is_local_path("s3://bucket/file.parquet"));
        assert!(!is_local_path("https://example.com/file.parquet"));
    }

    #[test]
    fn test_get_filename() {
        assert_eq!(get_filename("/path/to/file.parquet"), "file.parquet");
        assert_eq!(get_filename("file.parquet"), "file.parquet");
        assert_eq!(
            get_filename("s3://bucket/path/file.parquet"),
            "file.parquet"
        );
    }
}
