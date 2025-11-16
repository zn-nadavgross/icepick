//! Write manifest and manifest list files

use crate::error::Result;
use crate::io::FileIO;
use crate::spec::DataFile;

/// Write a manifest file containing data file entries
///
/// Returns the number of bytes written
pub async fn write_manifest(
    _file_io: &FileIO,
    _path: &str,
    _data_files: &[DataFile],
    _snapshot_id: i64,
    _sequence_number: i64,
) -> Result<i64> {
    todo!("Implement manifest writing")
}
