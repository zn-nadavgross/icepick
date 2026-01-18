//! Manifest file reading

use super::parse::{parse_manifest_entry, parse_manifest_entry_with_stats};
use super::{DataFileEntry, DataFileStats};
use crate::error::{Error, Result};
use crate::io::FileIO;
use apache_avro::types::Value;
use apache_avro::Reader as AvroReader;

/// Reads manifest files
pub struct ManifestReader;

impl ManifestReader {
    /// Read a manifest and return data file entries (excluding deleted files)
    pub async fn read(file_io: &FileIO, manifest_path: &str) -> Result<Vec<DataFileEntry>> {
        let bytes = file_io.read(manifest_path).await?;

        let reader = AvroReader::new(&bytes[..])
            .map_err(|e| Error::invalid_input(format!("Failed to read manifest: {}", e)))?;

        let mut data_files = Vec::new();

        for (idx, value) in reader.enumerate() {
            let value = value.map_err(|e| {
                Error::invalid_input(format!("Failed to parse manifest entry {}: {}", idx, e))
            })?;

            if let Value::Record(fields) = value {
                if let Some(entry) = parse_manifest_entry(fields).map_err(|e| {
                    Error::invalid_input(format!("Invalid manifest entry {}: {}", idx, e))
                })? {
                    data_files.push(entry);
                }
            }
        }

        Ok(data_files)
    }

    /// Read a manifest and return data file entries with full statistics for pruning
    pub async fn read_with_stats(
        file_io: &FileIO,
        manifest_path: &str,
    ) -> Result<Vec<DataFileStats>> {
        let bytes = file_io.read(manifest_path).await?;

        let reader = AvroReader::new(&bytes[..])
            .map_err(|e| Error::invalid_input(format!("Failed to read manifest: {}", e)))?;

        let mut data_files = Vec::new();

        for (idx, value) in reader.enumerate() {
            let value = value.map_err(|e| {
                Error::invalid_input(format!("Failed to parse manifest entry {}: {}", idx, e))
            })?;

            if let Value::Record(fields) = value {
                let entry_opt = parse_manifest_entry_with_stats(fields).map_err(|e| {
                    Error::invalid_input(format!("Invalid manifest entry {}: {}", idx, e))
                })?;
                if let Some(entry) = entry_opt {
                    data_files.push(entry);
                }
            }
        }

        Ok(data_files)
    }
}
