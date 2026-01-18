//! Manifest list reading

use super::extract::extract_string;
use super::parse::parse_manifest_file_info;
use super::ManifestFileInfo;
use crate::error::{Error, Result};
use crate::io::FileIO;
use apache_avro::types::Value;
use apache_avro::Reader as AvroReader;

/// Reads manifest list files
pub struct ManifestListReader;

impl ManifestListReader {
    /// Read a manifest list and return the paths to manifest files
    pub async fn read(file_io: &FileIO, manifest_list_path: &str) -> Result<Vec<String>> {
        let bytes = file_io.read(manifest_list_path).await?;

        let reader = AvroReader::new(&bytes[..])
            .map_err(|e| Error::invalid_input(format!("Failed to read manifest list: {}", e)))?;

        Ok(reader
            .filter_map(|value| {
                let apache_avro::types::Value::Record(fields) = value.ok()? else {
                    return None;
                };
                fields.into_iter().find_map(|(name, value)| {
                    (name == "manifest_path").then_some(extract_string(&value))?
                })
            })
            .collect())
    }

    /// Read a manifest list and return detailed manifest file information
    pub async fn read_entries(
        file_io: &FileIO,
        manifest_list_path: &str,
    ) -> Result<Vec<ManifestFileInfo>> {
        let bytes = file_io.read(manifest_list_path).await?;

        let reader = AvroReader::new(&bytes[..])
            .map_err(|e| Error::invalid_input(format!("Failed to read manifest list: {}", e)))?;

        let mut entries = Vec::new();

        for (idx, value) in reader.enumerate() {
            let value = value.map_err(|e| {
                Error::invalid_input(format!(
                    "Failed to parse manifest list entry {}: {}",
                    idx, e
                ))
            })?;

            if let Value::Record(fields) = value {
                entries.push(parse_manifest_file_info(fields).map_err(|e| {
                    Error::invalid_input(format!("Invalid manifest list entry {}: {}", idx, e))
                })?);
            }
        }

        Ok(entries)
    }
}
