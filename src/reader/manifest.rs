//! Reading Iceberg manifest files

use crate::error::{Error, Result};
use crate::io::FileIO;
use apache_avro::Reader as AvroReader;

/// Information about a data file discovered from manifests
#[derive(Debug, Clone)]
pub struct DataFileEntry {
    /// Path to the data file
    pub file_path: String,
    /// Number of records in the file
    pub record_count: i64,
    /// Size of the file in bytes
    pub file_size_in_bytes: i64,
    /// File format (e.g., "PARQUET")
    pub file_format: String,
}

/// Reads manifest list files
pub struct ManifestListReader;

impl ManifestListReader {
    /// Read a manifest list and return the paths to manifest files
    pub async fn read(file_io: &FileIO, manifest_list_path: &str) -> Result<Vec<String>> {
        let bytes = file_io.read(manifest_list_path).await?;

        let reader = AvroReader::new(&bytes[..])
            .map_err(|e| Error::invalid_input(format!("Failed to read manifest list: {}", e)))?;

        let mut manifest_paths = Vec::new();

        for value in reader {
            let value = value.map_err(|e| {
                Error::invalid_input(format!("Failed to parse manifest list entry: {}", e))
            })?;

            // Extract manifest_path from the Avro record
            if let apache_avro::types::Value::Record(fields) = value {
                for (name, field_value) in fields {
                    if name == "manifest_path" {
                        if let apache_avro::types::Value::String(path) = field_value {
                            manifest_paths.push(path);
                        }
                    }
                }
            }
        }

        Ok(manifest_paths)
    }
}

/// Reads manifest files
pub struct ManifestReader;

impl ManifestReader {
    /// Read a manifest and return data file entries (excluding deleted files)
    pub async fn read(file_io: &FileIO, manifest_path: &str) -> Result<Vec<DataFileEntry>> {
        let bytes = file_io.read(manifest_path).await?;

        let reader = AvroReader::new(&bytes[..])
            .map_err(|e| Error::invalid_input(format!("Failed to read manifest: {}", e)))?;

        let mut data_files = Vec::new();

        for value in reader {
            let value = value.map_err(|e| {
                Error::invalid_input(format!("Failed to parse manifest entry: {}", e))
            })?;

            // Parse the manifest entry
            if let apache_avro::types::Value::Record(fields) = value {
                let mut status: Option<i32> = None;
                let mut data_file_value: Option<apache_avro::types::Value> = None;

                for (name, field_value) in fields {
                    match name.as_str() {
                        "status" => {
                            if let apache_avro::types::Value::Int(s) = field_value {
                                status = Some(s);
                            }
                        }
                        "data_file" => {
                            data_file_value = Some(field_value);
                        }
                        _ => {}
                    }
                }

                // Skip deleted entries (status = 2)
                if let Some(s) = status {
                    if s == 2 {
                        continue;
                    }
                }

                // Parse data_file record
                if let Some(apache_avro::types::Value::Record(data_file_fields)) = data_file_value {
                    let mut file_path: Option<String> = None;
                    let mut file_format: Option<String> = None;
                    let mut record_count: Option<i64> = None;
                    let mut file_size: Option<i64> = None;

                    for (name, field_value) in data_file_fields {
                        match name.as_str() {
                            "file_path" => {
                                if let apache_avro::types::Value::String(s) = field_value {
                                    file_path = Some(s);
                                }
                            }
                            "file_format" => {
                                if let apache_avro::types::Value::String(s) = field_value {
                                    file_format = Some(s);
                                }
                            }
                            "record_count" => {
                                if let apache_avro::types::Value::Long(n) = field_value {
                                    record_count = Some(n);
                                }
                            }
                            "file_size_in_bytes" => {
                                if let apache_avro::types::Value::Long(n) = field_value {
                                    file_size = Some(n);
                                }
                            }
                            _ => {}
                        }
                    }

                    if let (Some(path), Some(format), Some(count), Some(size)) =
                        (file_path, file_format, record_count, file_size)
                    {
                        data_files.push(DataFileEntry {
                            file_path: path,
                            file_format: format,
                            record_count: count,
                            file_size_in_bytes: size,
                        });
                    }
                }
            }
        }

        Ok(data_files)
    }
}
