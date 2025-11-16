//! Data file metadata
//! Vendored from iceberg-rust v0.7.0

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Content type of a data file
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataContentType {
    /// Regular data
    Data,
    /// Position deletes
    PositionDeletes,
    /// Equality deletes
    EqualityDeletes,
}

impl Default for DataContentType {
    fn default() -> Self {
        Self::Data
    }
}

/// Metadata about a data file in an Iceberg table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFile {
    #[serde(rename = "content")]
    content_type: DataContentType,
    #[serde(rename = "file-path")]
    file_path: String,
    #[serde(rename = "file-format")]
    file_format: String,
    #[serde(rename = "record-count")]
    record_count: i64,
    #[serde(rename = "file-size-in-bytes")]
    file_size_in_bytes: i64,
    #[serde(rename = "column-sizes", skip_serializing_if = "Option::is_none")]
    column_sizes: Option<HashMap<i32, i64>>,
    #[serde(rename = "value-counts", skip_serializing_if = "Option::is_none")]
    value_counts: Option<HashMap<i32, i64>>,
    #[serde(rename = "null-value-counts", skip_serializing_if = "Option::is_none")]
    null_value_counts: Option<HashMap<i32, i64>>,
    #[serde(rename = "lower-bounds", skip_serializing_if = "Option::is_none")]
    lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    #[serde(rename = "upper-bounds", skip_serializing_if = "Option::is_none")]
    upper_bounds: Option<HashMap<i32, Vec<u8>>>,
}

impl DataFile {
    /// Create a data file builder
    pub fn builder() -> DataFileBuilder {
        DataFileBuilder::default()
    }

    /// Get content type
    pub fn content_type(&self) -> DataContentType {
        self.content_type
    }

    /// Get file path
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Get file format
    pub fn file_format(&self) -> &str {
        &self.file_format
    }

    /// Get record count
    pub fn record_count(&self) -> i64 {
        self.record_count
    }

    /// Get file size in bytes
    pub fn file_size_in_bytes(&self) -> i64 {
        self.file_size_in_bytes
    }

    /// Get column sizes
    pub fn column_sizes(&self) -> Option<&HashMap<i32, i64>> {
        self.column_sizes.as_ref()
    }

    /// Get value counts
    pub fn value_counts(&self) -> Option<&HashMap<i32, i64>> {
        self.value_counts.as_ref()
    }

    /// Get null value counts
    pub fn null_value_counts(&self) -> Option<&HashMap<i32, i64>> {
        self.null_value_counts.as_ref()
    }
}

/// Builder for DataFile
#[derive(Default)]
pub struct DataFileBuilder {
    content_type: Option<DataContentType>,
    file_path: Option<String>,
    file_format: Option<String>,
    record_count: Option<i64>,
    file_size_in_bytes: Option<i64>,
    column_sizes: Option<HashMap<i32, i64>>,
    value_counts: Option<HashMap<i32, i64>>,
    null_value_counts: Option<HashMap<i32, i64>>,
    lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    upper_bounds: Option<HashMap<i32, Vec<u8>>>,
}

impl DataFileBuilder {
    pub fn with_content_type(mut self, content_type: DataContentType) -> Self {
        self.content_type = Some(content_type);
        self
    }

    pub fn with_file_path(mut self, path: &str) -> Self {
        self.file_path = Some(path.to_string());
        self
    }

    pub fn with_file_format(mut self, format: &str) -> Self {
        self.file_format = Some(format.to_string());
        self
    }

    pub fn with_record_count(mut self, count: i64) -> Self {
        self.record_count = Some(count);
        self
    }

    pub fn with_file_size_in_bytes(mut self, size: i64) -> Self {
        self.file_size_in_bytes = Some(size);
        self
    }

    pub fn with_column_sizes(mut self, sizes: HashMap<i32, i64>) -> Self {
        self.column_sizes = Some(sizes);
        self
    }

    pub fn with_value_counts(mut self, counts: HashMap<i32, i64>) -> Self {
        self.value_counts = Some(counts);
        self
    }

    pub fn with_null_value_counts(mut self, counts: HashMap<i32, i64>) -> Self {
        self.null_value_counts = Some(counts);
        self
    }

    pub fn build(self) -> Result<DataFile> {
        Ok(DataFile {
            content_type: self.content_type.unwrap_or_default(),
            file_path: self
                .file_path
                .ok_or_else(|| Error::InvalidInput("DataFile must have file path".to_string()))?,
            file_format: self
                .file_format
                .ok_or_else(|| Error::InvalidInput("DataFile must have file format".to_string()))?,
            record_count: self.record_count.ok_or_else(|| {
                Error::InvalidInput("DataFile must have record count".to_string())
            })?,
            file_size_in_bytes: self
                .file_size_in_bytes
                .ok_or_else(|| Error::InvalidInput("DataFile must have file size".to_string()))?,
            column_sizes: self.column_sizes,
            value_counts: self.value_counts,
            null_value_counts: self.null_value_counts,
            lower_bounds: self.lower_bounds,
            upper_bounds: self.upper_bounds,
        })
    }
}
