//! Reading data from Iceberg tables

pub mod manifest;

pub use manifest::{DataFileEntry, ManifestFileInfo, ManifestListReader, ManifestReader};
