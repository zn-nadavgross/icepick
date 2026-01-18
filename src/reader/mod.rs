//! Reading data from Iceberg tables

pub mod manifest;

pub use manifest::{
    DataFileEntry, DataFileStats, ManifestFileInfo, ManifestListReader, ManifestReader,
};
