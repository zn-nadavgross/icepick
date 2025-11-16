//! Iceberg manifest file handling
//!
//! Manifests are Avro files that track data files in an Iceberg table.
//! Each snapshot has a manifest list (Avro) that references one or more
//! manifest files (Avro), which contain data file metadata.
//!
//! This module provides:
//! - Avro schema definitions for v2 format
//! - Conversion from Iceberg types to Avro values
//! - Writers for manifest and manifest list files

pub mod avro;
pub mod schema;
pub mod writer;

pub use avro::data_file_to_avro;
pub use schema::{manifest_entry_schema_v2, manifest_list_schema_v2};
pub use writer::{write_manifest, write_manifest_list};
