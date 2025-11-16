//! Avro schemas for Iceberg manifest files (v2 format)

use apache_avro::Schema;

/// Returns the Avro schema for manifest entries in Iceberg v2 format
pub fn manifest_entry_schema_v2() -> Result<Schema, apache_avro::Error> {
    todo!("Implement manifest entry schema")
}

/// Returns the Avro schema for manifest lists in Iceberg v2 format
pub fn manifest_list_schema_v2() -> Result<Schema, apache_avro::Error> {
    todo!("Implement manifest list schema")
}
