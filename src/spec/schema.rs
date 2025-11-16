//! Iceberg schema
//! Vendored from iceberg-rust v0.7.0

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::spec::types::{NestedField, StructType};

/// An Iceberg schema
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    #[serde(rename = "schema-id")]
    schema_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    identifier_field_ids: Option<Vec<i32>>,
    #[serde(flatten)]
    struct_type: StructType,
}

impl Schema {
    /// Create a schema builder
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::default()
    }

    /// Get the schema ID
    pub fn schema_id(&self) -> i32 {
        self.schema_id
    }

    /// Get the fields
    pub fn fields(&self) -> &[NestedField] {
        self.struct_type.fields()
    }

    /// Get the schema as a struct type
    pub fn as_struct(&self) -> &StructType {
        &self.struct_type
    }

    /// Get identifier field IDs
    pub fn identifier_field_ids(&self) -> Option<&[i32]> {
        self.identifier_field_ids.as_deref()
    }
}

/// Builder for Schema
#[derive(Default)]
pub struct SchemaBuilder {
    schema_id: Option<i32>,
    identifier_field_ids: Option<Vec<i32>>,
    fields: Option<Vec<NestedField>>,
}

impl SchemaBuilder {
    /// Set the schema ID
    pub fn with_schema_id(mut self, schema_id: i32) -> Self {
        self.schema_id = Some(schema_id);
        self
    }

    /// Set identifier field IDs
    pub fn with_identifier_field_ids(mut self, ids: Vec<i32>) -> Self {
        self.identifier_field_ids = Some(ids);
        self
    }

    /// Set the fields
    pub fn with_fields(mut self, fields: Vec<NestedField>) -> Self {
        self.fields = Some(fields);
        self
    }

    /// Build the schema
    pub fn build(self) -> Result<Schema> {
        let fields = self
            .fields
            .ok_or_else(|| Error::invalid_request("Schema must have fields"))?;

        Ok(Schema {
            schema_id: self.schema_id.unwrap_or(1), // Default to 1 for new tables, matching iceberg-rust
            identifier_field_ids: self.identifier_field_ids,
            struct_type: StructType::new(fields),
        })
    }
}
