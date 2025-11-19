//! Table creation specification

use crate::error::Result;
use crate::spec::{PartitionSpec, Schema};
use std::collections::HashMap;

/// Specification for creating a new table
#[derive(Debug, Clone)]
pub struct TableCreation {
    name: String,
    schema: Schema,
    location: Option<String>,
    properties: HashMap<String, String>,
    partition_spec: Option<PartitionSpec>,
}

impl TableCreation {
    /// Create a new builder
    pub fn builder() -> TableCreationBuilder {
        TableCreationBuilder::default()
    }

    /// Get table name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get schema
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Get optional location
    pub fn location(&self) -> Option<&str> {
        self.location.as_deref()
    }

    /// Get properties
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    /// Get optional partition spec
    pub fn partition_spec(&self) -> Option<&PartitionSpec> {
        self.partition_spec.as_ref()
    }
}

/// Builder for TableCreation
#[derive(Default)]
pub struct TableCreationBuilder {
    name: Option<String>,
    schema: Option<Schema>,
    location: Option<String>,
    properties: HashMap<String, String>,
    partition_spec: Option<PartitionSpec>,
}

impl TableCreationBuilder {
    /// Set table name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set schema
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set optional location
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Add a property
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Set the partition spec
    pub fn with_partition_spec(mut self, spec: PartitionSpec) -> Self {
        self.partition_spec = Some(spec);
        self
    }

    /// Build TableCreation
    pub fn build(self) -> Result<TableCreation> {
        let name = self
            .name
            .ok_or_else(|| crate::error::Error::invalid_input("Table name is required"))?;
        let schema = self
            .schema
            .ok_or_else(|| crate::error::Error::invalid_input("Schema is required"))?;

        Ok(TableCreation {
            name,
            schema,
            location: self.location,
            properties: self.properties,
            partition_spec: self.partition_spec,
        })
    }
}
