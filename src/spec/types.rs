//! Iceberg data types
//! Vendored from iceberg-rust v0.7.0

use serde::{Deserialize, Serialize};

/// Primitive data types in Iceberg
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PrimitiveType {
    /// True or false
    Boolean,
    /// 32-bit signed integer
    Int,
    /// 64-bit signed integer
    Long,
    /// 32-bit IEEE 754 floating point
    Float,
    /// 64-bit IEEE 754 floating point
    Double,
    /// Fixed-point decimal
    Decimal {
        /// Precision (total number of digits)
        precision: u32,
        /// Scale (digits after decimal point)
        scale: u32,
    },
    /// Calendar date without timezone
    Date,
    /// Time of day without timezone (microsecond precision)
    Time,
    /// Timestamp without timezone (microsecond precision)
    Timestamp,
    /// Timestamp with timezone (microsecond precision)
    Timestamptz,
    /// Variable-length string
    String,
    /// UUID (16 bytes)
    Uuid,
    /// Fixed-length byte array
    Fixed(u64),
    /// Variable-length byte array
    Binary,
}

/// Iceberg type - either primitive or nested
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Type {
    /// Primitive type
    Primitive(PrimitiveType),
    /// Struct type (to be implemented)
    Struct(StructType),
    /// List type (to be implemented)
    List(ListType),
    /// Map type (to be implemented)
    Map(MapType),
}

/// A struct type (record with named fields)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructType {
    #[serde(rename = "type")]
    r#type: String,
    #[serde(rename = "fields")]
    fields: Vec<NestedField>,
}

impl StructType {
    /// Create a new struct type
    pub fn new(fields: Vec<NestedField>) -> Self {
        Self {
            r#type: "struct".to_string(),
            fields,
        }
    }

    /// Get the fields in this struct
    pub fn fields(&self) -> &[NestedField] {
        &self.fields
    }

    /// Get a field by name
    pub fn field_by_name(&self, name: &str) -> Option<&NestedField> {
        self.fields.iter().find(|f| f.name() == name)
    }

    /// Get a field by ID
    pub fn field_by_id(&self, id: i32) -> Option<&NestedField> {
        self.fields.iter().find(|f| f.id() == id)
    }
}

/// Placeholder for list type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListType {
    element_id: i32,
    element_required: bool,
    element_type: Box<Type>,
}

/// Placeholder for map type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapType {
    key_id: i32,
    key_type: Box<Type>,
    value_id: i32,
    value_required: bool,
    value_type: Box<Type>,
}

/// A field in a struct type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NestedField {
    id: i32,
    name: String,
    required: bool,
    #[serde(rename = "type")]
    field_type: Type,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc: Option<String>,
}

impl NestedField {
    /// Create a new nested field
    pub fn new(
        id: i32,
        name: String,
        field_type: Type,
        required: bool,
        doc: Option<String>,
    ) -> Self {
        Self {
            id,
            name,
            required,
            field_type,
            doc,
        }
    }

    /// Create a required field
    pub fn required_field(id: i32, name: String, field_type: Type) -> Self {
        Self::new(id, name, field_type, true, None)
    }

    /// Create an optional field
    pub fn optional_field(id: i32, name: String, field_type: Type) -> Self {
        Self::new(id, name, field_type, false, None)
    }

    /// Get field ID
    pub fn id(&self) -> i32 {
        self.id
    }

    /// Get field name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Check if field is required
    pub fn is_required(&self) -> bool {
        self.required
    }

    /// Get field type
    pub fn field_type(&self) -> &Type {
        &self.field_type
    }

    /// Get field documentation
    pub fn doc(&self) -> Option<&str> {
        self.doc.as_deref()
    }
}
