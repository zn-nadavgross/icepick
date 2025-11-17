//! Iceberg data types
//! Vendored from iceberg-rust v0.7.0

mod primitive;

pub use primitive::PrimitiveType;

use serde::{Deserialize, Serialize};

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
    #[serde(rename = "type")]
    r#type: String,
    #[serde(rename = "element-id")]
    element_id: i32,
    #[serde(rename = "element-required")]
    element_required: bool,
    #[serde(rename = "element")]
    element_type: Box<Type>,
}

impl ListType {
    /// Construct a new list type
    pub fn new(element_id: i32, element_required: bool, element_type: Type) -> Self {
        Self {
            r#type: "list".to_string(),
            element_id,
            element_required,
            element_type: Box::new(element_type),
        }
    }

    /// Get the element type
    pub fn element_type(&self) -> &Type {
        &self.element_type
    }

    /// Whether the element is required
    pub fn element_required(&self) -> bool {
        self.element_required
    }
}

/// Placeholder for map type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapType {
    #[serde(rename = "type")]
    r#type: String,
    #[serde(rename = "key-id")]
    key_id: i32,
    #[serde(rename = "key")]
    key_type: Box<Type>,
    #[serde(rename = "value-id")]
    value_id: i32,
    #[serde(rename = "value-required")]
    value_required: bool,
    #[serde(rename = "value")]
    value_type: Box<Type>,
}

impl MapType {
    /// Construct a new map type
    pub fn new(
        key_id: i32,
        key_type: Type,
        value_id: i32,
        value_required: bool,
        value_type: Type,
    ) -> Self {
        Self {
            r#type: "map".to_string(),
            key_id,
            key_type: Box::new(key_type),
            value_id,
            value_required,
            value_type: Box::new(value_type),
        }
    }

    /// Get the key type
    pub fn key_type(&self) -> &Type {
        &self.key_type
    }

    /// Get the value type
    pub fn value_type(&self) -> &Type {
        &self.value_type
    }

    /// Whether the value is required
    pub fn value_required(&self) -> bool {
        self.value_required
    }
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
