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

/// Placeholder for struct type (will implement in next task)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructType {
    fields: Vec<NestedField>,
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

/// Placeholder for nested field (will implement in next task)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NestedField {
    id: i32,
    name: String,
    required: bool,
    field_type: Type,
    doc: Option<String>,
}
