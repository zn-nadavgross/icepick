//! Iceberg data types
//! Vendored from iceberg-rust v0.7.0

use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Primitive data types in Iceberg
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

impl Serialize for PrimitiveType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            PrimitiveType::Boolean => serializer.serialize_str("boolean"),
            PrimitiveType::Int => serializer.serialize_str("int"),
            PrimitiveType::Long => serializer.serialize_str("long"),
            PrimitiveType::Float => serializer.serialize_str("float"),
            PrimitiveType::Double => serializer.serialize_str("double"),
            PrimitiveType::Decimal { precision, scale } => {
                let mut state = serializer.serialize_struct("DecimalType", 3)?;
                state.serialize_field("type", "decimal")?;
                state.serialize_field("precision", precision)?;
                state.serialize_field("scale", scale)?;
                state.end()
            }
            PrimitiveType::Date => serializer.serialize_str("date"),
            PrimitiveType::Time => serializer.serialize_str("time"),
            PrimitiveType::Timestamp => serializer.serialize_str("timestamp"),
            PrimitiveType::Timestamptz => serializer.serialize_str("timestamptz"),
            PrimitiveType::String => serializer.serialize_str("string"),
            PrimitiveType::Uuid => serializer.serialize_str("uuid"),
            PrimitiveType::Fixed(length) => {
                let mut state = serializer.serialize_struct("FixedType", 2)?;
                state.serialize_field("type", "fixed")?;
                state.serialize_field("length", length)?;
                state.end()
            }
            PrimitiveType::Binary => serializer.serialize_str("binary"),
        }
    }
}

impl<'de> Deserialize<'de> for PrimitiveType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PrimitiveVisitor;

        impl<'de> Visitor<'de> for PrimitiveVisitor {
            type Value = PrimitiveType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an Iceberg primitive type")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "boolean" => Ok(PrimitiveType::Boolean),
                    "int" => Ok(PrimitiveType::Int),
                    "long" => Ok(PrimitiveType::Long),
                    "float" => Ok(PrimitiveType::Float),
                    "double" => Ok(PrimitiveType::Double),
                    "date" => Ok(PrimitiveType::Date),
                    "time" => Ok(PrimitiveType::Time),
                    "timestamp" => Ok(PrimitiveType::Timestamp),
                    "timestamptz" => Ok(PrimitiveType::Timestamptz),
                    "string" => Ok(PrimitiveType::String),
                    "uuid" => Ok(PrimitiveType::Uuid),
                    "binary" => Ok(PrimitiveType::Binary),
                    _ => Err(E::unknown_variant(
                        value,
                        &[
                            "boolean",
                            "int",
                            "long",
                            "float",
                            "double",
                            "date",
                            "time",
                            "timestamp",
                            "timestamptz",
                            "string",
                            "uuid",
                            "binary",
                        ],
                    )),
                }
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut ty: Option<String> = None;
                let mut length: Option<u64> = None;
                let mut precision: Option<u32> = None;
                let mut scale: Option<u32> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "type" => {
                            if ty.is_some() {
                                return Err(de::Error::duplicate_field("type"));
                            }
                            ty = Some(map.next_value()?);
                        }
                        "length" => {
                            if length.is_some() {
                                return Err(de::Error::duplicate_field("length"));
                            }
                            length = Some(map.next_value()?);
                        }
                        "precision" => {
                            if precision.is_some() {
                                return Err(de::Error::duplicate_field("precision"));
                            }
                            precision = Some(map.next_value()?);
                        }
                        "scale" => {
                            if scale.is_some() {
                                return Err(de::Error::duplicate_field("scale"));
                            }
                            scale = Some(map.next_value()?);
                        }
                        _ => {
                            let _ = map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }

                let ty = ty.ok_or_else(|| de::Error::missing_field("type"))?;
                match ty.as_str() {
                    "fixed" => {
                        let length = length.ok_or_else(|| de::Error::missing_field("length"))?;
                        Ok(PrimitiveType::Fixed(length))
                    }
                    "decimal" => {
                        let precision =
                            precision.ok_or_else(|| de::Error::missing_field("precision"))?;
                        let scale = scale.ok_or_else(|| de::Error::missing_field("scale"))?;
                        Ok(PrimitiveType::Decimal { precision, scale })
                    }
                    other => Err(de::Error::unknown_variant(other, &["fixed", "decimal"])),
                }
            }
        }

        deserializer.deserialize_any(PrimitiveVisitor)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn get_type_field(value: &Value) -> Option<&str> {
        value.get("type")?.as_str()
    }

    #[test]
    fn struct_type_serializes_with_type_field() {
        let struct_type = StructType::new(vec![NestedField::required_field(
            1,
            "field".into(),
            Type::Primitive(PrimitiveType::Int),
        )]);
        let json = serde_json::to_value(&struct_type).unwrap();
        assert_eq!(get_type_field(&json), Some("struct"));
    }

    #[test]
    fn list_type_serializes_with_type_field() {
        let list_type = ListType::new(2, true, Type::Primitive(PrimitiveType::String));
        let json = serde_json::to_value(&list_type).unwrap();
        assert_eq!(get_type_field(&json), Some("list"));
        assert!(json.get("element-id").is_some());
        assert!(json.get("element-required").is_some());
        assert!(json.get("element").is_some());
    }

    #[test]
    fn map_type_serializes_with_type_field() {
        let map_type = MapType::new(
            3,
            Type::Primitive(PrimitiveType::String),
            4,
            false,
            Type::Primitive(PrimitiveType::Long),
        );
        let json = serde_json::to_value(&map_type).unwrap();
        assert_eq!(get_type_field(&json), Some("map"));
        assert!(json.get("key-id").is_some());
        assert!(json.get("key").is_some());
        assert!(json.get("value-id").is_some());
        assert!(json.get("value-required").is_some());
        assert!(json.get("value").is_some());
    }

    #[test]
    fn primitive_string_serializes_as_str() {
        let json = serde_json::to_value(Type::Primitive(PrimitiveType::String)).unwrap();
        assert_eq!(json, serde_json::Value::String("string".to_string()));
    }

    #[test]
    fn primitive_fixed_serializes_with_type() {
        let json = serde_json::to_value(Type::Primitive(PrimitiveType::Fixed(16))).unwrap();
        assert_eq!(get_type_field(&json), Some("fixed"));
        assert_eq!(json.get("length").and_then(|v| v.as_u64()), Some(16));
    }

    #[test]
    fn primitive_decimal_serializes_with_type() {
        let json = serde_json::to_value(Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        }))
        .unwrap();
        assert_eq!(get_type_field(&json), Some("decimal"));
        assert_eq!(json.get("precision").and_then(|v| v.as_u64()), Some(10));
        assert_eq!(json.get("scale").and_then(|v| v.as_u64()), Some(2));
    }

    #[test]
    fn primitive_fixed_deserializes_from_spec_json() {
        let value = serde_json::json!({"type": "fixed", "length": 4});
        let ty: Type = serde_json::from_value(value).unwrap();
        assert!(matches!(ty, Type::Primitive(PrimitiveType::Fixed(4))));
    }

    #[test]
    fn primitive_decimal_deserializes_from_spec_json() {
        let value = serde_json::json!({"type": "decimal", "precision": 5, "scale": 3});
        let ty: Type = serde_json::from_value(value).unwrap();
        assert!(matches!(
            ty,
            Type::Primitive(PrimitiveType::Decimal {
                precision: 5,
                scale: 3
            })
        ));
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
