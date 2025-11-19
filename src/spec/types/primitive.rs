use serde::de::{self, MapAccess, Visitor};
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
                serializer.serialize_str(&format!("decimal({precision},{scale})"))
            }
            PrimitiveType::Date => serializer.serialize_str("date"),
            PrimitiveType::Time => serializer.serialize_str("time"),
            PrimitiveType::Timestamp => serializer.serialize_str("timestamp"),
            PrimitiveType::Timestamptz => serializer.serialize_str("timestamptz"),
            PrimitiveType::String => serializer.serialize_str("string"),
            PrimitiveType::Uuid => serializer.serialize_str("uuid"),
            PrimitiveType::Fixed(length) => serializer.serialize_str(&format!("fixed[{length}]")),
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
                    other if other.starts_with("decimal(") && other.ends_with(')') => {
                        let trimmed = &other["decimal(".len()..other.len() - 1];
                        let (precision, scale) = trimmed.split_once(',').ok_or_else(|| {
                            E::custom(format!(
                                "decimal type must include precision and scale: {other}"
                            ))
                        })?;
                        let precision = precision.trim().parse().map_err(|err| {
                            E::custom(format!("invalid decimal precision {precision}: {err}"))
                        })?;
                        let scale = scale.trim().parse().map_err(|err| {
                            E::custom(format!("invalid decimal scale {scale}: {err}"))
                        })?;
                        Ok(PrimitiveType::Decimal { precision, scale })
                    }
                    other if other.starts_with("fixed[") && other.ends_with(']') => {
                        let len_str = &other["fixed[".len()..other.len() - 1];
                        let length = len_str.trim().parse().map_err(|err| {
                            E::custom(format!("invalid fixed length {len_str}: {err}"))
                        })?;
                        Ok(PrimitiveType::Fixed(length))
                    }
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
                            "fixed[n]",
                            "decimal(p,s)",
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
