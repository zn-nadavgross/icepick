//! Schema evolution utilities for merging schemas and checking compatibility

use crate::error::{Error, Result};
use crate::spec::schema::Schema;
use crate::spec::types::{NestedField, Type};
use std::collections::HashMap;

/// Check if two schemas are compatible for appending
///
/// Returns true if the incoming schema can be used to append to a table with the existing schema.
/// This allows for missing fields (existing fields not present in incoming) but rejects type changes.
pub fn schemas_compatible(existing: &Schema, incoming: &Schema) -> bool {
    // All fields in incoming must be compatible with corresponding fields in existing
    for incoming_field in incoming.fields() {
        if let Some(existing_field) = existing.as_struct().field_by_name(incoming_field.name()) {
            if !types_compatible(existing_field.field_type(), incoming_field.field_type()) {
                return false;
            }
        }
    }
    true
}

/// Check if an incoming schema has new fields compared to existing schema
pub fn has_new_fields(existing: &Schema, incoming: &Schema) -> bool {
    for incoming_field in incoming.fields() {
        if existing
            .as_struct()
            .field_by_name(incoming_field.name())
            .is_none()
        {
            return true;
        }
    }
    false
}

/// Merge two schemas, adding new fields from incoming schema to existing schema
///
/// Preserves all existing field IDs and assigns new IDs to new fields starting from
/// the maximum existing field ID + 1.
pub fn merge_schemas(existing: &Schema, incoming: &Schema) -> Result<Schema> {
    // Validate compatibility first
    if !schemas_compatible(existing, incoming) {
        return Err(Error::invalid_input(
            "Schemas are not compatible for merging - type mismatch detected",
        ));
    }

    // Find maximum field ID in existing schema
    let max_existing_id = find_max_field_id(existing);
    let mut next_id = max_existing_id + 1;

    // Build mapping of field names to existing fields
    let mut existing_fields: HashMap<String, &NestedField> = HashMap::new();
    for field in existing.fields() {
        existing_fields.insert(field.name().to_string(), field);
    }

    // Start with all existing fields
    let mut merged_fields = existing.fields().to_vec();

    // Add new fields from incoming schema
    for incoming_field in incoming.fields() {
        if !existing_fields.contains_key(incoming_field.name()) {
            // This is a new field - assign it a new ID
            let new_field = NestedField::new(
                next_id,
                incoming_field.name().to_string(),
                incoming_field.field_type().clone(),
                incoming_field.is_required(),
                incoming_field.doc().map(|s| s.to_string()),
            );
            merged_fields.push(new_field);
            next_id += 1;
        }
    }

    // Build new schema with merged fields
    Schema::builder()
        .with_schema_id(existing.schema_id())
        .with_fields(merged_fields)
        .build()
}

/// Find the maximum field ID in a schema (including nested fields)
fn find_max_field_id(schema: &Schema) -> i32 {
    let mut max_id = 0;
    for field in schema.fields() {
        max_id = max_id.max(field.id());
        max_id = max_id.max(find_max_field_id_in_type(field.field_type()));
    }
    max_id
}

/// Find the maximum field ID within a type (for nested structures)
fn find_max_field_id_in_type(field_type: &Type) -> i32 {
    match field_type {
        Type::Primitive(_) => 0,
        Type::Struct(struct_type) => {
            let mut max_id = 0;
            for field in struct_type.fields() {
                max_id = max_id.max(field.id());
                max_id = max_id.max(find_max_field_id_in_type(field.field_type()));
            }
            max_id
        }
        Type::List(list_type) => find_max_field_id_in_type(list_type.element_type()),
        Type::Map(map_type) => {
            let key_max = find_max_field_id_in_type(map_type.key_type());
            let value_max = find_max_field_id_in_type(map_type.value_type());
            key_max.max(value_max)
        }
    }
}

/// Check if two types are compatible
///
/// Types are compatible if:
/// - Both are the same primitive type
/// - Both are structs and all existing fields in the first struct have compatible types in the second
/// - Both are lists with compatible element types
/// - Both are maps with compatible key and value types
fn types_compatible(existing: &Type, incoming: &Type) -> bool {
    match (existing, incoming) {
        (Type::Primitive(a), Type::Primitive(b)) => a == b,
        (Type::Struct(a), Type::Struct(b)) => {
            // All existing fields must be present with compatible types
            // New fields in incoming are allowed
            for existing_field in a.fields() {
                match b.field_by_name(existing_field.name()) {
                    Some(incoming_field) => {
                        if !types_compatible(
                            existing_field.field_type(),
                            incoming_field.field_type(),
                        ) {
                            return false;
                        }
                    }
                    None => {
                        // Existing field missing in incoming - this is allowed if field is nullable
                        // For now we allow it (incoming batch just doesn't have this field)
                    }
                }
            }
            true
        }
        (Type::List(a), Type::List(b)) => types_compatible(a.element_type(), b.element_type()),
        (Type::Map(a), Type::Map(b)) => {
            types_compatible(a.key_type(), b.key_type())
                && types_compatible(a.value_type(), b.value_type())
        }
        _ => false, // Primitive <-> Complex is incompatible
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::types::PrimitiveType;

    #[test]
    fn test_types_compatible_primitives() {
        let int_type = Type::Primitive(PrimitiveType::Int);
        let long_type = Type::Primitive(PrimitiveType::Long);
        let string_type = Type::Primitive(PrimitiveType::String);

        assert!(types_compatible(&int_type, &int_type));
        assert!(!types_compatible(&int_type, &long_type));
        assert!(!types_compatible(&int_type, &string_type));
    }

    #[test]
    fn test_merge_schemas_adds_new_fields() {
        let existing = Schema::builder()
            .with_fields(vec![NestedField::required_field(
                1,
                "id".to_string(),
                Type::Primitive(PrimitiveType::Long),
            )])
            .build()
            .unwrap();

        let incoming = Schema::builder()
            .with_fields(vec![
                NestedField::required_field(
                    10,
                    "id".to_string(),
                    Type::Primitive(PrimitiveType::Long),
                ),
                NestedField::optional_field(
                    20,
                    "name".to_string(),
                    Type::Primitive(PrimitiveType::String),
                ),
            ])
            .build()
            .unwrap();

        let merged = merge_schemas(&existing, &incoming).unwrap();

        assert_eq!(merged.fields().len(), 2);
        assert_eq!(merged.fields()[0].name(), "id");
        assert_eq!(merged.fields()[0].id(), 1); // Preserved from existing
        assert_eq!(merged.fields()[1].name(), "name");
        assert_eq!(merged.fields()[1].id(), 2); // New ID assigned (max_existing + 1)
    }

    #[test]
    fn test_merge_schemas_rejects_type_changes() {
        let existing = Schema::builder()
            .with_fields(vec![NestedField::required_field(
                1,
                "id".to_string(),
                Type::Primitive(PrimitiveType::Long),
            )])
            .build()
            .unwrap();

        let incoming = Schema::builder()
            .with_fields(vec![NestedField::required_field(
                10,
                "id".to_string(),
                Type::Primitive(PrimitiveType::String),
            )])
            .build()
            .unwrap();

        let result = merge_schemas(&existing, &incoming);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not compatible"));
    }

    #[test]
    fn test_has_new_fields() {
        let existing = Schema::builder()
            .with_fields(vec![NestedField::required_field(
                1,
                "id".to_string(),
                Type::Primitive(PrimitiveType::Long),
            )])
            .build()
            .unwrap();

        let incoming_same = Schema::builder()
            .with_fields(vec![NestedField::required_field(
                1,
                "id".to_string(),
                Type::Primitive(PrimitiveType::Long),
            )])
            .build()
            .unwrap();

        let incoming_new = Schema::builder()
            .with_fields(vec![
                NestedField::required_field(
                    1,
                    "id".to_string(),
                    Type::Primitive(PrimitiveType::Long),
                ),
                NestedField::optional_field(
                    2,
                    "name".to_string(),
                    Type::Primitive(PrimitiveType::String),
                ),
            ])
            .build()
            .unwrap();

        assert!(!has_new_fields(&existing, &incoming_same));
        assert!(has_new_fields(&existing, &incoming_new));
    }
}
