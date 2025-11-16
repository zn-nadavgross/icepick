use anyhow::{anyhow, Result};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

/// Validate that all fields in an Arrow schema have PARQUET:field_id metadata
///
/// This function recursively checks all fields at all nesting levels to ensure
/// that the schema is compatible with rust-iceberg's requirements. The rust-iceberg
/// library does NOT auto-assign field IDs - any missing ID will cause a `DataInvalid`
/// error when attempting to write data.
///
/// # Arguments
///
/// * `schema` - The Arrow schema to validate
///
/// # Returns
///
/// * `Ok(())` if all fields have field IDs
/// * `Err` with a descriptive message indicating which field is missing a field ID
///
/// # Example
///
/// ```
/// use arrow::datatypes::{DataType, Field, Schema};
/// use hello_world_iceberg::validation::validate_field_ids;
/// use std::collections::HashMap;
///
/// // Schema with field IDs - valid
/// let field = Field::new("id", DataType::Int64, false)
///     .with_metadata(HashMap::from([("PARQUET:field_id".to_string(), "1".to_string())]));
/// let schema = Schema::new(vec![field]);
/// assert!(validate_field_ids(&schema).is_ok());
///
/// // Schema without field IDs - invalid
/// let field = Field::new("id", DataType::Int64, false);
/// let schema = Schema::new(vec![field]);
/// assert!(validate_field_ids(&schema).is_err());
/// ```
pub fn validate_field_ids(schema: &ArrowSchema) -> Result<()> {
    for field in schema.fields() {
        check_field(field, field.name())?;
    }
    Ok(())
}

/// Recursively check a field and its nested children for field IDs
fn check_field(field: &Field, path: &str) -> Result<()> {
    // Check this field has an ID
    if field.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none() {
        return Err(anyhow!(
            "Field '{}' is missing PARQUET:field_id metadata. \
             All fields at all nesting levels must have field IDs for \
             rust-iceberg compatibility. Use iceberg::arrow::schema_to_arrow_schema() \
             to convert from an Iceberg schema, which automatically adds field IDs.",
            path
        ));
    }

    // Recursively check nested fields based on data type
    match field.data_type() {
        DataType::Struct(fields) => {
            for nested in fields.iter() {
                check_field(nested, &format!("{}.{}", path, nested.name()))?;
            }
        }
        DataType::List(element) | DataType::LargeList(element) => {
            check_field(element, &format!("{}[]", path))?;
        }
        DataType::Map(field, _) => {
            // Maps are stored as List<Struct<key, value>>
            if let DataType::Struct(fields) = field.data_type() {
                for nested in fields.iter() {
                    check_field(nested, &format!("{}.{}", path, nested.name()))?;
                }
            }
        }
        _ => {
            // Primitive types - OK
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn field_with_id(name: &str, data_type: DataType, id: i32) -> Field {
        Field::new(name, data_type, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }

    fn field_without_id(name: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, false)
    }

    #[test]
    fn test_flat_schema_with_ids() {
        let schema = Schema::new(vec![
            field_with_id("id", DataType::Int64, 1),
            field_with_id("name", DataType::Utf8, 2),
        ]);

        assert!(validate_field_ids(&schema).is_ok());
    }

    #[test]
    fn test_flat_schema_missing_id() {
        let schema = Schema::new(vec![
            field_with_id("id", DataType::Int64, 1),
            field_without_id("name", DataType::Utf8),
        ]);

        let result = validate_field_ids(&schema);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("name"));
        assert!(err_msg.contains("missing PARQUET:field_id"));
    }

    #[test]
    fn test_nested_struct_with_ids() {
        let nested_fields = Fields::from(vec![
            field_with_id("street", DataType::Utf8, 3),
            field_with_id("city", DataType::Utf8, 4),
        ]);

        let schema = Schema::new(vec![
            field_with_id("id", DataType::Int64, 1),
            field_with_id("address", DataType::Struct(nested_fields), 2),
        ]);

        assert!(validate_field_ids(&schema).is_ok());
    }

    #[test]
    fn test_nested_struct_missing_nested_id() {
        let nested_fields = Fields::from(vec![
            field_with_id("street", DataType::Utf8, 3),
            field_without_id("city", DataType::Utf8), // Missing ID!
        ]);

        let schema = Schema::new(vec![
            field_with_id("id", DataType::Int64, 1),
            field_with_id("address", DataType::Struct(nested_fields), 2),
        ]);

        let result = validate_field_ids(&schema);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("address.city"));
        assert!(err_msg.contains("missing PARQUET:field_id"));
    }

    #[test]
    fn test_list_with_ids() {
        let element = field_with_id("element", DataType::Int64, 2);
        let schema = Schema::new(vec![field_with_id(
            "numbers",
            DataType::List(Arc::new(element)),
            1,
        )]);

        assert!(validate_field_ids(&schema).is_ok());
    }

    #[test]
    fn test_list_missing_element_id() {
        let element = field_without_id("element", DataType::Int64);
        let schema = Schema::new(vec![field_with_id(
            "numbers",
            DataType::List(Arc::new(element)),
            1,
        )]);

        let result = validate_field_ids(&schema);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("numbers[]"));
        assert!(err_msg.contains("missing PARQUET:field_id"));
    }

    #[test]
    fn test_deeply_nested_struct() {
        let inner_fields = Fields::from(vec![field_with_id("value", DataType::Int64, 4)]);

        let middle_fields = Fields::from(vec![field_with_id(
            "inner",
            DataType::Struct(inner_fields),
            3,
        )]);

        let outer_fields = Fields::from(vec![field_with_id(
            "middle",
            DataType::Struct(middle_fields),
            2,
        )]);

        let schema = Schema::new(vec![field_with_id(
            "outer",
            DataType::Struct(outer_fields),
            1,
        )]);

        assert!(validate_field_ids(&schema).is_ok());
    }

    #[test]
    fn test_deeply_nested_struct_missing_deep_id() {
        let inner_fields = Fields::from(vec![field_without_id("value", DataType::Int64)]); // Missing!

        let middle_fields = Fields::from(vec![field_with_id(
            "inner",
            DataType::Struct(inner_fields),
            3,
        )]);

        let outer_fields = Fields::from(vec![field_with_id(
            "middle",
            DataType::Struct(middle_fields),
            2,
        )]);

        let schema = Schema::new(vec![field_with_id(
            "outer",
            DataType::Struct(outer_fields),
            1,
        )]);

        let result = validate_field_ids(&schema);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("outer.middle.inner.value"));
        assert!(err_msg.contains("missing PARQUET:field_id"));
    }
}
