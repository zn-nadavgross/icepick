use anyhow::{Context, Result};
use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{NestedField, PrimitiveType, Schema, StructType, Type};
use std::sync::Arc;

/// Build a nested schema with a struct field
/// Schema: {
///   id: i64,
///   user: struct {
///     name: string,
///     age: i64
///   }
/// }
fn build_nested_schema() -> Result<Schema> {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(
                2,
                "user",
                Type::Struct(StructType::new(vec![
                    NestedField::required(3, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(4, "age", Type::Primitive(PrimitiveType::Long)).into(),
                ])),
            )
            .into(),
        ])
        .build()
        .context("Failed to build nested schema")?;

    Ok(schema)
}

/// Create sample data with nested struct
fn create_nested_data(iceberg_schema: &Schema) -> Result<RecordBatch> {
    // Convert Iceberg schema to Arrow schema - this adds PARQUET:field_id metadata at all nesting levels
    let arrow_schema = schema_to_arrow_schema(iceberg_schema)
        .context("Failed to convert Iceberg schema to Arrow schema")?;

    // Create data arrays
    let id_array = Int64Array::from(vec![1, 2, 3]);

    // Create nested struct data for 'user' field
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
    let age_array = Int64Array::from(vec![30, 25, 35]);

    // Find the user field in the Arrow schema to get its field definition with metadata
    let user_field = arrow_schema
        .field_with_name("user")
        .context("Failed to find 'user' field in Arrow schema")?;

    // Extract the nested fields from the struct type
    let struct_fields = if let arrow::datatypes::DataType::Struct(fields) = user_field.data_type() {
        fields.clone()
    } else {
        anyhow::bail!("Expected user field to be a struct");
    };

    // Create StructArray using the fields from the schema (which have field IDs in metadata)
    let user_struct = StructArray::try_new(
        struct_fields,
        vec![
            Arc::new(name_array) as ArrayRef,
            Arc::new(age_array) as ArrayRef,
        ],
        None,
    )
    .context("Failed to create user struct array")?;

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            Arc::new(id_array) as ArrayRef,
            Arc::new(user_struct) as ArrayRef,
        ],
    )
    .context("Failed to create record batch")?;

    Ok(batch)
}

fn main() -> Result<()> {
    println!("=== Nested Schema Example ===\n");

    let schema = build_nested_schema()?;
    println!("Iceberg Schema:");
    println!("{:#?}\n", schema);

    let arrow_schema =
        schema_to_arrow_schema(&schema).context("Failed to convert to Arrow schema")?;
    println!("Arrow Schema (with field IDs in metadata):");
    for field in arrow_schema.fields() {
        println!("Field: {}", field.name());
        println!("  Type: {:?}", field.data_type());
        println!("  Metadata: {:?}", field.metadata());

        // Print nested field metadata if it's a struct
        if let arrow::datatypes::DataType::Struct(nested_fields) = field.data_type() {
            for nested_field in nested_fields.iter() {
                println!("  Nested Field: {}", nested_field.name());
                println!("    Type: {:?}", nested_field.data_type());
                println!("    Metadata: {:?}", nested_field.metadata());
            }
        }
    }
    println!();

    let batch = create_nested_data(&schema)?;
    println!("Sample Data:");
    arrow::util::pretty::print_batches(std::slice::from_ref(&batch))
        .context("Failed to print batch")?;

    println!("\n✓ Successfully created RecordBatch with nested schema");
    println!("✓ All fields at all nesting levels have PARQUET:field_id metadata");

    Ok(())
}
