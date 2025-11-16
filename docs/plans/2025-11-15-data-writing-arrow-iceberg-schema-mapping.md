# Data Writing: Arrow ↔ Iceberg Schema Mapping

**Date:** 2025-11-15
**Status:** Design Complete
**Goal:** Enable writing Arrow RecordBatches to S3 Tables via rust-iceberg with proper field ID mapping

## Problem Statement

The current hello-world-iceberg implementation fails when writing data with the error:
```
Field id 1 not found in struct array
```

**Root Cause:**
- Iceberg schemas have field IDs (integers identifying each field)
- Arrow RecordBatches need `PARQUET:field_id` metadata to map to Iceberg fields
- Current code creates Iceberg schema and Arrow data independently with no mapping

## Production Context

This hello-world example will eventually integrate with otlp2parquet-core, which:
- Converts OpenTelemetry Protocol (OTLP) data to Arrow RecordBatches
- Writes to Parquet files with Iceberg metadata
- Uses complex nested schemas (structs, lists, maps)
- Already embeds `PARQUET:field_id` in Arrow schemas using `field_with_id()` helper

The solution must support:
- Nested structures (not just flat schemas)
- Preserving existing field IDs from otlp2parquet
- Production-ready error handling

## Solution: Iceberg Schema as Source of Truth

### Key Principle

**The Iceberg schema is the single source of truth for field IDs and types.**

All Arrow schemas must be derived from Iceberg schemas to ensure `PARQUET:field_id` metadata is correct at every nesting level.

### Implementation Pattern

```rust
// 1. Define Iceberg schema (assigns field IDs)
let iceberg_schema = Schema::builder()
    .with_fields(vec![
        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
    ])
    .build()?;

// 2. Convert to Arrow schema (adds PARQUET:field_id metadata automatically)
use iceberg::spec::schema_to_arrow_schema;
let arrow_schema = schema_to_arrow_schema(&iceberg_schema)?;

// 3. Build RecordBatch with the derived Arrow schema
let id_array = Int64Array::from(vec![1, 2, 3]);
let batch = RecordBatch::try_new(
    Arc::new(arrow_schema),
    vec![Arc::new(id_array)]
)?;

// 4. Write - mapping now exists!
writer.write(batch).await?;
```

### Nested Structure Support

rust-iceberg's bidirectional conversion handles all types automatically:

| Iceberg Type | Arrow Type | Field ID Handling |
|--------------|------------|-------------------|
| Long | Int64 | Direct mapping |
| String | Utf8 | Direct mapping |
| Struct | Struct | Recursive - all nested fields get IDs |
| List | List | Element type gets field ID |
| Map | Map | Key and value types get field IDs |

**Example with nesting:**
```rust
let iceberg_schema = Schema::builder()
    .with_fields(vec![
        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
        NestedField::optional(2, "user", Type::Struct(StructType::new(vec![
            NestedField::required(3, "name", Type::Primitive(PrimitiveType::String)),
            NestedField::required(4, "age", Type::Primitive(PrimitiveType::Int)),
        ]))),
    ])
    .build()?;

// Arrow schema has PARQUET:field_id for ALL fields including nested
let arrow_schema = schema_to_arrow_schema(&iceberg_schema)?;
```

## Integration with otlp2parquet

### Current State

otlp2parquet schemas have:
- ✅ Top-level field IDs (1-27 for logs schema)
- ❌ Nested field IDs (missing in AnyValue, Map entries, etc.)

### Required Changes (External to this Project)

Add field IDs to all nested structures:

```rust
// Before (will fail rust-iceberg conversion):
fn any_value_fields_for_builder() -> Fields {
    vec![
        Field::new(field::TYPE, DataType::Utf8, false),
        Field::new(field::STRING_VALUE, DataType::Utf8, true),
        // ...
    ].into()
}

// After (will work):
fn any_value_fields_for_builder() -> Fields {
    vec![
        field_with_id(field::TYPE, DataType::Utf8, false, 100),
        field_with_id(field::STRING_VALUE, DataType::Utf8, true, 101),
        field_with_id(field::BOOL_VALUE, DataType::Boolean, true, 102),
        field_with_id(field::INT_VALUE, DataType::Int64, true, 103),
        field_with_id(field::DOUBLE_VALUE, DataType::Float64, true, 104),
        field_with_id(field::BYTES_VALUE, DataType::Binary, true, 105),
        field_with_id(field::JSON_VALUE, DataType::LargeUtf8, true, 106),
    ].into()
}
```

**ID Allocation Strategy:**
- 1-99: Top-level fields (signal-specific)
- 100-199: Shared nested structures (AnyValue, etc.)
- 200+: Signal-specific nested fields

### Integration Pattern

Once otlp2parquet has complete field IDs:

```rust
// Get the Arrow schema from otlp2parquet (has all field IDs)
let arrow_schema = otel_logs_schema();

// Validate field IDs are present (helper function below)
validate_field_ids(&arrow_schema)?;

// Convert to Iceberg for table creation
let iceberg_schema = arrow_schema_to_schema(&arrow_schema)?;

// Create table with Iceberg schema
let table = catalog.create_table(&namespace, TableCreation::builder()
    .name(table_name)
    .schema(iceberg_schema)
    .build()
).await?;

// Write data - RecordBatches from otlp2parquet already have correct schema
writer.write(record_batch).await?;
```

## Critical Discovery: No Auto-Assignment

**Investigation Result:**

rust-iceberg does NOT auto-assign field IDs. The conversion function fails if ANY field at ANY nesting level lacks a `PARQUET:field_id`:

```rust
// From rust-iceberg source:
pub(super) fn get_field_id(field: &Field) -> Result<i32> {
    if let Some(value) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) {
        return value.parse::<i32>()...
    }
    Err(Error::new(
        ErrorKind::DataInvalid,
        "Field id not found in metadata",  // ← FAILS immediately
    ))
}
```

**Implications:**
- ✅ No risk of field ID conflicts
- ✅ ID allocation strategy (1-99, 100-199, etc.) is safe
- ❌ ALL nested fields MUST have IDs or conversion fails
- ❌ Partial schemas will cause errors

## Validation & Error Handling

### Validation Helper

Add to hello-world-iceberg for testing and documentation:

```rust
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

/// Validate that all fields in an Arrow schema have PARQUET:field_id metadata
///
/// Returns Err if any field at any nesting level is missing an ID.
/// This is required for rust-iceberg compatibility.
fn validate_field_ids(schema: &ArrowSchema) -> Result<()> {
    fn check_field(field: &Field, path: &str) -> Result<()> {
        // Check this field has an ID
        if field.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none() {
            return Err(anyhow!(
                "Field '{}' is missing PARQUET:field_id metadata. \
                 All fields at all nesting levels must have field IDs for \
                 rust-iceberg compatibility.",
                path
            ));
        }

        // Recursively check nested fields
        match field.data_type() {
            DataType::Struct(fields) => {
                for nested in fields {
                    check_field(nested, &format!("{}.{}", path, nested.name()))?;
                }
            }
            DataType::List(element) | DataType::LargeList(element) => {
                check_field(element, &format!("{}[]", path))?;
            }
            DataType::Map(field, _) => {
                if let DataType::Struct(fields) = field.data_type() {
                    for nested in fields {
                        check_field(nested, &format!("{}.{}", path, nested.name()))?;
                    }
                }
            }
            _ => {} // Primitive types - OK
        }
        Ok(())
    }

    for field in schema.fields() {
        check_field(field, field.name())?;
    }
    Ok(())
}
```

### Error Scenarios

1. **Missing field ID**: Validation catches before conversion attempt
2. **Schema conversion fails**: Arrow schema has unsupported types
3. **RecordBatch schema mismatch**: Batch built with wrong Arrow schema
4. **S3 Tables rejects write**: Table schema evolved, incompatible change

### Test Cases

```rust
#[test]
fn test_simple_schema_roundtrip() {
    // Iceberg → Arrow → RecordBatch → Write
    let iceberg_schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
        ])
        .build()
        .unwrap();

    let arrow_schema = schema_to_arrow_schema(&iceberg_schema).unwrap();
    validate_field_ids(&arrow_schema).unwrap();

    let id_array = Int64Array::from(vec![1, 2, 3]);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(id_array)]
    ).unwrap();

    // Write and verify (actual test implementation)
}

#[test]
fn test_nested_schema_has_all_field_ids() {
    // Schema with struct and list
    let iceberg_schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(2, "user", Type::Struct(StructType::new(vec![
                NestedField::required(3, "name", Type::Primitive(PrimitiveType::String)),
                NestedField::required(4, "tags", Type::List(ListType::new(
                    NestedField::required(5, "element", Type::Primitive(PrimitiveType::String))
                ))),
            ]))),
        ])
        .build()
        .unwrap();

    let arrow_schema = schema_to_arrow_schema(&iceberg_schema).unwrap();

    // Should pass - all nested fields have IDs
    validate_field_ids(&arrow_schema).unwrap();
}

#[test]
fn test_missing_nested_field_id_fails() {
    // Manually create schema with missing nested ID
    let nested_fields = Fields::from(vec![
        Field::new("name", DataType::Utf8, false), // ← No ID!
    ]);
    let fields = vec![
        field_with_id("user", DataType::Struct(nested_fields), false, 1),
    ];
    let schema = ArrowSchema::new(fields);

    // Validation should catch the missing nested ID
    let result = validate_field_ids(&schema);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("user.name"));
}

#[test]
fn test_arrow_to_iceberg_conversion_requires_all_ids() {
    // Schema missing nested ID
    let nested_fields = Fields::from(vec![
        Field::new("name", DataType::Utf8, false),
    ]);
    let fields = vec![
        field_with_id("user", DataType::Struct(nested_fields), false, 1),
    ];
    let schema = ArrowSchema::new(fields);

    // Conversion should fail with clear error
    let result = arrow_schema_to_schema(&schema);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Field id not found"));
}
```

## Implementation Plan

### Phase 1: Fix Hello-World Example

1. Update `build_schema()` to return Iceberg schema
2. Update `create_sample_data()` to accept Arrow schema parameter
3. Use `schema_to_arrow_schema()` to derive Arrow schema
4. Verify end-to-end write succeeds

### Phase 2: Add Nested Example

1. Create schema with struct and list
2. Demonstrate field ID preservation at all levels
3. Write and read back to verify

### Phase 3: Add Validation

1. Implement `validate_field_ids()` helper
2. Add test cases for missing IDs
3. Document the requirement in README

### Phase 4: Documentation

1. Update README with schema mapping pattern
2. Add examples for otlp2parquet integration
3. Document field ID allocation strategy

## Future Considerations

### Field ID Stability

When schemas evolve (add/remove/rename fields):
- Field IDs must remain stable for existing fields
- New fields get new IDs (never reuse old IDs)
- Iceberg handles schema evolution automatically if IDs are stable

### Performance

- Schema conversion is cheap (metadata operations)
- RecordBatch construction unchanged
- No runtime overhead compared to current approach

### Testing Strategy

- Unit tests: Schema conversion and validation
- Integration tests: Write → Read → Verify
- End-to-end: Full pipeline with S3 Tables

## References

- [Apache Iceberg Spec - Schemas](https://iceberg.apache.org/spec/#schemas)
- [rust-iceberg Arrow Schema Conversion](https://docs.rs/iceberg/latest/iceberg/arrow/schema)
- [Parquet Field ID Metadata](https://parquet.apache.org/docs/file-format/metadata/)
