# Append Result API Design

**Date:** 2025-11-18
**Status:** Approved
**Authors:** Design session with user

## Problem Statement

The current `AppendOnlyTableWriter::append_batch` API has three critical gaps:

1. **No commit artifacts returned**: Returns `Result<()>` instead of the `DataFile` containing path, stats, and partition metadata, forcing downstream code (otlp2parquet) to fabricate metadata
2. **Missing partition support**: Partition spec can be configured but partition values aren't extracted from data or passed through to DataFile, making partition-pruning impossible
3. **No schema evolution visibility**: When tables are created or schemas change, callers have no visibility into what happened, preventing proper logging and error handling

## Goals

1. Return fully populated `DataFile` from append operations with path, statistics, and partition data
2. Extract partition values from RecordBatch data using the table's partition spec
3. Surface schema evolution status (created, evolved, appended) so callers can log and react appropriately
4. Support opt-in schema evolution to handle evolving OTLP schemas

## Non-Goals

- Schema evolution for type changes (only adding new fields)
- Advanced partition strategies (composite keys, dynamic partitioning)
- Batch splitting when multiple partition values detected (return error, caller must split)

## Design

### API Surface Changes

#### AppendResult Enum

```rust
/// Result of appending data to a table
#[derive(Debug, Clone)]
pub enum AppendResult {
    /// Table was created for the first time
    TableCreated {
        data_file: DataFile,
        schema: Schema,
    },
    /// Schema was evolved to accommodate new fields
    SchemaEvolved {
        data_file: DataFile,
        old_schema: Schema,
        new_schema: Schema,
    },
    /// Data appended to existing table without schema changes
    Appended {
        data_file: DataFile,
    },
}

impl AppendResult {
    /// Get the DataFile regardless of variant
    pub fn data_file(&self) -> &DataFile {
        match self {
            Self::TableCreated { data_file, .. } => data_file,
            Self::SchemaEvolved { data_file, .. } => data_file,
            Self::Appended { data_file } => data_file,
        }
    }
}
```

#### Updated Signatures

```rust
// Change from Result<()> to Result<AppendResult>
pub async fn append_batch(&self, batch: RecordBatch) -> Result<AppendResult>
pub async fn append_batches(&self, batches: Vec<RecordBatch>) -> Result<Vec<AppendResult>>
```

#### Schema Evolution Policy

```rust
#[derive(Debug, Clone, Copy, Default)]
pub enum SchemaEvolutionPolicy {
    /// Reject batches with different schemas (safe default)
    #[default]
    Reject,
    /// Automatically add new fields from incoming batch
    AddFields,
}

// Added to TableWriterOptions
impl TableWriterOptions {
    pub fn with_schema_evolution(mut self, policy: SchemaEvolutionPolicy) -> Self {
        self.schema_evolution = policy;
        self
    }
}
```

### Partition Value Extraction

The `DataFile` partition field is a `HashMap<String, String>` mapping partition field names to string-encoded values. Values are extracted from the RecordBatch using the table's partition spec.

#### Extraction Logic

```rust
fn extract_partition_values(
    &self,
    batch: &RecordBatch,
    partition_spec: &PartitionSpec,
    schema: &Schema,
) -> Result<HashMap<String, String>> {
    let mut partition_values = HashMap::new();

    for partition_field in partition_spec.fields() {
        // 1. Find source column in batch by field ID
        let source_field = schema.field_by_id(partition_field.source_id())?;
        let column_name = source_field.name();
        let array = batch.column_by_name(column_name)
            .ok_or_else(|| Error::invalid_input(
                format!("Partition column '{}' not found", column_name)
            ))?;

        // 2. Apply transform to first non-null value
        let value = apply_partition_transform(
            array,
            partition_field.transform(),
            source_field.field_type()
        )?;

        // 3. Encode as string per Iceberg spec
        partition_values.insert(partition_field.name().to_string(), value);
    }

    Ok(partition_values)
}
```

#### Supported Transforms

- **Identity**: Extract raw value from first row
- **Year/Month/Day/Hour**: Parse timestamp, apply temporal bucketing
- **Bucket[N]**: Hash value modulo N
- **Truncate[W]**: Truncate strings/integers to width W

#### Validation

- All rows in batch must belong to same partition (validate first and last row match)
- If multiple partition values detected, return error asking caller to split batch
- Empty batches are rejected before partition extraction

### Schema Evolution

#### Evolution Rules (AddFields policy)

When `SchemaEvolutionPolicy::AddFields` is enabled:

1. **Field matching**: Match fields by name (Arrow field name == Iceberg field name)
2. **New fields**: Any field in incoming batch not in table schema gets added with next available field ID
3. **Existing fields**: Must have compatible types (no type changes allowed)
4. **Nested fields**: For structs, recursively merge nested fields
5. **Field deletion**: Missing fields in batch are allowed (nullable in Iceberg schema)

#### Type Compatibility

```rust
fn are_types_compatible(existing: &Type, incoming: &Type) -> bool {
    match (existing, incoming) {
        (Type::Primitive(a), Type::Primitive(b)) => a == b,
        (Type::Struct(a), Type::Struct(b)) => {
            // All existing fields must be present with compatible types
            // New fields are allowed
        }
        (Type::List(a), Type::List(b)) => {
            are_types_compatible(&a.element_type(), &b.element_type())
        }
        (Type::Map(a), Type::Map(b)) => {
            are_types_compatible(&a.key_type(), &b.key_type())
                && are_types_compatible(&a.value_type(), &b.value_type())
        }
        _ => false, // Primitive <-> Complex is incompatible
    }
}
```

#### Evolution Transaction

1. Build new schema by merging existing + new fields
2. Assign field IDs (preserve existing IDs, assign new ones starting from `max_existing_id + 1`)
3. Use catalog's update-table-schema operation
4. Fallback: If catalog doesn't support schema evolution API, return error with details

## Implementation Plan

### File Changes

**New files:**
- `src/writer/partition_extract.rs` - Partition value extraction logic
- `src/spec/schema_evolution.rs` - Schema merging and evolution utilities

**Modified files:**
- `src/writer/high_level.rs` - Update AppendOnlyTableWriter with new return types
- `src/writer/mod.rs` - Export AppendResult enum
- `src/spec/schema.rs` - Add field ID assignment methods

### Implementation Phases

**Phase 1: Return DataFile from append operations**
- Modify `append_single_batch` to return DataFile (already created by `finish_data_file()`)
- Wrap in AppendResult::Appended variant
- Update `append_batch` and `append_batches` signatures
- All batches go through Appended variant initially

**Phase 2: Add partition value extraction**
- Implement `PartitionValueExtractor` for each transform type
- Extract partition values from first row of batch
- Pass to `DataFile::builder().with_partition(values)`
- Validate all rows belong to same partition

**Phase 3: Add schema evolution policy**
- Add `SchemaEvolutionPolicy` enum and option to `TableWriterOptions`
- Implement schema comparison logic
- Detect TableCreated vs Appended cases
- Return appropriate AppendResult variant
- Keep Reject behavior for schema mismatches

**Phase 4: Implement AddFields evolution**
- Implement schema merging (preserve IDs, add new fields)
- Add catalog method for updating table schema
- Return `AppendResult::SchemaEvolved` with old/new schemas
- Add integration test with evolving schemas

## Backward Compatibility

This is a **breaking change** requiring a major version bump.

### Migration Path

```rust
// Old code
writer.append_batch(batch).await?;

// New code (minimal change)
let _result = writer.append_batch(batch).await?;

// New code (using result)
match writer.append_batch(batch).await? {
    AppendResult::TableCreated { schema, .. } => {
        log::info!("Created table with schema: {:?}", schema);
    },
    AppendResult::Appended { data_file } => {
        log::debug!("Appended {}", data_file.file_path());
    },
    AppendResult::SchemaEvolved { old_schema, new_schema, .. } => {
        log::warn!("Schema evolved from {:?} to {:?}", old_schema, new_schema);
    },
}
```

## Testing Strategy

- Unit tests for partition value extraction (each transform type)
- Unit tests for schema compatibility checking
- Unit tests for schema merging
- Integration test: Create table, verify TableCreated result
- Integration test: Append to existing table, verify Appended result
- Integration test: Evolve schema with AddFields policy
- Integration test: Reject schema changes with Reject policy
- Integration test: Multi-partition validation (should error on mixed partitions)

## Success Metrics

1. otlp2parquet can delete fabricated metadata code and use DataFile directly
2. Partition values appear correctly in manifest files
3. ClickHouse-compatible partition layouts work without manual intervention
4. Schema evolution logs are actionable (show what changed)
