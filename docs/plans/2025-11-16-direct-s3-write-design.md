# Direct S3 Write Design

**Date:** 2025-11-16
**Status:** Approved

## Overview

Add a simple, ergonomic API for writing Arrow data directly to S3 as Parquet files, bypassing Iceberg metadata entirely. Primary use case: external system integration where data needs to be consumed by Spark, DuckDB, or other tools that don't require Iceberg metadata.

## Requirements

- Accept Arrow `RecordBatch` and write to S3 as Parquet
- Simple single-batch API (not streaming/multi-batch)
- Reuse existing `FileIO` for S3 credentials and access
- Optional compression configuration
- User controls file paths completely (no automatic partitioning)
- Use all existing dependencies (no new crates)

## API Design

### Core API

```rust
use icepick::arrow_to_parquet;
use arrow::record_batch::RecordBatch;
use icepick::FileIO;
use parquet::basic::Compression;

// Simplest case - infer everything, use default compression
arrow_to_parquet(&batch, "s3://bucket/data.parquet", &file_io).await?;

// With compression control
arrow_to_parquet(&batch, "s3://bucket/data.parquet", &file_io)
    .with_compression(Compression::ZSTD(None))
    .await?;

// Manual partition paths (Hive-style or any structure)
let path = format!("s3://bucket/data/date={}/region={}/data.parquet", date, region);
arrow_to_parquet(&batch, &path, &file_io).await?;
```

### Function Signature

```rust
pub fn arrow_to_parquet<'a>(
    batch: &'a RecordBatch,
    path: impl Into<String>,
    file_io: &'a FileIO,
) -> ArrowParquetBuilder<'a>
```

### Builder Pattern

```rust
pub struct ArrowParquetBuilder<'a> {
    batch: &'a RecordBatch,
    path: String,
    file_io: &'a FileIO,
    compression: Compression,
}

impl<'a> ArrowParquetBuilder<'a> {
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    pub async fn finish(self) -> Result<()> {
        // Implementation
    }
}

// Enable await directly on builder (without .finish())
impl<'a> std::future::IntoFuture for ArrowParquetBuilder<'a> {
    type Output = Result<()>;
    // ...
}
```

## Implementation Structure

### Module Organization

```
src/
  writer/
    mod.rs           // Re-exports
    parquet.rs       // Existing ParquetWriter (used by Iceberg)
    arrow_parquet.rs // New: simple arrow→parquet for direct S3 writes
```

### Implementation Approach

The new `arrow_parquet.rs` provides a lightweight wrapper that:

1. Creates in-memory Parquet file using `parquet::arrow::ArrowWriter`
2. Infers schema from `RecordBatch` Arrow schema
3. Writes batch to Parquet with specified compression
4. Uploads bytes to S3 via `FileIO::write()`
5. Returns `Result<()>`

### Key Difference from Existing `ParquetWriter`

- **Existing `ParquetWriter`**: Collects statistics, creates `DataFile` metadata for Iceberg commits
- **New `arrow_to_parquet()`**: Just writes Parquet, no Iceberg metadata, no stats collection

## Error Handling

Reuses existing `Result<()>` and `Error` types:

1. **Parquet creation errors** → `Error::InvalidInput`
   - Invalid Arrow schema
   - Unsupported data types

2. **Parquet write/flush errors** → `Error::InvalidInput`
   - Writer internal errors

3. **S3 upload errors** → `Error::IoError` (via `FileIO`)
   - Network failures
   - Permission errors
   - Invalid paths

### Edge Cases

- **Empty batches** (0 rows): Write valid Parquet file with schema but no data
- **Large batches**: No special handling, document memory usage (entire file buffered in memory)
- **Path validation**: Delegate to `FileIO` (already handles s3:// validation)
- **Compression codec unavailability**: Let Parquet crate error naturally, propagate to user
- **No retry logic**: Single-shot operation, user can retry if needed

## Example Usage

```rust
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use icepick::{arrow_to_parquet, FileIO};
use parquet::basic::Compression;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup FileIO with S3 credentials
    let file_io = FileIO::from_aws_credentials(...);

    // Create sample Arrow data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )?;

    // Simple write with defaults
    arrow_to_parquet(&batch, "s3://my-bucket/output.parquet", &file_io).await?;

    // With compression
    arrow_to_parquet(&batch, "s3://my-bucket/compressed.parquet", &file_io)
        .with_compression(Compression::ZSTD(None))
        .await?;

    // Partitioned (manual path construction)
    let date = "2025-01-15";
    let path = format!("s3://my-bucket/data/date={}/data.parquet", date);
    arrow_to_parquet(&batch, &path, &file_io).await?;

    Ok(())
}
```

## Public API Changes

Add to `src/lib.rs`:

```rust
pub use writer::arrow_to_parquet;
```

## Documentation

Function docstring should explain:
- Purpose: Write Arrow data directly to S3 as Parquet (bypasses Iceberg)
- Use case: External system integration (Spark, DuckDB, etc.)
- Note: For Iceberg tables, use `Transaction` API instead
- Memory usage: Entire Parquet file buffered in memory before upload

## Dependencies

Uses existing dependencies only:
- `arrow` - RecordBatch input
- `parquet` - Parquet writing, compression
- `opendal` (via `FileIO`) - S3 upload

## Non-Goals

- Multi-batch accumulation
- Automatic partitioning
- File size limits or splitting
- Statistics collection
- Retry logic
- Streaming writes

## Future Enhancements (out of scope)

- Writer properties customization (row group size, encoding, etc.)
- Automatic Hive-style partitioning from partition columns
- Multi-batch accumulation with size-based file splitting
- Async streaming from `RecordBatch` stream
