# Transaction.commit() Design

**Date:** 2025-11-16
**Status:** Design
**Author:** Brainstorming session with Claude

## Problem Statement

We have built a WASM-compatible Iceberg foundation with:
- Vendored spec types (identifiers, schema, metadata, snapshots, data files)
- OpenDAL-based FileIO
- Table and Transaction API with `append(Vec<DataFile>)`

We need to implement `Transaction.commit()` to enable users to write data to Iceberg tables by creating proper snapshots with Avro manifests.

## Goals

1. **Enable end-to-end write workflow** - Users can commit DataFile metadata to create Iceberg snapshots
2. **Full Iceberg compatibility** - Other tools (Spark, Trino, PyIceberg) can read our tables
3. **Support concurrent writers** - Multiple Cloudflare Workers can write to the same table safely
4. **Maintain WASM compatibility** - All dependencies must work with wasm32-unknown-unknown

## Non-Goals (MVP)

- Partitioned tables (unpartitioned only for MVP)
- Delete file support (append-only operations)
- Parquet file writing (users write Parquet separately)
- Automatic cleanup of orphaned files
- Schema evolution during commit
- Advanced retry strategies

## Design Decisions

### 1. User Writes Parquet Files Separately

**Decision:** Transaction.commit() handles only Iceberg metadata, not Parquet writing.

**Rationale:**
- Separation of concerns - Parquet writing is orthogonal to Iceberg metadata
- Users may want custom Parquet settings (compression, encoding, row groups)
- Simpler initial implementation
- Matches PyIceberg's `add_files()` API (low-level path)

**User workflow:**
```rust
// 1. User writes Parquet file with arrow/parquet crates
let path = write_parquet_file(record_batch, "s3://bucket/data/file1.parquet").await?;

// 2. User creates DataFile metadata
let data_file = DataFile::builder()
    .with_file_path(&path)
    .with_file_format("PARQUET")
    .with_record_count(1000)
    .with_file_size_in_bytes(50_000)
    .with_value_counts(stats)  // Optional
    .build()?;

// 3. User commits metadata
table.transaction()
    .append(vec![data_file])
    .commit().await?;
```

**Future:** Can add `append_record_batches()` later as convenience wrapper.

### 2. Avro Manifests for Full Compatibility

**Decision:** Use Avro for manifest and manifest list files (Iceberg v2 format).

**Rationale:**
- The whole point is WASM-compatible **Iceberg** tables
- Other tools can't read non-Avro manifests
- `apache-avro = "0.21"` crate works with WASM (has wasm-demo)
- Worth the complexity for true interoperability

**Alternative rejected:** JSON manifests would be simpler but break compatibility.

### 3. Optimistic Concurrency with Version Files

**Decision:** Write new `v{N}.metadata.json` files, catalog tracks current version.

**Rationale:**
- Standard Iceberg pattern (REST/Hive catalogs)
- Works on eventually-consistent S3/R2
- No distributed locks needed
- Failures detected on catalog update

**Commit atomicity:**
1. Write manifest files → `{table}/metadata/{uuid}-m0.avro`
2. Write manifest list → `{table}/metadata/snap-{id}-1-{uuid}.avro`
3. Write metadata JSON → `{table}/metadata/v{N}.metadata.json`
4. Update catalog pointer → atomic check of current version
5. If version mismatch → concurrent modification error

### 4. PyIceberg-Compatible File Naming

**Decision:** Use PyIceberg's naming conventions, not pure UUIDs.

**File naming:**
- **Metadata:** `v{N}.metadata.json` (sequential, where N = snapshot count)
- **Manifest list:** `snap-{snapshotId}-{attempt}-{commitUUID}.avro`
- **Manifest file:** `{commitUUID}-m{manifestCount}.avro`

**Rationale:**
- Matches PyIceberg/Spark output exactly
- Snapshot ID in names aids debugging
- Shared UUID shows relationship between manifest list and manifests
- Still safe for concurrent workers (each generates unique UUID)

**Example:**
```
s3://bucket/table/metadata/
  v0.metadata.json
  v1.metadata.json
  snap-1-1-a1b2c3d4.avro          # Manifest list for snapshot 1
  a1b2c3d4-m0.avro                # Manifest file (same UUID)
  snap-2-1-e5f6g7h8.avro          # Manifest list for snapshot 2
  e5f6g7h8-m0.avro                # Manifest file
```

### 5. Automatic Retry with Exponential Backoff

**Decision:** commit() automatically retries on concurrent modification errors.

**Rationale:**
- Serverless workers have unpredictable timing
- Most conflicts resolve quickly (one worker wins)
- Users don't want to handle optimistic concurrency manually
- Common pattern in PyIceberg and database clients

**Implementation:**
```rust
async fn commit(&self) -> Result<()> {
    for attempt in 0..3 {  // Max 3 attempts
        match try_commit().await {
            Ok(()) => return Ok(()),
            Err(e) if is_concurrent_modification(&e) => {
                // Exponential backoff: 100ms, 200ms, 400ms
                sleep(Duration::from_millis(100 * 2_u64.pow(attempt))).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(Error::MaxRetriesExceeded)
}
```

### 6. Snapshot IDs Increment from Last

**Decision:** Generate `snapshot_id = last_snapshot_id + 1`, not timestamps.

**Rationale:**
- You already read current metadata for optimistic concurrency
- Truly monotonic, no clock skew issues
- Matches Iceberg semantics (snapshots build on each other)
- Natural with commit flow

**ID generation:**
```rust
let snapshot_id = current_metadata
    .current_snapshot_id()
    .map(|id| id + 1)
    .unwrap_or(1);
let sequence_number = snapshot_id;  // Same value for v2
```

### 7. Include Column Statistics in Manifests

**Decision:** Include optional column statistics (value_counts, null_counts, etc.).

**Rationale:**
- DataFile already has these optional fields
- Query engines need stats for file pruning
- Small incremental complexity over minimal manifest
- If user provides stats, write them; if not, write null

**Manifest entry fields:**
```rust
ManifestEntry {
    status: ADDED,
    snapshot_id,
    sequence_number,
    data_file: {
        file_path, file_format, record_count, file_size_in_bytes,
        column_sizes?,      // Optional
        value_counts?,      // Optional
        null_value_counts?, // Optional
        lower_bounds?,      // Optional
        upper_bounds?,      // Optional
    }
}
```

### 8. Include File Counts in Manifest Lists

**Decision:** Include added/deleted/existing file and row counts.

**Rationale:**
- Query engines need these for planning
- You're already calculating them when building manifests
- Not much more complex than paths-only

**Manifest list entry:**
```rust
ManifestFile {
    manifest_path,
    manifest_length,
    partition_spec_id: 0,  // Unpartitioned
    content: DATA,
    sequence_number,
    min_sequence_number,
    added_snapshot_id,
    added_files_count,
    existing_files_count: 0,  // Always 0 for append
    deleted_files_count: 0,   // Always 0 for append
    added_rows_count,
    // ... partition info (null for MVP)
}
```

### 9. Leave Orphaned Files on Failure

**Decision:** Don't clean up files if commit fails partway through.

**Rationale:**
- Simplest for MVP - just write and bail on error
- Matches S3 semantics (no transactions)
- Can add garbage collection later
- Focus on happy path first

**Future:** Implement `expire_snapshots()` or periodic cleanup scripts.

### 10. Iceberg v2 Format Only

**Decision:** Use Iceberg v2 manifest format with sequence numbers.

**Rationale:**
- Modern standard (Spark 3.3+, Trino expect v2)
- TableMetadata already defaults to format-version: 2
- Building new tables, not reading legacy ones
- Simpler: one schema, not two

## Architecture

### Commit Flow

```
table.transaction()
    .append(vec![data_file])
    .commit().await
       ↓
┌──────────────────────────────────────┐
│ 1. Load current TableMetadata        │
│    from catalog                      │
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│ 2. Generate snapshot ID              │
│    = last_snapshot_id + 1            │
│    Generate commit UUID              │
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│ 3. Write manifest Avro file          │
│    {uuid}-m0.avro                    │
│    (list of DataFile entries)        │
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│ 4. Write manifest list Avro file     │
│    snap-{id}-1-{uuid}.avro           │
│    (references manifest file)        │
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│ 5. Create Snapshot object            │
│    with summary stats                │
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│ 6. Build new TableMetadata           │
│    with added snapshot               │
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│ 7. Write metadata JSON file          │
│    v{N}.metadata.json                │
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│ 8. Update catalog pointer            │
│    (optimistic concurrency check)    │
└──────────────────────────────────────┘
       ↓
   On conflict? → Retry (max 3x)
       ↓
   Success! → Return Ok(())
```

### File Organization

```
s3://bucket/table/
  metadata/
    v0.metadata.json              # Initial metadata
    v1.metadata.json              # After first commit
    v2.metadata.json              # After second commit
    snap-1-1-{uuid1}.avro         # Manifest list for snapshot 1
    snap-2-1-{uuid2}.avro         # Manifest list for snapshot 2
    {uuid1}-m0.avro               # Manifest file for snapshot 1
    {uuid2}-m0.avro               # Manifest file for snapshot 2
  data/
    {user-file1}.parquet          # User's data files
    {user-file2}.parquet
```

### Avro Schemas

**Manifest Entry Schema (Iceberg v2):**

Fields from Iceberg spec v2:
- `status`: int (0=EXISTING, 1=ADDED, 2=DELETED)
- `snapshot_id`: long (optional)
- `sequence_number`: long (optional)
- `file_sequence_number`: long (optional)
- `data_file`: struct with:
  - `content`: int (0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES)
  - `file_path`: string
  - `file_format`: string
  - `partition`: map<string, value> (empty for unpartitioned)
  - `record_count`: long
  - `file_size_in_bytes`: long
  - `column_sizes`: map<int, long> (optional)
  - `value_counts`: map<int, long> (optional)
  - `null_value_counts`: map<int, long> (optional)
  - `lower_bounds`: map<int, bytes> (optional)
  - `upper_bounds`: map<int, bytes> (optional)
  - Plus others set to null for MVP

**Manifest List Entry Schema (Iceberg v2):**

Fields from Iceberg spec v2:
- `manifest_path`: string
- `manifest_length`: long
- `partition_spec_id`: int
- `content`: int (0=DATA, 1=DELETES)
- `sequence_number`: long
- `min_sequence_number`: long
- `added_snapshot_id`: long
- `added_files_count`: int
- `existing_files_count`: int
- `deleted_files_count`: int
- `added_rows_count`: long
- `existing_rows_count`: long
- `deleted_rows_count`: long
- `partitions`: array<FieldSummary> (optional, null for unpartitioned)
- `key_metadata`: bytes (optional, null for MVP)

### Snapshot Creation

```rust
Snapshot {
    snapshot_id: i64,              // Incremented
    parent_snapshot_id: Option<i64>,
    sequence_number: i64,          // Same as snapshot_id
    timestamp_ms: i64,             // Current time
    manifest_list: String,         // Path to snap-*.avro
    summary: Summary {
        operation: "append",
        added-data-files: "N",
        added-records: "M",
        total-data-files: "N",
        total-records: "M",
    },
    schema_id: i32,
}
```

### Catalog Integration

The catalog trait needs a new method:

```rust
#[async_trait]
pub trait Catalog {
    /// Update table metadata atomically
    ///
    /// Returns ConcurrentModification error if expected_version doesn't match
    async fn update_table_metadata(
        &self,
        table_ident: &TableIdent,
        new_metadata_location: String,
        expected_version: i32,
    ) -> Result<()>;
}
```

Implementation varies by catalog:
- **R2Catalog:** Check version-hint.txt, update if matches
- **S3TablesCatalog:** Use S3 Tables API atomic update
- **REST Catalog:** POST to /v1/{prefix}/namespaces/{ns}/tables/{table}

## Implementation Plan

### Phase 1: Avro Foundation
1. Add `apache-avro` dependency
2. Vendor Iceberg v2 Avro schemas as constants
3. Implement helper to convert DataFile → Avro Value
4. Unit tests for Avro serialization

### Phase 2: Manifest Writing
1. Implement `write_manifest()` - write manifest Avro file
2. Implement `write_manifest_list()` - write manifest list Avro file
3. Calculate file/row counts and statistics
4. Unit tests for manifest generation

### Phase 3: Snapshot & Metadata
1. Implement `create_snapshot()` - build Snapshot from operations
2. Implement `build_new_metadata()` - clone + update TableMetadata
3. Implement `write_metadata()` - serialize as JSON
4. Unit tests for snapshot/metadata logic

### Phase 4: Commit Orchestration
1. Implement `try_commit()` - orchestrate all steps
2. Implement `commit_with_retries()` - retry loop
3. Add Error::ConcurrentModification variant
4. Integration tests with memory storage

### Phase 5: Catalog Integration
1. Add `update_table_metadata()` to Catalog trait
2. Implement for R2Catalog (version-hint pattern)
3. Implement for S3TablesCatalog (API update)
4. End-to-end tests with real catalog

### Phase 6: Concurrent Writers Test
1. Write test with multiple async tasks
2. All commit to same table simultaneously
3. Verify all succeed (retries handle conflicts)
4. Verify final table has all data

## Dependencies

```toml
[dependencies]
apache-avro = "0.21"         # Avro serialization (WASM-compatible)
uuid = { version = "1.0", features = ["v4"] }  # Already added
```

## Testing Strategy

### Unit Tests
- Avro serialization (DataFile → Avro Value)
- Snapshot creation (summary stats)
- Metadata building (version increments)
- File naming (follows PyIceberg conventions)

### Integration Tests
- End-to-end commit with memory storage
- Verify manifest files are valid Avro
- Verify metadata JSON is valid
- Verify snapshot references are correct

### Concurrent Tests
- Multiple workers writing to same table
- Verify retries handle conflicts
- Verify no data loss
- Verify final metadata is consistent

### Compatibility Tests
- Read committed table with PyIceberg (future)
- Read committed table with Spark (future)

## Success Criteria

- [ ] Users can call `transaction.commit().await`
- [ ] Manifests are valid Iceberg v2 Avro files
- [ ] Manifest lists are valid Iceberg v2 Avro files
- [ ] Metadata JSON follows Iceberg spec
- [ ] Multiple concurrent workers can commit safely
- [ ] All code compiles to wasm32-unknown-unknown
- [ ] Integration tests pass with memory storage

## Future Enhancements

**Write convenience:**
- `append_record_batches()` - write Parquet + metadata in one call
- Automatic column statistics calculation from RecordBatch

**Advanced features:**
- Partitioned table support
- Delete file support (row-level deletes)
- Schema evolution on commit
- Replace operation (overwrite)

**Operational:**
- Garbage collection / expire_snapshots()
- Configurable retry strategy
- Cleanup orphaned files on failure
- Compression for metadata JSON

**Optimization:**
- Multiple manifest files per commit (large commits)
- Manifest file reuse (existing files)
- Metadata caching

## References

- [Iceberg Table Spec](https://iceberg.apache.org/spec/)
- [PyIceberg file naming conventions](https://tomtan.dev/blog/2025-01-12-iceberg-file-name-convention/)
- [Apache Avro Rust crate](https://docs.rs/apache-avro)
- [Existing WASM design doc](./2025-11-16-wasm-compatible-icepick-design.md)
