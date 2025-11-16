# Iceberg-Rust Replacement Design

**Date:** 2025-11-16
**Status:** Approved
**Goal:** Remove iceberg-rust dependency and replace with icepick implementations, enabling WASM compilation while maintaining write functionality.

---

## Architecture Overview

### Primary Goal
Remove the `iceberg = "0.7.0"` dependency entirely and replace it with icepick's own implementations, enabling WASM compilation while maintaining write functionality.

### System Architecture

```
┌─────────────────────────────────────────┐
│  Examples (r2_basic.rs, etc.)           │
│  - Use icepick APIs exclusively         │
└─────────────┬───────────────────────────┘
              │
┌─────────────▼───────────────────────────┐
│  icepick Library                        │
│                                         │
│  ┌─────────────┐  ┌──────────────┐    │
│  │  Catalogs   │  │   Table      │    │
│  │ (R2, S3T)   │  │   Type       │    │
│  └──────┬──────┘  └──────┬───────┘    │
│         │                 │             │
│  ┌──────▼─────────────────▼────────┐  │
│  │    RestCatalog (core)           │  │
│  │    - load_table()                │  │
│  │    - create_table()              │  │
│  │    - update_table_metadata()     │  │
│  └─────────────────────────────────┘  │
│                                         │
│  ┌─────────────────────────────────┐  │
│  │   ParquetWriter                 │  │
│  │   - write_batch()                │  │
│  │   - finish() -> DataFile         │  │
│  └─────────────────────────────────┘  │
│                                         │
│  ┌─────────────────────────────────┐  │
│  │   Transaction                   │  │
│  │   - append()                     │  │
│  │   - commit() [already works!]    │  │
│  └─────────────────────────────────┘  │
└─────────────────────────────────────────┘
              │
┌─────────────▼───────────────────────────┐
│  External Dependencies (WASM-safe)      │
│  - parquet crate (write only)           │
│  - arrow crate                          │
│  - opendal (FileIO)                     │
└─────────────────────────────────────────┘
```

### Key Architectural Decision
Transaction.commit() already works from the previous implementation plan. This design builds on that foundation by adding the remaining pieces needed to remove the iceberg-rust dependency.

---

## Type System

### Core Types

**Already Implemented:**
- `TableIdent` - Table identifier (namespace + name)
- `NamespaceIdent` - Namespace identifier
- `Schema` - Iceberg table schema
- `DataFile` - Data file metadata
- `TableMetadata` - Table metadata (snapshots, schemas, etc.)
- `Snapshot` - Point-in-time snapshot
- `Transaction` - Transaction builder with commit()

**Need to Implement:**

```rust
pub struct Table {
    identifier: TableIdent,
    metadata: TableMetadata,
    metadata_location: String,
    file_io: FileIO,
}

impl Table {
    // Core accessors
    pub fn identifier(&self) -> &TableIdent;
    pub fn metadata(&self) -> &TableMetadata;
    pub fn file_io(&self) -> &FileIO;
    pub fn location(&self) -> &str;
    pub fn metadata_location(&self) -> &str;

    // Snapshot access
    pub fn current_snapshot(&self) -> Option<&Snapshot>;

    // Transaction API
    pub fn transaction(&self) -> Transaction;
}
```

### Catalog Trait Extensions

```rust
#[async_trait]
pub trait Catalog: Send + Sync {
    // Already implemented:
    async fn create_namespace(...) -> Result<()>;
    async fn namespace_exists(...) -> Result<bool>;
    async fn list_tables(...) -> Result<Vec<TableIdent>>;
    async fn table_exists(...) -> Result<bool>;
    async fn drop_table(...) -> Result<()>;
    async fn update_table_metadata(...) -> Result<()>;

    // Need to add:
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table>;

    async fn load_table(&self, identifier: &TableIdent) -> Result<Table>;
}
```

### TableCreation Builder

```rust
pub struct TableCreation {
    name: String,
    schema: Schema,
    location: Option<String>,
    properties: HashMap<String, String>,
}

impl TableCreation {
    pub fn builder() -> TableCreationBuilder;
}
```

---

## ParquetWriter Implementation

### Purpose
Thin wrapper around the `parquet` crate that writes Arrow batches and produces `DataFile` with statistics.

### Design

```rust
pub struct ParquetWriter {
    schema: Schema,
    buffer: Vec<u8>,  // In-memory buffer
    parquet_writer: ArrowWriter<Vec<u8>>,
    stats_collector: StatsCollector,
}

impl ParquetWriter {
    /// Create a new Parquet writer
    pub fn new(schema: Schema) -> Result<Self> {
        // Convert icepick::Schema -> arrow::Schema
        let arrow_schema = schema_to_arrow(&schema)?;

        // Create in-memory parquet writer
        let buffer = Vec::new();
        let props = WriterProperties::builder().build();
        let parquet_writer = ArrowWriter::try_new(buffer, arrow_schema, Some(props))?;

        Ok(Self {
            schema,
            buffer,
            parquet_writer,
            stats_collector: StatsCollector::new(),
        })
    }

    /// Write an Arrow RecordBatch
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // Track statistics
        self.stats_collector.collect(batch)?;

        // Write to parquet
        self.parquet_writer.write(batch)?;

        Ok(())
    }

    /// Finish writing and upload to storage, returning DataFile
    pub async fn finish(
        mut self,
        file_io: &FileIO,
        path: String,  // Full path like "s3://bucket/data/file1.parquet"
    ) -> Result<DataFile> {
        // Close parquet writer
        self.parquet_writer.close()?;

        // Get buffer with parquet data
        let parquet_bytes = self.parquet_writer.into_inner()?;
        let file_size = parquet_bytes.len() as i64;

        // Upload to storage
        file_io.write(&path, parquet_bytes).await?;

        // Build DataFile with statistics
        let stats = self.stats_collector.finalize();

        Ok(DataFile::builder()
            .with_file_path(path)
            .with_file_format("PARQUET")
            .with_record_count(stats.record_count)
            .with_file_size_in_bytes(file_size)
            .with_column_sizes(stats.column_sizes)
            .with_value_counts(stats.value_counts)
            .with_null_value_counts(stats.null_value_counts)
            .build()?)
    }
}
```

### Statistics Collection

```rust
struct StatsCollector {
    record_count: i64,
    column_sizes: HashMap<i32, i64>,
    value_counts: HashMap<i32, i64>,
    null_value_counts: HashMap<i32, i64>,
}
```

The `StatsCollector` iterates through Arrow arrays to count nulls and values per column. This provides the metadata needed for query planning.

### Key Design Decisions

**Synchronous API with async finish:**
- `write_batch()` is synchronous - writes to in-memory buffer
- `finish()` is async - uploads to storage
- Simpler implementation, acceptable for moderate data sizes
- Examples in codebase use small datasets (3 rows)

**In-memory buffering:**
- Entire Parquet file built in memory before upload
- Trade-off: Simpler code, but memory usage scales with file size
- Acceptable for MVP, can optimize later if needed

---

## RestCatalog Implementation

### update_table_metadata

Implements the Iceberg REST spec commit operation with snapshot-based optimistic locking:

```rust
async fn update_table_metadata(
    &self,
    identifier: &TableIdent,
    old_metadata_location: &str,
    new_metadata_location: &str,
) -> Result<()> {
    // 1. Load current metadata to get current snapshot ID
    let current_metadata = self.load_metadata(old_metadata_location).await?;
    let current_snapshot_id = current_metadata.current_snapshot_id();

    // 2. Load new metadata to get new snapshot ID
    let new_metadata = self.load_metadata(new_metadata_location).await?;
    let new_snapshot_id = new_metadata.current_snapshot_id()
        .ok_or_else(|| Error::invalid_input("New metadata has no snapshot"))?;

    // 3. Build REST API request
    // POST /v1/namespaces/{namespace}/tables/{table}
    let request = CommitTableRequest {
        identifier: identifier.clone(),
        requirements: vec![
            TableRequirement::AssertCurrentSnapshotId {
                snapshot_id: current_snapshot_id,
            },
        ],
        updates: vec![
            TableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                snapshot_id: new_snapshot_id,
                ref_type: "branch".to_string(),
            },
            TableUpdate::SetCurrentSnapshotId {
                snapshot_id: new_snapshot_id,
            },
        ],
    };

    // 4. Send to REST endpoint
    let response = self.rest_client
        .post_commit_table(identifier, request)
        .await?;

    // 5. Handle optimistic locking failure
    if response.status == 409 {
        return Err(Error::concurrent_modification(
            "Snapshot ID changed during commit"
        ));
    }

    Ok(())
}
```

### REST API Types

```rust
struct CommitTableRequest {
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

enum TableRequirement {
    AssertCurrentSnapshotId { snapshot_id: Option<i64> },
}

enum TableUpdate {
    SetSnapshotRef { ref_name: String, snapshot_id: i64, ref_type: String },
    SetCurrentSnapshotId { snapshot_id: i64 },
}
```

### Key Features

- **Snapshot-based optimistic locking:** Uses `AssertCurrentSnapshotId` requirement
- **Atomic updates:** Updates both snapshot ref and current snapshot ID
- **Conflict handling:** Returns `ConcurrentModification` error on 409 status
- **Retry integration:** Works with existing retry logic in `commit_orchestrator.rs`

### create_table & load_table

```rust
impl Catalog for RestCatalog {
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        // POST /v1/namespaces/{ns}/tables
        let response = self.rest_client
            .create_table(namespace, creation)
            .await?;

        // Response contains: metadata, metadata-location
        Ok(Table::new(
            TableIdent::new(namespace.clone(), creation.name),
            response.metadata,
            response.metadata_location,
            self.file_io.clone(),
        ))
    }

    async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
        // GET /v1/namespaces/{ns}/tables/{table}
        let response = self.rest_client
            .load_table(identifier)
            .await?;

        Ok(Table::new(
            identifier.clone(),
            response.metadata,
            response.metadata_location,
            self.file_io.clone(),
        ))
    }
}
```

### R2Catalog and S3TablesCatalog Delegation

Both catalogs already delegate to RestCatalog internally. They just need pass-through implementations:

```rust
impl Catalog for R2Catalog {
    async fn create_table(...) -> Result<Table> {
        self.rest_catalog.create_table(namespace, creation).await
    }

    async fn load_table(...) -> Result<Table> {
        self.rest_catalog.load_table(identifier).await
    }

    async fn update_table_metadata(...) -> Result<()> {
        self.rest_catalog.update_table_metadata(
            identifier,
            old_metadata_location,
            new_metadata_location
        ).await
    }
}

// S3TablesCatalog identical
```

---

## Example Updates

### Before (using iceberg-rust)

```rust
use iceberg::spec::{Schema, NestedField, Type, PrimitiveType};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::{Catalog, TableCreation, Transaction};
use icepick::R2Catalog;

// ... setup ...

let table = catalog.load_table(&table_ident).await?;

// Complex iceberg-rust writer setup
let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
let file_name_generator = DefaultFileNameGenerator::new(
    "data".to_string(),
    Some(uuid::Uuid::new_v4().to_string()),
    DataFileFormat::Parquet,
);

let parquet_writer = ParquetWriterBuilder::new(
    WriterProperties::default(),
    table.metadata().current_schema().clone(),
    None,
    table.file_io().clone(),
    location_generator,
    file_name_generator,
);

let mut data_file_writer = DataFileWriterBuilder::new(parquet_writer, None, 0)
    .build()
    .await?;

data_file_writer.write(batch).await?;
let data_files = data_file_writer.close().await?;

// iceberg-rust transaction
let tx = Transaction::new(&table);
let action = tx.fast_append().add_data_files(data_files);
let tx = action.apply(tx)?;
let table = tx.commit(&catalog).await?;
```

### After (using icepick only)

```rust
use icepick::{R2Catalog, Catalog, TableCreation, ParquetWriter};
use icepick::spec::{Schema, NestedField, Type, PrimitiveType};
use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;

// ... setup ...

let table = catalog.load_table(&table_ident).await?;

// Simple icepick writer
let mut writer = ParquetWriter::new(table.metadata().current_schema().clone())?;
writer.write_batch(&batch)?;

let file_path = format!("{}/data/file-{}.parquet",
    table.location(),
    uuid::Uuid::new_v4()
);
let data_file = writer.finish(table.file_io(), file_path).await?;

// icepick transaction (already works!)
table.transaction()
    .append(vec![data_file])
    .commit()
    .await?;
```

### Key Simplifications

- **No location generators:** Simple string formatting for paths
- **Direct ParquetWriter API:** No nested builders
- **Transaction API already working:** From previous implementation
- **No read/scan:** Examples only demonstrate writing

---

## Implementation Phases

### Phase 1: Core Types & Table
- Implement `Table` struct with all accessors
- Implement `TableCreation` builder
- Add `create_table` and `load_table` to `Catalog` trait
- Update `RestCatalog` to implement these methods
- Update `R2Catalog` and `S3TablesCatalog` delegation
- Unit tests for Table type

### Phase 2: ParquetWriter
- Implement `ParquetWriter` with in-memory buffering
- Implement `StatsCollector` for column statistics
- Add Arrow schema conversion helper
- Unit tests for writer + stats collection
- Integration tests writing to memory storage

### Phase 3: RestCatalog.update_table_metadata
- Implement REST commit with snapshot-based locking
- Add `TableRequirement` and `TableUpdate` types
- Wire into existing `commit_orchestrator.rs` retry logic
- Integration tests with mock REST server

### Phase 4: Remove iceberg-rust Dependency
- Delete `iceberg = "0.7.0"` from Cargo.toml
- Remove all `use iceberg::*` imports from codebase
- Fix compilation errors
- Update all examples to use icepick APIs only
- Verify all tests pass

---

## Success Criteria

### Must Have
✅ **No iceberg-rust dependency** - Cargo.toml has no `iceberg = "0.7.0"` line
✅ **Examples work** - r2_basic.rs and s3_tables_basic.rs run successfully
✅ **Can write data** - Examples create tables, write Parquet files, commit snapshots
✅ **All tests pass** - Existing test suite continues to pass
✅ **WASM compilation** - `cargo check --target wasm32-unknown-unknown` succeeds (with appropriate feature flags)
✅ **No functionality loss for write path** - Everything that worked before still works

### Known Limitations (Acceptable for this phase)
❌ **No read/scan functionality** - Examples cannot read data back
❌ **No nested schema examples** - nested_schema.rs example removed or simplified
❌ **No incremental writes** - No append to existing Parquet files

---

## Testing Strategy

### Unit Tests
- `Table` type accessors and methods
- `ParquetWriter` with various schemas
- `StatsCollector` with null values, different types
- Arrow schema conversion helpers

### Integration Tests
- End-to-end write: create table → write parquet → commit → verify metadata
- Catalog operations: create namespace → create table → load table
- REST catalog commit with optimistic locking
- Retry logic on concurrent modification

### Example Validation
- Run r2_basic.rs against real R2 catalog
- Run s3_tables_basic.rs against real S3 Tables
- Verify Parquet files are valid (can be read by PyIceberg/Spark)
- Verify metadata JSON follows Iceberg spec

---

## Future Work (Out of Scope)

### Phase B: Full Write/Read Support
- Implement scan and read infrastructure
- Arrow stream reading from Parquet files
- Filter pushdown and projection
- Update examples to read back data

### Phase C: Advanced Features
- Partitioned table support
- Delete file support
- Schema evolution
- Time travel queries
- Metadata-only operations

---

## References

- Iceberg Spec: https://iceberg.apache.org/spec/
- Iceberg REST Catalog Spec: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
- Apache Parquet Rust: https://docs.rs/parquet
- Apache Arrow Rust: https://docs.rs/arrow
- Previous implementation: docs/plans/2025-11-16-transaction-commit-implementation.md
