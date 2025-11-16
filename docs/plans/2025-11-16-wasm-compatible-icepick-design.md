# WASM-Compatible Icepick Design

**Date:** 2025-11-16
**Status:** Design
**Author:** Brainstorming session with Claude

## Problem Statement

The current Icepick implementation depends on `iceberg-rust v0.7.0`, which has the following blocking issues for WASM compilation:

1. **Tokio is a default feature** - `iceberg-rust` includes tokio as a default dependency
2. **No WASM-friendly FileIO exists** - FileIO is tightly coupled with tokio-based async runtime
3. **No core-only feature flags** - Cannot use just data structures without pulling in I/O dependencies

However, the underlying storage layer **is WASM-compatible**:
- OpenDAL supports `wasm32-unknown-unknown` target
- OpenDAL S3 backend works in browsers and Cloudflare Workers
- Parquet crate compiles to WASM (with `snap` compression, not `zstd`)

## Architectural Decision

We will implement **Approach A: Minimal Vendoring with New API**

### Why Not Other Approaches?

**Rejected: Use iceberg-rust with `default-features = false`**
- Still brings in OpenDAL which brings in async dependencies
- FileIO trait would need custom WASM implementation anyway
- Unclear if enough can be stripped out

**Rejected: Wait for iceberg-rust WASM support**
- Community has discussed it but no concrete timeline
- We control our own destiny

**Rejected: Metadata-only catalog (return JSON, external I/O)**
- Our examples show full read/write workflows are required
- Would violate conceptual integrity of "production-ready cloud catalogs"

### Why This Approach (Brooks' Reasoning)

Following Fred Brooks' principle of **conceptual integrity**:

> "Conceptual integrity is the most important consideration in system design."

**Icepick's Core Concept:** Production-ready Iceberg catalogs for S3-compatible cloud storage

The catalog, storage, and I/O are **one conceptual unit**:
- R2Catalog → R2 storage (inherent coupling)
- S3TablesCatalog → S3 storage (inherent coupling)

This isn't accidental complexity - it's essential coupling. Users should work with Tables, not files.

**Brooks would reject** premature generalization (supporting arbitrary storage backends) when we have **known storage targets**.

## API Design

### Core Catalog Trait

```rust
use async_trait::async_trait;
use std::collections::HashMap;

#[async_trait]
pub trait Catalog: Send + Sync {
    // === Namespace Operations ===

    /// Create a namespace (idempotent - returns Ok if already exists)
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()>;

    /// Check if a namespace exists
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool>;

    // === Table Operations ===

    /// List all tables in a namespace
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>>;

    /// Load an existing table
    async fn load_table(&self, identifier: &TableIdent) -> Result<Table>;

    /// Create a new table
    async fn create_table(&self, creation: TableCreation) -> Result<Table>;

    /// Check if a table exists
    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool>;

    /// Delete a table
    async fn drop_table(&self, identifier: &TableIdent) -> Result<()>;
}
```

**Key Design Decisions:**

1. **Idempotent namespace creation** - Simplifies user code (R2 requires explicit creation)
2. **No `list_namespaces`** - YAGNI, catalogs typically have few namespaces
3. **No rename/register/update_table** - Not in current examples, add later if needed
4. **Simple `TableCreation` struct** - name, namespace, schema, properties (no complex builders)

### Table API with Hidden FileIO

```rust
/// An Iceberg table with integrated OpenDAL-based storage
pub struct Table {
    identifier: TableIdent,
    metadata: TableMetadata,
    metadata_location: String,
    file_io: FileIO, // Private - implementation detail
}

impl Table {
    // === Metadata Access ===

    /// Get the table identifier (namespace + name)
    pub fn identifier(&self) -> &TableIdent;

    /// Get the current table metadata
    pub fn metadata(&self) -> &TableMetadata;

    /// Get the current schema
    pub fn schema(&self) -> &Schema {
        self.metadata.current_schema()
    }

    /// Get the table's base location (S3/R2 path)
    pub fn location(&self) -> &str {
        self.metadata.location()
    }

    // === Data Operations ===

    /// Create a table scan builder for reading data
    pub fn scan(&self) -> ScanBuilder;

    /// Start a new transaction for writing data
    pub fn transaction(&self) -> Transaction;
}
```

**Key Design Decision:** FileIO is hidden - users work with Tables, not files. This enforces conceptual integrity.

### Simplified Transaction API

```rust
/// Transaction builder for writing data to tables
pub struct Transaction<'a> {
    table: &'a Table,
    operations: Vec<TransactionOperation>,
}

impl<'a> Transaction<'a> {
    /// Append data files to the table (creates new snapshot)
    pub fn append(mut self, data_files: Vec<DataFile>) -> Self {
        self.operations.push(TransactionOperation::Append(data_files));
        self
    }

    /// Commit the transaction and return the updated table
    pub async fn commit(self, catalog: &impl Catalog) -> Result<Table>;
}
```

**Before (iceberg-rust):**
```rust
let tx = Transaction::new(&table);
let action = tx.fast_append().add_data_files(data_files);
let tx = action.apply(tx)?;
let table = tx.commit(&catalog).await?;
```

**After (icepick):**
```rust
let table = table.transaction()
    .append(data_files)
    .commit(&catalog).await?;
```

**Key Design Decisions:**

1. **Just `append()`, not `fast_append()`** - Simple, fast by default
2. **Chainable builder** - Can add multiple operations before commit
3. **Returns new Table** - Immutable update pattern
4. **Consumes transaction** - Prevents reuse bugs

## Types to Vendor from iceberg-rust

### Core Identifiers (Simple)
- `TableIdent` - namespace + table name (~50 lines)
- `NamespaceIdent` - namespace identifier (~50 lines)

### Schema Types (Complex)
- `Schema` - table schema with field IDs (~200 lines)
- `NestedField` - field definition (~100 lines)
- `Type` - data types enum (~300 lines)
- `PrimitiveType` - Long, String, Binary, etc.
- `StructType` - nested struct definition

### Metadata (Critical)
- `TableMetadata` - snapshots, schemas, partition specs (~400 lines + serde)
- `Snapshot` - point-in-time table state (~100 lines)
- `ManifestList` - references to manifest files (~100 lines)
- `ManifestFile` - individual manifest metadata (~100 lines)
- `PartitionSpec` - partition configuration (~100 lines, minimal for now)

### Data Files
- `DataFile` - Parquet file metadata (~200 lines)
- `DataFileBuilder` - for constructing DataFiles (~100 lines)

### Arrow Conversion (Critical for Parquet)
- `schema_to_arrow_schema()` - Converts Schema to Arrow with field ID metadata (~150 lines)
- `arrow_to_schema()` - Reverse conversion (~150 lines)

### New Implementations (Build Ourselves)
- `TableCreation` - simple struct
- `ScanBuilder` - builds scans with filters/projections
- `Scan` - represents a scan operation
- `FileIO` - OpenDAL wrapper (WASM-compatible)

**Total vendored code estimate:** ~2,500-3,000 lines

## File Structure

```
icepick/
├── src/
│   ├── spec/              # Vendored from iceberg::spec
│   │   ├── identifier.rs  # TableIdent, NamespaceIdent
│   │   ├── schema.rs      # Schema, NestedField
│   │   ├── types.rs       # Type, PrimitiveType, StructType
│   │   ├── metadata.rs    # TableMetadata
│   │   ├── snapshot.rs    # Snapshot, SnapshotRef
│   │   ├── manifest.rs    # ManifestList, ManifestFile
│   │   ├── partition.rs   # PartitionSpec
│   │   ├── data_file.rs   # DataFile, DataFileBuilder
│   │   └── mod.rs
│   │
│   ├── arrow/             # Vendored from iceberg::arrow
│   │   ├── schema_to_arrow.rs  # schema_to_arrow_schema()
│   │   ├── arrow_to_schema.rs  # arrow_schema_to_schema()
│   │   └── mod.rs
│   │
│   ├── io/                # New - OpenDAL-based FileIO
│   │   ├── file_io.rs     # FileIO wrapper around OpenDAL
│   │   └── mod.rs
│   │
│   ├── catalog/           # Existing code, updated
│   │   ├── mod.rs
│   │   ├── trait.rs       # New Catalog trait
│   │   ├── r2.rs          # Updated to use new types
│   │   ├── s3_tables.rs   # Updated to use new types
│   │   └── rest/          # Existing REST client code
│   │
│   ├── table.rs           # New Table struct + API
│   ├── transaction.rs     # New Transaction API
│   ├── scan.rs            # New Scan + ScanBuilder
│   ├── error.rs           # Existing error types
│   └── lib.rs
```

## Phased Implementation Plan

### Phase 1: Core Identifiers (Validate the approach)

**Goal:** Get basic catalog operations working with minimal vendoring

**Vendor:**
- `TableIdent` (~50 lines)
- `NamespaceIdent` (~50 lines)

**Build:**
- Update `Catalog` trait to use vendored types
- Update R2Catalog/S3TablesCatalog to return vendored types

**Validation:** `cargo build --target wasm32-unknown-unknown` succeeds

**Estimated Effort:** 1-2 hours

---

### Phase 2: Schema Support

**Goal:** Enable table creation with schemas

**Vendor:**
- `Schema` (~200 lines)
- `NestedField` (~100 lines)
- `Type`, `PrimitiveType`, `StructType` (~300 lines)
- `schema_to_arrow_schema()` (~150 lines)

**Build:**
- `TableCreation` struct
- Update `create_table()` to work with Schema

**Validation:** Can create tables with nested schemas (like `examples/nested_schema.rs`)

**Estimated Effort:** 4-6 hours

---

### Phase 3: Metadata Foundation

**Goal:** Load complete table metadata from catalogs

**Vendor:**
- `TableMetadata` (~400 lines + serde)
- `Snapshot` (~100 lines)
- `PartitionSpec` (~100 lines, minimal, no partitioning logic yet)

**Build:**
- Basic `Table` struct with metadata accessors
- `Table::identifier()`, `metadata()`, `schema()`, `location()`

**Validation:** Can load tables and inspect metadata

**Estimated Effort:** 4-6 hours

---

### Phase 4: OpenDAL FileIO

**Goal:** WASM-compatible file operations

**Build (no vendoring):**
- `FileIO` wrapper around OpenDAL `Operator`
- Conditionally compile for WASM vs native
- Methods: `read()`, `write()`, `delete()`, `exists()`

**Implementation notes:**
- Use `#[cfg(target_arch = "wasm32")]` for WASM-specific setup
- Use `#[cfg(not(target_arch = "wasm32"))]` for native-specific setup
- Async operations use `wasm-bindgen-futures` in WASM

**Validation:** Can read/write files to R2/S3 from WASM

**Estimated Effort:** 3-4 hours

---

### Phase 5: Complete Table API

**Goal:** Tables can scan and start transactions

**Build:**
- `Table::scan()` → `ScanBuilder` (stub for now)
- `Table::transaction()` → `Transaction`
- Wire FileIO into Table

**Validation:** Can create Table instances with working I/O

**Estimated Effort:** 2-3 hours

---

### Phase 6: Write Support

**Goal:** Append Parquet files via transactions

**Vendor:**
- `DataFile` (~200 lines)
- `DataFileBuilder` (~100 lines)
- `ManifestList`, `ManifestFile` (~200 lines)

**Build:**
- `Transaction::append()` implementation
- `Transaction::commit()` - create snapshots, write metadata
- Parquet writer integration (using existing pattern from examples)

**Validation:** Can write data and commit (like `examples/r2_basic.rs`)

**Estimated Effort:** 6-8 hours

---

### Phase 7: Read Support

**Goal:** Scan and read Parquet data

**Vendor:**
- Manifest reading logic (~200 lines)
- File scanning logic (~300 lines)

**Build:**
- `ScanBuilder::build()` → `Scan`
- `Scan::to_arrow()` - returns Arrow RecordBatch stream
- Parquet reader integration

**Validation:** Full read/write cycle works end-to-end

**Estimated Effort:** 6-8 hours

---

**Total Estimated Effort:** 26-37 hours

## WASM Compatibility Strategy

### Dependencies

**WASM-compatible:**
```toml
[dependencies]
opendal = { version = "0.54.1", default-features = false }
arrow = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
async-trait = "0.1"

[target.'cfg(target_arch = "wasm32")'.dependencies]
parquet = { workspace = true, features = ["arrow", "async", "snap"] }
wasm-bindgen-futures = "0.4"

[target.'cfg(not(target_arch = "wasm32"))'.dependencies]
parquet = { workspace = true, features = ["arrow", "async", "snap", "zstd"] }
tokio = { workspace = true, features = ["rt", "macros"] }
```

**Key points:**
- No `iceberg` dependency
- Tokio only for native targets
- Snappy compression for WASM, ZSTD for native
- OpenDAL with `default-features = false`

### Async Runtime Strategy

**WASM:** Use browser event loop via `wasm-bindgen-futures::spawn_local`
**Native:** Use tokio (optional feature)

```rust
#[cfg(target_arch = "wasm32")]
pub async fn execute_async<F: Future>(f: F) -> F::Output {
    wasm_bindgen_futures::spawn_local(f);
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn execute_async<F: Future>(f: F) -> F::Output {
    tokio::spawn(f).await.unwrap()
}
```

## Success Criteria

1. **WASM Compilation:** `cargo build --target wasm32-unknown-unknown` succeeds
2. **Native Compilation:** `cargo build` succeeds with all features
3. **Examples Work:** All existing examples (`r2_basic.rs`, `s3_tables_basic.rs`) work unchanged
4. **API Simplicity:** Transaction API is simpler than iceberg-rust
5. **Full Workflow:** Can create namespace, create table, write data, read data (end-to-end)

## Risks and Mitigations

### Risk: Vendored code diverges from iceberg-rust

**Mitigation:**
- Document which iceberg-rust version we vendor from
- Include LICENSE and NOTICE files
- Consider upstreaming WASM support to iceberg-rust later

### Risk: Missing critical iceberg-rust features

**Mitigation:**
- Start with minimal working set
- Add features incrementally as needed
- Examples validate completeness

### Risk: Breaking changes in OpenDAL

**Mitigation:**
- Pin OpenDAL version
- Test WASM compilation in CI
- Document known-working versions

### Risk: WASM async execution complexity

**Mitigation:**
- Use battle-tested `wasm-bindgen-futures`
- Single-threaded execution model (simpler)
- Browser provides event loop (don't fight it)

## Future Enhancements

After core implementation is complete:

1. **Partition support** - Currently minimal, add full partition pruning
2. **Time travel** - Read from historical snapshots
3. **Schema evolution** - Add/remove/rename columns
4. **Overwrite operations** - Replace data, not just append
5. **Delete operations** - Remove rows by filter
6. **Merge operations** - Upsert patterns
7. **REST catalog improvements** - Full Iceberg REST spec compliance

## References

- [iceberg-rust](https://github.com/apache/iceberg-rust)
- [OpenDAL WASM Support Tracking](https://github.com/apache/opendal/issues/3803)
- [Fred Brooks - The Mythical Man-Month](https://en.wikipedia.org/wiki/The_Mythical_Man-Month)
- [Iceberg Table Spec](https://iceberg.apache.org/spec/)
