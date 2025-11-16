# Iceberg-Rust Replacement Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove iceberg-rust dependency and replace with icepick implementations, enabling WASM compilation while maintaining write functionality.

**Architecture:** Implement Table type, ParquetWriter wrapper, and RestCatalog commit operations. Remove iceberg crate dependency and update examples to use pure icepick APIs.

**Tech Stack:** Rust, Apache Arrow, Apache Parquet (via arrow crate), OpenDAL, async-trait

---

## Phase 1: Core Types & Table

### Task 1.1: Add TableCreation Type

**Files:**
- Create: `src/spec/table_creation.rs`
- Modify: `src/spec/mod.rs`

**Step 1: Write test for TableCreation builder**

Create `tests/test_table_creation.rs`:

```rust
use icepick::spec::{TableCreation, Schema, NestedField, PrimitiveType, Type};

#[test]
fn test_table_creation_builder_minimal() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let creation = TableCreation::builder()
        .with_name("test_table")
        .with_schema(schema.clone())
        .build()
        .unwrap();

    assert_eq!(creation.name(), "test_table");
    assert_eq!(creation.schema().fields().len(), 1);
    assert!(creation.location().is_none());
    assert!(creation.properties().is_empty());
}

#[test]
fn test_table_creation_with_location() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let creation = TableCreation::builder()
        .with_name("test_table")
        .with_schema(schema)
        .with_location("s3://bucket/warehouse/table")
        .build()
        .unwrap();

    assert_eq!(creation.location(), Some("s3://bucket/warehouse/table"));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_table_creation`
Expected: FAIL - module `table_creation` not found

**Step 3: Implement TableCreation**

Create `src/spec/table_creation.rs`:

```rust
//! Table creation specification

use crate::spec::Schema;
use crate::error::Result;
use std::collections::HashMap;

/// Specification for creating a new table
#[derive(Debug, Clone)]
pub struct TableCreation {
    name: String,
    schema: Schema,
    location: Option<String>,
    properties: HashMap<String, String>,
}

impl TableCreation {
    /// Create a new builder
    pub fn builder() -> TableCreationBuilder {
        TableCreationBuilder::default()
    }

    /// Get table name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get schema
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Get optional location
    pub fn location(&self) -> Option<&str> {
        self.location.as_deref()
    }

    /// Get properties
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

/// Builder for TableCreation
#[derive(Default)]
pub struct TableCreationBuilder {
    name: Option<String>,
    schema: Option<Schema>,
    location: Option<String>,
    properties: HashMap<String, String>,
}

impl TableCreationBuilder {
    /// Set table name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set schema
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set optional location
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Add a property
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Build TableCreation
    pub fn build(self) -> Result<TableCreation> {
        let name = self.name
            .ok_or_else(|| crate::error::Error::invalid_input("Table name is required"))?;
        let schema = self.schema
            .ok_or_else(|| crate::error::Error::invalid_input("Schema is required"))?;

        Ok(TableCreation {
            name,
            schema,
            location: self.location,
            properties: self.properties,
        })
    }
}
```

**Step 4: Export from spec module**

Add to `src/spec/mod.rs`:

```rust
pub mod table_creation;

pub use table_creation::{TableCreation, TableCreationBuilder};
```

**Step 5: Run test to verify it passes**

Run: `cargo test test_table_creation`
Expected: PASS

**Step 6: Commit**

```bash
git add src/spec/table_creation.rs src/spec/mod.rs tests/test_table_creation.rs
git commit -m "feat: add TableCreation type and builder"
```

---

### Task 1.2: Implement Table Type

**Files:**
- Create: `src/table.rs`
- Modify: `src/lib.rs`

**Step 1: Write test for Table creation**

Create `tests/test_table_type.rs`:

```rust
use icepick::table::Table;
use icepick::spec::{TableIdent, NamespaceIdent, TableMetadata, Schema, NestedField, PrimitiveType, Type};
use icepick::io::FileIO;
use opendal::Operator;

#[test]
fn test_table_new() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);

    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://bucket/warehouse/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["default".to_string()]),
        "test_table".to_string(),
    );

    let table = Table::new(
        ident.clone(),
        metadata,
        "s3://bucket/warehouse/table/metadata/v1.metadata.json".to_string(),
        file_io,
    );

    assert_eq!(table.identifier(), &ident);
    assert_eq!(table.location(), "s3://bucket/warehouse/table");
    assert_eq!(table.metadata_location(), "s3://bucket/warehouse/table/metadata/v1.metadata.json");
}

#[test]
fn test_table_current_snapshot() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);

    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://bucket/warehouse/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["default".to_string()]),
        "test_table".to_string(),
    );

    let table = Table::new(ident, metadata, "meta.json".to_string(), file_io);

    assert!(table.current_snapshot().is_none());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_table_new`
Expected: FAIL - module `table` not found

**Step 3: Implement Table type**

Create `src/table.rs`:

```rust
//! Iceberg table

use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{TableIdent, TableMetadata, Snapshot};
use crate::transaction::Transaction;

/// Iceberg table
#[derive(Debug, Clone)]
pub struct Table {
    identifier: TableIdent,
    metadata: TableMetadata,
    metadata_location: String,
    file_io: FileIO,
}

impl Table {
    /// Create a new table
    pub fn new(
        identifier: TableIdent,
        metadata: TableMetadata,
        metadata_location: String,
        file_io: FileIO,
    ) -> Self {
        Self {
            identifier,
            metadata,
            metadata_location,
            file_io,
        }
    }

    /// Get table identifier
    pub fn identifier(&self) -> &TableIdent {
        &self.identifier
    }

    /// Get table metadata
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Get metadata file location
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }

    /// Get table location (base path)
    pub fn location(&self) -> &str {
        self.metadata.location()
    }

    /// Get file IO
    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Get current snapshot
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        self.metadata.current_snapshot()
    }

    /// Create a new transaction
    pub fn transaction(&self) -> Transaction {
        Transaction::new(self)
    }
}
```

**Step 4: Export from lib.rs**

Add to `src/lib.rs`:

```rust
pub mod table;

pub use table::Table;
```

**Step 5: Run test to verify it passes**

Run: `cargo test test_table_new`
Expected: PASS

**Step 6: Commit**

```bash
git add src/table.rs src/lib.rs tests/test_table_type.rs
git commit -m "feat: add Table type with accessors"
```

---

### Task 1.3: Update Catalog Trait

**Files:**
- Modify: `src/catalog/catalog_trait.rs`

**Step 1: Add create_table and load_table signatures**

Update `src/catalog/catalog_trait.rs`:

```rust
use crate::table::Table;
use crate::spec::TableCreation;

#[async_trait]
pub trait Catalog: Send + Sync {
    // ... existing methods ...

    /// Create a new table
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table>;

    /// Load an existing table
    async fn load_table(&self, identifier: &TableIdent) -> Result<Table>;
}
```

**Step 2: Verify compilation**

Run: `cargo check`
Expected: FAIL - trait implementations missing new methods

**Step 3: Commit trait change**

```bash
git add src/catalog/catalog_trait.rs
git commit -m "feat: add create_table and load_table to Catalog trait"
```

---

### Task 1.4: Add Stub Implementations to Catalogs

**Files:**
- Modify: `src/catalog/r2.rs`
- Modify: `src/catalog/s3_tables.rs`
- Modify: `src/catalog/rest/catalog_impl.rs`

**Step 1: Add stub to R2Catalog**

Find `impl Catalog for R2Catalog` in `src/catalog/r2.rs` and add:

```rust
async fn create_table(
    &self,
    namespace: &NamespaceIdent,
    creation: TableCreation,
) -> Result<Table> {
    // Delegate to RestCatalog
    todo!("Implement R2Catalog::create_table delegation")
}

async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
    // Delegate to RestCatalog
    todo!("Implement R2Catalog::load_table delegation")
}
```

**Step 2: Add stub to S3TablesCatalog**

Find `impl Catalog for S3TablesCatalog` in `src/catalog/s3_tables.rs` and add:

```rust
async fn create_table(
    &self,
    namespace: &NamespaceIdent,
    creation: TableCreation,
) -> Result<Table> {
    // Delegate to RestCatalog
    todo!("Implement S3TablesCatalog::create_table delegation")
}

async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
    // Delegate to RestCatalog
    todo!("Implement S3TablesCatalog::load_table delegation")
}
```

**Step 3: Add stub to RestCatalog**

Find `impl Catalog for RestCatalog` in `src/catalog/rest/catalog_impl.rs` and add:

```rust
async fn create_table(
    &self,
    namespace: &NamespaceIdent,
    creation: TableCreation,
) -> Result<Table> {
    todo!("Implement RestCatalog::create_table")
}

async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
    todo!("Implement RestCatalog::load_table")
}
```

**Step 4: Verify compilation**

Run: `cargo check`
Expected: PASS

**Step 5: Commit**

```bash
git add src/catalog/r2.rs src/catalog/s3_tables.rs src/catalog/rest/catalog_impl.rs
git commit -m "feat: add stub create_table and load_table implementations"
```

---

## Phase 2: ParquetWriter

### Task 2.1: Add Arrow Schema Conversion Helper

**Files:**
- Create: `src/arrow_convert.rs`
- Modify: `src/lib.rs`

**Step 1: Write test for schema conversion**

Create `tests/test_arrow_convert.rs`:

```rust
use icepick::arrow_convert::schema_to_arrow;
use icepick::spec::{Schema, NestedField, PrimitiveType, Type};

#[test]
fn test_schema_to_arrow_simple() {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required_field(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
            NestedField::optional_field(2, "name".to_string(), Type::Primitive(PrimitiveType::String)),
        ])
        .build()
        .unwrap();

    let arrow_schema = schema_to_arrow(&schema).unwrap();

    assert_eq!(arrow_schema.fields().len(), 2);
    assert_eq!(arrow_schema.field(0).name(), "id");
    assert_eq!(arrow_schema.field(1).name(), "name");
    assert!(!arrow_schema.field(0).is_nullable());
    assert!(arrow_schema.field(1).is_nullable());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_schema_to_arrow`
Expected: FAIL - module `arrow_convert` not found

**Step 3: Implement schema conversion**

Create `src/arrow_convert.rs`:

```rust
//! Convert between Iceberg and Arrow types

use crate::error::{Error, Result};
use crate::spec::{PrimitiveType, Schema, Type};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use std::sync::Arc;

/// Convert Iceberg schema to Arrow schema
pub fn schema_to_arrow(schema: &Schema) -> Result<ArrowSchema> {
    let fields: Result<Vec<Field>> = schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = type_to_arrow(field.field_type())?;
            Ok(Field::new(
                field.name(),
                data_type,
                !field.required(),
            ))
        })
        .collect();

    Ok(ArrowSchema::new(fields?))
}

/// Convert Iceberg type to Arrow data type
fn type_to_arrow(iceberg_type: &Type) -> Result<DataType> {
    match iceberg_type {
        Type::Primitive(prim) => match prim {
            PrimitiveType::Boolean => Ok(DataType::Boolean),
            PrimitiveType::Int => Ok(DataType::Int32),
            PrimitiveType::Long => Ok(DataType::Int64),
            PrimitiveType::Float => Ok(DataType::Float32),
            PrimitiveType::Double => Ok(DataType::Float64),
            PrimitiveType::String => Ok(DataType::Utf8),
            PrimitiveType::Binary => Ok(DataType::Binary),
            PrimitiveType::Date => Ok(DataType::Date32),
            PrimitiveType::Time => Ok(DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)),
            PrimitiveType::Timestamp => Ok(DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None,
            )),
            PrimitiveType::Timestamptz => Ok(DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some(Arc::from("UTC")),
            )),
            PrimitiveType::Decimal { precision, scale } => {
                Ok(DataType::Decimal128(*precision as u8, *scale as i8))
            }
            _ => Err(Error::invalid_input(format!(
                "Unsupported primitive type: {:?}",
                prim
            ))),
        },
        Type::Struct(struct_type) => {
            let fields: Result<Vec<Field>> = struct_type
                .fields()
                .iter()
                .map(|field| {
                    let data_type = type_to_arrow(field.field_type())?;
                    Ok(Field::new(field.name(), data_type, !field.required()))
                })
                .collect();
            Ok(DataType::Struct(fields?.into()))
        }
        Type::List(list_type) => {
            let element_field = list_type.element_field();
            let element_type = type_to_arrow(element_field.field_type())?;
            Ok(DataType::List(Arc::new(Field::new(
                "element",
                element_type,
                !element_field.required(),
            ))))
        }
        Type::Map(_) => Err(Error::invalid_input("Map type not yet supported")),
    }
}
```

**Step 4: Add arrow dependency**

Update `Cargo.toml` dependencies:

```toml
arrow = { version = "53.0", default-features = false, features = ["prettyprint"] }
```

**Step 5: Export from lib.rs**

Add to `src/lib.rs`:

```rust
pub mod arrow_convert;
```

**Step 6: Run test to verify it passes**

Run: `cargo test test_schema_to_arrow`
Expected: PASS

**Step 7: Commit**

```bash
git add src/arrow_convert.rs src/lib.rs tests/test_arrow_convert.rs Cargo.toml
git commit -m "feat: add Arrow schema conversion helper"
```

---

### Task 2.2: Implement StatsCollector

**Files:**
- Create: `src/writer/stats.rs`
- Create: `src/writer/mod.rs`
- Modify: `src/lib.rs`

**Step 1: Write test for stats collection**

Create `tests/test_stats_collector.rs`:

```rust
use icepick::writer::stats::StatsCollector;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[test]
fn test_stats_collector_basic() {
    let mut collector = StatsCollector::new();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap();

    collector.collect(&batch).unwrap();

    let stats = collector.finalize();
    assert_eq!(stats.record_count, 3);
    assert_eq!(stats.value_counts.get(&0), Some(&3));
    assert_eq!(stats.value_counts.get(&1), Some(&2)); // 2 non-null
    assert_eq!(stats.null_value_counts.get(&1), Some(&1)); // 1 null
}

#[test]
fn test_stats_collector_multiple_batches() {
    let mut collector = StatsCollector::new();

    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int64Array::from(vec![3, 4, 5]))],
    )
    .unwrap();

    collector.collect(&batch1).unwrap();
    collector.collect(&batch2).unwrap();

    let stats = collector.finalize();
    assert_eq!(stats.record_count, 5);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_stats_collector`
Expected: FAIL - module `writer` not found

**Step 3: Create writer module**

Create `src/writer/mod.rs`:

```rust
//! Parquet writer and statistics collection

pub mod stats;
```

Add to `src/lib.rs`:

```rust
pub mod writer;
```

**Step 4: Implement StatsCollector**

Create `src/writer/stats.rs`:

```rust
//! Statistics collection for Parquet files

use crate::error::Result;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

/// Statistics collected from Arrow batches
#[derive(Debug, Clone)]
pub struct FileStats {
    pub record_count: i64,
    pub column_sizes: HashMap<i32, i64>,
    pub value_counts: HashMap<i32, i64>,
    pub null_value_counts: HashMap<i32, i64>,
}

/// Collector for file statistics
pub struct StatsCollector {
    record_count: i64,
    value_counts: HashMap<i32, i64>,
    null_value_counts: HashMap<i32, i64>,
}

impl StatsCollector {
    /// Create a new stats collector
    pub fn new() -> Self {
        Self {
            record_count: 0,
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
        }
    }

    /// Collect statistics from a record batch
    pub fn collect(&mut self, batch: &RecordBatch) -> Result<()> {
        self.record_count += batch.num_rows() as i64;

        for (col_idx, column) in batch.columns().iter().enumerate() {
            let field_id = col_idx as i32; // Simple mapping for now

            // Count non-null values
            let non_null_count = column.len() - column.null_count();
            *self.value_counts.entry(field_id).or_insert(0) += non_null_count as i64;

            // Count null values
            let null_count = column.null_count();
            if null_count > 0 {
                *self.null_value_counts.entry(field_id).or_insert(0) += null_count as i64;
            }
        }

        Ok(())
    }

    /// Finalize and return statistics
    pub fn finalize(self) -> FileStats {
        FileStats {
            record_count: self.record_count,
            column_sizes: HashMap::new(), // Not tracking byte sizes for MVP
            value_counts: self.value_counts,
            null_value_counts: self.null_value_counts,
        }
    }
}

impl Default for StatsCollector {
    fn default() -> Self {
        Self::new()
    }
}
```

**Step 5: Run test to verify it passes**

Run: `cargo test test_stats_collector`
Expected: PASS

**Step 6: Commit**

```bash
git add src/writer/mod.rs src/writer/stats.rs src/lib.rs tests/test_stats_collector.rs
git commit -m "feat: add StatsCollector for Parquet statistics"
```

---

### Task 2.3: Implement ParquetWriter

**Files:**
- Create: `src/writer/parquet.rs`
- Modify: `src/writer/mod.rs`

**Step 1: Write test for ParquetWriter**

Create `tests/test_parquet_writer.rs`:

```rust
use icepick::writer::ParquetWriter;
use icepick::spec::{Schema, NestedField, PrimitiveType, Type};
use icepick::io::FileIO;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use opendal::Operator;
use std::sync::Arc;

#[tokio::test]
async fn test_parquet_writer_simple() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let mut writer = ParquetWriter::new(schema).unwrap();

    let arrow_schema = ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    writer.write_batch(&batch).unwrap();

    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

    let data_file = writer
        .finish(&file_io, "test.parquet".to_string())
        .await
        .unwrap();

    assert_eq!(data_file.file_path(), "test.parquet");
    assert_eq!(data_file.file_format(), "PARQUET");
    assert_eq!(data_file.record_count(), 3);
    assert!(data_file.file_size_in_bytes() > 0);

    // Verify file was written
    let exists = op.exists("test.parquet").await.unwrap();
    assert!(exists);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_parquet_writer_simple`
Expected: FAIL - `ParquetWriter` not found

**Step 3: Add parquet dependency**

Update `Cargo.toml` dependencies:

```toml
parquet = { version = "53.0", default-features = false, features = ["arrow"] }
```

**Step 4: Implement ParquetWriter**

Create `src/writer/parquet.rs`:

```rust
//! Parquet file writer

use crate::arrow_convert::schema_to_arrow;
use crate::error::{Error, Result};
use crate::io::FileIO;
use crate::spec::{DataFile, Schema};
use crate::writer::stats::StatsCollector;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Parquet file writer
pub struct ParquetWriter {
    schema: Schema,
    parquet_writer: ArrowWriter<Vec<u8>>,
    stats_collector: StatsCollector,
}

impl ParquetWriter {
    /// Create a new Parquet writer
    pub fn new(schema: Schema) -> Result<Self> {
        let arrow_schema = schema_to_arrow(&schema)?;

        let buffer = Vec::new();
        let props = WriterProperties::builder().build();

        let parquet_writer = ArrowWriter::try_new(buffer, arrow_schema.into(), Some(props))
            .map_err(|e| Error::invalid_input(format!("Failed to create Parquet writer: {}", e)))?;

        Ok(Self {
            schema,
            parquet_writer,
            stats_collector: StatsCollector::new(),
        })
    }

    /// Write an Arrow RecordBatch
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.stats_collector.collect(batch)?;

        self.parquet_writer
            .write(batch)
            .map_err(|e| Error::invalid_input(format!("Failed to write batch: {}", e)))?;

        Ok(())
    }

    /// Finish writing and upload to storage, returning DataFile
    pub async fn finish(
        mut self,
        file_io: &FileIO,
        path: String,
    ) -> Result<DataFile> {
        // Close parquet writer
        self.parquet_writer
            .close()
            .map_err(|e| Error::invalid_input(format!("Failed to close writer: {}", e)))?;

        // Get buffer with parquet data
        let parquet_bytes = self.parquet_writer
            .into_inner()
            .map_err(|e| Error::invalid_input(format!("Failed to get buffer: {}", e)))?;

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
            .with_value_counts(stats.value_counts)
            .with_null_value_counts(stats.null_value_counts)
            .build()?)
    }
}
```

**Step 5: Export from writer module**

Update `src/writer/mod.rs`:

```rust
pub mod parquet;
pub mod stats;

pub use parquet::ParquetWriter;
```

**Step 6: Run test to verify it passes**

Run: `cargo test test_parquet_writer_simple`
Expected: PASS

**Step 7: Commit**

```bash
git add src/writer/parquet.rs src/writer/mod.rs tests/test_parquet_writer.rs Cargo.toml
git commit -m "feat: implement ParquetWriter with statistics"
```

---

## Phase 3: RestCatalog Implementation

### Task 3.1: Add REST Commit Types

**Files:**
- Create: `src/catalog/rest/commit_types.rs`
- Modify: `src/catalog/rest/mod.rs`

**Step 1: Implement commit request/response types**

Create `src/catalog/rest/commit_types.rs`:

```rust
//! Types for REST catalog commit operations

use serde::{Deserialize, Serialize};

/// Request to commit table changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitTableRequest {
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
}

/// Requirements that must be met for commit to succeed
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TableRequirement {
    #[serde(rename = "assert-current-schema-id")]
    AssertCurrentSchemaId {
        #[serde(rename = "current-schema-id")]
        current_schema_id: i32
    },

    #[serde(rename = "assert-last-assigned-field-id")]
    AssertLastAssignedFieldId {
        #[serde(rename = "last-assigned-field-id")]
        last_assigned_field_id: i32
    },

    #[serde(rename = "assert-current-snapshot-id")]
    AssertCurrentSnapshotId {
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
}

/// Updates to apply to the table
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    #[serde(rename = "set-snapshot-ref")]
    SetSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
        #[serde(rename = "type")]
        ref_type: String,
    },

    #[serde(rename = "upgrade-format-version")]
    UpgradeFormatVersion {
        #[serde(rename = "format-version")]
        format_version: i32,
    },
}

/// Response from commit operation
#[derive(Debug, Clone, Deserialize)]
pub struct CommitTableResponse {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    pub metadata: crate::spec::TableMetadata,
}
```

**Step 2: Export from rest module**

Add to `src/catalog/rest/mod.rs`:

```rust
pub mod commit_types;
```

**Step 3: Verify compilation**

Run: `cargo check`
Expected: PASS

**Step 4: Commit**

```bash
git add src/catalog/rest/commit_types.rs src/catalog/rest/mod.rs
git commit -m "feat: add REST catalog commit types"
```

---

### Task 3.2: Implement RestCatalog.update_table_metadata

**Files:**
- Modify: `src/catalog/rest/catalog_impl.rs`
- Modify: `src/catalog/rest/client.rs`

**Step 1: Add commit_table method to RestClient**

Add to `src/catalog/rest/client.rs`:

```rust
use crate::catalog::rest::commit_types::{CommitTableRequest, CommitTableResponse};
use crate::spec::TableIdent;

impl RestClient {
    /// Commit table changes
    pub async fn commit_table(
        &self,
        identifier: &TableIdent,
        request: CommitTableRequest,
    ) -> Result<CommitTableResponse> {
        let namespace = identifier.namespace().as_ref().join("/");
        let table_name = identifier.name();

        let url = format!(
            "{}/v1/namespaces/{}/tables/{}",
            self.base_url, namespace, table_name
        );

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::io_error(format!("Failed to commit table: {}", e)))?;

        if response.status().as_u16() == 409 {
            return Err(Error::concurrent_modification(
                "Concurrent modification detected"
            ));
        }

        let commit_response: CommitTableResponse = response
            .json()
            .await
            .map_err(|e| Error::io_error(format!("Failed to parse response: {}", e)))?;

        Ok(commit_response)
    }
}
```

**Step 2: Implement update_table_metadata in RestCatalog**

Replace the `todo!()` in `src/catalog/rest/catalog_impl.rs`:

```rust
use crate::catalog::rest::commit_types::{CommitTableRequest, TableRequirement, TableUpdate};

async fn update_table_metadata(
    &self,
    identifier: &TableIdent,
    old_metadata_location: &str,
    new_metadata_location: &str,
) -> Result<()> {
    // 1. Load current metadata to get current snapshot ID
    let current_metadata_bytes = self.file_io.read(old_metadata_location).await?;
    let current_metadata: crate::spec::TableMetadata =
        serde_json::from_slice(&current_metadata_bytes)?;
    let current_snapshot_id = current_metadata.current_snapshot_id();

    // 2. Load new metadata to get new snapshot ID
    let new_metadata_bytes = self.file_io.read(new_metadata_location).await?;
    let new_metadata: crate::spec::TableMetadata =
        serde_json::from_slice(&new_metadata_bytes)?;
    let new_snapshot_id = new_metadata
        .current_snapshot_id()
        .ok_or_else(|| Error::invalid_input("New metadata has no snapshot"))?;

    // 3. Build commit request
    let request = CommitTableRequest {
        requirements: vec![TableRequirement::AssertCurrentSnapshotId {
            snapshot_id: current_snapshot_id,
        }],
        updates: vec![
            TableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                snapshot_id: new_snapshot_id,
                ref_type: "branch".to_string(),
            },
        ],
    };

    // 4. Send to REST endpoint
    self.rest_client.commit_table(identifier, request).await?;

    Ok(())
}
```

**Step 3: Verify compilation**

Run: `cargo check`
Expected: PASS

**Step 4: Write integration test**

Create `tests/test_rest_commit.rs`:

```rust
// This would be a full integration test with mock REST server
// For now, just verify the types compile and methods exist

#[test]
fn test_commit_types_serialize() {
    use icepick::catalog::rest::commit_types::*;

    let req = CommitTableRequest {
        requirements: vec![TableRequirement::AssertCurrentSnapshotId {
            snapshot_id: Some(1),
        }],
        updates: vec![TableUpdate::SetSnapshotRef {
            ref_name: "main".to_string(),
            snapshot_id: 2,
            ref_type: "branch".to_string(),
        }],
    };

    let json = serde_json::to_string(&req).unwrap();
    assert!(json.contains("assert-current-snapshot-id"));
}
```

**Step 5: Run test**

Run: `cargo test test_commit_types`
Expected: PASS

**Step 6: Commit**

```bash
git add src/catalog/rest/catalog_impl.rs src/catalog/rest/client.rs tests/test_rest_commit.rs
git commit -m "feat: implement RestCatalog.update_table_metadata"
```

---

### Task 3.3: Implement RestCatalog create_table and load_table

**Files:**
- Modify: `src/catalog/rest/catalog_impl.rs`
- Modify: `src/catalog/rest/client.rs`
- Modify: `src/catalog/rest/types.rs`

**Step 1: Add create/load response types**

Add to `src/catalog/rest/types.rs`:

```rust
use crate::spec::TableMetadata;

#[derive(Debug, Deserialize)]
pub struct LoadTableResponse {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    pub metadata: TableMetadata,
}

#[derive(Debug, Serialize)]
pub struct CreateTableRequest {
    pub name: String,
    pub schema: crate::spec::Schema,
    pub location: Option<String>,
    pub properties: HashMap<String, String>,
}
```

**Step 2: Add client methods**

Add to `src/catalog/rest/client.rs`:

```rust
impl RestClient {
    pub async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<LoadTableResponse> {
        let ns = namespace.as_ref().join("/");
        let url = format!("{}/v1/namespaces/{}/tables", self.base_url, ns);

        let request = CreateTableRequest {
            name: creation.name().to_string(),
            schema: creation.schema().clone(),
            location: creation.location().map(|s| s.to_string()),
            properties: creation.properties().clone(),
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::io_error(format!("Failed to create table: {}", e)))?;

        response
            .json()
            .await
            .map_err(|e| Error::io_error(format!("Failed to parse response: {}", e)))
    }

    pub async fn load_table(&self, identifier: &TableIdent) -> Result<LoadTableResponse> {
        let namespace = identifier.namespace().as_ref().join("/");
        let table_name = identifier.name();
        let url = format!(
            "{}/v1/namespaces/{}/tables/{}",
            self.base_url, namespace, table_name
        );

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::io_error(format!("Failed to load table: {}", e)))?;

        response
            .json()
            .await
            .map_err(|e| Error::io_error(format!("Failed to parse response: {}", e)))
    }
}
```

**Step 3: Implement catalog methods**

Replace `todo!()` in `src/catalog/rest/catalog_impl.rs`:

```rust
async fn create_table(
    &self,
    namespace: &NamespaceIdent,
    creation: TableCreation,
) -> Result<Table> {
    let response = self.rest_client.create_table(namespace, creation).await?;

    Ok(Table::new(
        TableIdent::new(namespace.clone(), response.metadata.location().to_string()),
        response.metadata,
        response.metadata_location,
        self.file_io.clone(),
    ))
}

async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
    let response = self.rest_client.load_table(identifier).await?;

    Ok(Table::new(
        identifier.clone(),
        response.metadata,
        response.metadata_location,
        self.file_io.clone(),
    ))
}
```

**Step 4: Verify compilation**

Run: `cargo check`
Expected: PASS

**Step 5: Commit**

```bash
git add src/catalog/rest/catalog_impl.rs src/catalog/rest/client.rs src/catalog/rest/types.rs
git commit -m "feat: implement RestCatalog create_table and load_table"
```

---

### Task 3.4: Implement Delegation in R2 and S3Tables

**Files:**
- Modify: `src/catalog/r2.rs`
- Modify: `src/catalog/s3_tables.rs`

**Step 1: Implement R2Catalog delegation**

Replace `todo!()` in `src/catalog/r2.rs`:

```rust
async fn create_table(
    &self,
    namespace: &NamespaceIdent,
    creation: TableCreation,
) -> Result<Table> {
    self.rest_catalog.create_table(namespace, creation).await
}

async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
    self.rest_catalog.load_table(identifier).await
}

async fn update_table_metadata(
    &self,
    identifier: &TableIdent,
    old_metadata_location: &str,
    new_metadata_location: &str,
) -> Result<()> {
    self.rest_catalog
        .update_table_metadata(identifier, old_metadata_location, new_metadata_location)
        .await
}
```

**Step 2: Implement S3TablesCatalog delegation**

Replace `todo!()` in `src/catalog/s3_tables.rs`:

```rust
async fn create_table(
    &self,
    namespace: &NamespaceIdent,
    creation: TableCreation,
) -> Result<Table> {
    self.rest_catalog.create_table(namespace, creation).await
}

async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
    self.rest_catalog.load_table(identifier).await
}

async fn update_table_metadata(
    &self,
    identifier: &TableIdent,
    old_metadata_location: &str,
    new_metadata_location: &str,
) -> Result<()> {
    self.rest_catalog
        .update_table_metadata(identifier, old_metadata_location, new_metadata_location)
        .await
}
```

**Step 3: Verify compilation**

Run: `cargo check`
Expected: PASS

**Step 4: Commit**

```bash
git add src/catalog/r2.rs src/catalog/s3_tables.rs
git commit -m "feat: implement catalog method delegation for R2 and S3Tables"
```

---

## Phase 4: Remove iceberg-rust Dependency

### Task 4.1: Audit iceberg-rust Usage

**Files:**
- None (investigation step)

**Step 1: Find all iceberg imports**

Run: `grep -r "use iceberg::" src/ examples/ --include="*.rs" > /tmp/iceberg_usage.txt`

**Step 2: Review usage**

Run: `cat /tmp/iceberg_usage.txt`

Review the list and identify what needs to be replaced.

**Step 3: Document findings**

List the modules that need updates (likely: examples, catalog implementations)

---

### Task 4.2: Update Transaction to Use icepick::Table

**Files:**
- Modify: `src/transaction.rs`

**Step 1: Update Transaction to accept icepick::Table**

Modify `src/transaction.rs`:

```rust
use crate::table::Table;

impl<'a> Transaction<'a> {
    pub fn new(table: &'a Table) -> Self {
        Self {
            table,
            operations: Vec::new(),
        }
    }

    pub fn table(&self) -> &'a Table {
        self.table
    }
}
```

**Step 2: Verify compilation**

Run: `cargo check`
Expected: May have errors in commit orchestrator

**Step 3: Update commit orchestrator if needed**

Review `src/commit/orchestrator.rs` and ensure it uses `Table` methods correctly.

**Step 4: Commit**

```bash
git add src/transaction.rs src/commit/orchestrator.rs
git commit -m "refactor: update Transaction to use icepick::Table"
```

---

### Task 4.3: Remove iceberg from Cargo.toml

**Files:**
- Modify: `Cargo.toml`

**Step 1: Remove iceberg dependency**

Delete this line from `Cargo.toml`:

```toml
iceberg = "0.7.0"
```

**Step 2: Attempt compilation**

Run: `cargo check`
Expected: FAIL with compilation errors showing where iceberg is still used

**Step 3: Document errors**

List all compilation errors to plan fixes.

---

### Task 4.4: Update Examples - Remove iceberg Imports

**Files:**
- Modify: `examples/r2_basic.rs`
- Modify: `examples/s3_tables_basic.rs`
- Delete: `examples/nested_schema.rs` (relies on iceberg features we don't have)

**Step 1: Update r2_basic.rs imports**

Replace iceberg imports in `examples/r2_basic.rs`:

```rust
// Remove these:
// use iceberg::spec::{...};
// use iceberg::writer::...;
// use iceberg::{Catalog, TableCreation, ...};

// Replace with:
use icepick::{Catalog, TableCreation, ParquetWriter};
use icepick::spec::{Schema, NestedField, Type, PrimitiveType};
use icepick::R2Catalog;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
```

**Step 2: Update s3_tables_basic.rs imports**

Similar changes for `examples/s3_tables_basic.rs`.

**Step 3: Delete nested_schema.rs**

Run: `git rm examples/nested_schema.rs`

**Step 4: Verify compilation**

Run: `cargo check --examples`
Expected: Still have errors in example code

**Step 5: Commit**

```bash
git add examples/r2_basic.rs examples/s3_tables_basic.rs
git rm examples/nested_schema.rs
git commit -m "refactor: update example imports to use icepick"
```

---

### Task 4.5: Update r2_basic.rs - Replace Writer Code

**Files:**
- Modify: `examples/r2_basic.rs`

**Step 1: Replace writer setup**

Find the writer setup code in `examples/r2_basic.rs` (around lines 158-185) and replace:

```rust
// OLD: Complex iceberg writer setup
// DELETE: location_generator, file_name_generator, DataFileWriterBuilder, etc.

// NEW: Simple icepick writer
let mut writer = ParquetWriter::new(table.metadata().current_schema().clone())?;
writer.write_batch(&batch)?;

let file_path = format!(
    "{}/data/file-{}.parquet",
    table.location(),
    uuid::Uuid::new_v4()
);

let data_file = writer.finish(table.file_io(), file_path).await?;
```

**Step 2: Replace transaction code**

Find transaction code (around lines 206-214) and replace:

```rust
// OLD: iceberg Transaction::new
// DELETE: let tx = Transaction::new(&table); etc.

// NEW: icepick transaction
table
    .transaction()
    .append(vec![data_file])
    .commit()
    .await?;
```

**Step 3: Remove read/scan code**

Delete the scan and read code (lines 218-242):

```rust
// DELETE: Everything from table.scan() to the print loops
// We don't support reading yet
```

**Step 4: Update output messages**

Replace the final prints:

```rust
println!("✓ Committed snapshot to table");
println!("\nWrote {} rows to {}", batch.num_rows(), file_path);
println!("\nNote: Reading data back is not yet supported in icepick");
```

**Step 5: Test compilation**

Run: `cargo check --example r2_basic`
Expected: PASS

**Step 6: Commit**

```bash
git add examples/r2_basic.rs
git commit -m "refactor: update r2_basic to use icepick ParquetWriter and Transaction"
```

---

### Task 4.6: Update s3_tables_basic.rs

**Files:**
- Modify: `examples/s3_tables_basic.rs`

**Step 1: Apply same changes as r2_basic**

Make identical changes to `examples/s3_tables_basic.rs`:
- Replace writer code
- Replace transaction code
- Remove read/scan code
- Update output messages

**Step 2: Test compilation**

Run: `cargo check --example s3_tables_basic`
Expected: PASS

**Step 3: Commit**

```bash
git add examples/s3_tables_basic.rs
git commit -m "refactor: update s3_tables_basic to use icepick APIs"
```

---

### Task 4.7: Fix Remaining Compilation Errors

**Files:**
- Various (TBD based on errors)

**Step 1: Compile full project**

Run: `cargo build`

**Step 2: Fix each error**

Address any remaining compilation errors one by one.

Common issues:
- Missing trait imports
- Type mismatches
- Method signature changes

**Step 3: Verify all tests pass**

Run: `cargo test`
Expected: All existing tests pass

**Step 4: Commit**

```bash
git add .
git commit -m "fix: resolve remaining compilation errors after iceberg removal"
```

---

### Task 4.8: Verify WASM Compilation

**Files:**
- None (verification step)

**Step 1: Check WASM compilation**

Run: `cargo check --target wasm32-unknown-unknown --no-default-features`

**Step 2: Address WASM-specific issues**

If there are errors, fix them (likely feature flag related).

**Step 3: Document WASM status**

Note: Full WASM compilation may still fail due to dependencies like tokio. Document the current state.

**Step 4: Commit if changes made**

```bash
git add .
git commit -m "chore: verify WASM compilation compatibility"
```

---

## Testing & Verification

### Task 5.1: Run Full Test Suite

**Files:**
- None (verification step)

**Step 1: Run all tests**

Run: `cargo test --all-targets`
Expected: All tests pass

**Step 2: Run tests in release mode**

Run: `cargo test --release`
Expected: All tests pass

**Step 3: Check for warnings**

Run: `cargo clippy -- -D warnings`
Expected: No warnings

**Step 4: Format check**

Run: `cargo fmt -- --check`
Expected: All files formatted

**Step 5: Commit any formatting fixes**

```bash
cargo fmt
git add .
git commit -m "chore: format code"
```

---

### Task 5.2: Manual Testing with Examples

**Files:**
- None (manual testing)

**Step 1: Set up test environment**

Ensure `.env` file has required credentials:
- CLOUDFLARE_ACCOUNT_ID
- CLOUDFLARE_BUCKET_NAME
- CLOUDFLARE_API_TOKEN

**Step 2: Run r2_basic example**

Run: `cargo run --example r2_basic`
Expected: Successfully creates table and writes data

**Step 3: Verify in R2 dashboard**

Check that:
- Table metadata exists
- Parquet files are written
- Manifest files are created

**Step 4: Test s3_tables_basic if available**

Run: `cargo run --example s3_tables_basic`

**Step 5: Document results**

Note any issues or successes.

---

## Success Criteria Verification

### Task 6.1: Verify Success Criteria

**Files:**
- None (checklist verification)

**Step 1: Check iceberg-rust removal**

Run: `grep "iceberg =" Cargo.toml`
Expected: No matches (dependency removed)

**Step 2: Check imports**

Run: `grep -r "use iceberg::" src/ examples/`
Expected: No matches

**Step 3: Verify examples work**

Confirm: Both r2_basic and s3_tables_basic run successfully

**Step 4: Verify tests pass**

Confirm: All tests pass with `cargo test`

**Step 5: Check WASM compilation**

Run: `cargo check --target wasm32-unknown-unknown --no-default-features`
Document: Current status (may have dependency limitations)

**Step 6: Create summary document**

Create `docs/iceberg-replacement-complete.md` with:
- What was implemented
- What works
- Known limitations
- Future work

**Step 7: Final commit**

```bash
git add docs/iceberg-replacement-complete.md
git commit -m "docs: add iceberg replacement completion summary"
```

---

## Related Skills

- @superpowers:test-driven-development - Used throughout for TDD workflow
- @superpowers:verification-before-completion - Use before claiming tasks complete
- @superpowers:systematic-debugging - Use if tests fail unexpectedly
- @superpowers:code-reviewer - Use after each phase for review

## Future Work (Out of Scope)

**Read Path Implementation:**
- Scan API for reading tables
- Arrow stream from Parquet files
- Filter pushdown
- Projection pushdown

**Advanced Features:**
- Partitioned table support
- Delete file support
- Schema evolution
- Time travel queries

**WASM Full Compatibility:**
- Replace tokio with WASM-compatible async runtime
- Replace native dependencies with pure Rust alternatives
- Add wasm-bindgen integration

## References

- Design document: `docs/plans/2025-11-16-iceberg-rust-replacement-design.md`
- Previous transaction commit plan: `docs/plans/2025-11-16-transaction-commit-implementation.md`
- Iceberg spec: https://iceberg.apache.org/spec/
- Arrow Rust docs: https://docs.rs/arrow
- Parquet Rust docs: https://docs.rs/parquet
