# AGENTS.md - icepick

## EXECUTIVE SUMMARY

**icepick** is an experimental Rust client for Apache Iceberg that provides simple, production-ready access to cloud-native Iceberg catalogs (AWS S3 Tables and Cloudflare R2). Unlike the official iceberg-rust library, icepick targets WASM compilation for serverless environments and focuses on REST catalog implementations with minimal configuration. The library abstracts authentication, catalog REST APIs, and file I/O while exposing a clean, type-safe interface for reading and writing Iceberg tables.

## QUICK START

```toml
# Add to Cargo.toml
[dependencies]
icepick = "0.3"
tokio = { version = "1", features = ["full"] }
```

### AWS S3 Tables (native platforms only)

```rust
use icepick::catalog::Catalog;
use icepick::{S3TablesCatalog, spec::{NamespaceIdent, TableIdent}};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create catalog from S3 Tables ARN
    let catalog = S3TablesCatalog::from_arn(
        "my-catalog",
        "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
    ).await?;

    // Load and read a table
    let table_id = TableIdent::from_strs(&["namespace"], "table_name");
    let table = catalog.load_table(&table_id).await?;

    // Scan table data
    let scan = table.scan().build()?;
    let mut stream = scan.to_arrow().await?;

    Ok(())
}
```

### Cloudflare R2 (WASM-compatible)

```rust
use icepick::{R2Catalog, catalog::Catalog, spec::TableIdent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = R2Catalog::new(
        "my-catalog",
        "account-id",
        "bucket-name",
        "cloudflare-api-token"
    ).await?;

    let table_id = "namespace.table_name".parse()?;
    let table = catalog.load_table(&table_id).await?;

    Ok(())
}
```

## CORE CONCEPTS

- **REST Catalog Pattern**: All catalog operations use REST API calls with platform-specific authentication (SigV4 for AWS, bearer tokens for Cloudflare)
- **WASM Compatibility**: R2Catalog compiles to wasm32-unknown-unknown; S3TablesCatalog requires native AWS SDK and is native-only
- **Optimistic Concurrency**: Transactions use metadata location pointers for atomic commits with automatic retry on concurrent modification
- **FileIO Abstraction**: Built on OpenDAL for cross-platform storage access; supports both single-operator (R2) and multi-bucket dynamic operator creation (S3 Tables)
- **Type-Safe Identifiers**: `TableIdent` and `NamespaceIdent` enforce valid naming with compile-time safety

## API SURFACE

```
Module Structure:
├── catalog/          # Catalog implementations (S3TablesCatalog, R2Catalog)
│   ├── auth/        # Authentication (SigV4, bearer tokens)
│   ├── rest/        # REST catalog protocol
│   └── register/    # Register existing Parquet files without rewriting
├── spec/            # Iceberg specification types (Schema, TableIdent, etc.)
├── table/           # Table representation and operations
├── transaction/     # Write operations with ACID guarantees
├── scan/            # Table scanning and reading
├── io/              # FileIO abstraction over OpenDAL
├── writer/          # Parquet writing (both Iceberg and standalone)
├── reader/          # Manifest and data file reading
├── manifest/        # Iceberg manifest handling (Avro)
└── error/           # Structured error types
```

### Most Important Public Items

1. **S3TablesCatalog::from_arn()** - Create AWS S3 Tables catalog
2. **R2Catalog::new()** - Create Cloudflare R2 catalog
3. **Catalog trait** - Core operations (create_table, load_table, list_tables, drop_table)
4. **Table** - Iceberg table with scan() and transaction() methods
5. **Transaction::append().commit()** - Append data files atomically
6. **TableScan::to_arrow()** - Read table as Arrow RecordBatch stream
7. **arrow_to_parquet()** - Write Arrow data directly to S3 without Iceberg metadata
8. **register_data_files()** - Register existing Parquet files without rewriting data
9. **introspect_parquet_file()** - Extract schema, row count, and metrics from Parquet footer

## COMMON PATTERNS

### Pattern 1: Creating and writing to a table

```rust
use icepick::catalog::Catalog;
use icepick::spec::{NestedField, PrimitiveType, Schema, TableCreation, Type};

// Build Iceberg schema
let schema = Schema::builder()
    .with_fields(vec![
        NestedField::required_field(1, "id".to_string(),
            Type::Primitive(PrimitiveType::Long)),
        NestedField::optional_field(2, "name".to_string(),
            Type::Primitive(PrimitiveType::String)),
    ])
    .build()?;

// Create table
let table_creation = TableCreation::builder()
    .with_name("my_table")
    .with_schema(schema)
    .build()?;

let table = catalog.create_table(&namespace, table_creation).await?;

// Write data using ParquetWriter
use icepick::writer::ParquetWriter;
let mut writer = ParquetWriter::new(table.schema()?.clone())?;
writer.write_batch(&arrow_batch)?;

let data_file = writer.finish(
    table.file_io(),
    format!("{}/data/{}.parquet", table.location(), uuid::Uuid::new_v4())
).await?;

// Commit transaction
table.transaction()
    .append(vec![data_file])
    .commit(&catalog)
    .await?;
```

### Pattern 2: Reading table data

```rust
use futures::StreamExt;

let table = catalog.load_table(&table_id).await?;

// Option A: Get data file list
let files = table.files().await?;
for file in files {
    println!("{} - {} rows", file.file_path, file.record_count);
}

// Option B: Stream as Arrow batches
let scan = table.scan().build()?;
let mut stream = scan.to_arrow().await?;

while let Some(batch_result) = stream.next().await {
    let batch = batch_result?;
    // Process batch
}
```

### Pattern 3: Error handling

```rust
use icepick::Error;

match catalog.load_table(&table_id).await {
    Ok(table) => { /* use table */ },
    Err(Error::NotFound { resource }) => {
        eprintln!("Table not found: {}", resource);
    },
    Err(Error::ConcurrentModification { message }) => {
        // Retry transaction
    },
    Err(e) => return Err(e.into()),
}
```

### Pattern 4: Direct Parquet writes (without Iceberg metadata)

```rust
use icepick::{arrow_to_parquet, FileIO, io::AwsCredentials};
use parquet::basic::Compression;

let file_io = FileIO::from_aws_credentials(
    AwsCredentials {
        access_key_id: "key".to_string(),
        secret_access_key: "secret".to_string(),
        session_token: None,
    },
    "us-west-2".to_string()
);

// Simple write
arrow_to_parquet(&batch, "s3://bucket/data.parquet", &file_io).await?;

// With compression
arrow_to_parquet(&batch, "s3://bucket/data.parquet", &file_io)
    .with_compression(Compression::ZSTD(Default::default()))
    .await?;
```

### Pattern 5: Register existing Parquet files

```rust
use icepick::{introspect_parquet_file, DataFileRegistrar, RegisterOptions};
use icepick::spec::{NamespaceIdent, TableIdent};

// Introspect file to get metadata (schema, row count, size, partition values)
let introspection = introspect_parquet_file(
    catalog.file_io(),
    "s3://bucket/data/year=2025/file.parquet",
    Some(&partition_spec), // extracts Hive-style partition values from path
).await?;

// Register files - creates table if needed, skips already-committed files
let options = RegisterOptions::new()
    .allow_create_with_schema(introspection.schema.clone())
    .allow_noop(true); // idempotent

let result = catalog.register_data_files(
    namespace,
    table_ident,
    vec![introspection.data_file],
    options,
).await?;

println!("Added {} files, {} records", result.added_files, result.added_records);
```

## INTEGRATION POINTS

- **Async Runtime**: tokio (required for examples/tests, not enforced as dependency)
- **Serialization**: serde with JSON for REST API, apache-avro for manifest files
- **Arrow/Parquet**: Uses arrow 55.2.0 and parquet 55.2.0 crates directly
- **Storage Backend**: OpenDAL 0.51 with services-s3 and services-memory features
- **Authentication**:
  - Native: aws-config, aws-sdk-sts, aws-sigv4, reqwest with rustls-tls
  - WASM: reqwest with JSON (no TLS features)
- **Key Feature Flags**: None (platform selection via cfg(target_family = "wasm"))
- **Critical Dependencies**: opendal (storage abstraction), async-trait (catalog trait), thiserror (error types)

## CONSTRAINTS & GOTCHAS

- **MSRV**: Rust 2021 edition (likely 1.70+, not explicitly documented)
- **Platform-specific behavior**:
  - `S3TablesCatalog` unavailable on WASM (requires AWS SDK)
  - `R2Catalog` uses `?Send` async trait on WASM (single-threaded)
  - Some error variants (e.g., `Error::InvalidArn`) only exist on native platforms
- **Performance cliffs**:
  - `arrow_to_parquet()` buffers entire Parquet file in memory before upload
  - Table scans read all data files sequentially (no filtering/projection yet)
  - No connection pooling for REST catalog calls
- **Common misuse patterns**:
  - Don't call `table.files()` in a loop - cache the table metadata
  - Don't create new catalog instances per request - reuse them
  - Always reload table after commit to get latest metadata
- **Unsafe code**: None in the library
- **Concurrent modification handling**: Transactions will fail with `ConcurrentModification` error if table metadata changes between load and commit - client must retry with fresh table metadata

## TESTING GUIDANCE

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use icepick::spec::{NamespaceIdent, TableIdent, Schema, NestedField, Type, PrimitiveType};
    use icepick::io::FileIO;
    use opendal::Operator;

    #[tokio::test]
    async fn test_table_operations() {
        // Setup: Create in-memory FileIO for testing
        let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
        let file_io = FileIO::new(op);

        // Create test schema
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required_field(1, "id".to_string(),
                    Type::Primitive(PrimitiveType::Long))
            ])
            .build()
            .unwrap();

        // Test operations
        let metadata = TableMetadata::builder()
            .with_location("memory://test/table")
            .with_current_schema(schema)
            .build()
            .unwrap();

        let table_id = TableIdent::from_strs(&["test"], "table");
        let table = Table::new(table_id, metadata, "memory://test/metadata.json".to_string(), file_io);

        assert_eq!(table.location(), "memory://test/table");
    }
}
```

### Testing with real AWS/Cloudflare services

Use environment variables for credentials in integration tests:

```rust
#[tokio::test]
#[ignore] // Only run with --ignored flag
async fn test_s3_tables_integration() {
    dotenvy::dotenv().ok();
    let arn = std::env::var("S3_TABLES_ARN").unwrap();
    let catalog = S3TablesCatalog::from_arn("test", &arn).await.unwrap();
    // ... test operations
}
```

## CONTRIBUTION VECTORS

- **Code style**: Use `cargo fmt` (standard rustfmt.toml expected but not present)
- **Test coverage**: Unit tests for pure functions, integration tests (with `#[ignore]`) for cloud services
- **Benchmark requirements**: None currently - performance testing is manual
- **Documentation standards**:
  - All public items require doc comments with examples
  - Use `//!` for module-level docs
  - Include `# Errors` section for fallible functions
  - Add `# Examples` with `no_run` for async/cloud examples
- **Where to add new functionality**:
  - New catalog implementations: `src/catalog/<provider>.rs` + update `catalog/mod.rs`
  - New Iceberg spec types: `src/spec/<type>.rs`
  - New write operations: Extend `Transaction` in `src/transaction.rs`
  - New read capabilities: Extend `TableScan` in `src/scan.rs`
  - Storage backend changes: `src/io/file_io.rs`

## SEMANTIC VERSIONING CONTRACT

**Breaking changes** (require major version bump):
- Changes to public trait methods (Catalog, async fn signatures)
- Removal of public types or methods
- Changes to Error enum variants (code matching on them will break)
- Modifications to `TableIdent`, `NamespaceIdent`, or other core spec types
- FileIO method signature changes

**Non-breaking changes** (minor version bump):
- New catalog implementations
- New methods on existing types
- New error variants (if using catch-all patterns)
- Performance improvements without API changes
- New optional features

**Patch changes**:
- Bug fixes in existing functionality
- Documentation improvements
- Internal refactoring
- Dependency updates (within semver compatibility)

## FOR AI AGENTS

### Quick Reference

When working with this library:
1. Always use `catalog::Catalog` trait for catalog operations (don't call REST endpoints directly)
2. Platform check: Use `S3TablesCatalog` for native AWS, `R2Catalog` for WASM/Cloudflare
3. Run `cargo clippy -- -D warnings` before suggesting changes
4. For architecture decisions, this is a thin wrapper over Iceberg REST protocol - prioritize simplicity over feature completeness
5. Error pattern: All errors implement Display with context - use `?` operator and let errors propagate

### Key Invariants to Maintain

- **FileIO must never expose raw OpenDAL Operator** - all file operations go through FileIO methods
- **Catalog implementations must use optimistic locking** - always pass old_metadata_location when updating
- **WASM compatibility for R2Catalog** - never use AWS SDK types in R2 code paths

### When Generating Code Using This Library

**Always:**
- Reload table after commit: `let table = catalog.load_table(&table_id).await?;`
- Use `TableIdent::from_strs(&["namespace"], "table")` for simple construction
- Include proper error handling (don't unwrap on I/O operations)
- Use `#[tokio::main]` or equivalent async runtime in examples
- Add field IDs to Iceberg schemas (required for Parquet field mapping)

**Never:**
- Construct `Table` directly (use catalog methods)
- Reuse `Transaction` after commit (it consumes self)
- Mix S3TablesCatalog with WASM targets
- Assume tables have snapshots (check with `current_snapshot()`)
- Hardcode credentials in examples (use env vars or function parameters)

## PERFORMANCE PROFILE

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| `catalog.load_table()` | O(1) | Single REST API call + metadata JSON parse |
| `table.files()` | O(m) | Reads manifest list + m manifest files (Avro) |
| `table.scan().to_arrow()` | O(n) | Sequential read of n data files, no parallelism yet |
| `transaction.commit()` | O(m) | Write new manifest files + update metadata (atomic CAS) |
| `arrow_to_parquet()` | O(n) | Full buffer in memory before upload |

Where m = number of manifest files, n = number of data files

## COMPARISON MATRIX

| Feature | icepick | iceberg-rust |
|---------|---------|--------------|
| WASM Support | ✅ R2Catalog | ❌ |
| Native AWS | ✅ S3TablesCatalog | ✅ |
| Full Iceberg Spec | ❌ (REST catalogs only) | ✅ (complete) |
| Dependencies | Lightweight | Heavy (full AWS SDK) |
| Maturity | Experimental | Production (Apache) |
| Transaction API | Simplified (append only) | Full (delete, overwrite, etc.) |
| Query Optimization | None yet | Predicate pushdown, projection |

**When to use icepick**: WASM deployment, serverless environments (Cloudflare Workers), simpler API for append-only workloads, R2 Data Catalog support

**When to use iceberg-rust**: Full Iceberg feature support, non-REST catalogs (Glue, Hive, etc.), complex query patterns, production-critical workloads
