# Rust Iceberg + AWS S3 Tables PoC Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a minimal Rust program that validates rust-iceberg library works with AWS S3 Tables REST API by performing a write/read roundtrip.

**Architecture:** Single-file Rust binary that parses S3 Tables ARN, configures Iceberg REST catalog with SigV4 signing, creates a namespace and table, writes 3 rows with a simple schema (single i64 column), reads the data back, and prints both datasets for visual verification.

**Tech Stack:** Rust, rust-iceberg (REST catalog), Apache Arrow, Tokio async runtime, AWS S3 Tables REST API

---

## Task 1: Project Initialization

**Files:**
- Create: `Cargo.toml`
- Create: `src/main.rs`

**Step 1: Create Cargo.toml with dependencies**

Create `Cargo.toml`:

```toml
[package]
name = "hello-world-iceberg"
version = "0.1.0"
edition = "2021"

[dependencies]
iceberg = "0.7"
iceberg-catalog-rest = "0.7"
tokio = { version = "1", features = ["full"] }
anyhow = "1"
arrow = "56"
```

**Step 2: Create minimal main.rs stub**

Create `src/main.rs`:

```rust
fn main() {
    println!("Hello, Iceberg!");
}
```

**Step 3: Verify project builds**

Run: `cargo build`
Expected: Project compiles successfully

**Step 4: Verify project runs**

Run: `cargo run`
Expected: Prints "Hello, Iceberg!"

**Step 5: Commit**

```bash
git init
git add Cargo.toml src/main.rs
git commit -m "feat: initialize Rust project with dependencies"
```

---

## Task 2: ARN Parsing Function

**Files:**
- Modify: `src/main.rs`

**Step 1: Add ARN parsing function**

Replace `src/main.rs` with:

```rust
use anyhow::{Context, Result, ensure};

/// Parse S3 Tables ARN and extract region and bucket name
/// ARN format: arn:aws:s3tables:region:account:bucket/name
fn parse_s3_tables_arn(arn: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = arn.split(':').collect();
    ensure!(parts.len() == 6, "Invalid S3 Tables ARN format: expected 6 parts");
    ensure!(parts[0] == "arn", "ARN must start with 'arn'");
    ensure!(parts[2] == "s3tables", "Not an S3 Tables ARN");

    let region = parts[3].to_string();
    let bucket_name = parts[5]
        .strip_prefix("bucket/")
        .context("ARN must contain 'bucket/' prefix")?
        .to_string();

    Ok((region, bucket_name))
}

fn main() -> Result<()> {
    // Test ARN parsing
    let test_arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket";
    let (region, bucket) = parse_s3_tables_arn(test_arn)?;
    println!("Region: {}, Bucket: {}", region, bucket);
    Ok(())
}
```

**Step 2: Test ARN parsing**

Run: `cargo run`
Expected: Prints "Region: us-west-2, Bucket: my-bucket"

**Step 3: Test error handling with invalid ARN**

Temporarily modify main to test error:

```rust
fn main() -> Result<()> {
    let invalid_arn = "invalid-arn";
    let result = parse_s3_tables_arn(invalid_arn);
    println!("Error test: {:?}", result);
    Ok(())
}
```

Run: `cargo run`
Expected: Prints error about invalid ARN format

**Step 4: Revert to valid ARN test**

Restore main function from Step 1.

**Step 5: Commit**

```bash
git add src/main.rs
git commit -m "feat: add S3 Tables ARN parsing"
```

---

## Task 3: CLI Argument Parsing

**Files:**
- Modify: `src/main.rs`

**Step 1: Add CLI argument parsing**

Replace the `main` function in `src/main.rs`:

```rust
fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    ensure!(
        args.len() == 4,
        "Usage: {} <s3-tables-arn> <namespace> <table-name>",
        args[0]
    );

    let arn = &args[1];
    let namespace_name = &args[2];
    let table_name = &args[3];

    println!("ARN: {}", arn);
    println!("Namespace: {}", namespace_name);
    println!("Table: {}", table_name);

    let (region, bucket) = parse_s3_tables_arn(arn)?;
    println!("Parsed - Region: {}, Bucket: {}", region, bucket);

    Ok(())
}
```

**Step 2: Test with valid arguments**

Run: `cargo run -- arn:aws:s3tables:us-west-2:123456789012:bucket/test-bucket my_namespace hello_table`
Expected: Prints all parsed values correctly

**Step 3: Test without arguments**

Run: `cargo run`
Expected: Shows usage error message

**Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: add CLI argument parsing"
```

---

## Task 4: REST Catalog Connection

**Files:**
- Modify: `src/main.rs`

**Step 1: Add catalog connection function**

Add after the `parse_s3_tables_arn` function in `src/main.rs`:

```rust
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

/// Create REST catalog configured for S3 Tables
async fn create_s3_tables_catalog(arn: &str, region: &str) -> Result<RestCatalog> {
    let rest_uri = format!("https://s3tables.{}.amazonaws.com/iceberg", region);

    let config = RestCatalogConfig::builder()
        .uri(rest_uri)
        .warehouse(arn.to_string())
        .property("rest.sigv4-enabled", "true")
        .property("rest.signing-name", "s3tables")
        .property("rest.signing-region", region)
        .build()
        .context("Failed to build REST catalog config")?;

    let catalog = RestCatalog::new(config)
        .await
        .context("Failed to create REST catalog")?;

    Ok(catalog)
}
```

**Step 2: Update main to be async and test connection**

Update `src/main.rs` main function:

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    ensure!(
        args.len() == 4,
        "Usage: {} <s3-tables-arn> <namespace> <table-name>",
        args[0]
    );

    let arn = &args[1];
    let namespace_name = &args[2];
    let table_name = &args[3];

    let (region, _bucket) = parse_s3_tables_arn(arn)?;

    let catalog = create_s3_tables_catalog(arn, &region)
        .await
        .context("Failed to connect to S3 Tables catalog")?;

    println!("✓ Connected to S3 Tables catalog");

    Ok(())
}
```

**Step 3: Verify it compiles**

Run: `cargo build`
Expected: Compiles successfully (may see warnings about unused variables)

**Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: add REST catalog connection for S3 Tables"
```

---

## Task 5: Schema Definition

**Files:**
- Modify: `src/main.rs`

**Step 1: Add schema builder function**

Add after the `create_s3_tables_catalog` function in `src/main.rs`:

```rust
use iceberg::spec::{Schema, NestedField, PrimitiveType, Type};

/// Build simple schema: { id: i64 }
fn build_schema() -> Result<Schema> {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))
                .into()
        ])
        .build()
        .context("Failed to build schema")?;

    Ok(schema)
}
```

**Step 2: Test schema creation in main**

Add to main function before the final `Ok(())`:

```rust
    let schema = build_schema()?;
    println!("✓ Created schema with {} fields", schema.fields().len());
```

**Step 3: Verify it compiles and runs**

Run: `cargo run -- arn:aws:s3tables:us-west-2:123456789012:bucket/test my_ns my_table`
Expected: Prints "✓ Created schema with 1 fields"

**Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: add schema builder for simple i64 column"
```

---

## Task 6: Namespace Creation

**Files:**
- Modify: `src/main.rs`

**Step 1: Add namespace import and creation**

Update `src/main.rs` imports at the top:

```rust
use iceberg::Namespace;
use std::collections::HashMap;
```

Add to main function after catalog creation:

```rust
    let namespace = Namespace::new(vec![namespace_name.clone()])
        .context("Failed to create namespace identifier")?;

    // Try to create namespace (may already exist)
    match catalog.create_namespace(&namespace, HashMap::new()).await {
        Ok(_) => println!("✓ Created namespace: {}", namespace_name),
        Err(e) if e.to_string().contains("already exists") => {
            println!("✓ Namespace already exists: {}", namespace_name)
        }
        Err(e) => return Err(e).context("Failed to create namespace")?,
    }
```

**Step 2: Verify it compiles**

Run: `cargo build`
Expected: Compiles successfully

**Step 3: Commit**

```bash
git add src/main.rs
git commit -m "feat: add namespace creation with exists handling"
```

---

## Task 7: Table Creation

**Files:**
- Modify: `src/main.rs`

**Step 1: Add table creation**

Add to main function after namespace creation:

```rust
    let schema = build_schema()?;

    let table_ident = iceberg::TableIdent::new(namespace.clone(), table_name.clone());

    let table = catalog
        .create_table(&table_ident, schema)
        .await
        .context(format!("Failed to create table '{}'", table_name))?;

    println!("✓ Created table: {}.{}", namespace_name, table_name);
```

**Step 2: Add TableIdent import**

Add to imports at top of `src/main.rs`:

```rust
use iceberg::TableIdent;
```

**Step 3: Verify it compiles**

Run: `cargo build`
Expected: Compiles successfully

**Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: add table creation"
```

---

## Task 8: Data Writing

**Files:**
- Modify: `src/main.rs`

**Step 1: Add sample data creation function**

Add after `build_schema` function:

```rust
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Create sample data: [1, 2, 3]
fn create_sample_data() -> Result<RecordBatch> {
    let id_array = Int64Array::from(vec![1, 2, 3]);

    let arrow_schema = ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false)
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(id_array)]
    )
    .context("Failed to create record batch")?;

    Ok(batch)
}
```

**Step 2: Add data writing to main**

Add after table creation in main function:

```rust
    let batch = create_sample_data()?;

    let mut writer = table
        .writer()
        .build()
        .await
        .context("Failed to create table writer")?;

    writer
        .write(batch.clone())
        .await
        .context("Failed to write data")?;

    writer
        .close()
        .await
        .context("Failed to close writer")?;

    println!("✓ Wrote {} rows", batch.num_rows());
```

**Step 3: Verify it compiles**

Run: `cargo build`
Expected: Compiles successfully

**Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: add data writing with Arrow RecordBatch"
```

---

## Task 9: Data Reading

**Files:**
- Modify: `src/main.rs`

**Step 1: Add data reading to main**

Add after data writing in main function:

```rust
    let scan = table
        .scan()
        .build()
        .await
        .context("Failed to create table scan")?;

    let mut stream = scan
        .to_arrow()
        .await
        .context("Failed to create arrow stream")?;

    let mut read_batches = Vec::new();
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.context("Failed to read batch")?;
        read_batches.push(batch);
    }

    println!("✓ Read {} batches", read_batches.len());
```

**Step 2: Add stream import**

Add to imports at top:

```rust
use futures::stream::StreamExt;
```

**Step 3: Update dependencies in Cargo.toml**

Add to `[dependencies]` section:

```toml
futures = "0.3"
```

**Step 4: Verify it compiles**

Run: `cargo build`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add src/main.rs Cargo.toml
git commit -m "feat: add data reading with scan and arrow stream"
```

---

## Task 10: Visual Verification Output

**Files:**
- Modify: `src/main.rs`

**Step 1: Add print function**

Add after `create_sample_data` function:

```rust
use arrow::util::pretty::print_batches;

/// Print Arrow RecordBatch in pretty table format
fn print_batch(batch: &RecordBatch) -> Result<()> {
    print_batches(&[batch.clone()])
        .context("Failed to print batch")?;
    Ok(())
}
```

**Step 2: Update main to print written and read data**

Replace the write and read sections in main with:

```rust
    let batch = create_sample_data()?;

    let mut writer = table
        .writer()
        .build()
        .await
        .context("Failed to create table writer")?;

    writer
        .write(batch.clone())
        .await
        .context("Failed to write data")?;

    writer
        .close()
        .await
        .context("Failed to close writer")?;

    println!("✓ Wrote {} rows", batch.num_rows());

    let scan = table
        .scan()
        .build()
        .await
        .context("Failed to create table scan")?;

    let mut stream = scan
        .to_arrow()
        .await
        .context("Failed to create arrow stream")?;

    let mut read_batches = Vec::new();
    while let Some(batch_result) = stream.next().await {
        let read_batch = batch_result.context("Failed to read batch")?;
        read_batches.push(read_batch);
    }

    println!("✓ Read {} batches", read_batches.len());

    println!("\nWritten data:");
    print_batch(&batch)?;

    println!("\nRead data:");
    for read_batch in &read_batches {
        print_batch(read_batch)?;
    }
```

**Step 3: Verify it compiles**

Run: `cargo build`
Expected: Compiles successfully

**Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: add visual verification output for write/read roundtrip"
```

---

## Task 11: Final Testing & Documentation

**Files:**
- Create: `README.md`

**Step 1: Create README with usage instructions**

Create `README.md`:

```markdown
# Rust Iceberg + AWS S3 Tables PoC

Minimal proof-of-concept validating that rust-iceberg works with AWS S3 Tables REST API.

## Prerequisites

1. AWS credentials configured (via `~/.aws/credentials` or environment variables)
2. S3 Tables bucket created via AWS console
3. IAM permissions for `s3tables:*` operations

## Usage

```bash
cargo run -- \
  arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket \
  my_namespace \
  hello_table
```

## What it does

1. Parses S3 Tables ARN and extracts region
2. Connects to S3 Tables REST catalog with SigV4 signing
3. Creates namespace (if doesn't exist)
4. Creates table with simple schema: `{ id: i64 }`
5. Writes 3 rows: [1, 2, 3]
6. Reads data back
7. Prints both datasets for visual verification

## Expected Output

```
✓ Connected to S3 Tables catalog
✓ Created namespace: my_namespace
✓ Created table: my_namespace.hello_table
✓ Wrote 3 rows
✓ Read 1 batches

Written data:
+----+
| id |
+----+
| 1  |
| 2  |
| 3  |
+----+

Read data:
+----+
| id |
+----+
| 1  |
| 2  |
| 3  |
+----+
```

## Known S3 Tables Limitations

- Limited schema evolution
- No time travel/snapshots via REST
- Partition evolution restrictions
- Nested types may have limited support

This PoC uses minimal features to avoid these limitations.
```

**Step 2: Verify final build**

Run: `cargo build --release`
Expected: Builds successfully in release mode

**Step 3: Commit**

```bash
git add README.md
git commit -m "docs: add README with usage instructions"
```

---

## Implementation Complete

**Verification checklist:**
- [ ] Project builds: `cargo build`
- [ ] All functions compile without errors
- [ ] README documents usage
- [ ] Design document saved in `docs/plans/`

**Next steps:**
1. Create S3 Tables bucket in AWS console
2. Run program with real ARN
3. Verify write/read roundtrip completes successfully
4. Document any S3 Tables limitations encountered
