# icepick

[![Crates.io](https://img.shields.io/crates/v/icepick.svg)](https://crates.io/crates/icepick)
[![Documentation](https://docs.rs/icepick/badge.svg)](https://docs.rs/icepick)
[![License](https://img.shields.io/crates/l/icepick.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2021%2B-blue.svg)](https://www.rust-lang.org)

> **Experimental client for Apache Iceberg in Rust**

**icepick** provides simple access to Apache Iceberg tables in AWS S3 Tables and Cloudflare R2 Data Catalog. Built on the official [iceberg-rust](https://github.com/apache/iceberg-rust) library, icepick handles authentication, REST API details, and platform compatibility so you can focus on working with your data.

---

### Why icepick?

**Why not use [iceberg-rust](https://github.com/apache/iceberg-rust)?** This project targets WASM as a compilation target (not yet supported in `iceberg-rust`) and focuses on "serverless" catalogs that implement a subset of the overall Iceberg specification.

## Features

### Catalog Support
- **AWS S3 Tables** — Full support with SigV4 authentication (native platforms only)
- **Cloudflare R2 Data Catalog** — Full support with bearer token auth (WASM-compatible)
- **Direct S3 Parquet Writes** — Write Arrow data directly to S3 without Iceberg metadata

### Developer Experience
- **Clean API** — Simple factory methods, no complex builders
- **Type-safe errors** — Comprehensive error handling with context
- **Zero-config auth** — Uses AWS credential chain and Cloudflare API tokens
- **Production-ready** — Used in real applications with real data

## Platform Support

| Catalog | Linux/macOS/Windows | WASM (browser/Cloudflare Workers) |
|---------|---------------------|-----------------------------------|
| **S3 Tables** | ✅ | ❌ (requires AWS SDK) |
| **R2 Data Catalog** | ✅ | ✅ |
| **No Catalog** (direct parquet to object storage) | ✅ | ✅ |

> **Note:** R2 Data Catalog and direct Parquet writes are fully WASM-compatible, making them suitable for Cloudflare Workers, browser applications, and other WASM environments.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
icepick = "0.1"
```

## Quick Start

### AWS S3 Tables

```rust
use icepick::S3TablesCatalog;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create catalog from S3 Tables ARN
    let catalog = S3TablesCatalog::from_arn(
        "my-catalog",
        "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
    ).await?;

    // Load a table
    let table = catalog.load_table(
        &"namespace.table_name".parse()?
    ).await?;

    Ok(())
}
```

### Cloudflare R2 Data Catalog

```rust
use icepick::R2Catalog;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create catalog for R2
    let catalog = R2Catalog::new(
        "my-catalog",
        "account-id",
        "bucket-name",
        "api-token"
    ).await?;

    // Load a table
    let table = catalog.load_table(
        &"namespace.table_name".parse()?
    ).await?;

    Ok(())
}
```

## Authentication

### AWS S3 Tables

Uses the **AWS default credential provider chain** in the following order:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM instance profile (EC2)
4. ECS task role

> **Important:** Ensure your credentials have S3 Tables permissions.

### Cloudflare R2 Data Catalog

Uses **Cloudflare API tokens**. To set up:

1. Log into the Cloudflare dashboard
2. Navigate to **My Profile** → **API Tokens**
3. Create a token with **R2 read/write permissions**
4. Pass the token when constructing the catalog

## Direct S3 Parquet Writes

Need to write Parquet files directly to S3 for external tools (Spark, DuckDB, etc.) without Iceberg metadata? Use the `arrow_to_parquet` function:

```rust
use icepick::{arrow_to_parquet, FileIO, io::AwsCredentials};
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::basic::Compression;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup FileIO with AWS credentials
    let file_io = FileIO::from_aws_credentials(
        AwsCredentials {
            access_key_id: "your-key".to_string(),
            secret_access_key: "your-secret".to_string(),
            session_token: None,
        },
        "us-west-2".to_string()
    );

    // Create Arrow data
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
        .with_compression(Compression::ZSTD(parquet::basic::ZstdLevel::default()))
        .await?;

    // Manual partitioning (Hive-style or any structure)
    let date = "2025-01-15";
    let path = format!("s3://my-bucket/data/date={}/data.parquet", date);
    arrow_to_parquet(&batch, &path, &file_io).await?;

    Ok(())
}
```

**Note:** This writes standalone Parquet files without Iceberg metadata. For writing to Iceberg tables, use the `Transaction` API instead.

## Examples

Explore complete working examples in the [`examples/`](examples/) directory:

| Example | Description | Command |
|---------|-------------|---------|
| [`s3_tables_basic.rs`](examples/s3_tables_basic.rs) | Complete S3 Tables workflow | `cargo run --example s3_tables_basic` |
| [`r2_basic.rs`](examples/r2_basic.rs) | Complete R2 Data Catalog workflow | `cargo run --example r2_basic` |

## Development

### Running Tests

```bash
cargo test
```

### WASM Build

Verify R2Catalog compiles for WASM:

```bash
cargo build --target wasm32-unknown-unknown
```

### Code Quality

```bash
# Format code
cargo fmt

# Run linter
cargo clippy -- -D warnings

# Check documentation
cargo doc --no-deps --all-features
```

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## Acknowledgments

Built on the official [iceberg-rust](https://github.com/apache/iceberg-rust) library from the Apache Iceberg project.
