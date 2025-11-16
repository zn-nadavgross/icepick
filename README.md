# icepick

**Experimental client for Apache Iceberg in Rust**

icepick provides simple access to Apache Iceberg tables in AWS S3 Tables and Cloudflare R2 Data Catalog. Built on the official [iceberg-rust](https://github.com/apache/iceberg-rust) library, Icepick handles authentication, REST API details, and platform compatibility so you can focus on working with your data.

Why not use [iceberg-rust](https://github.com/apache/iceberg-rust)? This project targets wasm as a compliation target (not supported yet in `iceberg-rust`) and is focused on "serverless" catalogs that implement a subset of the overall Iceberg specification.

## Features

- **AWS S3 Tables** - Full support with SigV4 authentication (native platforms only)
- **Cloudflare R2 Data Catalog** - Full support with bearer token auth (WASM-compatible)
- **Clean API** - Simple factory methods, no complex builders
- **Type-safe errors** - Comprehensive error handling with context
- **Zero-config auth** - Uses AWS credential chain and Cloudflare API tokens
- **Production-ready** - Used in real applications with real data

## Platform Support

| Catalog | Linux/macOS/Windows | WASM (browser/Cloudflare Workers) |
|---------|---------------------|------------------------|
| S3 Tables | ✅ | ❌ (requires AWS SDK) |
| R2 Data Catalog | ✅ | ✅ |

R2Catalog is fully WASM-compatible, making it suitable for Cloudflare Workers, browser applications, and other WASM environments.

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

Uses the AWS default credential provider chain:
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- IAM instance profile (EC2)
- ECS task role

Ensure your credentials have S3 Tables permissions.

### Cloudflare R2 Data Catalog

Uses Cloudflare API tokens:

1. Log into the Cloudflare dashboard
2. Go to "My Profile" → "API Tokens"
3. Create a token with R2 read/write permissions
4. Pass the token when constructing the catalog

## Examples

See the [`examples/`](examples/) directory:

- [`s3_tables_basic.rs`](examples/s3_tables_basic.rs) - Complete S3 Tables workflow
- [`r2_basic.rs`](examples/r2_basic.rs) - Complete R2 Data Catalog workflow

Run examples:

```bash
# S3 Tables
cargo run --example s3_tables_basic

# R2 Data Catalog
cargo run --example r2_basic
```

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

### Format and Lint

```bash
cargo fmt
cargo clippy -- -D warnings
```

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## Acknowledgments

Built on the official [iceberg-rust](https://github.com/apache/iceberg-rust) library from the Apache Iceberg project.
