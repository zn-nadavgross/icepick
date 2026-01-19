# icepick

[![Crates.io](https://img.shields.io/crates/v/icepick.svg)](https://crates.io/crates/icepick)
[![Documentation](https://docs.rs/icepick/badge.svg)](https://docs.rs/icepick)
[![License](https://img.shields.io/crates/l/icepick.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2021%2B-blue.svg)](https://www.rust-lang.org)

A CLI tool and wasm-compatible library for managing Apache Iceberg tables in AWS S3 Tables and Cloudflare R2 Data Catalog.

## Table of Contents

- [What it does](#what-it-does)
- [Why?](#why)
- [Quickstart](#quickstart)
- [CLI Reference](#cli-reference)
  - [Namespaces](#namespaces)
  - [Tables](#tables)
  - [Commit Files](#commit-files)
  - [Compaction](#compaction)
  - [Snapshots](#snapshots)
- [Cloudflare R2](#cloudflare-r2)
- [AWS S3 Tables](#aws-s3-tables)
- [Library Usage](#library-usage)

## What it does

icepick provides a simple command-line interface and wasm-friendly library for working with Apache Iceberg tables:

- **List and inspect** namespaces and tables
- **Scan tables** with partition pruning and column statistics
- **Commit Parquet files** to tables (with auto-detection of Hive-style partitions)
- **Compact small files** using bin-pack compaction
- **Clean up snapshots** based on retention policies

## Why?

The official [iceberg-rust](https://github.com/apache/iceberg-rust) library doesn't yet support WASM compilation, and most Iceberg tools are built for JVM environments. icepick fills the gap for:

- **Serverless environments** like Cloudflare Workers
- **CLI-first workflows** without spinning up Spark or Flink
- **Lightweight table maintenance** (compaction, snapshot cleanup)
- **Quick data exploration** without complex query engines

## Quickstart

### Install

```bash
cargo install icepick --features cli
```

### Configure

Set your catalog credentials:

```bash
# For Cloudflare R2
export ICEPICK_CATALOG_URL="https://catalog.cloudflarestorage.com/<account-id>/<bucket>"
export ICEPICK_TOKEN="<cloudflare-api-token>"

# For AWS S3 Tables
export ICEPICK_CATALOG_ARN="arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
# Uses AWS credential chain (env vars, ~/.aws/credentials, IAM role)
```

### Verify Connection

```bash
# List namespaces
icepick namespace list

# List tables in a namespace
icepick table list --namespace my_namespace

# Get table info
icepick table info my_namespace.my_table
```

## CLI Reference

### Namespaces

```bash
# List all namespaces
icepick namespace list

# Create a namespace
icepick namespace create my_namespace

# Delete a namespace
icepick namespace delete my_namespace
```

### Tables

```bash
# List tables in a namespace
icepick table list --namespace my_namespace

# Get detailed table info (schema, partitioning, snapshots)
icepick table info my_namespace.my_table

# Scan table data (shows pruning stats with filters)
icepick table scan my_namespace.my_table

# Scan with filter
icepick table scan my_namespace.my_table --filter "date >= '2024-01-01'"

# Limit output rows
icepick table scan my_namespace.my_table --limit 100
```

### Commit Files

Commit existing Parquet files to an Iceberg table:

```bash
# Preview what would be committed (dry run)
icepick commit /data/**/*.parquet --namespace prod --table events --dry-run

# Commit files to existing table
icepick commit /data/**/*.parquet --namespace prod --table events

# Create new table with partition spec
icepick commit /data/**/*.parquet --namespace prod --table events \
  --create --partition year:int,month:int

# For non-Hive paths, specify partition values explicitly
icepick commit /flat/*.parquet --namespace prod --table events \
  --partition-values year=2024,month=01

# Use specific file as schema exemplar
icepick commit /data/**/*.parquet --namespace prod --table events \
  --exemplar /data/sample.parquet --create
```

The commit command:
- Uses first file's schema (or `--exemplar`) as the reference
- Validates all files match the schema
- Extracts partition values from Hive-style paths automatically
- Supports `--partition-values` for flat directory structures
- Shows detailed plan with `--dry-run` before committing

### Compaction

Merge small files into larger ones for better query performance:

```bash
# Preview compaction plan (dry run)
icepick compact my_namespace.my_table --dry-run

# Execute compaction with default settings
icepick compact my_namespace.my_table

# Custom target file size (256 MB)
icepick compact my_namespace.my_table --target-size 268435456

# Only compact files smaller than 128 MB
icepick compact my_namespace.my_table --max-input-size 134217728
```

### Snapshots

Manage table snapshots and clean up old versions:

```bash
# List all snapshots with age and status
icepick snapshot list my_namespace.my_table

# Preview cleanup (dry run)
icepick snapshot cleanup my_namespace.my_table --dry-run

# Execute cleanup with retention policy
icepick snapshot cleanup my_namespace.my_table \
  --older-than-days 7 \
  --retain-last 10
```

Snapshot cleanup respects:
- **Current snapshot** - Never expired (it's the current table state)
- **Referenced snapshots** - Never expired if referenced by branches or tags
- **Retention count** - Keeps the N most recent regardless of age
- **Age threshold** - Only expires snapshots older than the threshold

## Cloudflare R2

### Authentication

1. Log into the Cloudflare dashboard
2. Navigate to **My Profile** → **API Tokens**
3. Create a token with **R2 read/write permissions**
4. Set environment variables:

```bash
export ICEPICK_CATALOG_URL="https://catalog.cloudflarestorage.com/<account-id>/<bucket>"
export ICEPICK_TOKEN="<your-api-token>"
```

### WASM Compatibility

The R2 catalog is fully WASM-compatible, making it suitable for:
- Cloudflare Workers
- Browser applications (if your catalog REST API supports CORS)

## AWS S3 Tables

### Authentication

Uses the AWS default credential provider chain:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM instance profile (EC2)
4. ECS task role

```bash
export ICEPICK_CATALOG_ARN="arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
```

> **Important:** Ensure your credentials have S3 Tables permissions.

### Platform Support

S3 Tables requires the AWS SDK and is only available on native platforms (Linux, macOS, Windows). It does not compile to WASM.

## Library Usage

icepick can also be used as a Rust library for programmatic access to Iceberg tables. See [DEVELOPER.md](DEVELOPER.md) for:

- Rust API examples
- Direct Parquet writes
- Registering existing files
- WASM considerations

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## Acknowledgments

Built on the official [iceberg-rust](https://github.com/apache/iceberg-rust) library from the Apache Iceberg project.
