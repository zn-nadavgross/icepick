# Rust Iceberg + AWS S3 Tables PoC Design

**Date:** 2025-11-14
**Goal:** Validate that rust.iceberg.apache.org works with AWS S3 Tables REST API

## Overview

A minimal "hello world" Rust program that:
1. Connects to AWS S3 Tables as an Iceberg REST catalog
2. Creates a simple table with one column
3. Writes 3 rows of data
4. Reads the data back
5. Prints both datasets for visual verification

**Success criteria:** Write/read roundtrip completes successfully, proving rust-iceberg works with S3 Tables REST API.

## Project Structure

```
hello-world-iceberg/
├── Cargo.toml
├── docs/
│   └── plans/
│       └── 2025-11-14-rust-iceberg-s3tables-design.md
└── src/
    └── main.rs
```

Single-file implementation in `src/main.rs`.

## Dependencies

```toml
iceberg
iceberg-catalog-rest
tokio
anyhow
arrow
```

## AWS S3 Tables Integration

### ARN Format
```
arn:aws:s3tables:region:account:bucket/name
```

### ARN Parsing
Simple string split by `:`:
- Region at index 3
- Bucket name at index 5 (strip `bucket/` prefix)

### REST Endpoint
```
https://s3tables.{region}.amazonaws.com/iceberg
```

### Authentication
- SigV4 signing enabled via REST catalog config
- Service name: `s3tables`
- Uses AWS credential chain (env vars, ~/.aws/credentials, IAM roles)

### REST Catalog Configuration
```rust
RestCatalogConfig::builder()
    .uri(format!("https://s3tables.{}.amazonaws.com/iceberg", region))
    .warehouse(arn.to_string())
    .property("rest.sigv4-enabled", "true")
    .property("rest.signing-name", "s3tables")
    .property("rest.signing-region", &region)
    .build()
```

## Data Model

**Schema:** Single column
```rust
{ id: i64 }
```

**Sample Data:**
```rust
[1, 2, 3]
```

## Program Flow

1. **Parse CLI arguments:** `<s3-tables-arn> <namespace> <table-name>`
2. **Parse ARN** and extract region
3. **Connect to REST catalog** with SigV4 config
4. **Create namespace** (if doesn't exist)
5. **Create table** with simple schema
6. **Write data** (3 rows)
7. **Read data** back
8. **Print both datasets** for visual verification

## Error Handling

- Use `anyhow::Result` throughout
- Add `.context()` at each step for clear failure messages
- Fail fast with descriptive errors
- Help identify S3 Tables limitations when they occur

## Output Format

**Success:**
```
✓ Connected to S3 Tables catalog
✓ Created namespace: my_namespace
✓ Created table: my_namespace.hello_table
✓ Wrote 3 rows

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

**Failure:**
```
Error: Failed to create namespace 'my_namespace'

Caused by:
    REST catalog returned 400: ...
```

## Known S3 Tables Limitations

From AWS documentation:
- Limited schema evolution
- No time travel/snapshots via REST
- Partition evolution restrictions
- Nested types may have limited support

This PoC uses minimal features to avoid these limitations.

## Running the Program

**Prerequisites:**
1. AWS credentials configured
2. S3 Tables bucket created via AWS console
3. IAM permissions for `s3tables:*` operations

**Command:**
```bash
cargo run -- \
  arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket \
  my_namespace \
  hello_table
```

## Design Decisions

**Pure Iceberg REST approach:** Use standard Iceberg REST catalog operations rather than AWS SDK hybrid approach. This validates the integration cleanly.

**Single file:** Keep all code in `main.rs` for simplicity.

**Minimal schema:** Single i64 column avoids complexity and S3 Tables limitations.

**Visual verification:** Print data rather than automated assertions - easier for PoC debugging.

**CLI arguments:** Flexible without hardcoding credentials or ARNs.
