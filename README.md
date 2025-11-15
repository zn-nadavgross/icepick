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
