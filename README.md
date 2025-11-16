# Rust Iceberg + AWS S3 Tables PoC

Proof-of-concept demonstrating rust-iceberg integration with AWS S3 Tables using custom SigV4 authentication.

## ✅ POC Result: Working

**Finding:** rust-iceberg (v0.7.0) **can** work with AWS S3 Tables by implementing a custom catalog client with AWS SigV4 request signing.

See [Implementation](#implementation) below for technical details.

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
2. Connects to S3 Tables catalog using custom client with SigV4 signing
3. Creates namespace (if doesn't exist)
4. Creates table with simple schema: `{ id: i64 }`
5. Writes 3 rows: [1, 2, 3] to Parquet data files
6. Commits data files as table snapshot via Transaction API
7. Reads data back via table scan
8. Prints both datasets for visual verification

## Expected Output

```
✓ Connected to S3 Tables catalog
✓ Created namespace: my_namespace
✓ Created table: my_namespace.hello_table
✓ Wrote 3 rows to 1 data files
✓ Committed snapshot to table
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

## Implementation

### Architecture

This PoC implements a custom Iceberg catalog client that wraps S3 Tables REST API calls with AWS SigV4 authentication:

```
┌─────────────────────────────────────────┐
│ main.rs (application)                   │
│ - Uses iceberg::Catalog trait           │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│ s3tables::S3TablesCatalog               │
│ - Implements iceberg::Catalog trait     │
│ - Wraps S3TablesClient                  │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│ s3tables::S3TablesClient                │
│ - Signs all HTTP requests with SigV4    │
│ - Handles S3 Tables REST API specifics  │
└─────────────────────────────────────────┘
```

### Key Components

**1. S3TablesClient** (`src/s3tables/client.rs`)
- Low-level REST client for S3 Tables API
- Signs every request with AWS SigV4 using `reqsign` crate
- Implements Iceberg REST endpoints:
  - `POST /v1/namespaces` - Create namespace
  - `POST /v1/namespaces/{ns}/tables` - Create table
  - `GET /v1/namespaces/{ns}/tables/{table}` - Load table metadata
  - `POST /v1/namespaces/{ns}/tables/{table}` - Update table (commit snapshots)

**2. S3TablesCatalog** (`src/s3tables/catalog.rs`)
- Implements `iceberg::Catalog` trait
- Adapter between rust-iceberg and S3TablesClient
- Handles conversion between iceberg types and S3 Tables REST payloads
- Manages FileIO for S3 data access

**3. AWS SigV4 Signing** (via `reqsign` crate)
- Service: `s3tables`
- Region: Extracted from S3 Tables ARN
- Credentials: Loaded from AWS default credential chain
- Request components signed: method, URI, headers, body

### Technical Approach

**Why Custom Client?**

rust-iceberg's `RestCatalog` (v0.7.0) doesn't support SigV4:
- `RestCatalogBuilder.with_client()` accepts only `reqwest::Client`
- No built-in request interceptor/middleware support
- Cannot inject signing logic into existing catalog

**Solution:**

Implement `Catalog` trait with custom HTTP layer:
1. Parse S3 Tables ARN to extract region and endpoint
2. Initialize `reqsign` with AWS credential provider
3. Sign each HTTP request before sending:
   - Build `reqwest::Request` with method, URL, headers, body
   - Convert to `http::Request` for signing
   - Apply SigV4 signature to headers
   - Rebuild `reqwest::Request` with signed headers
   - Execute via `reqwest::Client`
4. Handle responses with proper error mapping

### Dependencies

Key additions to enable S3 Tables support:
- `reqwest = "0.12"` - HTTP client
- `reqsign = "0.18"` - AWS credential and signing framework
- `reqsign-aws-v4 = "2.0"` - AWS SigV4 implementation
- `reqsign-file-read-tokio = "2.0"` - Tokio filesystem for credential files
- `reqsign-http-send-reqwest = "2.0"` - Reqwest integration for credential fetching
- `serde_json = "1.0"` - REST payload serialization

Removed:
- `iceberg-catalog-rest = "0.7.0"` - Replaced by custom implementation

## Known S3 Tables Limitations

S3 Tables has these documented API limitations (as of 2025):
- Limited schema evolution capabilities
- No list operations (namespaces, tables)
- No delete operations (drop namespace/table)
- Partition evolution restrictions
- Time travel may have limited support

This PoC uses a minimal schema and basic operations to avoid these limitations.

## Future Improvements

**Potential rust-iceberg Contributions:**

1. **Add SigV4 support to `iceberg-catalog-rest`**
   - Integrate `reqsign` for AWS credential management
   - Add catalog config: `rest.sigv4-enabled`, `rest.signing-name`, `rest.signing-region`
   - Similar to Java's `RESTSigV4Signer` implementation

2. **Support request middleware**
   - Accept `reqwest_middleware::ClientWithMiddleware` in `RestCatalogBuilder`
   - Enable users to inject custom auth/signing logic
   - More flexible than built-in SigV4

3. **Formalize S3 Tables catalog**
   - Package `s3tables` module as `iceberg-catalog-s3tables` crate
   - Add to official rust-iceberg catalog implementations
   - Maintain alongside REST, Hive, Glue catalogs

**Production Readiness Gaps:**

- Error handling: More specific error types
- Retry logic: Exponential backoff on transient failures
- Testing: Integration tests against S3 Tables
- Performance: Connection pooling, request batching
- Monitoring: Metrics, tracing integration

## Development Workflow

### Git Hooks + Tooling

All Git hooks are managed through [`pre-commit`](https://pre-commit.com/) so you never have to install Python packages globally. The repo assumes you are using [Astral's `uv`](https://github.com/astral-sh/uv); run the installer via `uvx` (or the shorter `ux` alias if you have it configured) and let it bootstrap both the `pre-commit` binary and the hook environments:

```bash
# Install the pre-commit client + hook wrappers
uvx pre-commit install --hook-type pre-commit --hook-type pre-push --hook-type commit-msg

# Optional: run everything against the whole tree
uvx pre-commit run --all-files
```

The configured hooks cover these checks:

- Source hygiene: whitespace cleanup, EOF newlines, and preventing 5MB+ blobs
- `cargo fmt`, `cargo clippy --locked --all-targets --all-features -D warnings`, and `cargo test --locked --all-features` (tests run on `pre-push`)
- `scripts/enforce_quality.py` which enforces a per-file 400 LOC and complexity score of 60 by looking at control-flow keywords; tune via `MAX_LOC_PER_FILE` / `MAX_COMPLEXITY_SCORE`
- Conventional commit messages through `compilerla/conventional-pre-commit`

You can run the quality script directly if you want a quick readout:

```bash
python scripts/enforce_quality.py
# or specify tighter limits
python scripts/enforce_quality.py --max-loc 250 --max-complexity 45
```

These defaults should keep this small PoC tidy; adjust the thresholds as the codebase grows, but keep the hook steps in CI to prevent regressions.
