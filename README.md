# Rust Iceberg Cloud Catalogs Library

A Rust library providing rust-iceberg integration with cloud-based Iceberg catalogs:
- **AWS S3 Tables** - using custom SigV4 authentication
- **Cloudflare R2 Data Catalog** - using Bearer token authentication

## Features

- **Pluggable Authentication**: `AuthProvider` trait for extensible authentication
- **Multiple Cloud Providers**: Built-in support for S3 Tables and R2
- **WASM Compatible**: Catalog code is WASM-ready (AWS dependencies conditionally compiled)
- **Type-Safe**: Full Rust implementation of Iceberg REST catalog protocol

## ✅ Status: Working

rust-iceberg (v0.7.0) **works** with both AWS S3 Tables and Cloudflare R2 Data Catalog through a unified REST catalog with pluggable authentication.

See [Implementation](#implementation) below for technical details.

## Library Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
hello-world-iceberg = { path = "path/to/hello-world-iceberg" }
```

Example usage:

```rust
use hello_world_iceberg::catalog::IcebergRestCatalog;
use iceberg::Catalog;

// AWS S3 Tables
let catalog = IcebergRestCatalog::from_s3_tables_arn(
    "my-catalog".to_string(),
    "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
).await?;

// Cloudflare R2
let catalog = IcebergRestCatalog::from_r2(
    "my-catalog".to_string(),
    "account-id",
    "bucket-name",
    "api-token"
).await?;

// Use catalog with iceberg::Catalog trait
let table = catalog.load_table(&table_ident).await?;
```

## Examples

This repository includes working examples for both cloud providers.

### Prerequisites

**For AWS S3 Tables:**
1. AWS credentials configured (via `~/.aws/credentials` or environment variables)
2. S3 Tables bucket created via AWS console
3. IAM permissions for `s3tables:*` operations

**For Cloudflare R2:**
1. R2 bucket created in Cloudflare dashboard
2. API token with R2 read/write permissions
3. Account ID from Cloudflare dashboard

### Running Examples

**AWS S3 Tables:**

```bash
cargo run --example s3_tables_example -- \
  arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket \
  my_namespace \
  hello_table
```

**Cloudflare R2:**

```bash
cargo run --example r2_example -- \
  <account-id> \
  <bucket-name> \
  <api-token> \
  my_namespace \
  hello_table
```

### What the examples do

1. Connects to cloud catalog (S3 Tables with SigV4 or R2 with Bearer token)
2. Creates namespace (if doesn't exist)
3. Creates table with simple schema: `{ id: i64 }`
4. Writes 3 rows: [1, 2, 3] to Parquet data files
5. Commits data files as table snapshot via Transaction API
6. Reads data back via table scan
7. Prints both datasets for visual verification

## Expected Output

```
✓ Connected to [S3 Tables/R2 Data] catalog
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

This PoC implements a unified Iceberg REST catalog with pluggable authentication to support multiple cloud providers:

```
┌─────────────────────────────────────────┐
│ Application (main.rs, examples/)        │
│ - Uses iceberg::Catalog trait           │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│ catalog::IcebergRestCatalog             │
│ - Implements iceberg::Catalog trait     │
│ - Manages HTTP client + AuthProvider    │
│ - Factory methods for each cloud:       │
│   • from_s3_tables_arn()                │
│   • from_r2() / from_r2_config()        │
└──────────────────┬──────────────────────┘
                   │
                   ▼
        ┌──────────┴──────────┐
        │                     │
        ▼                     ▼
┌──────────────┐    ┌──────────────────┐
│ SigV4Auth    │    │ BearerTokenAuth  │
│ (S3 Tables)  │    │ (R2)             │
│ - AWS SigV4  │    │ - Simple token   │
│   signing    │    │   header         │
└──────────────┘    └──────────────────┘
```

### Key Components

**1. IcebergRestCatalog** (`src/catalog/rest/mod.rs`)
- Implements `iceberg::Catalog` trait for cloud REST catalogs
- Manages HTTP client and delegates authentication to `AuthProvider`
- Handles Iceberg REST endpoints:
  - `POST /v1/namespaces` - Create namespace
  - `POST /v1/namespaces/{ns}/tables` - Create table
  - `GET /v1/namespaces/{ns}/tables/{table}` - Load table metadata
  - `POST /v1/namespaces/{ns}/tables/{table}` - Update table (commit snapshots)
- Factory methods for different clouds:
  - `from_s3_tables_arn()` - AWS S3 Tables with ARN parsing
  - `from_r2()` / `from_r2_config()` - Cloudflare R2 with config

**2. AuthProvider Trait** (`src/catalog/mod.rs`)
- Abstraction for request authentication
- `async fn sign_request(&self, request: Request) -> Result<Request>`
- Enables pluggable authentication for different cloud providers

**3. SigV4AuthProvider** (`src/catalog/auth/sigv4.rs`)
- AWS SigV4 signing for S3 Tables
- Service: `s3tables`
- Region: Extracted from S3 Tables ARN
- Credentials: Loaded from AWS default credential chain
- Request components signed: method, URI, headers, body

**4. BearerTokenAuthProvider** (`src/catalog/auth/bearer.rs`)
- Simple Bearer token authentication for Cloudflare R2
- Adds `Authorization: Bearer <token>` header to requests
- Token provided via R2 API token

### Technical Approach

**Why Custom Catalog?**

rust-iceberg's `RestCatalog` (v0.7.0) doesn't support cloud-specific authentication:
- `RestCatalogBuilder.with_client()` accepts only `reqwest::Client`
- No built-in request interceptor/middleware support
- Cannot inject custom signing logic (SigV4, Bearer tokens, etc.)

**Solution:**

Implement `Catalog` trait with pluggable authentication layer:

1. **Define AuthProvider trait** for request signing abstraction
2. **Implement cloud-specific providers:**
   - `SigV4AuthProvider` for AWS S3 Tables
   - `BearerTokenAuthProvider` for Cloudflare R2
3. **IcebergRestCatalog delegates authentication:**
   - Build `reqwest::Request` with method, URL, headers, body
   - Call `auth_provider.sign_request()` to inject credentials
   - Execute via `reqwest::Client`
4. **Factory methods configure catalog for each cloud:**
   - `from_s3_tables_arn()` - Parse ARN, load AWS credentials, use SigV4
   - `from_r2()` - Use R2 endpoint, use Bearer token
5. Handle responses with proper error mapping (403 auth, 404 not found, etc.)

### Dependencies

Key additions to enable cloud catalog support:
- `reqwest = "0.12"` - HTTP client for REST API calls
- `thiserror = "2.0"` - Error type definitions
- `serde_json = "1.0"` - REST payload serialization
- `percent-encoding = "2.3"` - URL encoding for ARNs

**For AWS S3 Tables (conditional on non-WASM targets):**
- `aws-sigv4 = "1.3"` - AWS SigV4 signing
- `aws-credential-types = "1.2"` - AWS credential types
- `aws-config = "1.8"` - AWS SDK configuration
- `aws-sdk-sts = "1.55"` - AWS STS for credential management

Removed:
- Old `s3tables` module - Replaced by unified `catalog` module with pluggable auth

## Known Limitations

### AWS S3 Tables

S3 Tables has these documented API limitations (as of 2025):
- Limited schema evolution capabilities
- No list operations (namespaces, tables)
- No delete operations (drop namespace/table)
- Partition evolution restrictions
- Time travel may have limited support

### Cloudflare R2

R2 Data Catalog is currently in beta:
- Check Cloudflare documentation for current API limitations
- Some Iceberg features may have limited support

This PoC uses a minimal schema and basic operations to work within these constraints.

## Future Improvements

**Potential rust-iceberg Contributions:**

1. **Add pluggable authentication to `iceberg-catalog-rest`**
   - Introduce `AuthProvider` trait in upstream rust-iceberg
   - Provide built-in implementations: SigV4, Bearer token, OAuth
   - Add catalog config: `rest.auth-type`, `rest.auth-*` parameters
   - Similar to Java's authentication framework

2. **Support request middleware**
   - Accept `reqwest_middleware::ClientWithMiddleware` in `RestCatalogBuilder`
   - Enable users to inject custom auth/signing logic
   - More flexible than built-in auth providers

3. **Formalize cloud catalog implementations**
   - Package as separate crates: `iceberg-catalog-s3tables`, `iceberg-catalog-r2`
   - Add to official rust-iceberg catalog implementations
   - Maintain alongside REST, Hive, Glue catalogs

**Production Readiness Gaps:**

- Error handling: More specific error types, better error messages
- Retry logic: Exponential backoff on transient failures (429, 503)
- Testing: Integration tests against real S3 Tables and R2 endpoints
- Performance: Connection pooling, request batching
- Monitoring: Metrics, tracing integration (OpenTelemetry)
- WASM support: Full browser compatibility (currently blocked by arrow/parquet C deps)

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
