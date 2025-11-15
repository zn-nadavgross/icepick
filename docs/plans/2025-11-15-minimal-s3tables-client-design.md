# Minimal Iceberg REST Catalog Client for AWS S3 Tables

**Date:** 2025-11-15
**Goal:** Build a minimal, extensible Iceberg REST catalog client that replaces `iceberg-catalog-rest` with AWS SigV4 authentication support for S3 Tables.

## Overview

Replace rust-iceberg's REST catalog client with a minimal SigV4-enabled implementation while keeping the rest of the rust-iceberg ecosystem (types, FileIO, Transaction, Table). This enables full write/read operations with AWS S3 Tables.

**Key Design Principle:** Replace only the catalog REST client. Everything else stays with rust-iceberg.

## Architecture

```
┌─────────────────────────────────────────┐
│ Your Application (main.rs)              │
└─────────────────┬───────────────────────┘
                  │
    ┌─────────────┴─────────────┐
    │                           │
┌───▼──────────────┐  ┌────────▼─────────────┐
│ S3TablesClient   │  │ iceberg crate        │
│ (NEW)            │  │ (existing)           │
│                  │  │                      │
│ - create_ns      │  │ - Schema types       │
│ - create_table   │  │ - TableMetadata      │
│ - load_table     │  │ - Transaction        │
│ - update_table   │  │ - FileIO (S3)        │
└────┬─────────────┘  │ - Writers/Readers    │
     │                └──────────────────────┘
     │
┌────▼─────────────────────────────┐
│ reqwest + reqsign                │
│                                  │
│ - HTTP client                    │
│ - AWS SigV4 signing              │
│ - Credential loading             │
└──────────────────────────────────┘
```

**Dependencies:**
- `iceberg` - Keep for types, FileIO, Transaction, Table
- `reqwest` - HTTP client
- `reqsign` - AWS SigV4 signing (already in tree via iceberg)
- `serde`/`serde_json` - JSON serialization
- `tokio` - Async runtime
- Remove: `iceberg-catalog-rest`

## API Surface

**Core type:** `S3TablesClient` - minimal REST catalog client

```rust
pub struct S3TablesClient {
    endpoint: String,
    warehouse: String,
    region: String,
    http_client: reqwest::Client,
    signer: reqsign::AwsV4Signer,
}

impl S3TablesClient {
    /// Create client from S3 Tables ARN
    /// ARN format: arn:aws:s3tables:region:account:bucket/name
    pub async fn from_arn(arn: &str) -> Result<Self>;

    /// Create namespace (idempotent - succeeds if exists)
    pub async fn create_namespace(
        &self,
        namespace: &str,
        properties: HashMap<String, String>
    ) -> Result<()>;

    /// Create table with schema
    pub async fn create_table(
        &self,
        namespace: &str,
        table_name: &str,
        schema: Schema,
    ) -> Result<TableMetadata>;

    /// Load table metadata (for transaction refresh)
    pub async fn load_table(
        &self,
        namespace: &str,
        table_name: &str,
    ) -> Result<TableMetadata>;

    /// Update table (commit snapshots, schema changes, etc.)
    pub async fn update_table(
        &self,
        namespace: &str,
        table_name: &str,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<TableMetadata>;
}
```

**Design choices:**
- Simple string-based namespace (not `NamespaceIdent`)
- Uses iceberg crate types: `Schema`, `TableMetadata`, `TableUpdate`, `TableRequirement`
- `from_arn()` handles ARN parsing and AWS credential loading
- All methods async (required for HTTP + SigV4)

## HTTP Client + SigV4 Signing

**Challenge:** Every REST API call to S3 Tables must be signed with AWS SigV4.

**Implementation approach:**

```rust
use reqwest::{Client, Request};
use reqsign::{AwsConfig, AwsV4Signer};

impl S3TablesClient {
    async fn from_arn(arn: &str) -> Result<Self> {
        let (region, bucket_name) = parse_arn(arn)?;
        let endpoint = format!("https://s3tables.{}.amazonaws.com/iceberg", region);

        // Configure AWS credentials (env, ~/.aws/credentials, IAM roles)
        let aws_config = AwsConfig::default()
            .with_region(&region)
            .load_from_env();

        let signer = AwsV4Signer::new("s3tables", &region);
        let http_client = Client::new();

        Ok(Self {
            endpoint,
            warehouse: arn.to_string(),
            region,
            http_client,
            signer,
        })
    }

    async fn send_signed_request(&self, mut req: Request) -> Result<Response> {
        // Load credentials
        let credential = self.signer.load_credential().await?;

        // Sign request with SigV4
        self.signer.sign(&mut req, &credential)?;

        // Send signed request
        let response = self.http_client.execute(req).await?;

        // Check status and parse response
        if !response.status().is_success() {
            return Err(handle_error_response(response).await);
        }

        Ok(response)
    }
}
```

**Authentication sources** (via reqsign):
- Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- AWS config files: `~/.aws/credentials`, `~/.aws/config`
- IAM roles (EC2, ECS, Lambda)
- Assume role support

## REST API Endpoint Mapping

**S3 Tables Iceberg REST endpoints:**

Base URL: `https://s3tables.{region}.amazonaws.com/iceberg`

### 1. Create Namespace

```
POST {base}/v1/namespaces
Body: {
    "namespace": ["namespace_name"],
    "properties": { "key": "value", ... }
}
Response: 200 OK
{
    "namespace": ["namespace_name"],
    "properties": { ... }
}
```

### 2. Create Table

```
POST {base}/v1/namespaces/{namespace}/tables
Body: {
    "name": "table_name",
    "schema": { /* Iceberg schema JSON */ },
    "location": null,  // S3 Tables auto-assigns
    "partition-spec": { "spec-id": 0, "fields": [] },
    "write-order": { "order-id": 0, "fields": [] },
    "properties": {}
}
Response: 200 OK
{
    "metadata": { /* TableMetadata JSON */ },
    "metadata-location": "s3://...",
    "config": {}
}
```

### 3. Load Table

```
GET {base}/v1/namespaces/{namespace}/tables/{table}
Response: 200 OK
{
    "metadata": { /* TableMetadata JSON */ },
    "metadata-location": "s3://...",
    "config": {}
}
```

### 4. Update Table (Commit)

```
POST {base}/v1/namespaces/{namespace}/tables/{table}
Body: {
    "requirements": [
        { "type": "assert-table-uuid", "uuid": "..." },
        ...
    ],
    "updates": [
        { "action": "add-snapshot", "snapshot": {...} },
        { "action": "set-snapshot-ref", ... },
        ...
    ]
}
Response: 200 OK
{
    "metadata": { /* Updated TableMetadata JSON */ },
    "metadata-location": "s3://...",
    "config": {}
}
```

All requests/responses use JSON with `Content-Type: application/json`.

## Integration with rust-iceberg

**Challenge:** Your minimal client needs to work with rust-iceberg's `Transaction`, `Table`, and `FileIO`.

**Solution:** Implement the `Catalog` trait for compatibility.

```rust
use iceberg::{Catalog, NamespaceIdent, TableIdent, Table, TableCreation, TableCommit};
use async_trait::async_trait;

#[async_trait]
impl Catalog for S3TablesClient {
    // === SUPPORTED OPERATIONS ===

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let ns_name = namespace.as_ref()[0].as_str(); // Single-level only
        self.create_namespace(ns_name, properties).await?;
        Ok(Namespace::new(namespace.clone(), properties))
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let ns_name = namespace.as_ref()[0].as_str();
        let metadata = self.create_table(
            ns_name,
            creation.name(),
            creation.schema().clone(),
        ).await?;

        // Build Table from metadata
        Table::builder()
            .metadata(metadata)
            .identifier(TableIdent::new(namespace.clone(), creation.name()))
            .file_io(self.build_file_io()?)  // Use iceberg's S3 FileIO
            .build()
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        let ns = table.namespace().as_ref()[0].as_str();
        let metadata = self.load_table(ns, table.name()).await?;

        Table::builder()
            .metadata(metadata)
            .identifier(table.clone())
            .file_io(self.build_file_io()?)
            .build()
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let ns = commit.identifier().namespace().as_ref()[0].as_str();
        let name = commit.identifier().name();

        let metadata = self.update_table(
            ns,
            name,
            commit.requirements(),
            commit.updates(),
        ).await?;

        Table::builder()
            .metadata(metadata)
            .identifier(commit.identifier().clone())
            .file_io(self.build_file_io()?)
            .build()
    }

    // === UNSUPPORTED OPERATIONS ===

    async fn list_namespaces(&self, _parent: Option<&NamespaceIdent>)
        -> Result<Vec<NamespaceIdent>>
    {
        Err(Error::new(ErrorKind::FeatureUnsupported,
            "list_namespaces not implemented"))
    }

    async fn drop_table(&self, _table: &TableIdent) -> Result<()> {
        Err(Error::new(ErrorKind::FeatureUnsupported,
            "drop_table not implemented"))
    }

    // ... other unsupported methods return FeatureUnsupported errors
}

impl S3TablesClient {
    fn build_file_io(&self) -> Result<FileIO> {
        // Reuse iceberg's S3 FileIO (already has SigV4 via reqsign)
        FileIOBuilder::new("s3")
            .with_prop("s3.region", &self.region)
            .build()
    }
}
```

**Usage in main.rs:**

```rust
// Before (doesn't work - no SigV4):
let catalog = RestCatalogBuilder::default()
    .load("s3tables", props).await?;

// After (works - SigV4 enabled):
let catalog = S3TablesClient::from_arn(arn).await?;

// Then use normally with Transaction:
let table = catalog.create_table(&namespace, creation).await?;
let tx = Transaction::new(&table);
let action = tx.fast_append().add_data_files(data_files);
let updated_table = action.apply(tx)?.commit(&catalog).await?;
```

## Error Handling

**Error types and HTTP status codes:**

```rust
match status.as_u16() {
    200..=299 => {
        // Success - parse JSON response
        response.json().await
    }

    403 => {
        // Auth failure - SigV4 issue or missing permissions
        Err(Error::new(
            ErrorKind::Unauthorized,
            format!("S3 Tables authentication failed: {}", body)
        ))
    }

    404 => {
        // Namespace or table not found
        Err(Error::new(ErrorKind::NotFound, "Resource not found"))
    }

    409 => {
        // Conflict - optimistic locking failure (triggers retry)
        Err(Error::new(
            ErrorKind::Unexpected,
            format!("Commit conflict (requirements not met): {}", body)
        ))
    }

    400 => {
        // Bad request - invalid schema or malformed request
        Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid request: {}", body)
        ))
    }

    _ => {
        // Other errors
        Err(Error::new(
            ErrorKind::Unexpected,
            format!("HTTP {}: {}", status, body)
        ))
    }
}
```

**Error categories:**
1. Authentication errors (403) - credentials, permissions
2. Not found errors (404) - namespace/table doesn't exist
3. Conflict errors (409) - optimistic locking (triggers retry)
4. Validation errors (400) - invalid schema/JSON
5. Network/transport errors - connection, timeout, DNS

## Complete Write/Read Flow

**Write flow (end-to-end):**

```rust
// 1. Create catalog client with SigV4
let catalog = S3TablesClient::from_arn(
    "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
).await?;

// 2. Create namespace via REST API
catalog.create_namespace("my_namespace", HashMap::new()).await?;
// → POST /v1/namespaces with SigV4 signature

// 3. Create table via REST API
let table = catalog.create_table(&namespace, creation).await?;
// → POST /v1/namespaces/my_namespace/tables with SigV4 signature
// ← Returns TableMetadata with S3 paths

// 4. Write Parquet files to S3 (using iceberg's FileIO)
let mut writer = DataFileWriterBuilder::new(...)
    .build().await?;
writer.write(batch).await?;
let data_files = writer.close().await?;
// → Writes data-xxxxx.parquet to S3 with SigV4 (via iceberg FileIO)

// 5. Commit snapshot via REST API
let tx = Transaction::new(&table);
let action = tx.fast_append().add_data_files(data_files);
let updated_table = action.apply(tx)?.commit(&catalog).await?;
// → Transaction calls catalog.update_table()
// → POST /v1/namespaces/my_namespace/tables/my_table
//   Body: { requirements: [...], updates: [AddSnapshot, ...] }
// ← Returns updated TableMetadata with new snapshot
```

**Read flow:**

```rust
// 1. Load table metadata from REST API
let table = catalog.load_table(&table_ident).await?;
// → GET /v1/namespaces/my_namespace/tables/my_table with SigV4
// ← Returns TableMetadata with manifest locations

// 2. Scan reads manifests from S3
let scan = table.scan().build()?;
// → Reads manifest files from S3 (via iceberg FileIO with SigV4)

// 3. Stream reads Parquet files from S3
let mut stream = scan.to_arrow().await?;
while let Some(batch) = stream.next().await {
    // Process batch
}
// → Reads data-xxxxx.parquet files from S3 (via iceberg FileIO)
```

**Data flow layers:**

```
Application Layer
    ↓ create_table(), update_table()
S3TablesClient (NEW - REST catalog with SigV4)
    ↓ JSON over HTTPS with SigV4
S3 Tables REST API
    ↓ returns metadata with S3 paths
iceberg FileIO (existing - S3 with SigV4)
    ↓ reads/writes Parquet files
Amazon S3
```

**Key insight:** Two separate SigV4 signing paths:
1. **REST API** (your client) - signs catalog operations (service name "s3tables")
2. **FileIO** (iceberg's) - signs S3 file operations (service name "s3")

## Project Structure

```
hello-world-iceberg/
├── Cargo.toml
├── README.md
├── src/
│   ├── main.rs                    # Your application
│   ├── s3tables/
│   │   ├── mod.rs                 # Public API exports
│   │   ├── client.rs              # S3TablesClient implementation
│   │   ├── catalog.rs             # Catalog trait implementation
│   │   ├── signing.rs             # SigV4 signing helpers
│   │   └── error.rs               # Error handling
│   └── lib.rs                     # Optional: library interface
└── docs/
    └── plans/
        └── 2025-11-15-minimal-s3tables-client-design.md
```

**Cargo.toml dependencies:**

```toml
[dependencies]
# Keep existing
iceberg = "0.7.0"              # Types, FileIO, Transaction, Table
tokio = { version = "1", features = ["full"] }
anyhow = "1.0"
arrow = { version = "55", features = ["prettyprint"] }
parquet = "55"
futures = "0.3"

# Add new
reqwest = { version = "0.12", features = ["json"] }
reqsign = "0.18"               # Already in tree via iceberg
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
async-trait = "0.1"

# Remove
# iceberg-catalog-rest = "0.7.0"  # ← DELETE THIS
```

## Implementation Roadmap

**Phase 1: Core client (minimal working version)**
1. ARN parsing function
2. S3TablesClient struct with from_arn()
3. SigV4 signing with reqsign
4. create_namespace() - simplest endpoint
5. Test with real AWS credentials

**Phase 2: Table operations**
6. create_table() endpoint
7. load_table() endpoint
8. update_table() endpoint (most complex)
9. Error handling for all endpoints

**Phase 3: Catalog trait integration**
10. Implement Catalog trait
11. Integrate with Transaction::commit()
12. Update main.rs to use new client
13. End-to-end write/read test

**Phase 4: Polish**
14. Better error messages
15. Logging/debugging support
16. Documentation
17. Optional: extract to separate crate

## WASM Compatibility

For future WASM support:
- Use `reqwest` with `wasm` feature
- Replace `tokio` with `wasm-bindgen-futures`
- Use browser's fetch API via `web-sys` (no SigV4 signing in browser)
- Or use AWS Cognito for browser authentication

## Success Criteria

✅ Can create namespace via REST API with SigV4
✅ Can create table via REST API with SigV4
✅ Can write Parquet files via iceberg FileIO
✅ Can commit snapshots via REST API update_table
✅ Can read data back via scan
✅ Zero dependency on `iceberg-catalog-rest`
✅ Minimal additional dependencies (reqwest + reqsign)

## Validation

This design is validated by Daft's implementation:
- Daft uses PyIceberg for S3 Tables integration
- PyIceberg uses REST catalog with SigV4 for catalog operations
- PyIceberg uses separate FileIO with SigV4 for S3 file operations
- Same two-layer signing architecture as this design
