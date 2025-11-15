# Minimal S3 Tables REST Client Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a minimal Iceberg REST catalog client with AWS SigV4 authentication that replaces `iceberg-catalog-rest` for S3 Tables compatibility.

**Architecture:** Small focused REST client module using reqwest + reqsign for SigV4 signing. Implements iceberg's Catalog trait to integrate with Transaction/Table APIs. Keeps dependency on iceberg crate for types and FileIO.

**Tech Stack:** Rust, reqwest (HTTP), reqsign (AWS SigV4), iceberg (types/FileIO), tokio (async)

---

## Task 1: Update Dependencies

**Files:**
- Modify: `Cargo.toml`

**Step 1: Add new dependencies and remove iceberg-catalog-rest**

Update `Cargo.toml`:

```toml
[package]
name = "hello-world-iceberg"
version = "0.1.0"
edition = "2021"

[dependencies]
# Keep existing
iceberg = "0.7.0"
tokio = { version = "1.48.0", features = ["full"] }
anyhow = "1.0.100"
arrow = { version = "55.2.0", features = ["prettyprint"] }
parquet = "55.2.0"
futures = "0.3"

# Add new
reqwest = { version = "0.12", features = ["json"] }
reqsign = "0.18"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
async-trait = "0.1"

# Removed: iceberg-catalog-rest = "0.7.0"
```

**Step 2: Verify dependencies resolve**

Run: `cargo check`
Expected: Dependencies download and resolve successfully

**Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "deps: add reqwest/reqsign, remove iceberg-catalog-rest"
```

---

## Task 2: Create Module Structure

**Files:**
- Create: `src/s3tables/mod.rs`
- Create: `src/s3tables/client.rs`
- Create: `src/s3tables/error.rs`

**Step 1: Create s3tables module directory**

Run: `mkdir -p src/s3tables`

**Step 2: Create mod.rs with public exports**

Create `src/s3tables/mod.rs`:

```rust
//! Minimal Iceberg REST catalog client for AWS S3 Tables

mod client;
mod error;

pub use client::S3TablesClient;
pub use error::S3TablesError;
```

**Step 3: Create error module stub**

Create `src/s3tables/error.rs`:

```rust
use std::fmt;

#[derive(Debug)]
pub enum S3TablesError {
    InvalidArn(String),
    HttpError(String),
    AuthError(String),
    NotFound(String),
    Conflict(String),
    InvalidRequest(String),
    Unexpected(String),
}

impl fmt::Display for S3TablesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArn(msg) => write!(f, "Invalid S3 Tables ARN: {}", msg),
            Self::HttpError(msg) => write!(f, "HTTP error: {}", msg),
            Self::AuthError(msg) => write!(f, "Authentication failed: {}", msg),
            Self::NotFound(msg) => write!(f, "Not found: {}", msg),
            Self::Conflict(msg) => write!(f, "Conflict: {}", msg),
            Self::InvalidRequest(msg) => write!(f, "Invalid request: {}", msg),
            Self::Unexpected(msg) => write!(f, "Unexpected error: {}", msg),
        }
    }
}

impl std::error::Error for S3TablesError {}

pub type Result<T> = std::result::Result<T, S3TablesError>;
```

**Step 4: Create client module stub**

Create `src/s3tables/client.rs`:

```rust
use crate::s3tables::error::{Result, S3TablesError};

pub struct S3TablesClient {
    endpoint: String,
    warehouse: String,
    region: String,
}

impl S3TablesClient {
    pub async fn from_arn(arn: &str) -> Result<Self> {
        todo!("implement from_arn")
    }
}
```

**Step 5: Add s3tables module to main.rs**

Add to top of `src/main.rs` after existing use statements:

```rust
mod s3tables;
```

**Step 6: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully (warnings about unused code are OK)

**Step 7: Commit**

```bash
git add src/s3tables/
git add src/main.rs
git commit -m "feat: add s3tables module structure"
```

---

## Task 3: ARN Parsing

**Files:**
- Modify: `src/s3tables/client.rs`

**Step 1: Write test for valid ARN parsing**

Add to end of `src/s3tables/client.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_arn_valid() {
        let arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_ok());
        let (region, bucket) = result.unwrap();
        assert_eq!(region, "us-west-2");
        assert_eq!(bucket, "my-bucket");
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_parse_arn_valid`
Expected: FAIL - "cannot find function `parse_s3tables_arn`"

**Step 3: Implement ARN parsing function**

Add before the impl block in `src/s3tables/client.rs`:

```rust
/// Parse S3 Tables ARN and extract region and bucket name
/// ARN format: arn:aws:s3tables:region:account:bucket/name
fn parse_s3tables_arn(arn: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = arn.split(':').collect();

    if parts.len() != 6 {
        return Err(S3TablesError::InvalidArn(
            format!("Expected 6 parts, got {}", parts.len())
        ));
    }

    if parts[0] != "arn" {
        return Err(S3TablesError::InvalidArn(
            "Must start with 'arn'".to_string()
        ));
    }

    if parts[2] != "s3tables" {
        return Err(S3TablesError::InvalidArn(
            format!("Not an S3 Tables ARN: {}", parts[2])
        ));
    }

    let region = parts[3].to_string();
    let bucket_name = parts[5]
        .strip_prefix("bucket/")
        .ok_or_else(|| S3TablesError::InvalidArn(
            "Missing 'bucket/' prefix".to_string()
        ))?
        .to_string();

    Ok((region, bucket_name))
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test test_parse_arn_valid`
Expected: PASS

**Step 5: Write test for invalid ARN**

Add to tests module:

```rust
#[test]
fn test_parse_arn_invalid_format() {
    let arn = "invalid-arn";
    let result = parse_s3tables_arn(arn);
    assert!(result.is_err());
}

#[test]
fn test_parse_arn_wrong_service() {
    let arn = "arn:aws:s3:us-west-2:123456789012:bucket/my-bucket";
    let result = parse_s3tables_arn(arn);
    assert!(result.is_err());
}

#[test]
fn test_parse_arn_missing_bucket_prefix() {
    let arn = "arn:aws:s3tables:us-west-2:123456789012:my-bucket";
    let result = parse_s3tables_arn(arn);
    assert!(result.is_err());
}
```

**Step 6: Run all ARN tests**

Run: `cargo test parse_arn`
Expected: All 4 tests PASS

**Step 7: Commit**

```bash
git add src/s3tables/client.rs
git commit -m "feat: add ARN parsing with validation"
```

---

## Task 4: S3TablesClient Initialization

**Files:**
- Modify: `src/s3tables/client.rs`

**Step 1: Add HTTP client and signer fields**

Update the `S3TablesClient` struct:

```rust
use reqwest::Client;

pub struct S3TablesClient {
    endpoint: String,
    warehouse: String,
    region: String,
    http_client: Client,
}
```

**Step 2: Write test for from_arn**

Add to tests module:

```rust
#[tokio::test]
async fn test_from_arn_creates_client() {
    let arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/test-bucket";
    let result = S3TablesClient::from_arn(arn).await;
    assert!(result.is_ok());
    let client = result.unwrap();
    assert_eq!(client.region, "us-west-2");
    assert_eq!(client.warehouse, arn);
    assert_eq!(client.endpoint, "https://s3tables.us-west-2.amazonaws.com/iceberg");
}
```

**Step 3: Run test to verify it fails**

Run: `cargo test test_from_arn_creates_client`
Expected: FAIL - "not yet implemented: implement from_arn"

**Step 4: Implement from_arn**

Replace the `from_arn` method:

```rust
pub async fn from_arn(arn: &str) -> Result<Self> {
    let (region, _bucket_name) = parse_s3tables_arn(arn)?;
    let endpoint = format!("https://s3tables.{}.amazonaws.com/iceberg", region);

    let http_client = Client::new();

    Ok(Self {
        endpoint,
        warehouse: arn.to_string(),
        region,
        http_client,
    })
}
```

**Step 5: Run test to verify it passes**

Run: `cargo test test_from_arn_creates_client`
Expected: PASS

**Step 6: Commit**

```bash
git add src/s3tables/client.rs
git commit -m "feat: implement S3TablesClient::from_arn"
```

---

## Task 5: SigV4 Request Signing Helper

**Files:**
- Modify: `src/s3tables/client.rs`

**Step 1: Add signing dependencies**

Add imports at top of `src/s3tables/client.rs`:

```rust
use reqwest::{Client, Request, Response};
use reqsign::{AwsConfig, AwsV4Signer, AwsCredentialLoad};
```

**Step 2: Add signer field to struct**

Update `S3TablesClient`:

```rust
pub struct S3TablesClient {
    endpoint: String,
    warehouse: String,
    region: String,
    http_client: Client,
    aws_config: AwsConfig,
}
```

**Step 3: Update from_arn to initialize AWS config**

Replace `from_arn` implementation:

```rust
pub async fn from_arn(arn: &str) -> Result<Self> {
    let (region, _bucket_name) = parse_s3tables_arn(arn)?;
    let endpoint = format!("https://s3tables.{}.amazonaws.com/iceberg", region);

    let http_client = Client::new();

    // AWS config will load credentials from environment/config files
    let aws_config = AwsConfig::default().from_profile().from_env();

    Ok(Self {
        endpoint,
        warehouse: arn.to_string(),
        region: region.clone(),
        http_client,
        aws_config,
    })
}
```

**Step 4: Add signing helper method**

Add to impl block:

```rust
async fn send_signed_request(&self, mut req: Request) -> Result<Response> {
    // Create SigV4 signer for s3tables service
    let signer = AwsV4Signer::new("s3tables", &self.region);

    // Load credentials
    let credential = self.aws_config
        .credential_load()
        .await
        .map_err(|e| S3TablesError::AuthError(format!("Failed to load credentials: {}", e)))?;

    // Sign request
    signer.sign(&mut req, &credential)
        .map_err(|e| S3TablesError::AuthError(format!("Failed to sign request: {}", e)))?;

    // Send request
    let response = self.http_client.execute(req).await
        .map_err(|e| S3TablesError::HttpError(format!("Request failed: {}", e)))?;

    Ok(response)
}
```

**Step 5: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 6: Commit**

```bash
git add src/s3tables/client.rs
git commit -m "feat: add SigV4 request signing helper"
```

---

## Task 6: HTTP Response Handling

**Files:**
- Modify: `src/s3tables/client.rs`

**Step 1: Add response handler method**

Add to impl block:

```rust
async fn handle_response(&self, response: Response) -> Result<serde_json::Value> {
    let status = response.status();

    match status.as_u16() {
        200..=299 => {
            response.json().await
                .map_err(|e| S3TablesError::HttpError(
                    format!("Failed to parse JSON response: {}", e)
                ))
        }

        403 => {
            let body = response.text().await
                .unwrap_or_else(|_| "Unable to read response".to_string());
            Err(S3TablesError::AuthError(
                format!("Authentication failed: {}", body)
            ))
        }

        404 => {
            Err(S3TablesError::NotFound("Resource not found".to_string()))
        }

        409 => {
            let body = response.text().await
                .unwrap_or_else(|_| "Conflict".to_string());
            Err(S3TablesError::Conflict(
                format!("Requirements not met: {}", body)
            ))
        }

        400 => {
            let body = response.text().await
                .unwrap_or_else(|_| "Bad request".to_string());
            Err(S3TablesError::InvalidRequest(body))
        }

        _ => {
            let body = response.text().await
                .unwrap_or_else(|_| "Unknown error".to_string());
            Err(S3TablesError::Unexpected(
                format!("HTTP {}: {}", status, body)
            ))
        }
    }
}
```

**Step 2: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 3: Commit**

```bash
git add src/s3tables/client.rs
git commit -m "feat: add HTTP response error handling"
```

---

## Task 7: Create Namespace Endpoint

**Files:**
- Modify: `src/s3tables/client.rs`

**Step 1: Add serde imports**

Add to imports:

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
```

**Step 2: Add create_namespace request/response types**

Add before impl block:

```rust
#[derive(Serialize)]
struct CreateNamespaceRequest {
    namespace: Vec<String>,
    properties: HashMap<String, String>,
}

#[derive(Deserialize)]
struct CreateNamespaceResponse {
    namespace: Vec<String>,
    properties: HashMap<String, String>,
}
```

**Step 3: Implement create_namespace method**

Add to impl block:

```rust
pub async fn create_namespace(
    &self,
    namespace: &str,
    properties: HashMap<String, String>,
) -> Result<()> {
    let url = format!("{}/v1/namespaces", self.endpoint);

    let body = CreateNamespaceRequest {
        namespace: vec![namespace.to_string()],
        properties,
    };

    let req = self.http_client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&body)
        .build()
        .map_err(|e| S3TablesError::HttpError(format!("Failed to build request: {}", e)))?;

    let response = self.send_signed_request(req).await?;
    let _result: CreateNamespaceResponse = self.handle_response(response).await?
        .as_object()
        .ok_or_else(|| S3TablesError::Unexpected("Invalid response format".to_string()))?
        .clone()
        .into_iter()
        .collect::<serde_json::Map<_, _>>()
        .into();

    Ok(())
}
```

**Step 4: Fix deserialization**

Replace the create_namespace implementation with corrected version:

```rust
pub async fn create_namespace(
    &self,
    namespace: &str,
    properties: HashMap<String, String>,
) -> Result<()> {
    let url = format!("{}/v1/namespaces", self.endpoint);

    let body = CreateNamespaceRequest {
        namespace: vec![namespace.to_string()],
        properties,
    };

    let req = self.http_client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&body)
        .build()
        .map_err(|e| S3TablesError::HttpError(format!("Failed to build request: {}", e)))?;

    let response = self.send_signed_request(req).await?;
    let _json_value = self.handle_response(response).await?;

    // Namespace created successfully
    Ok(())
}
```

**Step 5: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 6: Commit**

```bash
git add src/s3tables/client.rs
git commit -m "feat: implement create_namespace endpoint"
```

---

## Task 8: Create Table Endpoint

**Files:**
- Modify: `src/s3tables/client.rs`

**Step 1: Add iceberg imports**

Add to imports:

```rust
use iceberg::spec::{Schema, TableMetadata};
```

**Step 2: Add create_table request/response types**

Add after the CreateNamespaceResponse:

```rust
#[derive(Serialize)]
struct CreateTableRequest {
    name: String,
    schema: Schema,
    location: Option<String>,
    #[serde(rename = "partition-spec")]
    partition_spec: serde_json::Value,
    #[serde(rename = "write-order")]
    write_order: serde_json::Value,
    properties: HashMap<String, String>,
}

#[derive(Deserialize)]
struct CreateTableResponse {
    metadata: TableMetadata,
    #[serde(rename = "metadata-location")]
    metadata_location: String,
}
```

**Step 3: Implement create_table method**

Add to impl block:

```rust
pub async fn create_table(
    &self,
    namespace: &str,
    table_name: &str,
    schema: Schema,
) -> Result<TableMetadata> {
    let url = format!("{}/v1/namespaces/{}/tables", self.endpoint, namespace);

    let body = CreateTableRequest {
        name: table_name.to_string(),
        schema,
        location: None,  // S3 Tables auto-assigns
        partition_spec: serde_json::json!({
            "spec-id": 0,
            "fields": []
        }),
        write_order: serde_json::json!({
            "order-id": 0,
            "fields": []
        }),
        properties: HashMap::new(),
    };

    let req = self.http_client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&body)
        .build()
        .map_err(|e| S3TablesError::HttpError(format!("Failed to build request: {}", e)))?;

    let response = self.send_signed_request(req).await?;
    let json_value = self.handle_response(response).await?;

    let table_response: CreateTableResponse = serde_json::from_value(json_value)
        .map_err(|e| S3TablesError::Unexpected(format!("Failed to parse table response: {}", e)))?;

    Ok(table_response.metadata)
}
```

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add src/s3tables/client.rs
git commit -m "feat: implement create_table endpoint"
```

---

## Task 9: Load Table Endpoint

**Files:**
- Modify: `src/s3tables/client.rs`

**Step 1: Add load_table response type**

Add after CreateTableResponse (reuse same structure):

```rust
// LoadTableResponse has same structure as CreateTableResponse
type LoadTableResponse = CreateTableResponse;
```

**Step 2: Implement load_table method**

Add to impl block:

```rust
pub async fn load_table(
    &self,
    namespace: &str,
    table_name: &str,
) -> Result<TableMetadata> {
    let url = format!(
        "{}/v1/namespaces/{}/tables/{}",
        self.endpoint, namespace, table_name
    );

    let req = self.http_client
        .get(&url)
        .header("Accept", "application/json")
        .build()
        .map_err(|e| S3TablesError::HttpError(format!("Failed to build request: {}", e)))?;

    let response = self.send_signed_request(req).await?;
    let json_value = self.handle_response(response).await?;

    let table_response: LoadTableResponse = serde_json::from_value(json_value)
        .map_err(|e| S3TablesError::Unexpected(format!("Failed to parse table response: {}", e)))?;

    Ok(table_response.metadata)
}
```

**Step 3: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 4: Commit**

```bash
git add src/s3tables/client.rs
git commit -m "feat: implement load_table endpoint"
```

---

## Task 10: Update Table Endpoint

**Files:**
- Modify: `src/s3tables/client.rs`

**Step 1: Add iceberg catalog types import**

Add to imports:

```rust
use iceberg::catalog::{TableRequirement, TableUpdate};
```

**Step 2: Add update_table request/response types**

Add after LoadTableResponse:

```rust
#[derive(Serialize)]
struct UpdateTableRequest {
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

// UpdateTableResponse has same structure as CreateTableResponse
type UpdateTableResponse = CreateTableResponse;
```

**Step 3: Implement update_table method**

Add to impl block:

```rust
pub async fn update_table(
    &self,
    namespace: &str,
    table_name: &str,
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
) -> Result<TableMetadata> {
    let url = format!(
        "{}/v1/namespaces/{}/tables/{}",
        self.endpoint, namespace, table_name
    );

    let body = UpdateTableRequest {
        requirements,
        updates,
    };

    let req = self.http_client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&body)
        .build()
        .map_err(|e| S3TablesError::HttpError(format!("Failed to build request: {}", e)))?;

    let response = self.send_signed_request(req).await?;
    let json_value = self.handle_response(response).await?;

    let table_response: UpdateTableResponse = serde_json::from_value(json_value)
        .map_err(|e| S3TablesError::Unexpected(format!("Failed to parse table response: {}", e)))?;

    Ok(table_response.metadata)
}
```

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add src/s3tables/client.rs
git commit -m "feat: implement update_table endpoint"
```

---

## Task 11: Implement Catalog Trait

**Files:**
- Create: `src/s3tables/catalog.rs`
- Modify: `src/s3tables/mod.rs`

**Step 1: Create catalog trait implementation file**

Create `src/s3tables/catalog.rs`:

```rust
use async_trait::async_trait;
use iceberg::catalog::{Catalog, Namespace, NamespaceIdent, TableIdent, TableCreation, TableCommit};
use iceberg::table::Table;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::{Error, ErrorKind, Result};
use std::collections::HashMap;

use crate::s3tables::S3TablesClient;

#[async_trait]
impl Catalog for S3TablesClient {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>
    ) -> Result<Vec<NamespaceIdent>> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "list_namespaces not implemented"
        ))
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let ns_name = namespace.as_ref()
            .first()
            .ok_or_else(|| Error::new(
                ErrorKind::DataInvalid,
                "Namespace must have at least one part"
            ))?;

        self.create_namespace(ns_name, properties.clone())
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        Ok(Namespace::new(namespace.clone(), properties))
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> Result<Namespace> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "get_namespace not implemented"
        ))
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> Result<bool> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "namespace_exists not implemented"
        ))
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "update_namespace not implemented"
        ))
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "drop_namespace not implemented"
        ))
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "list_tables not implemented"
        ))
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let ns_name = namespace.as_ref()
            .first()
            .ok_or_else(|| Error::new(
                ErrorKind::DataInvalid,
                "Namespace must have at least one part"
            ))?;

        let metadata = self.create_table(
            ns_name,
            creation.name(),
            creation.schema().clone(),
        )
        .await
        .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        let file_io = self.build_file_io()?;

        Table::builder()
            .metadata(metadata)
            .identifier(TableIdent::new(namespace.clone(), creation.name().to_string()))
            .file_io(file_io)
            .build()
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        let ns_name = table.namespace().as_ref()
            .first()
            .ok_or_else(|| Error::new(
                ErrorKind::DataInvalid,
                "Namespace must have at least one part"
            ))?;

        let metadata = self.load_table(ns_name, table.name())
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        let file_io = self.build_file_io()?;

        Table::builder()
            .metadata(metadata)
            .identifier(table.clone())
            .file_io(file_io)
            .build()
    }

    async fn drop_table(&self, _table: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "drop_table not implemented"
        ))
    }

    async fn table_exists(&self, _table: &TableIdent) -> Result<bool> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "table_exists not implemented"
        ))
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "rename_table not implemented"
        ))
    }

    async fn register_table(&self, _table: &TableIdent, _metadata_location: String) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "register_table not implemented"
        ))
    }

    async fn update_table(&self, mut commit: TableCommit) -> Result<Table> {
        let table_ident = commit.identifier();
        let ns_name = table_ident.namespace().as_ref()
            .first()
            .ok_or_else(|| Error::new(
                ErrorKind::DataInvalid,
                "Namespace must have at least one part"
            ))?;

        let requirements = commit.take_requirements();
        let updates = commit.take_updates();

        let metadata = self.update_table(
            ns_name,
            table_ident.name(),
            requirements,
            updates,
        )
        .await
        .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        let file_io = self.build_file_io()?;

        Table::builder()
            .metadata(metadata)
            .identifier(table_ident.clone())
            .file_io(file_io)
            .build()
    }
}

impl S3TablesClient {
    fn build_file_io(&self) -> Result<FileIO> {
        FileIOBuilder::new("s3")
            .with_prop("s3.region", &self.region)
            .build()
    }
}
```

**Step 2: Export catalog module**

Update `src/s3tables/mod.rs`:

```rust
//! Minimal Iceberg REST catalog client for AWS S3 Tables

mod client;
mod error;
mod catalog;

pub use client::S3TablesClient;
pub use error::S3TablesError;
```

**Step 3: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 4: Commit**

```bash
git add src/s3tables/catalog.rs src/s3tables/mod.rs
git commit -m "feat: implement Catalog trait for S3TablesClient"
```

---

## Task 12: Update main.rs to Use New Client

**Files:**
- Modify: `src/main.rs`

**Step 1: Remove old catalog imports**

Remove from `src/main.rs`:

```rust
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
```

**Step 2: Add new imports**

Add to imports in `src/main.rs`:

```rust
use crate::s3tables::S3TablesClient;
use iceberg::Catalog;
```

**Step 3: Replace create_s3_tables_catalog function**

Replace the entire `create_s3_tables_catalog` function with:

```rust
/// Create S3 Tables catalog with SigV4 authentication
async fn create_s3_tables_catalog(arn: &str, _region: &str) -> Result<S3TablesClient> {
    S3TablesClient::from_arn(arn)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create S3 Tables catalog: {}", e))
}
```

**Step 4: Update main function to use the new catalog**

The main function should already work since `S3TablesClient` implements `Catalog` trait. No changes needed to the usage code.

**Step 5: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 6: Build in release mode**

Run: `cargo build --release`
Expected: Builds successfully

**Step 7: Commit**

```bash
git add src/main.rs
git commit -m "feat: replace iceberg-catalog-rest with S3TablesClient"
```

---

## Task 13: Fix Transaction Integration

**Files:**
- Modify: `src/main.rs`

**Step 1: Add transaction imports**

Verify these imports exist in `src/main.rs`:

```rust
use iceberg::transaction::{Transaction, ApplyTransactionAction};
```

**Step 2: Update write flow to commit snapshots**

Find the write section (around lines 176-184) and replace with:

```rust
// Write data
data_file_writer.write(batch.clone()).await
    .context("Failed to write data")?;

// Close writer and retrieve data files
let data_files = data_file_writer.close().await
    .context("Failed to close writer")?;

println!("✓ Wrote {} rows to {} data files", batch.num_rows(), data_files.len());

// Commit snapshot via transaction
let tx = Transaction::new(&table);
let action = tx.fast_append(None, data_files.clone())
    .context("Failed to create append action")?;
let table = action.apply(tx)
    .context("Failed to apply transaction")?
    .commit(&catalog)
    .await
    .context("Failed to commit transaction")?;

println!("✓ Committed snapshot");
```

**Step 3: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: add snapshot commit via Transaction"
```

---

## Task 14: Update README Documentation

**Files:**
- Modify: `README.md`

**Step 1: Update README with new approach**

Replace the entire `README.md` content:

```markdown
# Rust Iceberg + AWS S3 Tables

Minimal Iceberg REST catalog client with AWS SigV4 authentication for S3 Tables.

## Status

✅ **Working** - Custom S3 Tables REST client with SigV4 support

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
6. Commits snapshot via Iceberg transaction
7. Reads data back
8. Prints both datasets for visual verification

## Architecture

**Replaced:** `iceberg-catalog-rest` (no SigV4 support)

**With:** Custom minimal REST client using:
- `reqwest` - HTTP client
- `reqsign` - AWS SigV4 signing
- `iceberg` - Types, FileIO, Transaction, Table

**Two-layer signing:**
1. REST API (catalog operations) - service "s3tables"
2. S3 FileIO (data files) - service "s3"

## Implementation

See `src/s3tables/` for minimal REST catalog client:
- `client.rs` - Core REST API operations
- `catalog.rs` - Iceberg Catalog trait implementation
- `error.rs` - Error types

## Dependencies

```toml
iceberg = "0.7.0"       # Types, FileIO, Transaction
reqwest = "0.12"        # HTTP client
reqsign = "0.18"        # AWS SigV4 signing
```

Removed: `iceberg-catalog-rest = "0.7.0"`
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: update README with new architecture"
```

---

## Implementation Complete

**Verification checklist:**

✅ Dependencies updated (reqwest/reqsign added, iceberg-catalog-rest removed)
✅ Module structure created (src/s3tables/)
✅ ARN parsing with tests
✅ S3TablesClient with SigV4 signing
✅ REST endpoints: create_namespace, create_table, load_table, update_table
✅ Catalog trait implementation
✅ Transaction integration for snapshot commits
✅ main.rs updated to use new client
✅ README documentation updated

**Next steps:**

1. **Test with real S3 Tables:**
   - Set AWS credentials: `export AWS_PROFILE=your-profile`
   - Create S3 Tables bucket in AWS console
   - Run: `cargo run -- arn:aws:s3tables:us-west-2:ACCOUNT:bucket/NAME ns table`
   - Verify end-to-end write/read succeeds

2. **Optional enhancements:**
   - Add logging (tracing crate)
   - Add retry logic for network failures
   - Extract to separate crate for reuse
   - Add more comprehensive error messages
   - Support multi-level namespaces

3. **WASM compatibility (future):**
   - Enable reqwest wasm feature
   - Replace tokio with wasm-bindgen-futures
   - Handle browser-based auth (Cognito)
