# R2 Data Catalog Integration Design

**Date:** 2025-11-15
**Status:** Approved

## Overview

This design adds support for Cloudflare R2 Data Catalog, a managed Apache Iceberg catalog service. The implementation reuses 99% of the existing S3 Tables code by extracting shared REST catalog logic and using pluggable authentication.

### Goals

- Support R2 Data Catalog using Cloudflare API tokens (Bearer auth)
- Reuse existing Iceberg REST catalog implementation from S3 Tables
- Ensure WASM compatibility for browser/edge runtime usage
- Simplify API by using factory methods on a single catalog type

### Non-Goals

- Cloudflare API Key + Email authentication (legacy, can add later if needed)
- Supporting non-Iceberg R2 features
- Backward compatibility (breaking change is acceptable)

## Architecture

Following PyIceberg's proven pattern: shared REST catalog implementation with pluggable authentication.

### Core Components

**1. `IcebergRestCatalog`** - Shared catalog for all REST-based Iceberg catalogs
- Implements `iceberg::Catalog` trait
- Handles all Iceberg REST API operations (create_table, load_table, update_table, etc.)
- Delegates authentication to pluggable `AuthProvider` trait
- Contains HTTP client, endpoint configuration, and FileIO

**2. `AuthProvider` trait** - Abstraction for authentication mechanisms
```rust
#[async_trait]
pub trait AuthProvider: Send + Sync + Debug {
    async fn sign_request(&self, request: reqwest::Request) -> Result<reqwest::Request>;
}
```

**3. Factory methods** - Convenient catalog constructors
- `IcebergRestCatalog::from_s3_tables_arn()` - Creates catalog with SigV4 auth
- `IcebergRestCatalog::from_r2()` - Creates catalog with Bearer token auth
- `IcebergRestCatalog::from_r2_config()` - Creates catalog with custom R2 config

### File Structure

```
src/
  catalog/
    mod.rs              # Public API, AuthProvider trait, CatalogError, R2Config
    rest.rs             # IcebergRestCatalog implementation
    auth/
      mod.rs            # Auth provider exports
      sigv4.rs          # SigV4AuthProvider (cfg gated)
      bearer.rs         # BearerTokenAuthProvider
```

## Authentication Design

### AuthProvider Trait

```rust
#[async_trait]
pub trait AuthProvider: Send + Sync + Debug {
    /// Sign/authenticate an HTTP request before sending
    async fn sign_request(&self, request: reqwest::Request) -> Result<reqwest::Request>;
}
```

### SigV4AuthProvider (S3 Tables)

```rust
pub struct SigV4AuthProvider {
    region: String,
    service: String,  // "s3tables"
    credentials: aws_credential_types::Credentials,
}
```

- Keeps existing SigV4 signing logic
- Uses `aws-sigv4` crate
- Native-only (requires AWS SDK for credential loading)

### BearerTokenAuthProvider (R2)

```rust
pub struct BearerTokenAuthProvider {
    token: String,
}

impl BearerTokenAuthProvider {
    pub fn new(token: impl Into<String>) -> Self {
        Self { token: token.into() }
    }
}
```

- Adds `Authorization: Bearer {token}` header
- Fully WASM-compatible
- No signing or time-based operations
- Token refresh is user's responsibility

## API Design

### R2Config

```rust
pub struct R2Config {
    pub account_id: String,
    pub bucket_name: String,
    pub api_token: String,
    pub endpoint_override: Option<String>,
}
```

**Default R2 Endpoint:**
```
https://api.cloudflare.com/client/v4/accounts/{account_id}/r2/buckets/{bucket_name}/data-catalog
```

### IcebergRestCatalog - Unified Catalog Type

```rust
pub struct IcebergRestCatalog {
    endpoint: String,
    prefix: String,          // URL path prefix (e.g., "v1/{warehouse}" or "v1")
    http_client: reqwest::Client,
    auth_provider: Box<dyn AuthProvider>,
    file_io: FileIO,
    name: String,
}

impl IcebergRestCatalog {
    /// Create catalog for AWS S3 Tables
    #[cfg(not(target_family = "wasm"))]
    pub async fn from_s3_tables_arn(name: String, arn: &str) -> Result<Self>;

    /// Create catalog for Cloudflare R2 Data Catalog (shortcut)
    pub async fn from_r2(
        name: String,
        account_id: impl Into<String>,
        bucket_name: impl Into<String>,
        api_token: impl Into<String>,
    ) -> Result<Self>;

    /// Create catalog for Cloudflare R2 Data Catalog (with config)
    pub async fn from_r2_config(name: String, config: R2Config) -> Result<Self>;
}
```

- Implements all `iceberg::Catalog` trait methods
- URL construction: `{endpoint}/{prefix}/namespaces/{ns}/tables/{table}`
- Single `send_request()` method that calls `auth_provider.sign_request()`
- Reuses request/response types from existing code

## WASM Compatibility

### Dependencies

**reqwest with WASM support:**
```toml
reqwest = { version = "0.12", features = ["json", "wasm"] }
```
- Single HTTP client for both native and WASM
- `wasm` feature uses browser's `fetch()` API

**Conditional AWS dependencies:**
```toml
[target.'cfg(not(target_family = "wasm"))'.dependencies]
aws-sigv4 = { version = "1.3.6", default-features = false, features = ["sign-http"] }
aws-credential-types = { version = "1.2", default-features = false }
aws-config = { version = "1.8", default-features = false, features = ["rustls", "behavior-version-latest", "rt-tokio"] }
```

### Method Gating

```rust
impl IcebergRestCatalog {
    // Only available on native platforms (requires AWS SDK)
    #[cfg(not(target_family = "wasm"))]
    pub async fn from_s3_tables_arn(name: String, arn: &str) -> Result<Self> { ... }

    // Always available (WASM compatible)
    pub async fn from_r2(...) -> Result<Self> { ... }
    pub async fn from_r2_config(...) -> Result<Self> { ... }
}
```

### WASM Compatibility Matrix

| Component | WASM Compatible? |
|-----------|------------------|
| `IcebergRestCatalog` (core) | ✅ Yes |
| `IcebergRestCatalog::from_r2()` | ✅ Yes |
| `IcebergRestCatalog::from_r2_config()` | ✅ Yes |
| `IcebergRestCatalog::from_s3_tables_arn()` | ⚠️ Native only |
| `BearerTokenAuthProvider` | ✅ Yes |
| `SigV4AuthProvider` | ⚠️ Native only |

### Testing Strategy

- Add `wasm32-unknown-unknown` to CI build targets
- Ensure `IcebergRestCatalog::from_r2()` compiles for WASM
- Integration tests run on native only (require actual credentials)

## Error Handling

### Unified Error Type

Rename `S3TablesError` → `CatalogError`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Authentication failed: {0}")]
    AuthError(String),

    #[error("HTTP error: {0}")]
    HttpError(String),

    #[error("Invalid ARN: {0}")]
    InvalidArn(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Unexpected error: {0}")]
    Unexpected(String),
}

pub type Result<T> = std::result::Result<T, CatalogError>;
```

### HTTP Status Code Mapping

- **200-299**: Success
- **400**: `InvalidRequest` - malformed request
- **401/403**: `AuthError` - authentication failure
- **404**: `NotFound` - resource doesn't exist
- **409**: `Conflict` - requirements not met (optimistic locking)
- **5xx**: `Unexpected` - server errors

### IcebergError Conversion

```rust
fn to_iceberg_error(e: CatalogError) -> IcebergError {
    match e {
        CatalogError::NotFound(msg) => IcebergError::new(ErrorKind::DataInvalid, msg),
        CatalogError::Conflict(msg) => IcebergError::new(ErrorKind::DataInvalid, msg),
        CatalogError::AuthError(msg) => IcebergError::new(ErrorKind::Unexpected, msg),
        // ... rest of mappings
    }
}
```

## Implementation Plan

1. **Create catalog module structure**
   - Create `src/catalog/` directory
   - Define `AuthProvider` trait in `catalog/mod.rs`
   - Define `CatalogError` and `R2Config` in `catalog/mod.rs`

2. **Extract and refactor REST catalog**
   - Move S3TablesClient logic to `catalog/rest.rs` as `IcebergRestCatalog`
   - Rename `S3TablesError` → `CatalogError`
   - Update to use `Box<dyn AuthProvider>` for authentication

3. **Implement auth providers**
   - Create `catalog/auth/` module
   - Extract SigV4 signing logic to `catalog/auth/sigv4.rs`
   - Create `BearerTokenAuthProvider` in `catalog/auth/bearer.rs`
   - Add `#[cfg(not(target_family = "wasm"))]` to SigV4 provider

4. **Add factory methods**
   - Implement `IcebergRestCatalog::from_s3_tables_arn()` (cfg gated)
   - Implement `IcebergRestCatalog::from_r2()`
   - Implement `IcebergRestCatalog::from_r2_config()`

5. **Deprecate old S3 Tables module**
   - Mark `src/s3tables/` as deprecated
   - Add re-export: `pub use crate::catalog::IcebergRestCatalog as S3TablesCatalog;`
   - Update examples to use new API

6. **WASM setup**
   - Update `Cargo.toml` with conditional AWS dependencies
   - Add `wasm` feature to reqwest
   - Add `wasm32-unknown-unknown` build to CI

7. **Testing**
   - Unit tests for auth providers
   - Integration tests for both S3 Tables and R2
   - Verify WASM compilation succeeds

## Usage Examples

### AWS S3 Tables

```rust
use hello_world_iceberg::catalog::IcebergRestCatalog;

let catalog = IcebergRestCatalog::from_s3_tables_arn(
    "my-catalog".to_string(),
    "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
).await?;

// Use catalog via iceberg::Catalog trait
let table = catalog.load_table(&table_ident).await?;
```

### Cloudflare R2 Data Catalog (Simple)

```rust
use hello_world_iceberg::catalog::IcebergRestCatalog;

let catalog = IcebergRestCatalog::from_r2(
    "my-catalog".to_string(),
    "my-account-id",
    "my-bucket",
    "my-api-token"
).await?;

let table = catalog.load_table(&table_ident).await?;
```

### Cloudflare R2 Data Catalog (Advanced)

```rust
use hello_world_iceberg::catalog::{IcebergRestCatalog, R2Config};

let config = R2Config {
    account_id: "my-account-id".into(),
    bucket_name: "my-bucket".into(),
    api_token: "my-api-token".into(),
    endpoint_override: Some("https://staging-api.cloudflare.com/...".into()),
};

let catalog = IcebergRestCatalog::from_r2_config(
    "my-catalog".to_string(),
    config
).await?;
```

## Migration from Old API

**Old S3 Tables API (deprecated):**
```rust
use hello_world_iceberg::s3tables::S3TablesCatalog;
let catalog = S3TablesCatalog::from_arn("my-catalog".to_string(), arn).await?;
```

**New unified API:**
```rust
use hello_world_iceberg::catalog::IcebergRestCatalog;
let catalog = IcebergRestCatalog::from_s3_tables_arn("my-catalog".to_string(), arn).await?;
```

## Open Questions

- Does R2 Data Catalog support the full Iceberg REST spec, or are there unsupported operations?
- Token refresh strategy - should we support automatic token rotation?
- Performance: should we add request/response caching?

## References

- [Cloudflare R2 Data Catalog Docs](https://developers.cloudflare.com/r2/data-catalog/)
- [PyIceberg Catalog Architecture](https://py.iceberg.apache.org/)
- [Apache Iceberg REST Catalog Spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
