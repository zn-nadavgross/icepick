# R2 Data Catalog Integration Design

**Date:** 2025-11-15
**Status:** Approved

## Overview

This design adds support for Cloudflare R2 Data Catalog, a managed Apache Iceberg catalog service. The implementation reuses 99% of the existing S3 Tables code by extracting shared REST catalog logic and using pluggable authentication.

### Goals

- Support R2 Data Catalog using Cloudflare API tokens (Bearer auth)
- Reuse existing Iceberg REST catalog implementation from S3 Tables
- Ensure WASM compatibility for browser/edge runtime usage
- Maintain backward compatibility with existing S3TablesCatalog API

### Non-Goals

- Cloudflare API Key + Email authentication (legacy, can add later if needed)
- Supporting non-Iceberg R2 features

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

**3. Catalog wrappers** - User-facing APIs
- `S3TablesCatalog` - Existing API, wraps `IcebergRestCatalog` with SigV4 auth
- `R2DataCatalog` - New API, wraps `IcebergRestCatalog` with Bearer token auth

### File Structure

```
src/
  catalog/
    mod.rs              # AuthProvider trait, CatalogError, common types
    rest.rs             # IcebergRestCatalog implementation
    s3tables.rs         # S3TablesCatalog wrapper
    r2.rs               # R2DataCatalog wrapper
  s3tables/             # Legacy location (deprecated, re-exports for compatibility)
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

### R2DataCatalog - User-Facing API

```rust
pub struct R2Config {
    pub account_id: String,
    pub bucket_name: String,
    pub api_token: String,
    pub endpoint_override: Option<String>,
}

pub struct R2DataCatalog {
    inner: IcebergRestCatalog,
}

impl R2DataCatalog {
    /// Shortcut for production R2 buckets
    pub async fn new(
        account_id: impl Into<String>,
        bucket_name: impl Into<String>,
        api_token: impl Into<String>,
    ) -> Result<Self>;

    /// Full control with config struct
    pub async fn from_config(config: R2Config) -> Result<Self>;
}
```

**Default Endpoint:**
```
https://api.cloudflare.com/client/v4/accounts/{account_id}/r2/buckets/{bucket_name}/data-catalog
```

### IcebergRestCatalog - Shared Implementation

```rust
pub struct IcebergRestCatalog {
    endpoint: String,
    prefix: String,          // URL path prefix (e.g., "v1/{warehouse}" or "v1")
    http_client: reqwest::Client,
    auth_provider: Box<dyn AuthProvider>,
    file_io: FileIO,
}
```

- Implements all `iceberg::Catalog` trait methods
- URL construction: `{endpoint}/{prefix}/namespaces/{ns}/tables/{table}`
- Single `send_request()` method that calls `auth_provider.sign_request()`
- Reuses request/response types

### S3TablesCatalog - Backward Compatible Wrapper

- Keep existing `from_arn()` API
- Internally creates `IcebergRestCatalog` with `SigV4AuthProvider`
- No breaking changes

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

### Module Gating

```rust
#[cfg(not(target_family = "wasm"))]
pub mod s3tables;

#[cfg(not(target_family = "wasm"))]
pub use s3tables::S3TablesCatalog;
```

### WASM Compatibility Matrix

| Component | WASM Compatible? |
|-----------|------------------|
| `IcebergRestCatalog` | ✅ Yes |
| `BearerTokenAuthProvider` | ✅ Yes |
| `R2DataCatalog` | ✅ Yes |
| `SigV4AuthProvider` | ⚠️ Native only |
| `S3TablesCatalog` | ⚠️ Native only |

### Testing Strategy

- Add `wasm32-unknown-unknown` to CI build targets
- Ensure R2DataCatalog compiles for WASM
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

1. **Extract shared catalog logic**
   - Create `catalog/` module structure
   - Move REST API implementation to `catalog/rest.rs`
   - Define `AuthProvider` trait

2. **Implement auth providers**
   - Extract SigV4 logic to `SigV4AuthProvider`
   - Create `BearerTokenAuthProvider`

3. **Create R2DataCatalog**
   - Implement `R2Config` and constructors
   - Wire up with `IcebergRestCatalog` + `BearerTokenAuthProvider`

4. **Refactor S3TablesCatalog**
   - Make it a thin wrapper around `IcebergRestCatalog`
   - Keep existing public API unchanged

5. **WASM setup**
   - Add conditional compilation for AWS dependencies
   - Add `wasm32-unknown-unknown` build to CI

6. **Testing**
   - Unit tests for auth providers
   - Integration tests for R2DataCatalog (manual, requires R2 setup)
   - Verify WASM compilation

## Migration Path

### For Existing S3 Tables Users

No changes required - existing code continues to work:
```rust
let catalog = S3TablesCatalog::from_arn("my-catalog", arn).await?;
```

### For New R2 Users

```rust
// Simple production usage
let catalog = R2DataCatalog::new(
    "account_id",
    "bucket_name",
    "api_token"
).await?;

// Advanced usage with custom endpoint
let config = R2Config {
    account_id: "...".into(),
    bucket_name: "...".into(),
    api_token: "...".into(),
    endpoint_override: Some("https://custom.endpoint".into()),
};
let catalog = R2DataCatalog::from_config(config).await?;
```

## Open Questions

- Does R2 Data Catalog support the full Iceberg REST spec, or are there unsupported operations?
- Token refresh strategy - should we support automatic token rotation?
- Performance: should we add request/response caching?

## References

- [Cloudflare R2 Data Catalog Docs](https://developers.cloudflare.com/r2/data-catalog/)
- [PyIceberg Catalog Architecture](https://py.iceberg.apache.org/)
- [Apache Iceberg REST Catalog Spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
