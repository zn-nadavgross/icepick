# Icepick: Production-Ready Cloud Iceberg Catalogs

**Date:** 2025-11-15
**Status:** Approved Design

## Overview

Icepick is a specialized Rust library for Apache Iceberg catalog operations on cloud providers. It provides production-ready implementations for AWS S3 Tables and Cloudflare R2 Data Catalog.

## Project Scope

### What Icepick Is

- A focused Rust library for Iceberg catalog operations on cloud providers
- Implements `iceberg::Catalog` trait with cloud-specific authentication
- Rock-solid support for AWS S3 Tables and Cloudflare R2 Data Catalog
- Production-ready: tested, documented, published to crates.io
- Optimized for Rust data engineers who already understand Iceberg

### What Icepick Is NOT

- Not a general-purpose REST catalog (that's rust-iceberg's domain)
- Not trying to support every cloud provider or authentication method
- Not attempting to upstream into rust-iceberg (independent library)
- Not including data format conversion (OTLP code will be removed)

### Target Users

Rust data engineers building production data pipelines who need reliable Iceberg catalog access on AWS or Cloudflare. Users already understand Iceberg concepts and need the cloud catalog glue.

### Relationship with rust-iceberg

Icepick is a specialized fork optimized specifically for cloud catalogs (S3 Tables, R2). While rust-iceberg focuses on the general REST catalog, icepick maintains its own direction and priorities. We occasionally sync changes but diverge on cloud-specific optimizations.

## Architecture

### Project Structure

```
icepick/
├── src/
│   ├── lib.rs              # Public API surface
│   ├── error.rs            # Clean error types
│   ├── catalog/
│   │   ├── mod.rs          # Catalog trait + AuthProvider trait (private)
│   │   ├── s3_tables.rs    # S3TablesCatalog implementation
│   │   ├── r2.rs           # R2Catalog implementation
│   │   └── auth/
│   │       ├── sigv4.rs    # AWS SigV4 signing
│   │       └── bearer.rs   # Bearer token authentication
├── examples/
│   ├── s3_tables_basic.rs  # S3 Tables example
│   └── r2_basic.rs         # R2 example
├── tests/                  # Unit tests
└── docs/
    └── plans/              # Design documents
```

### Public API Design

Two separate catalog types with simple factory methods:

```rust
// AWS S3 Tables
let catalog = icepick::S3TablesCatalog::from_arn(
    "my-catalog",
    "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
).await?;

// Cloudflare R2
let catalog = icepick::R2Catalog::new(
    "my-catalog",
    "account-id",
    "bucket-name",
    "api-token"
).await?;

// Both implement iceberg::Catalog
let table = catalog.load_table(&table_ident).await?;
```

### Key Design Decisions

- **Two catalog types** (`S3TablesCatalog`, `R2Catalog`) not one unified type
- **Factory methods** not builders - simple and direct
- **Private AuthProvider trait** - internal implementation detail, not public API
- **Minimal API surface** - only what's needed to construct and use catalogs

## Implementation Details

### Error Handling

Simple, clear error types:

```rust
pub enum Error {
    NotFound { resource: String },
    Unauthorized { provider: String },
    Forbidden { resource: String },
    ServerError { status: u16, message: String },
    NetworkError { source: reqwest::Error },
    InvalidArn { arn: String },
    // Additional error variants as needed
}
```

**Principles:**
- Map HTTP status codes to clear error types
- Error messages show what operation failed
- No internal implementation details exposed
- No retry logic built-in (users handle retries)
- No observability hooks (users add their own instrumentation)

### Testing Strategy

**Unit tests only:**
- Test all components: ARN parsing, auth signing, request building
- Mock HTTP responses using `wiremock` or similar
- No cloud credentials required
- All tests run offline in CI
- Target: 80%+ code coverage on core paths

**No integration tests against real cloud services.** This keeps CI simple, fast, and credential-free.

### Dependencies

Minimized dependency tree:

```toml
[dependencies]
# Core - no optional features
iceberg = "0.7"
reqwest = { version = "0.12", features = ["json"] }
http = "1.0"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
async-trait = "0.1"
thiserror = "2.0"

# AWS (non-WASM targets only)
[target.'cfg(not(target_family = "wasm"))'.dependencies]
aws-sigv4 = "1.3"
aws-credential-types = "1.2"
aws-config = "1.8"
aws-sdk-sts = "1.55"

[dev-dependencies]
tokio = { version = "1.48", features = ["full"] }
anyhow = "1.0"
arrow = { version = "55", features = ["prettyprint"] }
parquet = "55"
# ... other example dependencies
```

**Removed from main library:**
- `arrow` and `parquet` (moved to examples and dev-dependencies only)
- `uuid` (not needed in core)
- `futures` (not needed in core)

### WASM Support

Two-tier platform support using conditional compilation:

```rust
// src/lib.rs

// Always available
pub mod r2;
pub use r2::R2Catalog;

// Only on native platforms
#[cfg(not(target_family = "wasm"))]
pub mod s3_tables;
#[cfg(not(target_family = "wasm"))]
pub use s3_tables::S3TablesCatalog;
```

**Platform matrix:**

| Target | S3TablesCatalog | R2Catalog |
|--------|----------------|-----------|
| Linux/macOS/Windows | ✅ | ✅ |
| wasm32-unknown-unknown | ❌ | ✅ |

**Rationale:**
- **R2Catalog**: Full WASM support for browser and Cloudflare Workers use cases (only needs HTTP + bearer token)
- **S3TablesCatalog**: Server-side only due to AWS SDK dependencies that don't compile to WASM
- No Cargo features needed - `cfg` attributes handle everything
- Documentation clearly states platform limitations

## CI/CD & Release Process

### GitHub Actions CI

```yaml
# .github/workflows/ci.yml

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - cargo test --all-targets
      - cargo clippy -- -D warnings
      - cargo fmt --check
      - cargo doc --no-deps

  wasm:
    runs-on: ubuntu-latest
    steps:
      - rustup target add wasm32-unknown-unknown
      - cargo build --target wasm32-unknown-unknown
      - cargo clippy --target wasm32-unknown-unknown -- -D warnings
```

**Test targets:**
- Linux (amd64) - primary platform
- WASM (wasm32-unknown-unknown) - verify R2 builds for WASM

### Pre-commit Hooks

Keep existing setup:
- cargo fmt, clippy, test (on pre-push)
- 400 LOC per file limit
- Complexity score ≤ 60
- Conventional commits

### Release Workflow

```yaml
# .github/workflows/release.yml
# Manual trigger only

steps:
  - Run full CI suite (Linux + WASM)
  - cargo publish --dry-run
  - Create GitHub Release with changelog
  - cargo publish to crates.io
```

**Version strategy:**
- Start at `0.1.0` (signals "works but may have breaking changes")
- Strict semantic versioning
  - 0.x.y: breaking changes bump minor version
  - 1.0.0+: standard semver (major.minor.patch)
- Manual releases with hand-written CHANGELOG.md
- Move to 1.0.0 once API is stable and battle-tested

## Documentation

### README.md

Concise and practical:
- What is icepick (2-3 sentences)
- Installation: `cargo add icepick`
- Quick start for S3 Tables (3-5 lines of code)
- Quick start for R2 (3-5 lines of code)
- Link to docs.rs for full API reference
- WASM support note (R2 only)
- License, contributing links

### Rustdoc (docs.rs)

- Module-level documentation explaining catalog trait implementation
- Doc examples on `S3TablesCatalog` and `R2Catalog` constructors
- Authentication setup guidance in doc comments
  - AWS credentials: default credential chain, environment variables
  - R2 tokens: API token creation in Cloudflare dashboard
- Error type documentation with resolution guidance

### Examples

Runnable code in `examples/`:
- `s3_tables_basic.rs` - create namespace, table, write data, read back
- `r2_basic.rs` - same workflow for R2

Examples can use arrow/parquet dependencies (dev-dependencies only).

## Migration Plan

### 1. Project Cleanup

**Remove OTLP code:**
- Delete `otlp2parquet-core/` directory entirely
- Remove any workspace references
- Clean up OTLP mentions in README

**Rename project:**
- Update `Cargo.toml`: `name = "icepick"`
- Update `src/lib.rs` and all internal imports
- Update README with new name
- Rename GitHub repository

### 2. Dependency Minimization

- Move `arrow`, `parquet`, `uuid` to dev-dependencies
- Remove `futures` if unused
- Audit all dependencies for actual usage

### 3. Code Reorganization

- Refactor `src/catalog/rest/` structure to simpler layout
- Create separate `s3_tables.rs` and `r2.rs` modules
- Keep `AuthProvider` trait internal (not pub)
- Implement clean public API with factory methods

### 4. Testing Infrastructure

- Add unit tests for ARN parsing
- Add unit tests for SigV4 signing (with mocked credentials)
- Add unit tests for bearer token auth
- Add unit tests for HTTP request construction
- Set up `wiremock` for mocking catalog REST API responses
- Achieve 80%+ coverage on core paths

### 5. CI/CD Setup

- Create `.github/workflows/ci.yml` for Linux + WASM builds
- Create `.github/workflows/release.yml` for manual releases
- Update pre-commit hooks if needed
- Test full workflow before first release

### 6. Documentation

- Rewrite README to match new scope
- Add comprehensive rustdoc to all public APIs
- Update examples to use new API
- Create CHANGELOG.md for release tracking

### 7. First Release

- Tag `0.1.0`
- Publish to crates.io
- Verify docs.rs build
- Announce in Rust/Iceberg communities

## Success Criteria

Icepick is production-ready when:

1. ✅ Published to crates.io as `icepick`
2. ✅ Documentation builds on docs.rs
3. ✅ CI passes on Linux and WASM targets
4. ✅ 80%+ test coverage on core functionality
5. ✅ Both S3 Tables and R2 examples run successfully
6. ✅ README clearly explains setup for both cloud providers
7. ✅ Zero dependencies beyond what's strictly necessary
8. ✅ Follows semantic versioning with clear CHANGELOG

## Non-Goals

Explicitly out of scope for 1.0:

- Additional cloud providers beyond S3 Tables and R2
- Built-in retry logic (users implement their own)
- Observability instrumentation (users add their own)
- Metrics or tracing integration
- Complex builder patterns or configuration DSLs
- Upstreaming to rust-iceberg
