# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-01-17

### Added

#### List Namespaces
- Added `list_namespaces()` method to Catalog trait
- Implemented REST API integration for listing namespaces
- Updated CLI `namespace list` command to display namespaces

#### Vended Credentials
- Implemented `RestCredentialProvider` with credential caching
- Added path parsing to derive table identity from file paths
- Credentials fetched from REST catalog endpoint and cached per table location

## [0.4.0]

### Added
- Initial release of Icepick
- `S3TablesCatalog` for AWS S3 Tables with SigV4 authentication (native platforms only)
- `R2Catalog` for Cloudflare R2 Data Catalog with bearer token authentication
- Clean public API with factory methods (`from_arn`, `new`)
- Type-safe error handling with `Error` and `Result` types
- Full implementation of `iceberg::Catalog` trait for both catalogs
- Comprehensive examples for both S3 Tables and R2
- Unit tests for ARN parsing, SigV4 signing, and bearer token auth (22 tests)
- Doctests for all public APIs (6 tests)
- CI workflow for Linux builds, formatting, clippy, and tests
- Release workflow for publishing to crates.io
- Comprehensive documentation (README, rustdoc, examples)

### Changed
- Renamed project from `hello-world-iceberg` to `icepick`
- Made internal types private (`AuthProvider`, `IcebergRestCatalog`, etc.)
- Moved non-essential dependencies to dev-dependencies (arrow, parquet, uuid, futures)

### Removed
- Validation utilities (out of scope for catalog-focused library)
- Complex builder patterns in favor of simple factory methods

### Known Limitations
- **WASM Support**: R2Catalog is architecturally WASM-compatible but cannot currently build for WASM due to upstream `iceberg-rust` v0.7.0 lacking WASM support (tokio limitations). See `docs/wasm-compatibility-status.md` for details. This will be resolved once iceberg-rust adds WASM support.

## [0.1.0] - TBD

Initial release.

[unreleased]: https://github.com/smithclay/icepick/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/smithclay/icepick/releases/tag/v0.5.0
[0.4.0]: https://github.com/smithclay/icepick/releases/tag/v0.4.0
