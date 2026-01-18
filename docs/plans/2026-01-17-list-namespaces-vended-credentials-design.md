# List Namespaces + Vended Credentials Implementation

**Date**: 2026-01-17
**Status**: Approved
**Target Version**: 0.5.0

## Overview

This design adds two features to icepick:

1. **List Namespaces**: Add `list_namespaces()` to the Catalog trait and implement via REST API
2. **Vended Credentials**: Implement `RestCredentialProvider` to fetch table-scoped credentials from catalog

Both features enable full CLI functionality with R2 Data Catalog, including table info, file listing, and compaction operations.

## Motivation

Currently, the CLI cannot:
- List available namespaces in a catalog
- Access data files in R2 Data Catalog (fails with "Table-scoped credentials not yet implemented")

These limitations prevent using the CLI with R2 Data Catalog for operations that read data files (table info, compaction, file listing).

## Feature 1: List Namespaces

### API Changes

Add to `Catalog` trait in `src/catalog/catalog_trait.rs`:

```rust
/// List all namespaces in the catalog
async fn list_namespaces(&self) -> Result<Vec<NamespaceIdent>>;
```

### REST Implementation

Add to `src/catalog/rest/catalog_impl.rs`:

```rust
pub(super) async fn list_namespaces_impl(&self) -> Result<Vec<NamespaceIdent>> {
    let url = self.url("namespaces");
    let req = self.build_request(
        self.http_client.get(&url).header("Accept", "application/json")
    )?;

    let response: ListNamespacesResponse =
        self.execute_and_parse(req, "namespaces response").await?;

    Ok(response.namespaces.into_iter()
        .map(|ns| NamespaceIdent::new(ns))
        .collect())
}
```

### Response Type

Add to `src/catalog/rest/types.rs`:

```rust
#[derive(Deserialize)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<Vec<String>>,
}
```

### CLI Integration

Update `src/cli/commands/namespace.rs` to call `catalog.list_namespaces()` instead of returning placeholder message.

### Versioning Impact

Minor version bump (0.4.0 → 0.5.0) since we're adding a method to public trait.

## Feature 2: RestCredentialProvider

### Problem Statement

The `RestCredentialProvider` needs to:
1. Map file paths to table identifiers
2. Fetch vended credentials from REST endpoint
3. Cache credentials to minimize REST calls
4. Support R2 Data Catalog credential format

### Implementation Strategy

#### Path-to-Table Mapping

When FileIO requests credentials for a path like:
```
s3://bucket/warehouse/namespace.db/tablename/data/file.parquet
```

The provider must:
1. Extract table location prefix: `s3://bucket/warehouse/namespace.db/tablename`
2. Derive table identifier: `TableIdent("namespace", "tablename")`
3. Call credentials endpoint: `GET /v1/{prefix}/namespaces/namespace/tables/tablename/credentials`
4. Match file path against credential prefixes in response

#### Caching Design

```rust
struct RestCredentialProvider {
    endpoint: String,
    prefix: String,
    token: String,
    http_client: Client,
    s3_endpoint: Option<String>,
    // NEW: Cache credentials by table location prefix
    credential_cache: Arc<RwLock<HashMap<String, VendedCredentials>>>,
}
```

**Cache key**: Table location prefix (e.g., `s3://bucket/warehouse/ns.db/table`)
**Cache invalidation**: None (credentials live for session duration)
**Concurrency**: RwLock allows multiple readers, exclusive writer

#### get_credentials() Flow

```rust
async fn get_credentials(&self, path: &str) -> Result<VendedCredentials> {
    // 1. Check cache first
    if let Some(cached) = self.check_cache(path)? {
        return Ok(cached);
    }

    // 2. Parse table location from path
    let table_location = extract_table_location(path)?;

    // 3. Derive table identifier from location
    let (namespace, table_name) = parse_table_identifier_from_location(&table_location)?;

    // 4. Fetch credentials from REST endpoint
    let creds_response = self.fetch_credentials(&namespace, &table_name).await?;

    // 5. Find matching credential for this path
    let cred = creds_response.storage_credentials.iter()
        .find(|c| path.starts_with(&c.prefix))
        .ok_or_else(|| Error::IoError("No matching credential prefix".into()))?;

    // 6. Convert to VendedCredentials
    let vended = VendedCredentials {
        access_key_id: cred.config.access_key_id.clone().unwrap(),
        secret_access_key: cred.config.secret_access_key.clone().unwrap(),
        session_token: cred.config.session_token.clone(),
        endpoint: cred.config.endpoint.clone().or_else(|| self.s3_endpoint.clone()),
        region: cred.config.region.clone(),
    };

    // 7. Cache by table location
    self.cache_credentials(&table_location, vended.clone())?;

    Ok(vended)
}
```

#### Path Parsing Algorithm

For R2 Data Catalog, paths follow pattern:
```
s3://bucket/namespace.db/tablename/metadata/...
s3://bucket/namespace.db/tablename/data/...
```

Algorithm:
1. Strip `s3://bucket/` prefix
2. Split remaining path by `/`
3. Look for Iceberg directories (`data`, `metadata`) to find table boundary
4. Extract namespace (part before `.db`) and table name
5. Reconstruct table location prefix

Example:
- Input: `s3://bucket/warehouse/default.db/logs/data/00001.parquet`
- Table location: `s3://bucket/warehouse/default.db/logs`
- Namespace: `default`
- Table: `logs`

#### Error Handling

| Error Condition | Error Type | Recovery |
|----------------|------------|----------|
| Path doesn't match expected structure | `Error::IoError` | None - invalid path |
| Credentials endpoint returns 404 | `Error::NotFound` | None - table doesn't exist |
| No matching prefix in credentials | `Error::IoError` | None - config issue |
| Missing required credential fields | `Error::InvalidInput` | None - malformed response |
| Lock poisoning | `Error::IoError` | None - panic in other thread |

## Testing Strategy

### Live R2 Catalog Tests

Test against real R2 catalog:
```
Catalog URL: https://catalog.cloudflarestorage.com/e458468cdac9bcb674f1e25cda158320/frostbit-test12
Warehouse: e458468cdac9bcb674f1e25cda158320_frostbit-test12
Tables: default.logs, default.sum, default.gauge, default.traces
```

#### Test 1: List Namespaces
```bash
cargo run --features cli -- \
  --catalog-url "https://catalog.cloudflarestorage.com/..." \
  --token "..." \
  namespace list
```
Expected: Shows "default" namespace

#### Test 2: Table Info with Vended Credentials
```bash
cargo run --features cli -- \
  --catalog-url "..." \
  --token "..." \
  table info default.logs
```
Expected: Shows schema, snapshot info, file counts, total size (currently fails)

#### Test 3: Compaction Dry Run
```bash
cargo run --features cli -- \
  --catalog-url "..." \
  --token "..." \
  compact default.logs --dry-run
```
Expected: Shows compaction plan with input/output file estimates

#### Test 4: File Listing
```bash
cargo run --features cli -- \
  --catalog-url "..." \
  --token "..." \
  table files default.logs
```
Expected: Lists all data files with sizes and record counts

### Unit Tests

Add to `src/catalog/rest/credentials.rs`:
- `test_parse_table_location_from_path()` - various path formats
- `test_credential_caching()` - cache hit/miss scenarios
- `test_matching_credential_prefix()` - prefix selection logic

Add to `src/catalog/rest/catalog_impl.rs`:
- `test_list_namespaces_response_parsing()` - empty and non-empty lists

### Edge Cases

- Empty namespace list → return empty Vec
- Table with no vended credentials → 404 error
- Path doesn't match any credential prefix → IoError
- Concurrent credential fetches for same table → one fetch, others wait for cache

## Implementation Checklist

- [ ] Add `list_namespaces()` to Catalog trait
- [ ] Implement `list_namespaces_impl()` in IcebergRestCatalog
- [ ] Add `ListNamespacesResponse` type
- [ ] Wire trait method to REST impl for all catalog types
- [ ] Update CLI namespace list command
- [ ] Add `credential_cache` field to RestCredentialProvider
- [ ] Implement path parsing helpers
- [ ] Implement `get_credentials()` with caching
- [ ] Add unit tests for path parsing
- [ ] Add unit tests for caching logic
- [ ] Test with live R2 catalog (all 4 scenarios)
- [ ] Update CHANGELOG for 0.5.0
- [ ] Update version in Cargo.toml

## Success Criteria

1. `namespace list` command shows namespaces from R2 catalog
2. `table info default.logs` shows file statistics without credential errors
3. `compact default.logs --dry-run` produces compaction plan
4. `table files default.logs` lists all data files
5. All unit tests pass
6. No performance regression (caching keeps REST calls minimal)

## Non-Goals

- TTL-based credential expiration (session-scoped is sufficient)
- Support for non-Iceberg directory structures
- Credential refresh/rotation during operation
- List namespaces pagination (not in Iceberg REST spec)
