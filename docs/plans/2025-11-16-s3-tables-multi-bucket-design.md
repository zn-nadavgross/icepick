# S3 Tables Multi-Bucket FileIO Design

**Date:** 2025-11-16
**Status:** Approved
**Goal:** Enable S3 Tables support by allowing FileIO to handle multiple S3 buckets dynamically

## Problem Statement

AWS S3 Tables is a managed service where:
- The catalog bucket is user-managed (e.g., `test1234` in `us-west-2`)
- Table data is stored in AWS-managed buckets with virtual names (e.g., `925de699...--table-s3`)
- All buckets are in the **same region**, but have different bucket names

Current FileIO implementation uses a single OpenDAL operator tied to one bucket, making S3 Tables writes fail when the data bucket name differs from the catalog bucket name.

## Research Findings

### What Daft Does
- Delegates all file I/O to PyIceberg's FileIO abstraction
- PyIceberg handles multi-bucket access internally
- S3 Tables catalog raises error: "S3 Table writes require using Iceberg REST"

### What PyIceberg Does
- Uses AWS SDK's `cross_region_access_enabled` feature
- Property: `s3.cross-region-access-enabled = true`
- Note: For S3 Tables specifically, all buckets are same-region, but PyIceberg's approach handles general multi-bucket scenarios

### OpenDAL Limitations
- Version 0.51 (our current version) does NOT support `from_uri`
- Bucket must be specified at operator construction time
- One operator per bucket required
- Cannot dynamically switch buckets with a single operator

## Design Solution: Operator Factory Pattern

Transform FileIO from a simple wrapper to a smart factory that creates and caches operators per bucket.

### Architecture

```rust
pub struct FileIO {
    // For S3 Tables: AWS credentials for creating operators
    credentials: Option<AwsCredentials>,
    default_region: String,

    // Cache of operators per bucket (all same region)
    operator_cache: Arc<RwLock<HashMap<String, Operator>>>,

    // For R2/Memory: pre-configured single operator
    default_operator: Option<Operator>,
}
```

**Key insight:** S3 Tables uses same-region buckets, so we only need to cache by bucket name, not bucket+region.

### Path Routing Strategy

**Priority-based routing for `s3://` URIs:**

1. **Priority 1:** If `default_operator` exists → use it (R2 case)
2. **Priority 2:** If `credentials` exist → create dynamic operator (S3 Tables case)
3. **Priority 3:** Error - no operator configured

**Why this works:**
- **R2:** Has `default_operator` set, no `credentials` → uses single pre-configured operator
- **S3 Tables:** Has `credentials`, no `default_operator` → creates operators dynamically per bucket

### Region Resolution

**Simple approach - single region:**
- All S3 Tables buckets are in the same region as the catalog bucket
- Use `default_region` from catalog configuration for all operators
- No region detection or cross-region handling needed

**Why this works:**
- S3 Tables service manages bucket placement in same region
- Eliminates complexity of cross-region access
- Single region configuration is sufficient

### Operator Caching

```rust
async fn get_or_create_operator(&self, bucket: &str) -> Result<Operator> {
    // Fast path: read lock
    {
        let cache = self.operator_cache.read().unwrap();
        if let Some(op) = cache.get(bucket) {
            return Ok(op.clone());
        }
    }

    // Slow path: write lock
    let mut cache = self.operator_cache.write().unwrap();

    // Double-check pattern
    if let Some(op) = cache.get(bucket) {
        return Ok(op.clone());
    }

    let op = self.create_s3_operator(bucket, &self.default_region)?;
    cache.insert(bucket.to_string(), op.clone());
    Ok(op)
}
```

**Thread safety:** `Arc<RwLock<HashMap<String, Operator>>>` allows multiple concurrent reads, exclusive writes
**Cache key:** Just bucket name (all buckets in same region)

### URI Parsing

Extract bucket from S3 URIs:
- `s3://bucket/path/to/file.parquet` → bucket: `bucket`, path: `path/to/file.parquet`
- Strip `s3://bucket/` prefix before passing to OpenDAL

### FileIO Methods

All methods follow same pattern:

```rust
pub async fn read(&self, path: &str) -> Result<Vec<u8>> {
    let operator = self.get_operator_for_path(path).await?;
    let normalized = self.normalize_path(path);
    operator.read(normalized).await.map(|b| b.to_vec())
        .map_err(|e| Error::IoError(format!("Failed to read {}: {}", path, e)))
}
```

### Constructors

**R2 Constructor (existing):**
```rust
pub fn new(operator: Operator) -> Self {
    Self {
        credentials: None,
        default_region: String::new(),
        operator_cache: Arc::new(RwLock::new(HashMap::new())),
        default_operator: Some(operator),  // R2 path
    }
}
```

**S3 Tables Constructor (new):**
```rust
pub async fn from_aws_credentials(
    credentials: AwsCredentials,
    default_region: String,
) -> Result<Self> {
    Ok(Self {
        credentials: Some(credentials),
        default_region,
        operator_cache: Arc::new(RwLock::new(HashMap::new())),
        default_operator: None,  // S3 Tables path
    })
}
```

## Catalog Integration

### S3 Tables Catalog

Update `RestCatalog::from_s3_tables_arn`:
- Load AWS credentials via `aws-config`
- Extract region from credentials provider
- Create FileIO with `from_aws_credentials`
- FileIO will create operators on-demand per bucket

### R2 Catalog (Unchanged)

- Continues using `FileIO::new(operator)`
- Pre-configures single operator with R2 endpoint
- All `s3://` paths route through `default_operator`

## Implementation Plan

1. **Update FileIO struct** - Add fields for credentials, caching, default operator
2. **Simplify cache key** - Use bucket name only (no BucketKey struct needed)
3. **Implement from_aws_credentials** - New constructor for S3 Tables
4. **Update get_operator_for_path** - Priority-based routing logic
5. **Add extract_bucket_from_uri** - Parse S3 URIs
6. **Add get_or_create_operator** - Caching logic with double-check pattern
7. **Add create_s3_operator** - Build OpenDAL operator with credentials
8. **Update all FileIO methods** - Route through get_operator_for_path
9. **Update S3TablesCatalog** - Use new FileIO constructor
10. **Add tests** - Multi-bucket caching, R2 compatibility

## Benefits

✅ **S3 Tables support** - Handles AWS-managed buckets with different names
✅ **Backward compatible** - R2 and existing code unchanged
✅ **Thread-safe** - Concurrent access via RwLock
✅ **Efficient caching** - One operator per bucket, reused across operations
✅ **Simple single-region design** - No cross-region complexity needed
✅ **No breaking changes** - Existing FileIO::new() still works

## Trade-offs

**Pros:**
- Works with OpenDAL 0.51 (current version)
- No cross-region complexity
- Simple caching by bucket name only
- Clean separation: R2 vs S3 Tables paths
- All buckets in same region = simpler credential management

**Cons:**
- Cache grows unbounded (could add LRU eviction in future)
- Assumes S3 Tables always uses same-region buckets (validated assumption)

## Future Enhancements

- **LRU cache eviction** - Limit operator cache size
- **OpenDAL upgrade** - Use `from_uri` when available in future versions
- **Metrics** - Track cache hits/misses, operator creation rate
- **Multi-region support** - If needed for other use cases beyond S3 Tables

## References

- PyIceberg S3FileIO: https://py.iceberg.apache.org/configuration/
- OpenDAL RFC-0057: https://opendal.apache.org/docs/rust/opendal/docs/rfcs/rfc_0057_auto_region/
- AWS SDK cross-region: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/client.html
- Daft S3 Tables: https://github.com/Eventual-Inc/Daft
