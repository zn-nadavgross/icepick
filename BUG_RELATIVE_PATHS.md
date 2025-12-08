# Bug: Data Files Registered with Relative Paths Instead of Absolute URIs

Status: **Fixed** (relative paths are now rejected before registration)

## Summary

`register_data_files()` used to accept relative file paths (e.g., `logs/part.parquet`) and persist them verbatim in table metadata. That produced invalid Iceberg manifests and caused engines (DuckDB, Spark, etc.) to interpret the paths as local files.

## Iceberg Spec Requirement

The Iceberg specification (v1-v3) **requires absolute URIs** for all file paths:

> "All the file references are absolute paths right now."
> — [apache/iceberg#1617](https://github.com/apache/iceberg/issues/1617)

Relative path support is proposed for Iceberg v4 spec but not yet implemented:
- [apache/iceberg#13141](https://github.com/apache/iceberg/issues/13141)

## Reproduction

1. Use `register_data_files()` to register Parquet files to R2 Data Catalog
2. Query the table via DuckDB with Iceberg REST catalog

**Expected file path in metadata:**
```
s3://otlp2parquet-smoketest-v4/smoke-849b38a0/logs/product-catalog_v3/year=2025/month=10/day=17/hour=22/1760741572254301-bf42d54a58ee4086b90e4616c691b603.parquet
```

**Actual file path in metadata:**
```
smoke-849b38a0/logs/product-catalog_v3/year=2025/month=10/day=17/hour=22/1760741572254301-bf42d54a58ee4086b90e4616c691b603.parquet
```

## Error

When DuckDB queries the Iceberg table:

```
IO Error: Cannot open file "smoke-849b38a0/logs/product-catalog_v3/year=2025/month=10/day=17/hour=22/1760741572254301-bf42d54a58ee4086b90e4616c691b603.parquet": No such file or directory
```

DuckDB interprets the relative path as a local filesystem path instead of an S3 URI.

## Fix

Relative paths are now rejected before they can be written into metadata:
- `DataFileInput::into_data_file` validates `file_path` and returns `InvalidInput` unless it is an absolute URI (scheme like `s3://`, `memory://`, etc., or a rooted path).
- New test `tests/register_test.rs::register_rejects_relative_paths` covers the validation and ensures we never regress.

Callers must pass fully qualified URIs (e.g., `s3://bucket/key`); this keeps manifests spec-compliant and prevents query engines from resolving paths incorrectly.

## Environment

- icepick: rev 4e420845d1749f25c4c9cc01304261429fd5de93
- Catalog: R2 Data Catalog (Cloudflare)
- Query Engine: DuckDB with Iceberg extension
- Storage: Cloudflare R2 (S3-compatible)

## Related

- Iceberg Spec: https://iceberg.apache.org/spec/
- R2 Data Catalog: Uses Iceberg REST catalog protocol
