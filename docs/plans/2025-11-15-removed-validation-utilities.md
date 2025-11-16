# Removed: Arrow Schema Validation Utilities

**Date:** 2025-11-15

## Decision

Removed `src/validation.rs` which contained Arrow schema field ID validation utilities.

## Rationale

Icepick is a focused library for cloud Iceberg catalog operations. The validation utilities:
- Required `arrow`, `parquet`, and `anyhow` as dependencies
- Were concerned with data format validation, not catalog operations
- Did not align with the "catalog operations only" scope

Per the production-ready design:
> What Icepick Is NOT:
> - Not including data format conversion (OTLP code will be removed)

The validation utilities were helpful for users writing Iceberg data, but they belong in a separate utility library or user code, not in a catalog-focused library.

## What Was Removed

- `src/validation.rs` - Field ID validation for Arrow schemas
- Tests for nested struct, list, and map field ID validation
- Dependencies on `arrow`, `parquet`, `uuid`, `futures`, and `anyhow` moved to dev-dependencies
- Only `percent-encoding` remains (used by REST client for URL encoding)

## For Users Who Need This

Users needing schema validation should:
1. Use `iceberg::arrow::schema_to_arrow_schema()` which auto-assigns field IDs
2. Implement their own validation if needed (the removed code can serve as reference)
3. Check out PyIceberg's schema utilities as an alternative pattern
