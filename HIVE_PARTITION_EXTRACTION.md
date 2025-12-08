# Implement Hive-Style Partition Value Extraction in `introspect_parquet_file`

## Context

icepick's `introspect_parquet_file` function currently accepts an optional `PartitionSpec` but does **not** extract partition values from Hive-style directory paths. This means files written with paths like:

```
logs/my-service/year=2025/month=12/day=06/hour=15/1733497200000000-abc.parquet
```

...are registered without partition values, breaking query partition pruning.

## Current Behavior

```rust
pub async fn introspect_parquet_file(
    file_io: &FileIO,
    path: &str,
    partition_spec: Option<&PartitionSpec>,
) -> Result<ParquetIntrospection> {
    // Reads Parquet footer for schema, row count, file size
    // Does NOT parse partition values from path
    // Returns ParquetIntrospection with empty/missing partition_values
}
```

## Requested Change

When `partition_spec` is provided, parse the file path to extract Hive-style partition values (`key=value` segments) and populate them in the returned `DataFile`.

## Implementation Requirements

1. **Parse Hive-style path segments**: Extract `key=value` pairs from the path
   ```
   "year=2025/month=12/day=06/hour=15/file.parquet"
   → { "year": "2025", "month": "12", "day": "06", "hour": "15" }
   ```

2. **Match against PartitionSpec fields**: For each `PartitionField` in the spec, look up the corresponding path segment by field name

3. **Convert to appropriate Iceberg types**: The partition values in the path are strings, but need to be converted to the correct Iceberg `Datum` type based on the transform:
   - `year` transform → `i32`
   - `month` transform → `i32`
   - `day` transform → `i32`
   - `hour` transform → `i32`
   - `identity` transform → match source field type

4. **Build `Struct` for DataFile.partition**: Iceberg `DataFile` expects partition values as a `Struct` matching the partition spec's schema

5. **Handle missing/malformed segments gracefully**: If a partition field isn't found in the path, either:
   - Return an error, OR
   - Log a warning and skip (depending on desired strictness)

## Example

**Input:**
```rust
let spec = PartitionSpec::new(0, vec![
    PartitionField::new(1, 5, "year", "year"),   // field_id=1, source_id=5 (Timestamp)
    PartitionField::new(2, 5, "month", "month"),
    PartitionField::new(3, 5, "day", "day"),
    PartitionField::new(4, 5, "hour", "hour"),
]);

let path = "data/logs/my-service/year=2025/month=12/day=06/hour=15/1733497200-abc.parquet";

let result = introspect_parquet_file(&file_io, path, Some(&spec)).await?;
```

**Expected `result.data_file.partition()`:**
```rust
Struct {
    fields: [
        ("year", Datum::int(2025)),
        ("month", Datum::int(12)),
        ("day", Datum::int(6)),
        ("hour", Datum::int(15)),
    ]
}
```

## Suggested Helper Function

```rust
/// Extract Hive-style partition values from a file path.
///
/// Parses segments like "year=2025/month=12" into a map.
fn parse_hive_partition_values(path: &str) -> HashMap<String, String> {
    path.split('/')
        .filter_map(|segment| {
            let mut parts = segment.splitn(2, '=');
            match (parts.next(), parts.next()) {
                (Some(key), Some(value)) if !key.is_empty() => {
                    Some((key.to_string(), value.to_string()))
                }
                _ => None,
            }
        })
        .collect()
}
```

## Test Cases

```rust
#[test]
fn test_parse_hive_partition_values() {
    let path = "prefix/year=2025/month=01/day=15/hour=10/file.parquet";
    let values = parse_hive_partition_values(path);

    assert_eq!(values.get("year"), Some(&"2025".to_string()));
    assert_eq!(values.get("month"), Some(&"01".to_string()));
    assert_eq!(values.get("day"), Some(&"15".to_string()));
    assert_eq!(values.get("hour"), Some(&"10".to_string()));
}

#[test]
fn test_parse_ignores_non_partition_segments() {
    let path = "logs/my-service/year=2025/file.parquet";
    let values = parse_hive_partition_values(path);

    assert_eq!(values.len(), 1);
    assert_eq!(values.get("year"), Some(&"2025".to_string()));
    assert!(!values.contains_key("logs"));
    assert!(!values.contains_key("my-service"));
}

#[tokio::test]
async fn test_introspect_with_partition_spec() {
    // Setup: write a test parquet file to a Hive-style path
    // Call introspect_parquet_file with a partition spec
    // Assert: data_file.partition() contains correct values
}
```

## Files to Modify

- `src/catalog/register.rs` (or wherever `introspect_parquet_file` lives)
- Add helper function for Hive path parsing
- Update `ParquetIntrospection` or `DataFile` construction to include partition values

## Notes

- The partition field **name** in the spec (e.g., `"year"`) must match the **key** in the path (`year=...`)
- Leading zeros in path values (e.g., `month=01`) should parse correctly to integers
- This is a common pattern - Spark, Hive, Trino all use this convention
