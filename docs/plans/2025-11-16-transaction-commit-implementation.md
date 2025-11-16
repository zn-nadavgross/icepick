# Transaction.commit() Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement Transaction.commit() to enable WASM-compatible Iceberg table writes with Avro manifests, optimistic concurrency, and PyIceberg compatibility.

**Architecture:** Write Avro manifest files → manifest lists → update table metadata → atomic catalog update with retry on conflict. Follows Iceberg v2 spec with PyIceberg-compatible file naming.

**Tech Stack:** Rust, apache-avro 0.21, OpenDAL, wasm32-unknown-unknown target

---

## Phase 1: Dependencies and Error Handling

### Task 1.1: Add apache-avro Dependency

**Files:**
- Modify: `Cargo.toml`

**Step 1: Add apache-avro dependency**

Open `Cargo.toml` and add to the `[dependencies]` section:

```toml
apache-avro = "0.21"
```

**Step 2: Verify WASM compilation**

Run: `cargo check --target wasm32-unknown-unknown`
Expected: Compiles successfully with no errors

**Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "build: add apache-avro 0.21 for manifest serialization"
```

---

### Task 1.2: Add ConcurrentModification Error Variant

**Files:**
- Modify: `src/error.rs:8-66`
- Test: `src/error.rs:179-207`

**Step 1: Write test for concurrent modification error**

Add to `src/error.rs` in the tests module (after line 207):

```rust
#[test]
fn test_concurrent_modification_error() {
    let err = Error::concurrent_modification("expected v2, found v3");
    assert!(matches!(err, Error::ConcurrentModification { .. }));
    assert_eq!(err.to_string(), "Concurrent modification detected: expected v2, found v3");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_concurrent_modification_error`
Expected: FAIL - no variant or method named `concurrent_modification`

**Step 3: Add error variant**

Add to the `Error` enum (after line 61):

```rust
/// Concurrent modification detected (optimistic locking failure)
#[error("Concurrent modification detected: {message}")]
ConcurrentModification { message: String },
```

**Step 4: Add constructor method**

Add to the `impl Error` block (after line 129):

```rust
/// Create a ConcurrentModification error
pub fn concurrent_modification(message: impl Into<String>) -> Self {
    Self::ConcurrentModification {
        message: message.into(),
    }
}
```

**Step 5: Add conversion to iceberg::Error**

Add to the `From<Error> for iceberg::Error` impl (after line 174):

```rust
Error::ConcurrentModification { message } => {
    iceberg::Error::new(iceberg::ErrorKind::DataInvalid, message)
}
```

**Step 6: Run test to verify it passes**

Run: `cargo test test_concurrent_modification_error`
Expected: PASS

**Step 7: Commit**

```bash
git add src/error.rs
git commit -m "feat: add ConcurrentModification error variant for optimistic locking"
```

---

## Phase 2: Avro Schema Definitions

### Task 2.1: Create Manifest Module

**Files:**
- Create: `src/manifest/mod.rs`
- Create: `src/manifest/schema.rs`
- Modify: `src/lib.rs`

**Step 1: Create manifest module directory**

Run: `mkdir -p src/manifest`

**Step 2: Create mod.rs with module exports**

Create `src/manifest/mod.rs`:

```rust
//! Iceberg manifest file handling

pub mod schema;

pub use schema::{manifest_entry_schema_v2, manifest_list_schema_v2};
```

**Step 3: Export manifest module from lib.rs**

Add to `src/lib.rs` (after line 6, with other pub mod declarations):

```rust
pub mod manifest;
```

**Step 4: Verify compilation**

Run: `cargo check`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add src/manifest/mod.rs src/lib.rs
git commit -m "feat: add manifest module scaffold"
```

---

### Task 2.2: Implement Manifest Entry Avro Schema

**Files:**
- Create: `src/manifest/schema.rs`
- Test: inline doc tests

**Step 1: Write doc test for manifest entry schema**

Create `src/manifest/schema.rs`:

```rust
//! Avro schemas for Iceberg manifest files (v2 format)

use apache_avro::Schema;

/// Returns the Avro schema for manifest entries in Iceberg v2 format
///
/// # Example
/// ```
/// use icepick::manifest::schema::manifest_entry_schema_v2;
/// let schema = manifest_entry_schema_v2();
/// assert!(schema.is_ok());
/// ```
pub fn manifest_entry_schema_v2() -> Result<Schema, apache_avro::Error> {
    todo!("Implement manifest entry schema")
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --doc manifest_entry_schema_v2`
Expected: FAIL - panics with "not yet implemented"

**Step 3: Implement manifest entry schema**

Replace the `todo!()` with:

```rust
let schema_json = r#"{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {
      "name": "status",
      "type": "int",
      "field-id": 0,
      "doc": "0=EXISTING, 1=ADDED, 2=DELETED"
    },
    {
      "name": "snapshot_id",
      "type": ["null", "long"],
      "default": null,
      "field-id": 1
    },
    {
      "name": "sequence_number",
      "type": ["null", "long"],
      "default": null,
      "field-id": 3
    },
    {
      "name": "file_sequence_number",
      "type": ["null", "long"],
      "default": null,
      "field-id": 4
    },
    {
      "name": "data_file",
      "type": {
        "type": "record",
        "name": "data_file",
        "fields": [
          {
            "name": "content",
            "type": "int",
            "field-id": 134,
            "doc": "0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES"
          },
          {
            "name": "file_path",
            "type": "string",
            "field-id": 100
          },
          {
            "name": "file_format",
            "type": "string",
            "field-id": 101
          },
          {
            "name": "partition",
            "type": {
              "type": "map",
              "values": "string"
            },
            "field-id": 102
          },
          {
            "name": "record_count",
            "type": "long",
            "field-id": 103
          },
          {
            "name": "file_size_in_bytes",
            "type": "long",
            "field-id": 104
          },
          {
            "name": "column_sizes",
            "type": [
              "null",
              {
                "type": "map",
                "values": "long",
                "key-id": 117,
                "value-id": 118
              }
            ],
            "default": null,
            "field-id": 108
          },
          {
            "name": "value_counts",
            "type": [
              "null",
              {
                "type": "map",
                "values": "long",
                "key-id": 119,
                "value-id": 120
              }
            ],
            "default": null,
            "field-id": 109
          },
          {
            "name": "null_value_counts",
            "type": [
              "null",
              {
                "type": "map",
                "values": "long",
                "key-id": 121,
                "value-id": 122
              }
            ],
            "default": null,
            "field-id": 110
          },
          {
            "name": "lower_bounds",
            "type": [
              "null",
              {
                "type": "map",
                "values": "bytes",
                "key-id": 126,
                "value-id": 127
              }
            ],
            "default": null,
            "field-id": 125
          },
          {
            "name": "upper_bounds",
            "type": [
              "null",
              {
                "type": "map",
                "values": "bytes",
                "key-id": 129,
                "value-id": 130
              }
            ],
            "default": null,
            "field-id": 124
          },
          {
            "name": "key_metadata",
            "type": ["null", "bytes"],
            "default": null,
            "field-id": 105
          },
          {
            "name": "split_offsets",
            "type": [
              "null",
              {
                "type": "array",
                "items": "long",
                "element-id": 133
              }
            ],
            "default": null,
            "field-id": 106
          },
          {
            "name": "equality_ids",
            "type": [
              "null",
              {
                "type": "array",
                "items": "int",
                "element-id": 138
              }
            ],
            "default": null,
            "field-id": 135
          },
          {
            "name": "sort_order_id",
            "type": ["null", "int"],
            "default": null,
            "field-id": 140
          }
        ]
      },
      "field-id": 2
    }
  ]
}"#;

Schema::parse_str(schema_json)
```

**Step 4: Run test to verify it passes**

Run: `cargo test --doc manifest_entry_schema_v2`
Expected: PASS

**Step 5: Commit**

```bash
git add src/manifest/schema.rs
git commit -m "feat: add Avro schema for manifest entries (v2)"
```

---

### Task 2.3: Implement Manifest List Avro Schema

**Files:**
- Modify: `src/manifest/schema.rs`

**Step 1: Write doc test for manifest list schema**

Add to `src/manifest/schema.rs`:

```rust
/// Returns the Avro schema for manifest lists in Iceberg v2 format
///
/// # Example
/// ```
/// use icepick::manifest::schema::manifest_list_schema_v2;
/// let schema = manifest_list_schema_v2();
/// assert!(schema.is_ok());
/// ```
pub fn manifest_list_schema_v2() -> Result<Schema, apache_avro::Error> {
    todo!("Implement manifest list schema")
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --doc manifest_list_schema_v2`
Expected: FAIL - panics with "not yet implemented"

**Step 3: Implement manifest list schema**

Replace the `todo!()` with:

```rust
let schema_json = r#"{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {
      "name": "manifest_path",
      "type": "string",
      "field-id": 500
    },
    {
      "name": "manifest_length",
      "type": "long",
      "field-id": 501
    },
    {
      "name": "partition_spec_id",
      "type": "int",
      "field-id": 502
    },
    {
      "name": "content",
      "type": "int",
      "field-id": 517,
      "doc": "0=DATA, 1=DELETES"
    },
    {
      "name": "sequence_number",
      "type": "long",
      "field-id": 515
    },
    {
      "name": "min_sequence_number",
      "type": "long",
      "field-id": 516
    },
    {
      "name": "added_snapshot_id",
      "type": "long",
      "field-id": 503
    },
    {
      "name": "added_files_count",
      "type": "int",
      "field-id": 504
    },
    {
      "name": "existing_files_count",
      "type": "int",
      "field-id": 505
    },
    {
      "name": "deleted_files_count",
      "type": "int",
      "field-id": 506
    },
    {
      "name": "added_rows_count",
      "type": "long",
      "field-id": 512
    },
    {
      "name": "existing_rows_count",
      "type": "long",
      "field-id": 513
    },
    {
      "name": "deleted_rows_count",
      "type": "long",
      "field-id": 514
    },
    {
      "name": "partitions",
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "field_summary",
            "fields": [
              {
                "name": "contains_null",
                "type": "boolean",
                "field-id": 509
              },
              {
                "name": "contains_nan",
                "type": ["null", "boolean"],
                "default": null,
                "field-id": 518
              },
              {
                "name": "lower_bound",
                "type": ["null", "bytes"],
                "default": null,
                "field-id": 510
              },
              {
                "name": "upper_bound",
                "type": ["null", "bytes"],
                "default": null,
                "field-id": 511
              }
            ]
          },
          "element-id": 508
        }
      ],
      "default": null,
      "field-id": 507
    },
    {
      "name": "key_metadata",
      "type": ["null", "bytes"],
      "default": null,
      "field-id": 519
    }
  ]
}"#;

Schema::parse_str(schema_json)
```

**Step 4: Run test to verify it passes**

Run: `cargo test --doc manifest_list_schema_v2`
Expected: PASS

**Step 5: Commit**

```bash
git add src/manifest/schema.rs
git commit -m "feat: add Avro schema for manifest lists (v2)"
```

---

## Phase 3: DataFile to Avro Conversion

### Task 3.1: Create Avro Converter Module

**Files:**
- Create: `src/manifest/avro.rs`
- Modify: `src/manifest/mod.rs`

**Step 1: Create avro module and export**

Create `src/manifest/avro.rs`:

```rust
//! Convert Iceberg types to Avro values

use apache_avro::types::Value;
use crate::spec::DataFile;
use crate::error::Result;
```

Update `src/manifest/mod.rs` to add:

```rust
pub mod avro;

pub use avro::data_file_to_avro;
```

**Step 2: Verify compilation**

Run: `cargo check`
Expected: Compiles successfully

**Step 3: Commit**

```bash
git add src/manifest/avro.rs src/manifest/mod.rs
git commit -m "feat: add avro converter module scaffold"
```

---

### Task 3.2: Implement DataFile to Avro Conversion

**Files:**
- Modify: `src/manifest/avro.rs`
- Create: `tests/manifest_avro_tests.rs`

**Step 1: Write test for DataFile to Avro conversion**

Create `tests/manifest_avro_tests.rs`:

```rust
use icepick::manifest::avro::data_file_to_avro;
use icepick::spec::DataFile;
use std::collections::HashMap;

#[test]
fn test_data_file_to_avro_minimal() {
    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .build()
        .unwrap();

    let avro_value = data_file_to_avro(&data_file).unwrap();

    // Verify it's a Record
    if let apache_avro::types::Value::Record(fields) = avro_value {
        let file_path = fields.iter().find(|(k, _)| k == "file_path");
        assert!(file_path.is_some());
    } else {
        panic!("Expected Record value");
    }
}

#[test]
fn test_data_file_to_avro_with_stats() {
    let mut value_counts = HashMap::new();
    value_counts.insert(1, 100);

    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file2.parquet")
        .with_file_format("PARQUET")
        .with_record_count(200)
        .with_file_size_in_bytes(10000)
        .with_value_counts(value_counts)
        .build()
        .unwrap();

    let avro_value = data_file_to_avro(&data_file).unwrap();

    // Verify it's a Record with stats
    assert!(matches!(avro_value, apache_avro::types::Value::Record(_)));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_data_file_to_avro`
Expected: FAIL - function `data_file_to_avro` not found

**Step 3: Implement DataFile to Avro conversion**

Add to `src/manifest/avro.rs`:

```rust
/// Convert a DataFile to an Avro Record value for manifest entry
pub fn data_file_to_avro(data_file: &DataFile) -> Result<Value> {
    use std::collections::HashMap;

    let mut fields = vec![
        ("content".to_string(), Value::Int(0)), // 0 = DATA
        ("file_path".to_string(), Value::String(data_file.file_path().to_string())),
        ("file_format".to_string(), Value::String(data_file.file_format().to_string())),
        ("partition".to_string(), Value::Map(HashMap::new())), // Empty for unpartitioned
        ("record_count".to_string(), Value::Long(data_file.record_count())),
        ("file_size_in_bytes".to_string(), Value::Long(data_file.file_size_in_bytes())),
    ];

    // Optional column_sizes
    let column_sizes = if let Some(sizes) = data_file.column_sizes() {
        let map: HashMap<String, Value> = sizes
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Long(*v)))
            .collect();
        Value::Union(1, Box::new(Value::Map(map)))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("column_sizes".to_string(), column_sizes));

    // Optional value_counts
    let value_counts = if let Some(counts) = data_file.value_counts() {
        let map: HashMap<String, Value> = counts
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Long(*v)))
            .collect();
        Value::Union(1, Box::new(Value::Map(map)))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("value_counts".to_string(), value_counts));

    // Optional null_value_counts
    let null_value_counts = if let Some(counts) = data_file.null_value_counts() {
        let map: HashMap<String, Value> = counts
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Long(*v)))
            .collect();
        Value::Union(1, Box::new(Value::Map(map)))
    } else {
        Value::Union(0, Box::new(Value::Null))
    };
    fields.push(("null_value_counts".to_string(), null_value_counts));

    // Optional lower_bounds
    fields.push(("lower_bounds".to_string(), Value::Union(0, Box::new(Value::Null))));

    // Optional upper_bounds
    fields.push(("upper_bounds".to_string(), Value::Union(0, Box::new(Value::Null))));

    // Optional fields set to null for MVP
    fields.push(("key_metadata".to_string(), Value::Union(0, Box::new(Value::Null))));
    fields.push(("split_offsets".to_string(), Value::Union(0, Box::new(Value::Null))));
    fields.push(("equality_ids".to_string(), Value::Union(0, Box::new(Value::Null))));
    fields.push(("sort_order_id".to_string(), Value::Union(0, Box::new(Value::Null))));

    Ok(Value::Record(fields))
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test test_data_file_to_avro`
Expected: PASS

**Step 5: Commit**

```bash
git add src/manifest/avro.rs tests/manifest_avro_tests.rs
git commit -m "feat: implement DataFile to Avro conversion"
```

---

## Phase 4: Manifest File Writing

### Task 4.1: Create Manifest Writer Module

**Files:**
- Create: `src/manifest/writer.rs`
- Modify: `src/manifest/mod.rs`

**Step 1: Create writer module**

Create `src/manifest/writer.rs`:

```rust
//! Write manifest and manifest list files

use crate::error::Result;
use crate::io::FileIO;
use crate::spec::DataFile;

/// Write a manifest file containing data file entries
///
/// Returns the number of bytes written
pub async fn write_manifest(
    file_io: &FileIO,
    path: &str,
    data_files: &[DataFile],
    snapshot_id: i64,
    sequence_number: i64,
) -> Result<i64> {
    todo!("Implement manifest writing")
}
```

Update `src/manifest/mod.rs`:

```rust
pub mod writer;

pub use writer::write_manifest;
```

**Step 2: Verify compilation**

Run: `cargo check`
Expected: Compiles successfully

**Step 3: Commit**

```bash
git add src/manifest/writer.rs src/manifest/mod.rs
git commit -m "feat: add manifest writer module scaffold"
```

---

### Task 4.2: Implement Manifest File Writer

**Files:**
- Modify: `src/manifest/writer.rs`
- Create: `tests/manifest_writer_tests.rs`

**Step 1: Write test for manifest writing**

Create `tests/manifest_writer_tests.rs`:

```rust
use icepick::io::FileIO;
use icepick::manifest::writer::write_manifest;
use icepick::spec::DataFile;
use opendal::Operator;

#[tokio::test]
async fn test_write_manifest() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .build()
        .unwrap();

    let path = "metadata/test-m0.avro";
    let bytes_written = write_manifest(&file_io, path, &[data_file], 1, 1)
        .await
        .unwrap();

    assert!(bytes_written > 0);

    // Verify file exists
    let exists = op.exists(path).await.unwrap();
    assert!(exists);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_write_manifest`
Expected: FAIL - panics with "not yet implemented"

**Step 3: Implement manifest writing**

Replace the `todo!()` in `src/manifest/writer.rs` with:

```rust
use apache_avro::Writer;
use apache_avro::types::Value;
use crate::manifest::schema::manifest_entry_schema_v2;
use crate::manifest::avro::data_file_to_avro;

pub async fn write_manifest(
    file_io: &FileIO,
    path: &str,
    data_files: &[DataFile],
    snapshot_id: i64,
    sequence_number: i64,
) -> Result<i64> {
    let schema = manifest_entry_schema_v2()
        .map_err(|e| crate::error::Error::InvalidInput(format!("Invalid Avro schema: {}", e)))?;

    let mut writer = Writer::new(&schema, Vec::new());

    for data_file in data_files {
        let data_file_value = data_file_to_avro(data_file)?;

        let entry = Value::Record(vec![
            ("status".to_string(), Value::Int(1)), // 1 = ADDED
            ("snapshot_id".to_string(), Value::Union(1, Box::new(Value::Long(snapshot_id)))),
            ("sequence_number".to_string(), Value::Union(1, Box::new(Value::Long(sequence_number)))),
            ("file_sequence_number".to_string(), Value::Union(1, Box::new(Value::Long(sequence_number)))),
            ("data_file".to_string(), data_file_value),
        ]);

        writer.append(entry)
            .map_err(|e| crate::error::Error::InvalidInput(format!("Failed to append to Avro writer: {}", e)))?;
    }

    let avro_bytes = writer.into_inner()
        .map_err(|e| crate::error::Error::InvalidInput(format!("Failed to finalize Avro writer: {}", e)))?;

    let bytes_written = avro_bytes.len() as i64;
    file_io.write(path, avro_bytes).await?;

    Ok(bytes_written)
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test test_write_manifest`
Expected: PASS

**Step 5: Commit**

```bash
git add src/manifest/writer.rs tests/manifest_writer_tests.rs
git commit -m "feat: implement manifest file writing with Avro"
```

---

### Task 4.3: Implement Manifest List Writer

**Files:**
- Modify: `src/manifest/writer.rs`
- Modify: `tests/manifest_writer_tests.rs`

**Step 1: Write test for manifest list writing**

Add to `tests/manifest_writer_tests.rs`:

```rust
use icepick::manifest::writer::write_manifest_list;

#[tokio::test]
async fn test_write_manifest_list() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

    let manifest_path = "metadata/test-m0.avro";
    let manifest_length = 1000;
    let added_files_count = 5;
    let added_rows_count = 500;

    let list_path = "metadata/snap-1-1-test.avro";
    write_manifest_list(
        &file_io,
        list_path,
        manifest_path,
        manifest_length,
        1, // snapshot_id
        1, // sequence_number
        added_files_count,
        added_rows_count,
    )
    .await
    .unwrap();

    // Verify file exists
    let exists = op.exists(list_path).await.unwrap();
    assert!(exists);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_write_manifest_list`
Expected: FAIL - function `write_manifest_list` not found

**Step 3: Add function signature**

Add to `src/manifest/writer.rs`:

```rust
/// Write a manifest list file containing manifest file metadata
pub async fn write_manifest_list(
    file_io: &FileIO,
    path: &str,
    manifest_path: &str,
    manifest_length: i64,
    snapshot_id: i64,
    sequence_number: i64,
    added_files_count: i32,
    added_rows_count: i64,
) -> Result<()> {
    todo!("Implement manifest list writing")
}
```

**Step 4: Export from mod.rs**

Add to `src/manifest/mod.rs`:

```rust
pub use writer::write_manifest_list;
```

**Step 5: Implement manifest list writing**

Replace the `todo!()` with:

```rust
use crate::manifest::schema::manifest_list_schema_v2;

pub async fn write_manifest_list(
    file_io: &FileIO,
    path: &str,
    manifest_path: &str,
    manifest_length: i64,
    snapshot_id: i64,
    sequence_number: i64,
    added_files_count: i32,
    added_rows_count: i64,
) -> Result<()> {
    let schema = manifest_list_schema_v2()
        .map_err(|e| crate::error::Error::InvalidInput(format!("Invalid Avro schema: {}", e)))?;

    let mut writer = Writer::new(&schema, Vec::new());

    let entry = Value::Record(vec![
        ("manifest_path".to_string(), Value::String(manifest_path.to_string())),
        ("manifest_length".to_string(), Value::Long(manifest_length)),
        ("partition_spec_id".to_string(), Value::Int(0)), // Unpartitioned
        ("content".to_string(), Value::Int(0)), // 0 = DATA
        ("sequence_number".to_string(), Value::Long(sequence_number)),
        ("min_sequence_number".to_string(), Value::Long(sequence_number)),
        ("added_snapshot_id".to_string(), Value::Long(snapshot_id)),
        ("added_files_count".to_string(), Value::Int(added_files_count)),
        ("existing_files_count".to_string(), Value::Int(0)),
        ("deleted_files_count".to_string(), Value::Int(0)),
        ("added_rows_count".to_string(), Value::Long(added_rows_count)),
        ("existing_rows_count".to_string(), Value::Long(0)),
        ("deleted_rows_count".to_string(), Value::Long(0)),
        ("partitions".to_string(), Value::Union(0, Box::new(Value::Null))),
        ("key_metadata".to_string(), Value::Union(0, Box::new(Value::Null))),
    ]);

    writer.append(entry)
        .map_err(|e| crate::error::Error::InvalidInput(format!("Failed to append to Avro writer: {}", e)))?;

    let avro_bytes = writer.into_inner()
        .map_err(|e| crate::error::Error::InvalidInput(format!("Failed to finalize Avro writer: {}", e)))?;

    file_io.write(path, avro_bytes).await?;

    Ok(())
}
```

**Step 6: Run test to verify it passes**

Run: `cargo test test_write_manifest_list`
Expected: PASS

**Step 7: Commit**

```bash
git add src/manifest/writer.rs src/manifest/mod.rs tests/manifest_writer_tests.rs
git commit -m "feat: implement manifest list file writing"
```

---

## Phase 5: Snapshot and Metadata Updates

### Task 5.1: Add Snapshot ID Accessor to TableMetadata

**Files:**
- Modify: `src/spec/metadata.rs:79-89`

**Step 1: Write test for current_snapshot_id accessor**

Add to `src/spec/metadata.rs` tests (if test module doesn't exist, create one):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{NestedField, PrimitiveType, Type, Schema};

    #[test]
    fn test_current_snapshot_id() {
        let schema = Schema::builder()
            .with_fields(vec![NestedField::required_field(
                1,
                "id".to_string(),
                Type::Primitive(PrimitiveType::Long),
            )])
            .build()
            .unwrap();

        let metadata = TableMetadata::builder()
            .with_location("s3://test/table")
            .with_current_schema(schema)
            .build()
            .unwrap();

        assert_eq!(metadata.current_snapshot_id(), None);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_current_snapshot_id`
Expected: FAIL - no method `current_snapshot_id` found

**Step 3: Add current_snapshot_id method**

Add to the `impl TableMetadata` block (after line 83):

```rust
/// Get current snapshot ID
pub fn current_snapshot_id(&self) -> Option<i64> {
    self.current_snapshot_id
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test test_current_snapshot_id`
Expected: PASS

**Step 5: Commit**

```bash
git add src/spec/metadata.rs
git commit -m "feat: add current_snapshot_id accessor to TableMetadata"
```

---

### Task 5.2: Implement TableMetadata Clone and Update

**Files:**
- Modify: `src/spec/metadata.rs`

**Step 1: Write test for metadata update with snapshot**

Add to `src/spec/metadata.rs` tests module:

```rust
#[test]
fn test_add_snapshot_to_metadata() {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://test/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let snapshot = crate::spec::Snapshot::builder()
        .with_snapshot_id(1)
        .with_timestamp_ms(1000)
        .with_manifest_list("s3://test/metadata/snap-1.avro")
        .build()
        .unwrap();

    let updated = metadata.add_snapshot(snapshot);

    assert_eq!(updated.current_snapshot_id(), Some(1));
    assert_eq!(updated.snapshots().len(), 1);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_add_snapshot_to_metadata`
Expected: FAIL - no method `add_snapshot` found

**Step 3: Implement add_snapshot method**

Add to the `impl TableMetadata` block:

```rust
/// Create a new TableMetadata with an added snapshot
pub fn add_snapshot(&self, snapshot: Snapshot) -> Self {
    let mut snapshots = self.snapshots.clone();
    snapshots.push(snapshot.clone());

    Self {
        format_version: self.format_version,
        table_uuid: self.table_uuid.clone(),
        location: self.location.clone(),
        last_updated_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        last_column_id: self.last_column_id,
        schemas: self.schemas.clone(),
        current_schema_id: self.current_schema_id,
        snapshots,
        current_snapshot_id: Some(snapshot.snapshot_id()),
        properties: self.properties.clone(),
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test test_add_snapshot_to_metadata`
Expected: PASS

**Step 5: Commit**

```bash
git add src/spec/metadata.rs
git commit -m "feat: add snapshot update method to TableMetadata"
```

---

## Phase 6: Commit Orchestration

### Task 6.1: Add Commit Method Signature to Transaction

**Files:**
- Modify: `src/transaction.rs`

**Step 1: Add async commit method signature**

Add to the `impl<'a> Transaction<'a>` block (after line 49):

```rust
/// Commit the transaction, writing snapshots to the catalog
pub async fn commit(self) -> crate::error::Result<()> {
    todo!("Implement commit")
}
```

**Step 2: Verify compilation**

Run: `cargo check`
Expected: Compiles successfully

**Step 3: Commit**

```bash
git add src/transaction.rs
git commit -m "feat: add commit method signature to Transaction"
```

---

### Task 6.2: Create Commit Implementation Module

**Files:**
- Create: `src/commit/mod.rs`
- Create: `src/commit/orchestrator.rs`
- Modify: `src/lib.rs`

**Step 1: Create commit module directory**

Run: `mkdir -p src/commit`

**Step 2: Create mod.rs**

Create `src/commit/mod.rs`:

```rust
//! Transaction commit orchestration

mod orchestrator;

pub use orchestrator::commit_transaction;
```

**Step 3: Create orchestrator scaffold**

Create `src/commit/orchestrator.rs`:

```rust
//! Orchestrate transaction commit with retries

use crate::error::Result;
use crate::transaction::Transaction;

/// Commit a transaction with automatic retry on concurrent modification
pub async fn commit_transaction(transaction: Transaction<'_>) -> Result<()> {
    todo!("Implement commit orchestration")
}
```

**Step 4: Export commit module from lib.rs**

Add to `src/lib.rs`:

```rust
mod commit;
```

**Step 5: Verify compilation**

Run: `cargo check`
Expected: Compiles successfully

**Step 6: Commit**

```bash
git add src/commit/mod.rs src/commit/orchestrator.rs src/lib.rs
git commit -m "feat: add commit orchestrator module scaffold"
```

---

### Task 6.3: Implement File Path Generation

**Files:**
- Create: `src/commit/paths.rs`
- Modify: `src/commit/mod.rs`

**Step 1: Write tests for path generation**

Create `tests/commit_paths_tests.rs`:

```rust
use icepick::commit::paths::{manifest_path, manifest_list_path, metadata_path};

#[test]
fn test_manifest_path() {
    let uuid = "a1b2c3d4";
    let path = manifest_path("s3://bucket/table", uuid, 0);
    assert_eq!(path, "s3://bucket/table/metadata/a1b2c3d4-m0.avro");
}

#[test]
fn test_manifest_list_path() {
    let uuid = "e5f6g7h8";
    let path = manifest_list_path("s3://bucket/table", 1, uuid);
    assert_eq!(path, "s3://bucket/table/metadata/snap-1-1-e5f6g7h8.avro");
}

#[test]
fn test_metadata_path() {
    let path = metadata_path("s3://bucket/table", 2);
    assert_eq!(path, "s3://bucket/table/metadata/v2.metadata.json");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test commit_paths_tests`
Expected: FAIL - module `paths` not found

**Step 3: Create paths module**

Create `src/commit/paths.rs`:

```rust
//! File path generation for Iceberg metadata files

/// Generate manifest file path: {table}/metadata/{uuid}-m{n}.avro
pub fn manifest_path(table_location: &str, commit_uuid: &str, manifest_num: usize) -> String {
    format!("{}/metadata/{}-m{}.avro", table_location.trim_end_matches('/'), commit_uuid, manifest_num)
}

/// Generate manifest list path: {table}/metadata/snap-{id}-1-{uuid}.avro
pub fn manifest_list_path(table_location: &str, snapshot_id: i64, commit_uuid: &str) -> String {
    format!(
        "{}/metadata/snap-{}-1-{}.avro",
        table_location.trim_end_matches('/'),
        snapshot_id,
        commit_uuid
    )
}

/// Generate metadata file path: {table}/metadata/v{n}.metadata.json
pub fn metadata_path(table_location: &str, version: usize) -> String {
    format!("{}/metadata/v{}.metadata.json", table_location.trim_end_matches('/'), version)
}
```

**Step 4: Export from mod.rs**

Add to `src/commit/mod.rs`:

```rust
pub mod paths;
```

**Step 5: Run test to verify it passes**

Run: `cargo test commit_paths_tests`
Expected: PASS

**Step 6: Commit**

```bash
git add src/commit/paths.rs src/commit/mod.rs tests/commit_paths_tests.rs
git commit -m "feat: implement PyIceberg-compatible path generation"
```

---

### Task 6.4: Implement try_commit (Single Attempt)

**Files:**
- Modify: `src/commit/orchestrator.rs`
- Create: `tests/commit_orchestrator_tests.rs`

**Step 1: Write test for single commit attempt**

Create `tests/commit_orchestrator_tests.rs`:

```rust
use icepick::io::FileIO;
use icepick::spec::{DataFile, NamespaceIdent, NestedField, PrimitiveType, Schema, TableIdent, TableMetadata, Type};
use icepick::table::Table;
use opendal::Operator;

#[tokio::test]
async fn test_try_commit_first_snapshot() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://bucket/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["default".to_string()]),
        "test".to_string(),
    );

    let table = Table::new(ident, metadata, "s3://bucket/metadata/v0.metadata.json".to_string(), file_io.clone());

    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .build()
        .unwrap();

    let transaction = table.transaction().append(vec![data_file]);

    // This will fail until we implement try_commit
    let result = icepick::commit::orchestrator::try_commit(&transaction).await;
    assert!(result.is_ok());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_try_commit_first_snapshot`
Expected: FAIL - function `try_commit` not found or panics

**Step 3: Implement try_commit**

Replace `src/commit/orchestrator.rs` with:

```rust
//! Orchestrate transaction commit with retries

use crate::error::{Error, Result};
use crate::manifest::writer::{write_manifest, write_manifest_list};
use crate::commit::paths::{manifest_path, manifest_list_path, metadata_path};
use crate::spec::{Snapshot, Summary, TransactionOperation};
use crate::transaction::Transaction;
use uuid::Uuid;

/// Try to commit once (no retries)
pub async fn try_commit(transaction: &Transaction<'_>) -> Result<()> {
    let table = transaction.table();
    let metadata = table.metadata();
    let file_io = table.file_io();

    // Generate IDs
    let snapshot_id = metadata
        .current_snapshot_id()
        .map(|id| id + 1)
        .unwrap_or(1);
    let sequence_number = snapshot_id;
    let commit_uuid = Uuid::new_v4().to_string().replace('-', "");

    // Extract data files from operations
    let mut all_data_files = Vec::new();
    for op in transaction.operations() {
        if let TransactionOperation::Append(files) = op {
            all_data_files.extend(files.clone());
        }
    }

    if all_data_files.is_empty() {
        return Err(Error::InvalidInput("No data files to commit".to_string()));
    }

    // 1. Write manifest file
    let manifest_file_path = manifest_path(table.location(), &commit_uuid, 0);
    let manifest_bytes = write_manifest(
        file_io,
        &manifest_file_path,
        &all_data_files,
        snapshot_id,
        sequence_number,
    )
    .await?;

    // 2. Write manifest list
    let manifest_list_file_path = manifest_list_path(table.location(), snapshot_id, &commit_uuid);
    let added_files_count = all_data_files.len() as i32;
    let added_rows_count: i64 = all_data_files.iter().map(|f| f.record_count()).sum();

    write_manifest_list(
        file_io,
        &manifest_list_file_path,
        &manifest_file_path,
        manifest_bytes,
        snapshot_id,
        sequence_number,
        added_files_count,
        added_rows_count,
    )
    .await?;

    // 3. Create snapshot
    let summary = Summary::builder()
        .set("operation", "append")
        .set("added-data-files", &added_files_count.to_string())
        .set("added-records", &added_rows_count.to_string())
        .set("total-data-files", &added_files_count.to_string())
        .set("total-records", &added_rows_count.to_string())
        .build();

    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(metadata.current_snapshot_id().unwrap_or(0))
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        )
        .with_manifest_list(&manifest_list_file_path)
        .with_summary(summary)
        .with_schema_id(metadata.current_schema().schema_id())
        .build()?;

    // 4. Update metadata
    let new_metadata = metadata.add_snapshot(snapshot);

    // 5. Write new metadata file
    let new_version = metadata.snapshots().len() + 1;
    let new_metadata_path = metadata_path(table.location(), new_version);
    let metadata_json = serde_json::to_vec_pretty(&new_metadata)?;
    file_io.write(&new_metadata_path, metadata_json).await?;

    // TODO: Update catalog pointer (Phase 5)

    Ok(())
}

/// Commit a transaction with automatic retry on concurrent modification
pub async fn commit_transaction(transaction: Transaction<'_>) -> Result<()> {
    try_commit(&transaction).await
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test test_try_commit_first_snapshot`
Expected: PASS

**Step 5: Commit**

```bash
git add src/commit/orchestrator.rs tests/commit_orchestrator_tests.rs
git commit -m "feat: implement try_commit for single commit attempt"
```

---

### Task 6.5: Wire Commit to Transaction

**Files:**
- Modify: `src/transaction.rs`

**Step 1: Implement Transaction::commit**

Replace the `todo!()` in the `commit` method with:

```rust
crate::commit::orchestrator::commit_transaction(self).await
```

**Step 2: Write integration test**

Add to `tests/commit_orchestrator_tests.rs`:

```rust
#[tokio::test]
async fn test_transaction_commit_integration() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("memory://table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["default".to_string()]),
        "test".to_string(),
    );

    let table = Table::new(ident, metadata, "memory://metadata/v0.metadata.json".to_string(), file_io);

    let data_file = DataFile::builder()
        .with_file_path("memory://data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .build()
        .unwrap();

    // Test the full Transaction API
    table
        .transaction()
        .append(vec![data_file])
        .commit()
        .await
        .unwrap();

    // Verify files were written
    assert!(op.exists("memory://table/metadata/v1.metadata.json").await.unwrap());
}
```

**Step 3: Run test to verify it passes**

Run: `cargo test test_transaction_commit_integration`
Expected: PASS

**Step 4: Commit**

```bash
git add src/transaction.rs tests/commit_orchestrator_tests.rs
git commit -m "feat: wire commit orchestrator to Transaction::commit"
```

---

## Phase 7: Catalog Integration

### Task 7.1: Add update_table_metadata to Catalog Trait

**Files:**
- Modify: `src/catalog/catalog_trait.rs`

**Step 1: Write test expectation for catalog update**

Add to `src/catalog/catalog_trait.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // This is a documentation test showing expected usage
    // Actual implementation will be tested per-catalog
    #[test]
    fn test_catalog_trait_has_update_method() {
        // Verify trait compiles with new method
        fn _assert_catalog_has_update<C: Catalog>() {}
    }
}
```

**Step 2: Add method to trait**

Add to the `Catalog` trait (after line 30):

```rust
/// Update table metadata atomically
///
/// Returns ConcurrentModification error if expected_metadata_location doesn't match current
async fn update_table_metadata(
    &self,
    identifier: &TableIdent,
    new_metadata_location: String,
    expected_metadata_location: String,
) -> Result<()>;
```

**Step 3: Verify compilation**

Run: `cargo check`
Expected: FAIL - trait implementations missing the new method

**Step 4: Commit trait change**

```bash
git add src/catalog/catalog_trait.rs
git commit -m "feat: add update_table_metadata to Catalog trait"
```

---

### Task 7.2: Implement Stub Catalog Implementations

**Files:**
- Modify: `src/catalog/r2.rs`
- Modify: `src/catalog/s3_tables.rs`
- Modify: `src/catalog/rest/catalog_impl.rs`

**Step 1: Add stub to R2Catalog**

Find the `impl Catalog for R2Catalog` block in `src/catalog/r2.rs` and add:

```rust
async fn update_table_metadata(
    &self,
    _identifier: &TableIdent,
    _new_metadata_location: String,
    _expected_metadata_location: String,
) -> Result<()> {
    todo!("Implement R2Catalog::update_table_metadata")
}
```

**Step 2: Add stub to S3TablesCatalog**

Find the `impl Catalog for S3TablesCatalog` in `src/catalog/s3_tables.rs` and add:

```rust
async fn update_table_metadata(
    &self,
    _identifier: &TableIdent,
    _new_metadata_location: String,
    _expected_metadata_location: String,
) -> Result<()> {
    todo!("Implement S3TablesCatalog::update_table_metadata")
}
```

**Step 3: Add stub to RestCatalog**

Find the `impl Catalog for RestCatalog` in `src/catalog/rest/catalog_impl.rs` and add:

```rust
async fn update_table_metadata(
    &self,
    _identifier: &TableIdent,
    _new_metadata_location: String,
    _expected_metadata_location: String,
) -> Result<()> {
    todo!("Implement RestCatalog::update_table_metadata")
}
```

**Step 4: Verify compilation**

Run: `cargo check`
Expected: PASS

**Step 5: Commit**

```bash
git add src/catalog/r2.rs src/catalog/s3_tables.rs src/catalog/rest/catalog_impl.rs
git commit -m "feat: add stub update_table_metadata implementations"
```

---

## Phase 8: Retry Logic

### Task 8.1: Implement Commit with Retry

**Files:**
- Modify: `src/commit/orchestrator.rs`
- Create: `tests/commit_retry_tests.rs`

**Step 1: Write test for retry behavior**

Create `tests/commit_retry_tests.rs`:

```rust
// Note: This test will be a simulation since we can't easily trigger concurrent modifications
// in a single-threaded test. The test verifies the retry logic structure exists.

use icepick::error::Error;

#[test]
fn test_is_concurrent_modification_error() {
    let err = Error::concurrent_modification("version mismatch");
    assert!(matches!(err, Error::ConcurrentModification { .. }));
}
```

**Step 2: Run test**

Run: `cargo test test_is_concurrent_modification_error`
Expected: PASS (already implemented in Phase 1)

**Step 3: Implement retry logic in commit_transaction**

Update `commit_transaction` in `src/commit/orchestrator.rs`:

```rust
/// Commit a transaction with automatic retry on concurrent modification
pub async fn commit_transaction(transaction: Transaction<'_>) -> Result<()> {
    const MAX_RETRIES: u32 = 3;

    for attempt in 0..MAX_RETRIES {
        match try_commit(&transaction).await {
            Ok(()) => return Ok(()),
            Err(e) if matches!(e, Error::ConcurrentModification { .. }) => {
                if attempt == MAX_RETRIES - 1 {
                    return Err(Error::InvalidInput(format!(
                        "Max retries ({}) exceeded for commit",
                        MAX_RETRIES
                    )));
                }
                // Exponential backoff: 100ms, 200ms, 400ms
                #[cfg(not(target_family = "wasm"))]
                {
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        100 * 2_u64.pow(attempt),
                    ))
                    .await;
                }
                #[cfg(target_family = "wasm")]
                {
                    // WASM doesn't support sleep, just retry immediately
                    // In real WASM environment, would use JS setTimeout
                }
                continue;
            }
            Err(e) => return Err(e),
        }
    }

    unreachable!("Loop should return before reaching here")
}
```

**Step 4: Verify compilation**

Run: `cargo check`
Expected: PASS

**Step 5: Commit**

```bash
git add src/commit/orchestrator.rs tests/commit_retry_tests.rs
git commit -m "feat: implement exponential backoff retry for concurrent modifications"
```

---

## Phase 9: Integration Testing

### Task 9.1: End-to-End Commit Test

**Files:**
- Create: `tests/integration_commit_test.rs`

**Step 1: Write comprehensive integration test**

Create `tests/integration_commit_test.rs`:

```rust
use icepick::io::FileIO;
use icepick::spec::{DataFile, NamespaceIdent, NestedField, PrimitiveType, Schema, TableIdent, TableMetadata, Type};
use icepick::table::Table;
use opendal::Operator;
use std::collections::HashMap;

#[tokio::test]
async fn test_end_to_end_commit_with_stats() {
    // Setup
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required_field(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
            NestedField::optional_field(2, "name".to_string(), Type::Primitive(PrimitiveType::String)),
        ])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("memory://warehouse/db/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["db".to_string()]),
        "table".to_string(),
    );

    let table = Table::new(
        ident,
        metadata,
        "memory://warehouse/db/table/metadata/v0.metadata.json".to_string(),
        file_io.clone(),
    );

    // Create data file with stats
    let mut value_counts = HashMap::new();
    value_counts.insert(1, 1000);
    value_counts.insert(2, 950);

    let mut null_counts = HashMap::new();
    null_counts.insert(2, 50);

    let data_file = DataFile::builder()
        .with_file_path("memory://warehouse/db/table/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(1000)
        .with_file_size_in_bytes(50_000)
        .with_value_counts(value_counts)
        .with_null_value_counts(null_counts)
        .build()
        .unwrap();

    // Commit
    table
        .transaction()
        .append(vec![data_file])
        .commit()
        .await
        .unwrap();

    // Verify files exist
    let manifest_exists = op
        .list("memory://warehouse/db/table/metadata/")
        .await
        .unwrap()
        .into_iter()
        .any(|entry| entry.path().contains("-m0.avro"));
    assert!(manifest_exists, "Manifest file should exist");

    let manifest_list_exists = op
        .list("memory://warehouse/db/table/metadata/")
        .await
        .unwrap()
        .into_iter()
        .any(|entry| entry.path().contains("snap-1-1-"));
    assert!(manifest_list_exists, "Manifest list should exist");

    let metadata_exists = op
        .exists("memory://warehouse/db/table/metadata/v1.metadata.json")
        .await
        .unwrap();
    assert!(metadata_exists, "New metadata file should exist");

    // Read and verify metadata
    let metadata_bytes = op
        .read("memory://warehouse/db/table/metadata/v1.metadata.json")
        .await
        .unwrap();
    let new_metadata: TableMetadata =
        serde_json::from_slice(&metadata_bytes).unwrap();

    assert_eq!(new_metadata.current_snapshot_id(), Some(1));
    assert_eq!(new_metadata.snapshots().len(), 1);

    let snapshot = new_metadata.current_snapshot().unwrap();
    assert_eq!(snapshot.summary().get("operation"), Some(&"append".to_string()));
    assert_eq!(snapshot.summary().get("added-data-files"), Some(&"1".to_string()));
    assert_eq!(snapshot.summary().get("added-records"), Some(&"1000".to_string()));
}
```

**Step 2: Run test**

Run: `cargo test test_end_to_end_commit_with_stats`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/integration_commit_test.rs
git commit -m "test: add comprehensive end-to-end commit integration test"
```

---

### Task 9.2: Multiple Commits Test

**Files:**
- Modify: `tests/integration_commit_test.rs`

**Step 1: Write test for sequential commits**

Add to `tests/integration_commit_test.rs`:

```rust
#[tokio::test]
async fn test_multiple_sequential_commits() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op.clone());

    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("memory://warehouse/test/multi")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["test".to_string()]),
        "multi".to_string(),
    );

    let mut table = Table::new(
        ident.clone(),
        metadata,
        "memory://warehouse/test/multi/metadata/v0.metadata.json".to_string(),
        file_io.clone(),
    );

    // First commit
    let data_file1 = DataFile::builder()
        .with_file_path("memory://warehouse/test/multi/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .build()
        .unwrap();

    table.transaction().append(vec![data_file1]).commit().await.unwrap();

    // Read updated metadata for second commit
    let metadata_bytes = op
        .read("memory://warehouse/test/multi/metadata/v1.metadata.json")
        .await
        .unwrap();
    let updated_metadata: TableMetadata = serde_json::from_slice(&metadata_bytes).unwrap();

    table = Table::new(
        ident,
        updated_metadata,
        "memory://warehouse/test/multi/metadata/v1.metadata.json".to_string(),
        file_io.clone(),
    );

    // Second commit
    let data_file2 = DataFile::builder()
        .with_file_path("memory://warehouse/test/multi/data/file2.parquet")
        .with_file_format("PARQUET")
        .with_record_count(200)
        .with_file_size_in_bytes(10000)
        .build()
        .unwrap();

    table.transaction().append(vec![data_file2]).commit().await.unwrap();

    // Verify
    let final_metadata_bytes = op
        .read("memory://warehouse/test/multi/metadata/v2.metadata.json")
        .await
        .unwrap();
    let final_metadata: TableMetadata = serde_json::from_slice(&final_metadata_bytes).unwrap();

    assert_eq!(final_metadata.current_snapshot_id(), Some(2));
    assert_eq!(final_metadata.snapshots().len(), 2);
}
```

**Step 2: Run test**

Run: `cargo test test_multiple_sequential_commits`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/integration_commit_test.rs
git commit -m "test: add multiple sequential commits test"
```

---

## Phase 10: Documentation and Verification

### Task 10.1: Add Module Documentation

**Files:**
- Modify: `src/commit/mod.rs`
- Modify: `src/manifest/mod.rs`

**Step 1: Add comprehensive module docs**

Update `src/commit/mod.rs`:

```rust
//! Transaction commit orchestration
//!
//! This module implements the commit workflow for Iceberg tables:
//! 1. Write manifest files (Avro) containing data file metadata
//! 2. Write manifest list (Avro) referencing manifests
//! 3. Create snapshot with summary statistics
//! 4. Write new table metadata JSON
//! 5. Update catalog pointer atomically
//!
//! Follows PyIceberg file naming conventions and Iceberg v2 format.

mod orchestrator;
pub mod paths;

pub use orchestrator::{commit_transaction, try_commit};
```

Update `src/manifest/mod.rs`:

```rust
//! Iceberg manifest file handling
//!
//! Manifests are Avro files that track data files in an Iceberg table.
//! Each snapshot has a manifest list (Avro) that references one or more
//! manifest files (Avro), which contain data file metadata.
//!
//! This module provides:
//! - Avro schema definitions for v2 format
//! - Conversion from Iceberg types to Avro values
//! - Writers for manifest and manifest list files

pub mod avro;
pub mod schema;
pub mod writer;

pub use avro::data_file_to_avro;
pub use schema::{manifest_entry_schema_v2, manifest_list_schema_v2};
pub use writer::{write_manifest, write_manifest_list};
```

**Step 2: Verify docs build**

Run: `cargo doc --no-deps`
Expected: Generates documentation successfully

**Step 3: Commit**

```bash
git add src/commit/mod.rs src/manifest/mod.rs
git commit -m "docs: add comprehensive module documentation"
```

---

### Task 10.2: Verify WASM Compilation

**Files:**
- None (verification step)

**Step 1: Check WASM compilation**

Run: `cargo check --target wasm32-unknown-unknown`
Expected: Compiles successfully with no errors

**Step 2: Build release WASM**

Run: `cargo build --target wasm32-unknown-unknown --release`
Expected: Builds successfully

**Step 3: Verify no std library usage in critical paths**

Run: `cargo tree --target wasm32-unknown-unknown | grep -i "std"`
Expected: Only expected std usage from dependencies

**Step 4: Document verification**

Add note to commit message about WASM compatibility verification

**Step 5: Commit**

```bash
git commit --allow-empty -m "verify: confirm wasm32-unknown-unknown compilation success"
```

---

### Task 10.3: Run Full Test Suite

**Files:**
- None (verification step)

**Step 1: Run all tests**

Run: `cargo test --all-targets`
Expected: All tests pass

**Step 2: Run tests with release mode**

Run: `cargo test --release`
Expected: All tests pass

**Step 3: Check for warnings**

Run: `cargo clippy -- -D warnings`
Expected: No clippy warnings

**Step 4: Format check**

Run: `cargo fmt -- --check`
Expected: All code is properly formatted

**Step 5: Commit any fixes**

```bash
git add .
git commit -m "chore: fix clippy warnings and formatting"
```

---

## Success Criteria Checklist

After completing all tasks, verify:

- [ ] Users can call `transaction.commit().await`
- [ ] Manifests are valid Iceberg v2 Avro files
- [ ] Manifest lists are valid Iceberg v2 Avro files
- [ ] Metadata JSON follows Iceberg spec
- [ ] Retry logic handles concurrent modifications (structure in place)
- [ ] All code compiles to wasm32-unknown-unknown
- [ ] Integration tests pass with memory storage
- [ ] File naming matches PyIceberg conventions
- [ ] Column statistics are included when provided
- [ ] Snapshot IDs increment correctly

## Future Work (Not in This Plan)

**Catalog Implementation:**
- Task: Implement R2Catalog::update_table_metadata with version-hint.txt pattern
- Task: Implement S3TablesCatalog::update_table_metadata with API
- Task: Implement RestCatalog::update_table_metadata

**Concurrent Writers:**
- Task: Implement concurrent commit test with multiple async tasks
- Task: Verify all commits succeed with retries

**Advanced Features:**
- Partitioned table support
- Delete file support
- Schema evolution
- Garbage collection

## Related Skills

- @superpowers:test-driven-development - Used throughout for TDD workflow
- @superpowers:verification-before-completion - Use before claiming tasks complete
- @superpowers:systematic-debugging - Use if tests fail unexpectedly

## References

- Design document: `docs/plans/2025-11-16-transaction-commit-design.md`
- Iceberg spec: https://iceberg.apache.org/spec/
- PyIceberg file naming: https://tomtan.dev/blog/2025-01-12-iceberg-file-name-convention/
- Apache Avro Rust: https://docs.rs/apache-avro
