# WASM-Compatible Icepick Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a WASM-compatible Iceberg catalog library by vendoring core types from iceberg-rust and implementing a simplified API with OpenDAL-based storage.

**Architecture:** Minimal vendoring approach - copy essential types from iceberg-rust v0.7.0 (identifiers, schema, metadata, data files) and build new Catalog/Table/Transaction APIs around OpenDAL for WASM-compatible I/O.

**Tech Stack:** Rust, OpenDAL (WASM-compatible storage), Arrow, Parquet, serde, async-trait, wasm-bindgen-futures

---

## Phase 1: Core Identifiers & Catalog Trait

### Task 1: Vendor NamespaceIdent

**Files:**
- Create: `icepick/src/spec/mod.rs`
- Create: `icepick/src/spec/identifier.rs`

**Step 1: Write test for NamespaceIdent**

Create: `icepick/tests/test_identifiers.rs`

```rust
use icepick::spec::NamespaceIdent;

#[test]
fn test_namespace_ident_single_level() {
    let ns = NamespaceIdent::new(vec!["default".to_string()]);
    assert_eq!(ns.as_ref(), &["default"]);
    assert_eq!(ns.to_string(), "default");
}

#[test]
fn test_namespace_ident_multi_level() {
    let ns = NamespaceIdent::new(vec!["catalog".to_string(), "db".to_string()]);
    assert_eq!(ns.as_ref(), &["catalog", "db"]);
    assert_eq!(ns.to_string(), "catalog.db");
}

#[test]
fn test_namespace_ident_empty_fails() {
    let result = std::panic::catch_unwind(|| {
        NamespaceIdent::new(vec![]);
    });
    assert!(result.is_err());
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_namespace_ident --lib
```

Expected: Compilation error - module `spec` not found

**Step 3: Vendor NamespaceIdent from iceberg-rust**

Create: `icepick/src/spec/mod.rs`

```rust
//! Iceberg specification types
//!
//! Vendored from iceberg-rust v0.7.0
//! Copyright 2024 Apache Software Foundation
//! Licensed under Apache License 2.0

pub mod identifier;

pub use identifier::{NamespaceIdent, TableIdent};
```

Create: `icepick/src/spec/identifier.rs`

```rust
//! Namespace and table identifiers
//! Vendored from iceberg-rust v0.7.0

use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

/// Identifier for a namespace (database)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceIdent(Vec<String>);

impl NamespaceIdent {
    /// Create a new namespace identifier
    pub fn new(parts: Vec<String>) -> Self {
        assert!(!parts.is_empty(), "Namespace cannot be empty");
        Self(parts)
    }

    /// Create from a slice of strings
    pub fn from_strs(parts: &[&str]) -> Self {
        Self::new(parts.iter().map(|s| s.to_string()).collect())
    }

    /// Get namespace parts as a slice
    pub fn as_slice(&self) -> &[String] {
        &self.0
    }

    /// Convert to Vec
    pub fn into_vec(self) -> Vec<String> {
        self.0
    }
}

impl Deref for NamespaceIdent {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for NamespaceIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.join("."))
    }
}

impl From<Vec<String>> for NamespaceIdent {
    fn from(parts: Vec<String>) -> Self {
        Self::new(parts)
    }
}

impl<'a> From<&'a [&'a str]> for NamespaceIdent {
    fn from(parts: &'a [&'a str]) -> Self {
        Self::from_strs(parts)
    }
}
```

**Step 4: Update lib.rs to expose spec module**

Modify: `icepick/src/lib.rs`

```rust
pub mod spec;
pub mod catalog;
pub mod error;

pub use error::{Error, Result};
```

**Step 5: Run tests to verify they pass**

```bash
cargo test test_namespace_ident --lib
```

Expected: All 3 tests pass

**Step 6: Commit**

```bash
git add icepick/src/spec/ icepick/tests/test_identifiers.rs icepick/src/lib.rs
git commit -m "feat: vendor NamespaceIdent from iceberg-rust v0.7.0"
```

---

### Task 2: Vendor TableIdent

**Files:**
- Modify: `icepick/src/spec/identifier.rs`
- Modify: `icepick/tests/test_identifiers.rs`

**Step 1: Write test for TableIdent**

Modify: `icepick/tests/test_identifiers.rs`

```rust
use icepick::spec::{NamespaceIdent, TableIdent};

// ... existing NamespaceIdent tests ...

#[test]
fn test_table_ident_creation() {
    let ns = NamespaceIdent::new(vec!["default".to_string()]);
    let table = TableIdent::new(ns.clone(), "users".to_string());

    assert_eq!(table.namespace(), &ns);
    assert_eq!(table.name(), "users");
    assert_eq!(table.to_string(), "default.users");
}

#[test]
fn test_table_ident_multi_level_namespace() {
    let ns = NamespaceIdent::new(vec!["catalog".to_string(), "db".to_string()]);
    let table = TableIdent::new(ns.clone(), "events".to_string());

    assert_eq!(table.to_string(), "catalog.db.events");
}

#[test]
fn test_table_ident_from_strs() {
    let table = TableIdent::from_strs(&["default"], "users");
    assert_eq!(table.to_string(), "default.users");
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_table_ident --lib
```

Expected: Compilation error - `TableIdent` not found

**Step 3: Add TableIdent implementation**

Modify: `icepick/src/spec/identifier.rs` (append to file)

```rust
/// Identifier for a table
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableIdent {
    namespace: NamespaceIdent,
    name: String,
}

impl TableIdent {
    /// Create a new table identifier
    pub fn new(namespace: NamespaceIdent, name: String) -> Self {
        assert!(!name.is_empty(), "Table name cannot be empty");
        Self { namespace, name }
    }

    /// Create from string slices
    pub fn from_strs(namespace: &[&str], name: &str) -> Self {
        Self::new(NamespaceIdent::from_strs(namespace), name.to_string())
    }

    /// Get the namespace
    pub fn namespace(&self) -> &NamespaceIdent {
        &self.namespace
    }

    /// Get the table name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Consume and return namespace and name
    pub fn into_parts(self) -> (NamespaceIdent, String) {
        (self.namespace, self.name)
    }
}

impl fmt::Display for TableIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}
```

**Step 4: Run tests to verify they pass**

```bash
cargo test test_table_ident --lib
```

Expected: All 3 new tests pass

**Step 5: Commit**

```bash
git add icepick/src/spec/identifier.rs icepick/tests/test_identifiers.rs
git commit -m "feat: vendor TableIdent from iceberg-rust v0.7.0"
```

---

### Task 3: Define New Catalog Trait

**Files:**
- Create: `icepick/src/catalog/trait.rs`
- Modify: `icepick/src/catalog/mod.rs`

**Step 1: Write catalog trait (no tests yet, just API definition)**

Create: `icepick/src/catalog/trait.rs`

```rust
//! Core Catalog trait for Iceberg catalogs
//! New design - simpler than iceberg-rust

use async_trait::async_trait;
use std::collections::HashMap;

use crate::spec::{NamespaceIdent, TableIdent};
use crate::error::Result;

/// Core catalog operations for Iceberg tables
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Create a namespace (idempotent - returns Ok if already exists)
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()>;

    /// Check if a namespace exists
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool>;

    /// List all tables in a namespace
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>>;

    /// Check if a table exists
    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool>;

    /// Delete a table
    async fn drop_table(&self, identifier: &TableIdent) -> Result<()>;
}
```

**Step 2: Update catalog/mod.rs**

Modify: `icepick/src/catalog/mod.rs`

```rust
//! Catalog implementations for Iceberg tables

mod r2;
mod s3_tables;
pub mod rest;

// New trait-based API
mod trait_impl;

pub use trait_impl::Catalog;
pub use r2::R2Catalog;
pub use s3_tables::S3TablesCatalog;
```

Wait, I need to rename the file correctly:

Modify: `icepick/src/catalog/mod.rs`

```rust
//! Catalog implementations for Iceberg tables

pub mod r2;
pub mod s3_tables;
pub mod rest;

// New trait-based API
mod catalog_trait;

pub use catalog_trait::Catalog;
pub use r2::R2Catalog;
pub use s3_tables::S3TablesCatalog;
```

And rename the file:

Create: `icepick/src/catalog/catalog_trait.rs` (instead of trait.rs)

```rust
//! Core Catalog trait for Iceberg catalogs
//! New design - simpler than iceberg-rust

use async_trait::async_trait;
use std::collections::HashMap;

use crate::spec::{NamespaceIdent, TableIdent};
use crate::error::Result;

/// Core catalog operations for Iceberg tables
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Create a namespace (idempotent - returns Ok if already exists)
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()>;

    /// Check if a namespace exists
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool>;

    /// List all tables in a namespace
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>>;

    /// Check if a table exists
    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool>;

    /// Delete a table
    async fn drop_table(&self, identifier: &TableIdent) -> Result<()>;
}
```

**Step 3: Verify compilation**

```bash
cargo check
```

Expected: Success (trait compiles, existing catalog code still works)

**Step 4: Commit**

```bash
git add icepick/src/catalog/catalog_trait.rs icepick/src/catalog/mod.rs
git commit -m "feat: define new simplified Catalog trait"
```

---

### Task 4: Verify WASM Compilation (Phase 1 Checkpoint)

**Files:**
- None (verification only)

**Step 1: Check current dependencies support WASM**

```bash
cargo tree --target wasm32-unknown-unknown -e normal
```

Expected: Should show dependency tree (or fail if rust target not installed)

**Step 2: Install WASM target if needed**

```bash
rustup target add wasm32-unknown-unknown
```

**Step 3: Try WASM build**

```bash
cargo build --target wasm32-unknown-unknown --lib
```

Expected: May fail due to tokio/async dependencies in existing code - that's OK, we'll fix incrementally

**Step 4: Document current WASM status**

Create: `docs/wasm-status.md`

```markdown
# WASM Compilation Status

**Last Updated:** 2025-11-16
**Phase:** 1 - Core Identifiers

## Current Status

- ✅ Core types (NamespaceIdent, TableIdent) are WASM-compatible
- ✅ Catalog trait is WASM-compatible (async-trait works)
- ⏳ Full catalog implementations pending (have tokio dependencies)

## Known Blockers

1. R2Catalog/S3TablesCatalog use `iceberg-rust` with tokio
2. Need to implement WASM-compatible FileIO (Phase 4)

## Next Steps

- Complete Phase 2 (Schema types)
- These are pure data structures, should be WASM-compatible
```

**Step 5: Commit**

```bash
git add docs/wasm-status.md
git commit -m "docs: add WASM compilation status tracking"
```

---

## Phase 2: Schema Support

### Task 5: Vendor PrimitiveType

**Files:**
- Create: `icepick/src/spec/types.rs`
- Modify: `icepick/src/spec/mod.rs`
- Create: `icepick/tests/test_types.rs`

**Step 1: Write test for PrimitiveType**

Create: `icepick/tests/test_types.rs`

```rust
use icepick::spec::types::{PrimitiveType, Type};

#[test]
fn test_primitive_type_boolean() {
    let t = Type::Primitive(PrimitiveType::Boolean);
    assert!(matches!(t, Type::Primitive(PrimitiveType::Boolean)));
}

#[test]
fn test_primitive_type_integer() {
    let t = Type::Primitive(PrimitiveType::Int);
    assert!(matches!(t, Type::Primitive(PrimitiveType::Int)));
}

#[test]
fn test_primitive_type_long() {
    let t = Type::Primitive(PrimitiveType::Long);
    assert!(matches!(t, Type::Primitive(PrimitiveType::Long)));
}

#[test]
fn test_primitive_type_string() {
    let t = Type::Primitive(PrimitiveType::String);
    assert!(matches!(t, Type::Primitive(PrimitiveType::String)));
}

#[test]
fn test_primitive_type_binary() {
    let t = Type::Primitive(PrimitiveType::Binary);
    assert!(matches!(t, Type::Primitive(PrimitiveType::Binary)));
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_primitive_type --lib
```

Expected: Compilation error - module `types` not found

**Step 3: Vendor PrimitiveType enum**

Create: `icepick/src/spec/types.rs`

```rust
//! Iceberg data types
//! Vendored from iceberg-rust v0.7.0

use serde::{Deserialize, Serialize};

/// Primitive data types in Iceberg
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PrimitiveType {
    /// True or false
    Boolean,
    /// 32-bit signed integer
    Int,
    /// 64-bit signed integer
    Long,
    /// 32-bit IEEE 754 floating point
    Float,
    /// 64-bit IEEE 754 floating point
    Double,
    /// Fixed-point decimal
    Decimal {
        /// Precision (total number of digits)
        precision: u32,
        /// Scale (digits after decimal point)
        scale: u32,
    },
    /// Calendar date without timezone
    Date,
    /// Time of day without timezone (microsecond precision)
    Time,
    /// Timestamp without timezone (microsecond precision)
    Timestamp,
    /// Timestamp with timezone (microsecond precision)
    Timestamptz,
    /// Variable-length string
    String,
    /// UUID (16 bytes)
    Uuid,
    /// Fixed-length byte array
    Fixed(u64),
    /// Variable-length byte array
    Binary,
}

/// Iceberg type - either primitive or nested
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Type {
    /// Primitive type
    Primitive(PrimitiveType),
    /// Struct type (to be implemented)
    Struct(StructType),
    /// List type (to be implemented)
    List(ListType),
    /// Map type (to be implemented)
    Map(MapType),
}

/// Placeholder for struct type (will implement in next task)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructType {
    fields: Vec<NestedField>,
}

/// Placeholder for list type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListType {
    element_id: i32,
    element_required: bool,
    element_type: Box<Type>,
}

/// Placeholder for map type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapType {
    key_id: i32,
    key_type: Box<Type>,
    value_id: i32,
    value_required: bool,
    value_type: Box<Type>,
}

/// Placeholder for nested field (will implement in next task)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NestedField {
    id: i32,
    name: String,
    required: bool,
    field_type: Type,
    doc: Option<String>,
}
```

**Step 4: Update spec/mod.rs**

Modify: `icepick/src/spec/mod.rs`

```rust
//! Iceberg specification types
//!
//! Vendored from iceberg-rust v0.7.0
//! Copyright 2024 Apache Software Foundation
//! Licensed under Apache License 2.0

pub mod identifier;
pub mod types;

pub use identifier::{NamespaceIdent, TableIdent};
pub use types::{Type, PrimitiveType, StructType, NestedField};
```

**Step 5: Run tests to verify they pass**

```bash
cargo test test_primitive_type --lib
```

Expected: All tests pass

**Step 6: Commit**

```bash
git add icepick/src/spec/types.rs icepick/src/spec/mod.rs icepick/tests/test_types.rs
git commit -m "feat: vendor PrimitiveType and Type enum from iceberg-rust"
```

---

### Task 6: Implement NestedField

**Files:**
- Modify: `icepick/src/spec/types.rs`
- Modify: `icepick/tests/test_types.rs`

**Step 1: Write test for NestedField**

Modify: `icepick/tests/test_types.rs`

```rust
use icepick::spec::types::{PrimitiveType, Type, NestedField};

// ... existing primitive type tests ...

#[test]
fn test_nested_field_required() {
    let field = NestedField::new(
        1,
        "id".to_string(),
        Type::Primitive(PrimitiveType::Long),
        true,
        None,
    );

    assert_eq!(field.id(), 1);
    assert_eq!(field.name(), "id");
    assert_eq!(field.required(), true);
    assert!(matches!(field.field_type(), Type::Primitive(PrimitiveType::Long)));
}

#[test]
fn test_nested_field_optional_with_doc() {
    let field = NestedField::new(
        2,
        "email".to_string(),
        Type::Primitive(PrimitiveType::String),
        false,
        Some("User email address".to_string()),
    );

    assert_eq!(field.id(), 2);
    assert_eq!(field.required(), false);
    assert_eq!(field.doc(), Some("User email address"));
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_nested_field --lib
```

Expected: Compilation error - `NestedField::new` not found, accessor methods missing

**Step 3: Implement NestedField properly**

Modify: `icepick/src/spec/types.rs` - replace the placeholder `NestedField`:

```rust
/// A field in a struct type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NestedField {
    id: i32,
    name: String,
    required: bool,
    #[serde(rename = "type")]
    field_type: Type,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc: Option<String>,
}

impl NestedField {
    /// Create a new nested field
    pub fn new(
        id: i32,
        name: String,
        field_type: Type,
        required: bool,
        doc: Option<String>,
    ) -> Self {
        Self {
            id,
            name,
            required,
            field_type,
            doc,
        }
    }

    /// Create a required field
    pub fn required(id: i32, name: String, field_type: Type) -> Self {
        Self::new(id, name, field_type, true, None)
    }

    /// Create an optional field
    pub fn optional(id: i32, name: String, field_type: Type) -> Self {
        Self::new(id, name, field_type, false, None)
    }

    /// Get field ID
    pub fn id(&self) -> i32 {
        self.id
    }

    /// Get field name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Check if field is required
    pub fn required(&self) -> bool {
        self.required
    }

    /// Get field type
    pub fn field_type(&self) -> &Type {
        &self.field_type
    }

    /// Get field documentation
    pub fn doc(&self) -> Option<&str> {
        self.doc.as_deref()
    }
}
```

**Step 4: Run tests to verify they pass**

```bash
cargo test test_nested_field --lib
```

Expected: Both tests pass

**Step 5: Commit**

```bash
git add icepick/src/spec/types.rs icepick/tests/test_types.rs
git commit -m "feat: implement NestedField with accessors"
```

---

### Task 7: Implement StructType

**Files:**
- Modify: `icepick/src/spec/types.rs`
- Modify: `icepick/tests/test_types.rs`

**Step 1: Write test for StructType**

Modify: `icepick/tests/test_types.rs`

```rust
use icepick::spec::types::{PrimitiveType, Type, NestedField, StructType};

// ... existing tests ...

#[test]
fn test_struct_type_simple() {
    let fields = vec![
        NestedField::required(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
        NestedField::required(2, "name".to_string(), Type::Primitive(PrimitiveType::String)),
    ];

    let struct_type = StructType::new(fields.clone());
    assert_eq!(struct_type.fields().len(), 2);
    assert_eq!(struct_type.fields()[0].name(), "id");
    assert_eq!(struct_type.fields()[1].name(), "name");
}

#[test]
fn test_struct_type_nested() {
    let address_fields = vec![
        NestedField::required(3, "street".to_string(), Type::Primitive(PrimitiveType::String)),
        NestedField::optional(4, "city".to_string(), Type::Primitive(PrimitiveType::String)),
    ];

    let fields = vec![
        NestedField::required(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
        NestedField::optional(2, "address".to_string(), Type::Struct(StructType::new(address_fields))),
    ];

    let struct_type = StructType::new(fields);
    assert_eq!(struct_type.fields().len(), 2);

    // Check nested struct
    if let Type::Struct(nested) = struct_type.fields()[1].field_type() {
        assert_eq!(nested.fields().len(), 2);
    } else {
        panic!("Expected nested struct");
    }
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_struct_type --lib
```

Expected: Compilation error - `StructType::new` method missing

**Step 3: Implement StructType properly**

Modify: `icepick/src/spec/types.rs` - replace the placeholder `StructType`:

```rust
/// A struct type (record with named fields)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructType {
    #[serde(rename = "fields")]
    fields: Vec<NestedField>,
}

impl StructType {
    /// Create a new struct type
    pub fn new(fields: Vec<NestedField>) -> Self {
        Self { fields }
    }

    /// Get the fields in this struct
    pub fn fields(&self) -> &[NestedField] {
        &self.fields
    }

    /// Get a field by name
    pub fn field_by_name(&self, name: &str) -> Option<&NestedField> {
        self.fields.iter().find(|f| f.name() == name)
    }

    /// Get a field by ID
    pub fn field_by_id(&self, id: i32) -> Option<&NestedField> {
        self.fields.iter().find(|f| f.id() == id)
    }
}
```

**Step 4: Run tests to verify they pass**

```bash
cargo test test_struct_type --lib
```

Expected: Both tests pass

**Step 5: Commit**

```bash
git add icepick/src/spec/types.rs icepick/tests/test_types.rs
git commit -m "feat: implement StructType with field lookup methods"
```

---

### Task 8: Implement Schema

**Files:**
- Create: `icepick/src/spec/schema.rs`
- Modify: `icepick/src/spec/mod.rs`
- Create: `icepick/tests/test_schema.rs`

**Step 1: Write test for Schema**

Create: `icepick/tests/test_schema.rs`

```rust
use icepick::spec::{Schema, NestedField, Type, PrimitiveType, StructType};

#[test]
fn test_schema_simple() {
    let fields = vec![
        NestedField::required(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
        NestedField::required(2, "name".to_string(), Type::Primitive(PrimitiveType::String)),
        NestedField::optional(3, "email".to_string(), Type::Primitive(PrimitiveType::String)),
    ];

    let schema = Schema::builder()
        .with_fields(fields.clone())
        .build()
        .unwrap();

    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.fields()[0].id(), 1);
    assert_eq!(schema.fields()[1].id(), 2);
}

#[test]
fn test_schema_with_struct_type() {
    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
        ])
        .build()
        .unwrap();

    assert_eq!(schema.schema_id(), 1);
    let struct_type = schema.as_struct();
    assert_eq!(struct_type.fields().len(), 1);
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_schema --lib
```

Expected: Compilation error - `Schema` not found

**Step 3: Implement Schema**

Create: `icepick/src/spec/schema.rs`

```rust
//! Iceberg schema
//! Vendored from iceberg-rust v0.7.0

use serde::{Deserialize, Serialize};
use crate::spec::types::{StructType, NestedField};
use crate::error::{Error, Result};

/// An Iceberg schema
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    #[serde(rename = "schema-id")]
    schema_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    identifier_field_ids: Option<Vec<i32>>,
    #[serde(flatten)]
    struct_type: StructType,
}

impl Schema {
    /// Create a schema builder
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::default()
    }

    /// Get the schema ID
    pub fn schema_id(&self) -> i32 {
        self.schema_id
    }

    /// Get the fields
    pub fn fields(&self) -> &[NestedField] {
        self.struct_type.fields()
    }

    /// Get the schema as a struct type
    pub fn as_struct(&self) -> &StructType {
        &self.struct_type
    }

    /// Get identifier field IDs
    pub fn identifier_field_ids(&self) -> Option<&[i32]> {
        self.identifier_field_ids.as_deref()
    }
}

/// Builder for Schema
#[derive(Default)]
pub struct SchemaBuilder {
    schema_id: Option<i32>,
    identifier_field_ids: Option<Vec<i32>>,
    fields: Option<Vec<NestedField>>,
}

impl SchemaBuilder {
    /// Set the schema ID
    pub fn with_schema_id(mut self, schema_id: i32) -> Self {
        self.schema_id = Some(schema_id);
        self
    }

    /// Set identifier field IDs
    pub fn with_identifier_field_ids(mut self, ids: Vec<i32>) -> Self {
        self.identifier_field_ids = Some(ids);
        self
    }

    /// Set the fields
    pub fn with_fields(mut self, fields: Vec<NestedField>) -> Self {
        self.fields = Some(fields);
        self
    }

    /// Build the schema
    pub fn build(self) -> Result<Schema> {
        let fields = self.fields.ok_or_else(|| {
            Error::InvalidInput("Schema must have fields".to_string())
        })?;

        Ok(Schema {
            schema_id: self.schema_id.unwrap_or(0),
            identifier_field_ids: self.identifier_field_ids,
            struct_type: StructType::new(fields),
        })
    }
}
```

**Step 4: Update spec/mod.rs**

Modify: `icepick/src/spec/mod.rs`

```rust
//! Iceberg specification types
//!
//! Vendored from iceberg-rust v0.7.0
//! Copyright 2024 Apache Software Foundation
//! Licensed under Apache License 2.0

pub mod identifier;
pub mod types;
pub mod schema;

pub use identifier::{NamespaceIdent, TableIdent};
pub use types::{Type, PrimitiveType, StructType, NestedField};
pub use schema::Schema;
```

**Step 5: Run tests to verify they pass**

```bash
cargo test test_schema --lib
```

Expected: Both tests pass

**Step 6: Commit**

```bash
git add icepick/src/spec/schema.rs icepick/src/spec/mod.rs icepick/tests/test_schema.rs
git commit -m "feat: implement Schema with builder pattern"
```

---

## Phase 3: Metadata Foundation

### Task 9: Vendor Snapshot Types

**Files:**
- Create: `icepick/src/spec/snapshot.rs`
- Modify: `icepick/src/spec/mod.rs`
- Create: `icepick/tests/test_snapshot.rs`

**Step 1: Write test for Snapshot**

Create: `icepick/tests/test_snapshot.rs`

```rust
use icepick::spec::{Snapshot, Summary};

#[test]
fn test_snapshot_creation() {
    let summary = Summary::builder()
        .set("operation", "append")
        .set("added-files", "1")
        .build();

    let snapshot = Snapshot::builder()
        .with_snapshot_id(1)
        .with_timestamp_ms(1234567890000)
        .with_manifest_list("s3://bucket/metadata/snap-1.avro")
        .with_summary(summary)
        .build()
        .unwrap();

    assert_eq!(snapshot.snapshot_id(), 1);
    assert_eq!(snapshot.timestamp_ms(), 1234567890000);
    assert_eq!(snapshot.manifest_list(), "s3://bucket/metadata/snap-1.avro");
}

#[test]
fn test_snapshot_summary() {
    let summary = Summary::builder()
        .set("operation", "append")
        .set("added-records", "100")
        .build();

    let snapshot = Snapshot::builder()
        .with_snapshot_id(1)
        .with_timestamp_ms(1234567890000)
        .with_manifest_list("s3://bucket/metadata/snap-1.avro")
        .with_summary(summary.clone())
        .build()
        .unwrap();

    assert_eq!(snapshot.summary().get("operation"), Some(&"append".to_string()));
    assert_eq!(snapshot.summary().get("added-records"), Some(&"100".to_string()));
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_snapshot --lib
```

Expected: Compilation error - `Snapshot` not found

**Step 3: Implement Snapshot types**

Create: `icepick/src/spec/snapshot.rs`

```rust
//! Iceberg snapshots
//! Vendored from iceberg-rust v0.7.0

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::error::{Error, Result};

/// Summary of a snapshot
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    operation: String,
    #[serde(flatten)]
    additional_properties: HashMap<String, String>,
}

impl Summary {
    /// Create a summary builder
    pub fn builder() -> SummaryBuilder {
        SummaryBuilder::default()
    }

    /// Get the operation type
    pub fn operation(&self) -> &str {
        &self.operation
    }

    /// Get a property by key
    pub fn get(&self, key: &str) -> Option<&String> {
        if key == "operation" {
            Some(&self.operation)
        } else {
            self.additional_properties.get(key)
        }
    }

    /// Get all properties
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.additional_properties
    }
}

/// Builder for Summary
#[derive(Default)]
pub struct SummaryBuilder {
    operation: Option<String>,
    properties: HashMap<String, String>,
}

impl SummaryBuilder {
    /// Set a property
    pub fn set(mut self, key: &str, value: &str) -> Self {
        if key == "operation" {
            self.operation = Some(value.to_string());
        } else {
            self.properties.insert(key.to_string(), value.to_string());
        }
        self
    }

    /// Build the summary
    pub fn build(self) -> Summary {
        Summary {
            operation: self.operation.unwrap_or_else(|| "append".to_string()),
            additional_properties: self.properties,
        }
    }
}

/// A snapshot of a table at a point in time
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_snapshot_id: Option<i64>,
    #[serde(rename = "sequence-number", skip_serializing_if = "Option::is_none")]
    sequence_number: Option<i64>,
    #[serde(rename = "timestamp-ms")]
    timestamp_ms: i64,
    #[serde(rename = "manifest-list")]
    manifest_list: String,
    summary: Summary,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_id: Option<i32>,
}

impl Snapshot {
    /// Create a snapshot builder
    pub fn builder() -> SnapshotBuilder {
        SnapshotBuilder::default()
    }

    /// Get snapshot ID
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Get parent snapshot ID
    pub fn parent_snapshot_id(&self) -> Option<i64> {
        self.parent_snapshot_id
    }

    /// Get sequence number
    pub fn sequence_number(&self) -> Option<i64> {
        self.sequence_number
    }

    /// Get timestamp in milliseconds
    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    /// Get manifest list location
    pub fn manifest_list(&self) -> &str {
        &self.manifest_list
    }

    /// Get summary
    pub fn summary(&self) -> &Summary {
        &self.summary
    }

    /// Get schema ID
    pub fn schema_id(&self) -> Option<i32> {
        self.schema_id
    }
}

/// Builder for Snapshot
#[derive(Default)]
pub struct SnapshotBuilder {
    snapshot_id: Option<i64>,
    parent_snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    timestamp_ms: Option<i64>,
    manifest_list: Option<String>,
    summary: Option<Summary>,
    schema_id: Option<i32>,
}

impl SnapshotBuilder {
    pub fn with_snapshot_id(mut self, id: i64) -> Self {
        self.snapshot_id = Some(id);
        self
    }

    pub fn with_parent_snapshot_id(mut self, id: i64) -> Self {
        self.parent_snapshot_id = Some(id);
        self
    }

    pub fn with_sequence_number(mut self, seq: i64) -> Self {
        self.sequence_number = Some(seq);
        self
    }

    pub fn with_timestamp_ms(mut self, timestamp: i64) -> Self {
        self.timestamp_ms = Some(timestamp);
        self
    }

    pub fn with_manifest_list(mut self, location: &str) -> Self {
        self.manifest_list = Some(location.to_string());
        self
    }

    pub fn with_summary(mut self, summary: Summary) -> Self {
        self.summary = Some(summary);
        self
    }

    pub fn with_schema_id(mut self, id: i32) -> Self {
        self.schema_id = Some(id);
        self
    }

    pub fn build(self) -> Result<Snapshot> {
        Ok(Snapshot {
            snapshot_id: self.snapshot_id.ok_or_else(|| {
                Error::InvalidInput("Snapshot must have ID".to_string())
            })?,
            parent_snapshot_id: self.parent_snapshot_id,
            sequence_number: self.sequence_number,
            timestamp_ms: self.timestamp_ms.ok_or_else(|| {
                Error::InvalidInput("Snapshot must have timestamp".to_string())
            })?,
            manifest_list: self.manifest_list.ok_or_else(|| {
                Error::InvalidInput("Snapshot must have manifest list".to_string())
            })?,
            summary: self.summary.unwrap_or_else(|| Summary::builder().build()),
            schema_id: self.schema_id,
        })
    }
}
```

**Step 4: Update spec/mod.rs**

Modify: `icepick/src/spec/mod.rs`

```rust
pub mod identifier;
pub mod types;
pub mod schema;
pub mod snapshot;

pub use identifier::{NamespaceIdent, TableIdent};
pub use types::{Type, PrimitiveType, StructType, NestedField};
pub use schema::Schema;
pub use snapshot::{Snapshot, Summary};
```

**Step 5: Run tests to verify they pass**

```bash
cargo test test_snapshot --lib
```

Expected: Both tests pass

**Step 6: Commit**

```bash
git add icepick/src/spec/snapshot.rs icepick/src/spec/mod.rs icepick/tests/test_snapshot.rs
git commit -m "feat: implement Snapshot and Summary types"
```

---

### Task 10: Implement TableMetadata (Simplified)

**Files:**
- Create: `icepick/src/spec/metadata.rs`
- Modify: `icepick/src/spec/mod.rs`
- Create: `icepick/tests/test_metadata.rs`

**Step 1: Write test for TableMetadata**

Create: `icepick/tests/test_metadata.rs`

```rust
use icepick::spec::{
    TableMetadata, Schema, NestedField, Type, PrimitiveType,
    Snapshot, Summary,
};

#[test]
fn test_table_metadata_basic() {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
        ])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://bucket/warehouse/db/table")
        .with_current_schema(schema.clone())
        .build()
        .unwrap();

    assert_eq!(metadata.location(), "s3://bucket/warehouse/db/table");
    assert_eq!(metadata.current_schema().schema_id(), 0);
}

#[test]
fn test_table_metadata_with_snapshot() {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
        ])
        .build()
        .unwrap();

    let summary = Summary::builder()
        .set("operation", "append")
        .build();

    let snapshot = Snapshot::builder()
        .with_snapshot_id(1)
        .with_timestamp_ms(1234567890000)
        .with_manifest_list("s3://bucket/metadata/snap-1.avro")
        .with_summary(summary)
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://bucket/warehouse/db/table")
        .with_current_schema(schema)
        .with_current_snapshot(snapshot.clone())
        .build()
        .unwrap();

    assert!(metadata.current_snapshot().is_some());
    assert_eq!(metadata.current_snapshot().unwrap().snapshot_id(), 1);
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_table_metadata --lib
```

Expected: Compilation error - `TableMetadata` not found

**Step 3: Implement TableMetadata (simplified version)**

Create: `icepick/src/spec/metadata.rs`

```rust
//! Iceberg table metadata
//! Vendored and simplified from iceberg-rust v0.7.0

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::spec::{Schema, Snapshot};
use crate::error::{Error, Result};

/// Metadata for an Iceberg table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableMetadata {
    #[serde(rename = "format-version")]
    format_version: i32,
    #[serde(rename = "table-uuid")]
    table_uuid: String,
    location: String,
    #[serde(rename = "last-updated-ms")]
    last_updated_ms: i64,
    #[serde(rename = "last-column-id")]
    last_column_id: i32,
    schemas: Vec<Schema>,
    #[serde(rename = "current-schema-id")]
    current_schema_id: i32,
    #[serde(default)]
    snapshots: Vec<Snapshot>,
    #[serde(rename = "current-snapshot-id", skip_serializing_if = "Option::is_none")]
    current_snapshot_id: Option<i64>,
    #[serde(default)]
    properties: HashMap<String, String>,
}

impl TableMetadata {
    /// Create a metadata builder
    pub fn builder() -> TableMetadataBuilder {
        TableMetadataBuilder::default()
    }

    /// Get format version
    pub fn format_version(&self) -> i32 {
        self.format_version
    }

    /// Get table UUID
    pub fn table_uuid(&self) -> &str {
        &self.table_uuid
    }

    /// Get table location
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Get last updated timestamp
    pub fn last_updated_ms(&self) -> i64 {
        self.last_updated_ms
    }

    /// Get all schemas
    pub fn schemas(&self) -> &[Schema] {
        &self.schemas
    }

    /// Get current schema
    pub fn current_schema(&self) -> &Schema {
        self.schemas
            .iter()
            .find(|s| s.schema_id() == self.current_schema_id)
            .expect("Current schema must exist")
    }

    /// Get all snapshots
    pub fn snapshots(&self) -> &[Snapshot] {
        &self.snapshots
    }

    /// Get current snapshot
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        self.current_snapshot_id.and_then(|id| {
            self.snapshots.iter().find(|s| s.snapshot_id() == id)
        })
    }

    /// Get table properties
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

/// Builder for TableMetadata
#[derive(Default)]
pub struct TableMetadataBuilder {
    format_version: Option<i32>,
    table_uuid: Option<String>,
    location: Option<String>,
    last_updated_ms: Option<i64>,
    last_column_id: Option<i32>,
    schemas: Vec<Schema>,
    current_schema_id: Option<i32>,
    snapshots: Vec<Snapshot>,
    current_snapshot_id: Option<i64>,
    properties: HashMap<String, String>,
}

impl TableMetadataBuilder {
    pub fn with_format_version(mut self, version: i32) -> Self {
        self.format_version = Some(version);
        self
    }

    pub fn with_table_uuid(mut self, uuid: String) -> Self {
        self.table_uuid = Some(uuid);
        self
    }

    pub fn with_location(mut self, location: &str) -> Self {
        self.location = Some(location.to_string());
        self
    }

    pub fn with_last_updated_ms(mut self, timestamp: i64) -> Self {
        self.last_updated_ms = Some(timestamp);
        self
    }

    pub fn with_current_schema(mut self, schema: Schema) -> Self {
        let schema_id = schema.schema_id();
        self.current_schema_id = Some(schema_id);
        self.schemas.push(schema);
        self
    }

    pub fn with_current_snapshot(mut self, snapshot: Snapshot) -> Self {
        let snapshot_id = snapshot.snapshot_id();
        self.current_snapshot_id = Some(snapshot_id);
        self.snapshots.push(snapshot);
        self
    }

    pub fn with_property(mut self, key: String, value: String) -> Self {
        self.properties.insert(key, value);
        self
    }

    pub fn build(self) -> Result<TableMetadata> {
        let location = self.location.ok_or_else(|| {
            Error::InvalidInput("TableMetadata must have location".to_string())
        })?;

        let schemas = if self.schemas.is_empty() {
            return Err(Error::InvalidInput("TableMetadata must have at least one schema".to_string()));
        } else {
            self.schemas
        };

        // Find max field ID across all schemas
        let last_column_id = schemas
            .iter()
            .flat_map(|s| s.fields())
            .map(|f| f.id())
            .max()
            .unwrap_or(0);

        Ok(TableMetadata {
            format_version: self.format_version.unwrap_or(2),
            table_uuid: self.table_uuid.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            location,
            last_updated_ms: self.last_updated_ms.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64
            }),
            last_column_id,
            schemas,
            current_schema_id: self.current_schema_id.unwrap_or(0),
            snapshots: self.snapshots,
            current_snapshot_id: self.current_snapshot_id,
            properties: self.properties,
        })
    }
}
```

**Step 4: Update spec/mod.rs**

Modify: `icepick/src/spec/mod.rs`

```rust
pub mod identifier;
pub mod types;
pub mod schema;
pub mod snapshot;
pub mod metadata;

pub use identifier::{NamespaceIdent, TableIdent};
pub use types::{Type, PrimitiveType, StructType, NestedField};
pub use schema::Schema;
pub use snapshot::{Snapshot, Summary};
pub use metadata::TableMetadata;
```

**Step 5: Add uuid dependency**

Modify: `icepick/Cargo.toml`

Add to `[dependencies]`:
```toml
uuid = { version = "1.0", features = ["v4", "serde"] }
```

**Step 6: Run tests to verify they pass**

```bash
cargo test test_table_metadata --lib
```

Expected: Both tests pass

**Step 7: Commit**

```bash
git add icepick/src/spec/metadata.rs icepick/src/spec/mod.rs icepick/tests/test_metadata.rs icepick/Cargo.toml
git commit -m "feat: implement simplified TableMetadata with builder"
```

---

## Phase 4: OpenDAL FileIO

### Task 11: Create WASM-Compatible FileIO Interface

**Files:**
- Create: `icepick/src/io/mod.rs`
- Create: `icepick/src/io/file_io.rs`
- Modify: `icepick/src/lib.rs`

**Step 1: Write test for FileIO (will be platform-specific)**

Create: `icepick/tests/test_file_io.rs`

```rust
use icepick::io::FileIO;
use opendal::Operator;

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn test_file_io_write_read() {
    // Use memory backend for testing
    let op = Operator::via_map(opendal::Scheme::Memory, Default::default())
        .unwrap();

    let file_io = FileIO::new(op);

    // Write data
    let data = b"Hello, Iceberg!";
    file_io.write("test.txt", data.to_vec()).await.unwrap();

    // Read data back
    let read_data = file_io.read("test.txt").await.unwrap();
    assert_eq!(read_data, data);
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn test_file_io_exists() {
    let op = Operator::via_map(opendal::Scheme::Memory, Default::default())
        .unwrap();

    let file_io = FileIO::new(op);

    // File doesn't exist initially
    assert!(!file_io.exists("missing.txt").await.unwrap());

    // Write file
    file_io.write("exists.txt", b"data".to_vec()).await.unwrap();

    // Now it exists
    assert!(file_io.exists("exists.txt").await.unwrap());
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_file_io --lib
```

Expected: Compilation error - module `io` not found

**Step 3: Implement FileIO wrapper**

Create: `icepick/src/io/mod.rs`

```rust
//! I/O operations for Iceberg files
//! WASM-compatible via OpenDAL

mod file_io;

pub use file_io::FileIO;
```

Create: `icepick/src/io/file_io.rs`

```rust
//! FileIO implementation using OpenDAL
//! Compatible with both WASM and native targets

use opendal::Operator;
use crate::error::{Error, Result};

/// File I/O abstraction for reading/writing Iceberg files
#[derive(Clone)]
pub struct FileIO {
    operator: Operator,
}

impl FileIO {
    /// Create a new FileIO with the given OpenDAL operator
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Read a file completely
    pub async fn read(&self, path: &str) -> Result<Vec<u8>> {
        self.operator
            .read(path)
            .await
            .map(|b| b.to_vec())
            .map_err(|e| Error::IoError(format!("Failed to read {}: {}", path, e)))
    }

    /// Write data to a file
    pub async fn write(&self, path: &str, data: Vec<u8>) -> Result<()> {
        self.operator
            .write(path, data)
            .await
            .map_err(|e| Error::IoError(format!("Failed to write {}: {}", path, e)))
    }

    /// Check if a file exists
    pub async fn exists(&self, path: &str) -> Result<bool> {
        match self.operator.is_exist(path).await {
            Ok(exists) => Ok(exists),
            Err(e) => Err(Error::IoError(format!("Failed to check existence of {}: {}", path, e))),
        }
    }

    /// Delete a file
    pub async fn delete(&self, path: &str) -> Result<()> {
        self.operator
            .delete(path)
            .await
            .map_err(|e| Error::IoError(format!("Failed to delete {}: {}", path, e)))
    }

    /// Get the underlying operator (for advanced use cases)
    pub fn operator(&self) -> &Operator {
        &self.operator
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_io_creation() {
        let op = Operator::via_map(opendal::Scheme::Memory, Default::default())
            .unwrap();
        let _file_io = FileIO::new(op);
        // Just verify it compiles and constructs
    }
}
```

**Step 4: Update lib.rs**

Modify: `icepick/src/lib.rs`

```rust
pub mod spec;
pub mod catalog;
pub mod io;
pub mod error;

pub use error::{Error, Result};
```

**Step 5: Update error types**

Modify: `icepick/src/error.rs` to add IoError variant:

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // ... existing variants ...

    #[error("I/O error: {0}")]
    IoError(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),
}
```

**Step 6: Run tests to verify they pass**

```bash
cargo test test_file_io
```

Expected: All tests pass

**Step 7: Verify WASM compilation**

```bash
cargo build --target wasm32-unknown-unknown --lib
```

Expected: Should compile successfully (FileIO is WASM-compatible)

**Step 8: Commit**

```bash
git add icepick/src/io/ icepick/src/lib.rs icepick/src/error.rs icepick/tests/test_file_io.rs
git commit -m "feat: implement WASM-compatible FileIO using OpenDAL"
```

---

## Phase 5: Complete Table API

### Task 12: Create Table Struct with Metadata

**Files:**
- Create: `icepick/src/table.rs`
- Modify: `icepick/src/lib.rs`
- Create: `icepick/tests/test_table.rs`

**Step 1: Write test for Table**

Create: `icepick/tests/test_table.rs`

```rust
use icepick::{
    Table, TableMetadata, Schema, NestedField, Type, PrimitiveType,
    TableIdent, NamespaceIdent, FileIO,
};
use opendal::Operator;

#[tokio::test]
async fn test_table_accessors() {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
        ])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://bucket/warehouse/db/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["db".to_string()]),
        "table".to_string(),
    );

    let op = Operator::via_map(opendal::Scheme::Memory, Default::default()).unwrap();
    let file_io = FileIO::new(op);

    let table = Table::new(
        ident.clone(),
        metadata.clone(),
        "s3://bucket/warehouse/db/table/metadata/v1.json".to_string(),
        file_io,
    );

    assert_eq!(table.identifier().to_string(), "db.table");
    assert_eq!(table.location(), "s3://bucket/warehouse/db/table");
    assert_eq!(table.schema().schema_id(), 0);
    assert_eq!(table.metadata_location(), "s3://bucket/warehouse/db/table/metadata/v1.json");
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_table_accessors
```

Expected: Compilation error - `Table` not found

**Step 3: Implement Table struct**

Create: `icepick/src/table.rs`

```rust
//! Iceberg table representation

use crate::spec::{TableIdent, TableMetadata, Schema};
use crate::io::FileIO;

/// An Iceberg table with integrated storage
pub struct Table {
    identifier: TableIdent,
    metadata: TableMetadata,
    metadata_location: String,
    file_io: FileIO,
}

impl Table {
    /// Create a new table instance
    pub fn new(
        identifier: TableIdent,
        metadata: TableMetadata,
        metadata_location: String,
        file_io: FileIO,
    ) -> Self {
        Self {
            identifier,
            metadata,
            metadata_location,
            file_io,
        }
    }

    /// Get the table identifier
    pub fn identifier(&self) -> &TableIdent {
        &self.identifier
    }

    /// Get the table metadata
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Get the current schema
    pub fn schema(&self) -> &Schema {
        self.metadata.current_schema()
    }

    /// Get the table location
    pub fn location(&self) -> &str {
        self.metadata.location()
    }

    /// Get the metadata file location
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }

    /// Get the FileIO (for internal use)
    pub(crate) fn file_io(&self) -> &FileIO {
        &self.file_io
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{NamespaceIdent, NestedField, Type, PrimitiveType};
    use opendal::Operator;

    #[test]
    fn test_table_creation() {
        let schema = crate::spec::Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
            ])
            .build()
            .unwrap();

        let metadata = TableMetadata::builder()
            .with_location("s3://test/table")
            .with_current_schema(schema)
            .build()
            .unwrap();

        let ident = TableIdent::new(
            NamespaceIdent::new(vec!["default".to_string()]),
            "test".to_string(),
        );

        let op = Operator::via_map(opendal::Scheme::Memory, Default::default()).unwrap();
        let file_io = FileIO::new(op);

        let table = Table::new(ident, metadata, "s3://test/metadata.json".to_string(), file_io);
        assert_eq!(table.location(), "s3://test/table");
    }
}
```

**Step 4: Update lib.rs**

Modify: `icepick/src/lib.rs`

```rust
pub mod spec;
pub mod catalog;
pub mod io;
pub mod error;
mod table;

pub use error::{Error, Result};
pub use spec::{
    TableIdent, NamespaceIdent, Schema, Type, PrimitiveType,
    StructType, NestedField, TableMetadata, Snapshot, Summary,
};
pub use io::FileIO;
pub use table::Table;
```

**Step 5: Run tests to verify they pass**

```bash
cargo test test_table
```

Expected: All tests pass

**Step 6: Commit**

```bash
git add icepick/src/table.rs icepick/src/lib.rs icepick/tests/test_table.rs
git commit -m "feat: implement Table struct with metadata accessors"
```

---

### Task 13: Add Transaction Stub to Table

**Files:**
- Create: `icepick/src/transaction.rs`
- Modify: `icepick/src/table.rs`
- Modify: `icepick/src/lib.rs`

**Step 1: Write test for transaction creation**

Modify: `icepick/tests/test_table.rs`

```rust
// ... existing imports and tests ...

#[tokio::test]
async fn test_table_transaction() {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
        ])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://bucket/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["db".to_string()]),
        "table".to_string(),
    );

    let op = Operator::via_map(opendal::Scheme::Memory, Default::default()).unwrap();
    let file_io = FileIO::new(op);

    let table = Table::new(ident, metadata, "s3://bucket/metadata.json".to_string(), file_io);

    // Create transaction
    let _tx = table.transaction();
    // Just verify it compiles for now
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_table_transaction
```

Expected: Compilation error - `transaction` method not found

**Step 3: Create Transaction stub**

Create: `icepick/src/transaction.rs`

```rust
//! Transaction API for writing to Iceberg tables

use crate::table::Table;

/// A transaction for modifying a table
pub struct Transaction<'a> {
    table: &'a Table,
}

impl<'a> Transaction<'a> {
    /// Create a new transaction
    pub(crate) fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Get the table this transaction operates on
    pub fn table(&self) -> &Table {
        self.table
    }
}
```

**Step 4: Add transaction() method to Table**

Modify: `icepick/src/table.rs`

```rust
use crate::spec::{TableIdent, TableMetadata, Schema};
use crate::io::FileIO;
use crate::transaction::Transaction;

// ... existing Table impl ...

impl Table {
    // ... existing methods ...

    /// Start a new transaction for writing data
    pub fn transaction(&self) -> Transaction {
        Transaction::new(self)
    }
}
```

**Step 5: Update lib.rs**

Modify: `icepick/src/lib.rs`

```rust
pub mod spec;
pub mod catalog;
pub mod io;
pub mod error;
mod table;
mod transaction;

pub use error::{Error, Result};
pub use spec::{
    TableIdent, NamespaceIdent, Schema, Type, PrimitiveType,
    StructType, NestedField, TableMetadata, Snapshot, Summary,
};
pub use io::FileIO;
pub use table::Table;
pub use transaction::Transaction;
```

**Step 6: Run tests to verify they pass**

```bash
cargo test test_table_transaction
```

Expected: Test passes

**Step 7: Commit**

```bash
git add icepick/src/transaction.rs icepick/src/table.rs icepick/src/lib.rs icepick/tests/test_table.rs
git commit -m "feat: add Transaction stub to Table API"
```

---

## Phase 6: Write Support

### Task 14: Vendor DataFile Types

**Files:**
- Create: `icepick/src/spec/data_file.rs`
- Modify: `icepick/src/spec/mod.rs`
- Create: `icepick/tests/test_data_file.rs`

**Step 1: Write test for DataFile**

Create: `icepick/tests/test_data_file.rs`

```rust
use icepick::spec::{DataFile, DataContentType};
use std::collections::HashMap;

#[test]
fn test_data_file_builder() {
    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .build()
        .unwrap();

    assert_eq!(data_file.file_path(), "s3://bucket/data/file1.parquet");
    assert_eq!(data_file.file_format(), "PARQUET");
    assert_eq!(data_file.record_count(), 100);
    assert_eq!(data_file.file_size_in_bytes(), 5000);
}

#[test]
fn test_data_file_content_type() {
    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .with_content_type(DataContentType::Data)
        .build()
        .unwrap();

    assert_eq!(data_file.content_type(), DataContentType::Data);
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_data_file
```

Expected: Compilation error - `DataFile` not found

**Step 3: Implement DataFile**

Create: `icepick/src/spec/data_file.rs`

```rust
//! Data file metadata
//! Vendored from iceberg-rust v0.7.0

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::error::{Error, Result};

/// Content type of a data file
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataContentType {
    /// Regular data
    Data,
    /// Position deletes
    PositionDeletes,
    /// Equality deletes
    EqualityDeletes,
}

impl Default for DataContentType {
    fn default() -> Self {
        Self::Data
    }
}

/// Metadata about a data file in an Iceberg table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFile {
    #[serde(rename = "content")]
    content_type: DataContentType,
    #[serde(rename = "file-path")]
    file_path: String,
    #[serde(rename = "file-format")]
    file_format: String,
    #[serde(rename = "record-count")]
    record_count: i64,
    #[serde(rename = "file-size-in-bytes")]
    file_size_in_bytes: i64,
    #[serde(rename = "column-sizes", skip_serializing_if = "Option::is_none")]
    column_sizes: Option<HashMap<i32, i64>>,
    #[serde(rename = "value-counts", skip_serializing_if = "Option::is_none")]
    value_counts: Option<HashMap<i32, i64>>,
    #[serde(rename = "null-value-counts", skip_serializing_if = "Option::is_none")]
    null_value_counts: Option<HashMap<i32, i64>>,
    #[serde(rename = "lower-bounds", skip_serializing_if = "Option::is_none")]
    lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    #[serde(rename = "upper-bounds", skip_serializing_if = "Option::is_none")]
    upper_bounds: Option<HashMap<i32, Vec<u8>>>,
}

impl DataFile {
    /// Create a data file builder
    pub fn builder() -> DataFileBuilder {
        DataFileBuilder::default()
    }

    /// Get content type
    pub fn content_type(&self) -> DataContentType {
        self.content_type
    }

    /// Get file path
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Get file format
    pub fn file_format(&self) -> &str {
        &self.file_format
    }

    /// Get record count
    pub fn record_count(&self) -> i64 {
        self.record_count
    }

    /// Get file size in bytes
    pub fn file_size_in_bytes(&self) -> i64 {
        self.file_size_in_bytes
    }

    /// Get column sizes
    pub fn column_sizes(&self) -> Option<&HashMap<i32, i64>> {
        self.column_sizes.as_ref()
    }

    /// Get value counts
    pub fn value_counts(&self) -> Option<&HashMap<i32, i64>> {
        self.value_counts.as_ref()
    }

    /// Get null value counts
    pub fn null_value_counts(&self) -> Option<&HashMap<i32, i64>> {
        self.null_value_counts.as_ref()
    }
}

/// Builder for DataFile
#[derive(Default)]
pub struct DataFileBuilder {
    content_type: Option<DataContentType>,
    file_path: Option<String>,
    file_format: Option<String>,
    record_count: Option<i64>,
    file_size_in_bytes: Option<i64>,
    column_sizes: Option<HashMap<i32, i64>>,
    value_counts: Option<HashMap<i32, i64>>,
    null_value_counts: Option<HashMap<i32, i64>>,
    lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    upper_bounds: Option<HashMap<i32, Vec<u8>>>,
}

impl DataFileBuilder {
    pub fn with_content_type(mut self, content_type: DataContentType) -> Self {
        self.content_type = Some(content_type);
        self
    }

    pub fn with_file_path(mut self, path: &str) -> Self {
        self.file_path = Some(path.to_string());
        self
    }

    pub fn with_file_format(mut self, format: &str) -> Self {
        self.file_format = Some(format.to_string());
        self
    }

    pub fn with_record_count(mut self, count: i64) -> Self {
        self.record_count = Some(count);
        self
    }

    pub fn with_file_size_in_bytes(mut self, size: i64) -> Self {
        self.file_size_in_bytes = Some(size);
        self
    }

    pub fn with_column_sizes(mut self, sizes: HashMap<i32, i64>) -> Self {
        self.column_sizes = Some(sizes);
        self
    }

    pub fn with_value_counts(mut self, counts: HashMap<i32, i64>) -> Self {
        self.value_counts = Some(counts);
        self
    }

    pub fn with_null_value_counts(mut self, counts: HashMap<i32, i64>) -> Self {
        self.null_value_counts = Some(counts);
        self
    }

    pub fn build(self) -> Result<DataFile> {
        Ok(DataFile {
            content_type: self.content_type.unwrap_or_default(),
            file_path: self.file_path.ok_or_else(|| {
                Error::InvalidInput("DataFile must have file path".to_string())
            })?,
            file_format: self.file_format.ok_or_else(|| {
                Error::InvalidInput("DataFile must have file format".to_string())
            })?,
            record_count: self.record_count.ok_or_else(|| {
                Error::InvalidInput("DataFile must have record count".to_string())
            })?,
            file_size_in_bytes: self.file_size_in_bytes.ok_or_else(|| {
                Error::InvalidInput("DataFile must have file size".to_string())
            })?,
            column_sizes: self.column_sizes,
            value_counts: self.value_counts,
            null_value_counts: self.null_value_counts,
            lower_bounds: self.lower_bounds,
            upper_bounds: self.upper_bounds,
        })
    }
}
```

**Step 4: Update spec/mod.rs**

Modify: `icepick/src/spec/mod.rs`

```rust
pub mod identifier;
pub mod types;
pub mod schema;
pub mod snapshot;
pub mod metadata;
pub mod data_file;

pub use identifier::{NamespaceIdent, TableIdent};
pub use types::{Type, PrimitiveType, StructType, NestedField};
pub use schema::Schema;
pub use snapshot::{Snapshot, Summary};
pub use metadata::TableMetadata;
pub use data_file::{DataFile, DataContentType};
```

**Step 5: Run tests to verify they pass**

```bash
cargo test test_data_file
```

Expected: Both tests pass

**Step 6: Commit**

```bash
git add icepick/src/spec/data_file.rs icepick/src/spec/mod.rs icepick/tests/test_data_file.rs
git commit -m "feat: vendor DataFile and DataContentType from iceberg-rust"
```

---

### Task 15: Implement Transaction.append()

**Files:**
- Modify: `icepick/src/transaction.rs`
- Create: `icepick/tests/test_transaction.rs`

**Step 1: Write test for append**

Create: `icepick/tests/test_transaction.rs`

```rust
use icepick::{
    Table, TableMetadata, Schema, NestedField, Type, PrimitiveType,
    TableIdent, NamespaceIdent, FileIO, DataFile, DataContentType,
};
use opendal::Operator;

#[tokio::test]
async fn test_transaction_append() {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
        ])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("s3://bucket/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["db".to_string()]),
        "table".to_string(),
    );

    let op = Operator::via_map(opendal::Scheme::Memory, Default::default()).unwrap();
    let file_io = FileIO::new(op);

    let table = Table::new(ident, metadata, "s3://bucket/metadata.json".to_string(), file_io);

    // Create data file
    let data_file = DataFile::builder()
        .with_file_path("s3://bucket/data/file1.parquet")
        .with_file_format("PARQUET")
        .with_record_count(100)
        .with_file_size_in_bytes(5000)
        .build()
        .unwrap();

    // Create transaction with append
    let tx = table.transaction().append(vec![data_file]);

    // Verify we can build the transaction (commit will be tested later)
    assert!(tx.has_operations());
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test test_transaction_append
```

Expected: Compilation error - `append` method not found

**Step 3: Implement append and operations tracking**

Modify: `icepick/src/transaction.rs`

```rust
//! Transaction API for writing to Iceberg tables

use crate::table::Table;
use crate::spec::DataFile;

/// Operations that can be performed in a transaction
#[derive(Debug, Clone)]
pub enum TransactionOperation {
    /// Append data files
    Append(Vec<DataFile>),
}

/// A transaction for modifying a table
pub struct Transaction<'a> {
    table: &'a Table,
    operations: Vec<TransactionOperation>,
}

impl<'a> Transaction<'a> {
    /// Create a new transaction
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            operations: Vec::new(),
        }
    }

    /// Get the table this transaction operates on
    pub fn table(&self) -> &Table {
        self.table
    }

    /// Append data files to the table
    pub fn append(mut self, data_files: Vec<DataFile>) -> Self {
        self.operations.push(TransactionOperation::Append(data_files));
        self
    }

    /// Check if transaction has any operations
    pub fn has_operations(&self) -> bool {
        !self.operations.is_empty()
    }

    /// Get the operations (for internal use)
    pub(crate) fn operations(&self) -> &[TransactionOperation] {
        &self.operations
    }
}
```

**Step 4: Update lib.rs to export DataFile**

Modify: `icepick/src/lib.rs`

```rust
pub use spec::{
    TableIdent, NamespaceIdent, Schema, Type, PrimitiveType,
    StructType, NestedField, TableMetadata, Snapshot, Summary,
    DataFile, DataContentType,
};
```

**Step 5: Run tests to verify they pass**

```bash
cargo test test_transaction_append
```

Expected: Test passes

**Step 6: Commit**

```bash
git add icepick/src/transaction.rs icepick/src/lib.rs icepick/tests/test_transaction.rs
git commit -m "feat: implement Transaction.append() with operation tracking"
```

---

## Success Criteria & Next Steps

This plan covers **Phases 1-6** of the design document. At this point, you will have:

✅ **WASM Compilation:** Core types compile to `wasm32-unknown-unknown`
✅ **Catalog Trait:** New simplified API defined
✅ **Schema Support:** Full schema types with nesting
✅ **Metadata:** TableMetadata with snapshots
✅ **FileIO:** WASM-compatible storage via OpenDAL
✅ **Table API:** Table struct with transaction support
✅ **Write Foundation:** DataFile types and Transaction.append()

**Remaining work** (to be planned separately):
- Transaction.commit() - Create snapshots, write metadata
- Manifest file types and writing
- Read support (ScanBuilder, Scan)
- Full catalog implementations (R2Catalog, S3TablesCatalog)
- Integration tests with real examples

**Total implemented:** ~15 tasks, ~2,500 lines of vendored code + new APIs

---

## Execution Instructions

**Each task follows TDD:**
1. Write test first
2. Run to see it fail
3. Implement minimal code
4. Run to see it pass
5. Commit

**Verification checkpoints:**
- After Task 4: Verify WASM target setup
- After Task 8: All schema types complete
- After Task 11: FileIO compiles for WASM
- After Task 15: Transaction API ready for commit implementation

**Dependencies:**
- `uuid` - for table UUID generation
- `opendal` - for WASM-compatible storage
- `async-trait` - for async catalog trait
- `serde`, `serde_json` - for metadata serialization

**Testing strategy:**
- Unit tests for each type
- Integration tests using memory backend
- WASM compilation verification at checkpoints
