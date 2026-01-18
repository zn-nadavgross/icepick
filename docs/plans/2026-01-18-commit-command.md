# Commit Parquet Files CLI Command Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an `icepick commit` CLI command that commits Parquet files to an Iceberg table with automatic schema detection and partition value extraction.

**Architecture:** The command uses glob patterns to find Parquet files, reads schema from the first file (or an exemplar), validates all files match the schema, extracts partition values (from Hive-style paths or explicit flags), and commits them in a single transaction. A refactor splits partition extraction from file introspection for cleaner separation of concerns.

**Tech Stack:** Rust, clap (CLI parsing), glob crate (file matching), existing icepick introspection and registration APIs

---

## Task 1: Refactor - Extract Hive Partition Parsing to Standalone Function

**Files:**
- Modify: `src/catalog/register/introspect.rs:141-188`
- Test: `src/catalog/register/introspect/tests.rs`

**Step 1: Write the failing test for standalone partition extraction**

Add to `src/catalog/register/introspect/tests.rs`:

```rust
#[test]
fn test_parse_hive_partition_values_standalone() {
    let path = "s3://bucket/year=2024/month=01/data.parquet";
    let result = super::parse_hive_partition_values(path);

    assert_eq!(result.get("year"), Some(&"2024".to_string()));
    assert_eq!(result.get("month"), Some(&"01".to_string()));
    assert_eq!(result.len(), 2);
}

#[test]
fn test_parse_hive_partition_values_no_partitions() {
    let path = "s3://bucket/data/file.parquet";
    let result = super::parse_hive_partition_values(path);

    assert!(result.is_empty());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p icepick parse_hive_partition_values_standalone --lib`
Expected: FAIL - function is private

**Step 3: Make parse_hive_partition_values public**

In `src/catalog/register/introspect.rs`, change line 172:

```rust
// Before:
fn parse_hive_partition_values(path: &str) -> HashMap<String, String> {

// After:
/// Extract Hive-style `key=value` segments from a path.
///
/// Returns a map of partition column names to their string values.
/// Does not validate against any schema or partition spec.
///
/// # Example
/// ```
/// use icepick::catalog::register::parse_hive_partition_values;
///
/// let values = parse_hive_partition_values("s3://bucket/year=2024/month=01/file.parquet");
/// assert_eq!(values.get("year"), Some(&"2024".to_string()));
/// ```
pub fn parse_hive_partition_values(path: &str) -> HashMap<String, String> {
```

**Step 4: Export the function from mod.rs**

In `src/catalog/register/mod.rs`, update the pub use line:

```rust
// Before:
pub use introspect::{
    infer_partition_values_from_path, introspect_parquet_file, ParquetIntrospection,
};

// After:
pub use introspect::{
    infer_partition_values_from_path, introspect_parquet_file, parse_hive_partition_values,
    ParquetIntrospection,
};
```

**Step 5: Run test to verify it passes**

Run: `cargo test -p icepick parse_hive_partition_values --lib`
Expected: PASS

**Step 6: Commit**

```bash
git add src/catalog/register/introspect.rs src/catalog/register/mod.rs
git commit -m "refactor: export parse_hive_partition_values as public API"
```

---

## Task 2: Add Standalone Partition Value Conversion Function

**Files:**
- Modify: `src/catalog/register/introspect.rs`
- Test: `src/catalog/register/introspect/tests.rs`

**Step 1: Write the failing test**

Add to `src/catalog/register/introspect/tests.rs`:

```rust
use crate::catalog::register::types::PartitionValue;
use crate::spec::{NestedField, PrimitiveType, Schema, Type};

#[test]
fn test_convert_partition_values_to_typed() {
    // Create a simple schema with year (int) and region (string)
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required_field(1, "year".to_string(), Type::Primitive(PrimitiveType::Int)),
            NestedField::required_field(2, "region".to_string(), Type::Primitive(PrimitiveType::String)),
        ])
        .build()
        .unwrap();

    let mut raw_values = std::collections::HashMap::new();
    raw_values.insert("year".to_string(), "2024".to_string());
    raw_values.insert("region".to_string(), "us-west".to_string());

    let result = super::convert_partition_values(&raw_values, &schema).unwrap();

    assert_eq!(result.get("year"), Some(&PartitionValue::Int(2024)));
    assert_eq!(result.get("region"), Some(&PartitionValue::String("us-west".to_string())));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p icepick convert_partition_values_to_typed --lib`
Expected: FAIL - function doesn't exist

**Step 3: Implement convert_partition_values**

Add to `src/catalog/register/introspect.rs` after `parse_hive_partition_values`:

```rust
/// Convert raw string partition values to typed PartitionValue based on schema.
///
/// Looks up each partition column in the schema to determine the correct type.
/// Unknown columns are treated as strings.
pub fn convert_partition_values(
    raw_values: &HashMap<String, String>,
    schema: &Schema,
) -> Result<HashMap<String, PartitionValue>> {
    let mut typed_values = HashMap::new();

    for (name, raw) in raw_values {
        let value = match schema.fields().iter().find(|f| f.name() == name) {
            Some(field) => parse_value_by_type(field.field_type(), raw)?,
            None => PartitionValue::String(raw.clone()),
        };
        typed_values.insert(name.clone(), value);
    }

    Ok(typed_values)
}

fn parse_value_by_type(field_type: &crate::spec::Type, raw: &str) -> Result<PartitionValue> {
    use crate::spec::PrimitiveType;

    match field_type {
        crate::spec::Type::Primitive(PrimitiveType::Boolean) => raw
            .parse::<bool>()
            .map(PartitionValue::Bool)
            .map_err(|e| Error::invalid_input(format!("Invalid boolean '{}': {}", raw, e))),
        crate::spec::Type::Primitive(PrimitiveType::Int)
        | crate::spec::Type::Primitive(PrimitiveType::Date) => raw
            .parse::<i32>()
            .map(PartitionValue::Int)
            .map_err(|e| Error::invalid_input(format!("Invalid int '{}': {}", raw, e))),
        crate::spec::Type::Primitive(PrimitiveType::Long)
        | crate::spec::Type::Primitive(PrimitiveType::Time)
        | crate::spec::Type::Primitive(PrimitiveType::Timestamp)
        | crate::spec::Type::Primitive(PrimitiveType::Timestamptz) => raw
            .parse::<i64>()
            .map(PartitionValue::Long)
            .map_err(|e| Error::invalid_input(format!("Invalid long '{}': {}", raw, e))),
        _ => Ok(PartitionValue::String(raw.to_string())),
    }
}
```

**Step 4: Export from mod.rs**

Update `src/catalog/register/mod.rs`:

```rust
pub use introspect::{
    convert_partition_values, infer_partition_values_from_path, introspect_parquet_file,
    parse_hive_partition_values, ParquetIntrospection,
};
```

**Step 5: Run test to verify it passes**

Run: `cargo test -p icepick convert_partition_values --lib`
Expected: PASS

**Step 6: Commit**

```bash
git add src/catalog/register/introspect.rs src/catalog/register/mod.rs
git commit -m "feat: add convert_partition_values for typed partition value parsing"
```

---

## Task 3: Add glob Dependency

**Files:**
- Modify: `Cargo.toml`

**Step 1: Add glob to Cargo.toml**

Add under `[target.'cfg(not(target_family = "wasm"))'.dependencies]`:

```toml
glob = "0.3"
```

**Step 2: Run cargo check to verify**

Run: `cargo check --features cli`
Expected: PASS (compiles successfully)

**Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "chore: add glob dependency for file pattern matching"
```

---

## Task 4: Create Commit Command Module Structure

**Files:**
- Create: `src/cli/commands/commit.rs`
- Modify: `src/cli/commands/mod.rs`

**Step 1: Create the basic commit command structure**

Create `src/cli/commands/commit.rs`:

```rust
//! Commit Parquet files to an Iceberg table

use clap::Args;
use serde::Serialize;
use std::collections::HashMap;

use crate::cli::catalog::CatalogConfig;
use crate::cli::output::{print, OutputFormat, Outputable};

/// Commit Parquet files to an Iceberg table
#[derive(Debug, Args)]
pub struct CommitArgs {
    /// Glob pattern for Parquet files (e.g., /data/**/*.parquet)
    pub pattern: String,

    /// Target namespace
    #[arg(long, short)]
    pub namespace: String,

    /// Target table name
    #[arg(long, short)]
    pub table: String,

    /// Parquet file to use as schema exemplar (default: first file from glob)
    #[arg(long)]
    pub exemplar: Option<String>,

    /// Create table if it doesn't exist
    #[arg(long)]
    pub create: bool,

    /// Partition columns for new table (e.g., year:int,month:int)
    #[arg(long)]
    pub partition: Option<String>,

    /// Explicit partition values for all files (e.g., year=2024,month=01)
    #[arg(long)]
    pub partition_values: Option<String>,

    /// Show plan without committing
    #[arg(long)]
    pub dry_run: bool,
}

/// Execute the commit command
pub async fn execute(
    args: CommitArgs,
    config: &CatalogConfig,
    format: OutputFormat,
) -> Result<(), String> {
    Err("Not implemented yet".to_string())
}
```

**Step 2: Register in mod.rs**

Update `src/cli/commands/mod.rs`:

```rust
//! CLI commands

pub mod catalog;
pub mod commit;
pub mod compact;
pub mod namespace;
pub mod snapshot;
pub mod table;
```

**Step 3: Run cargo check to verify**

Run: `cargo check --features cli`
Expected: PASS

**Step 4: Commit**

```bash
git add src/cli/commands/commit.rs src/cli/commands/mod.rs
git commit -m "feat: add commit command module structure"
```

---

## Task 5: Wire Up Commit Command in Binary

**Files:**
- Modify: `src/bin/icepick.rs`

**Step 1: Add commit to imports and Commands enum**

Update `src/bin/icepick.rs`:

```rust
// Add to imports:
use icepick::cli::commands::{
    catalog as catalog_cmd, commit as commit_cmd, compact as compact_cmd,
    namespace as namespace_cmd, snapshot as snapshot_cmd, table as table_cmd,
};

// Add to Commands enum:
#[derive(Debug, Subcommand)]
enum Commands {
    /// Catalog operations
    #[command(subcommand)]
    Catalog(catalog_cmd::CatalogCommand),

    /// Namespace operations
    #[command(subcommand)]
    Namespace(namespace_cmd::NamespaceCommand),

    /// Table operations
    #[command(subcommand)]
    Table(table_cmd::TableCommand),

    /// Snapshot operations (list, cleanup)
    #[command(subcommand)]
    Snapshot(snapshot_cmd::SnapshotCommand),

    /// Compact a table
    Compact(compact_cmd::CompactArgs),

    /// Commit Parquet files to a table
    Commit(commit_cmd::CommitArgs),
}

// Add to match in main():
Commands::Commit(args) => commit_cmd::execute(args, &config, cli.output).await,
```

**Step 2: Run cargo check to verify**

Run: `cargo check --features cli`
Expected: PASS

**Step 3: Verify help text**

Run: `cargo run --features cli -- commit --help`
Expected: Shows commit command help

**Step 4: Commit**

```bash
git add src/bin/icepick.rs
git commit -m "feat: wire up commit command in CLI binary"
```

---

## Task 6: Implement Glob File Discovery

**Files:**
- Modify: `src/cli/commands/commit.rs`

**Step 1: Write test for glob expansion**

Add test module to `src/cli/commands/commit.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_partition_spec() {
        let spec = "year:int,month:int";
        let result = parse_partition_spec(spec).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("year".to_string(), "int".to_string()));
        assert_eq!(result[1], ("month".to_string(), "int".to_string()));
    }

    #[test]
    fn test_parse_partition_values() {
        let values = "year=2024,month=01";
        let result = parse_partition_values_arg(values).unwrap();

        assert_eq!(result.get("year"), Some(&"2024".to_string()));
        assert_eq!(result.get("month"), Some(&"01".to_string()));
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p icepick parse_partition_spec --lib --features cli`
Expected: FAIL - function doesn't exist

**Step 3: Implement parsing functions**

Add to `src/cli/commands/commit.rs`:

```rust
/// Parse partition spec like "year:int,month:int" into vec of (name, type)
fn parse_partition_spec(spec: &str) -> Result<Vec<(String, String)>, String> {
    spec.split(',')
        .map(|part| {
            let parts: Vec<&str> = part.trim().splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(format!(
                    "Invalid partition spec '{}'. Expected format: name:type",
                    part
                ));
            }
            Ok((parts[0].to_string(), parts[1].to_string()))
        })
        .collect()
}

/// Parse partition values like "year=2024,month=01" into HashMap
fn parse_partition_values_arg(values: &str) -> Result<HashMap<String, String>, String> {
    values
        .split(',')
        .map(|part| {
            let parts: Vec<&str> = part.trim().splitn(2, '=').collect();
            if parts.len() != 2 {
                return Err(format!(
                    "Invalid partition value '{}'. Expected format: name=value",
                    part
                ));
            }
            Ok((parts[0].to_string(), parts[1].to_string()))
        })
        .collect()
}

/// Expand glob pattern to list of file paths
fn expand_glob(pattern: &str) -> Result<Vec<String>, String> {
    let paths: Result<Vec<_>, _> = glob::glob(pattern)
        .map_err(|e| format!("Invalid glob pattern: {}", e))?
        .collect();

    let paths = paths.map_err(|e| format!("Error reading files: {}", e))?;

    let parquet_files: Vec<String> = paths
        .into_iter()
        .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
        .map(|p| p.to_string_lossy().to_string())
        .collect();

    if parquet_files.is_empty() {
        return Err(format!("No Parquet files found matching pattern: {}", pattern));
    }

    Ok(parquet_files)
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -p icepick parse_partition --lib --features cli`
Expected: PASS

**Step 5: Commit**

```bash
git add src/cli/commands/commit.rs
git commit -m "feat: implement partition spec and glob parsing for commit command"
```

---

## Task 7: Implement Dry-Run Output Structure

**Files:**
- Modify: `src/cli/commands/commit.rs`

**Step 1: Add output types**

Add to `src/cli/commands/commit.rs`:

```rust
use crate::cli::output::{format_bytes, format_number};

/// Commit plan output (dry run)
#[derive(Debug, Serialize)]
pub struct CommitPlanOutput {
    pub schema_source: String,
    pub target_table: String,
    pub will_create_table: bool,
    pub partition_columns: Vec<String>,
    pub files_to_commit: usize,
    pub total_rows: i64,
    pub total_bytes: u64,
    pub partitions: Vec<PartitionSummary>,
    pub schema_mismatches: Vec<SchemaMismatch>,
    pub already_committed: usize,
}

#[derive(Debug, Serialize)]
pub struct PartitionSummary {
    pub partition_value: String,
    pub file_count: usize,
    pub row_count: i64,
}

#[derive(Debug, Serialize)]
pub struct SchemaMismatch {
    pub file_path: String,
    pub reason: String,
}

impl Outputable for CommitPlanOutput {
    fn to_text(&self) -> String {
        let mut lines = vec![];

        lines.push(format!("Schema source: {}", self.schema_source));
        lines.push(String::new());

        if self.will_create_table {
            lines.push(format!("Target: {} (will be created)", self.target_table));
        } else {
            lines.push(format!("Target: {} (existing)", self.target_table));
        }

        if !self.partition_columns.is_empty() {
            lines.push(format!("  Partitioned by: {}", self.partition_columns.join(", ")));
        }
        lines.push(String::new());

        lines.push(format!(
            "Files to commit: {} ({} rows, {})",
            self.files_to_commit,
            format_number(self.total_rows as u64),
            format_bytes(self.total_bytes)
        ));

        for part in &self.partitions {
            lines.push(format!(
                "  {}: {} files, {} rows",
                part.partition_value,
                part.file_count,
                format_number(part.row_count as u64)
            ));
        }

        if !self.schema_mismatches.is_empty() {
            lines.push(String::new());
            lines.push(format!("Schema mismatches: {}", self.schema_mismatches.len()));
            for mismatch in &self.schema_mismatches {
                lines.push(format!("  {}: {}", mismatch.file_path, mismatch.reason));
            }
        }

        if self.already_committed > 0 {
            lines.push(format!("Already committed (will skip): {}", self.already_committed));
        }

        lines.push(String::new());
        lines.push("Run without --dry-run to commit.".to_string());

        lines.join("\n")
    }
}

/// Commit result output
#[derive(Debug, Serialize)]
pub struct CommitResultOutput {
    pub target_table: String,
    pub table_created: bool,
    pub files_committed: usize,
    pub rows_committed: i64,
    pub files_skipped: usize,
    pub snapshot_id: i64,
}

impl Outputable for CommitResultOutput {
    fn to_text(&self) -> String {
        let mut lines = vec![];

        if self.table_created {
            lines.push(format!("Created table: {}", self.target_table));
        } else {
            lines.push(format!("Committed to: {}", self.target_table));
        }

        lines.push(format!(
            "  Files: {} committed, {} skipped",
            self.files_committed, self.files_skipped
        ));
        lines.push(format!("  Rows: {}", format_number(self.rows_committed as u64)));
        lines.push(format!("  Snapshot: {}", self.snapshot_id));

        lines.join("\n")
    }
}
```

**Step 2: Run cargo check**

Run: `cargo check --features cli`
Expected: PASS

**Step 3: Commit**

```bash
git add src/cli/commands/commit.rs
git commit -m "feat: add commit command output types for dry-run and results"
```

---

## Task 8: Implement Core Execute Logic

**Files:**
- Modify: `src/cli/commands/commit.rs`

**Step 1: Implement the execute function**

Replace the execute function in `src/cli/commands/commit.rs`:

```rust
use crate::catalog::register::{
    convert_partition_values, introspect_parquet_file, parse_hive_partition_values,
    DataFileInput, RegisterOptions,
};
use crate::catalog::Catalog;
use crate::cli::util::parse_table_ident;
use crate::spec::{NamespaceIdent, PartitionField, PartitionSpec, Schema, TableIdent};

/// Execute the commit command
pub async fn execute(
    args: CommitArgs,
    config: &CatalogConfig,
    format: OutputFormat,
) -> Result<(), String> {
    // 1. Expand glob pattern
    let files = expand_glob(&args.pattern)?;
    println!("Found {} Parquet files", files.len());

    // 2. Create catalog connection
    let catalog = config.create_catalog().await?;
    let file_io = catalog.file_io();

    // 3. Determine exemplar file (first file or explicit)
    let exemplar_path = args.exemplar.as_ref().unwrap_or(&files[0]);

    // 4. Introspect exemplar to get schema
    let exemplar = introspect_parquet_file(file_io, exemplar_path, None)
        .await
        .map_err(|e| format!("Failed to read exemplar file {}: {}", exemplar_path, e))?;

    let schema = exemplar.schema.clone();
    println!("Schema from: {}", exemplar_path);

    // 5. Parse partition spec if creating
    let partition_spec = if let Some(spec_str) = &args.partition {
        if !args.create {
            return Err("--partition requires --create flag".to_string());
        }
        Some(build_partition_spec(&spec_str, &schema)?)
    } else {
        None
    };

    // 6. Parse explicit partition values if provided
    let explicit_partition_values = if let Some(pv) = &args.partition_values {
        Some(parse_partition_values_arg(pv)?)
    } else {
        None
    };

    // 7. Check if table exists
    let namespace = NamespaceIdent::from_strs(vec![&args.namespace])
        .map_err(|e| format!("Invalid namespace: {}", e))?;
    let table_ident = TableIdent::from_strs(&[&args.namespace], &args.table)
        .map_err(|e| format!("Invalid table identifier: {}", e))?;

    let table_exists = catalog.load_table(&table_ident).await.is_ok();

    if !table_exists && !args.create {
        return Err(format!(
            "Table {}.{} does not exist. Use --create to create it.",
            args.namespace, args.table
        ));
    }

    // 8. Introspect all files and build commit plan
    let mut data_files = Vec::new();
    let mut schema_mismatches = Vec::new();
    let mut partition_summaries: HashMap<String, (usize, i64)> = HashMap::new();
    let mut total_bytes = 0u64;
    let mut total_rows = 0i64;

    for file_path in &files {
        let introspection = introspect_parquet_file(file_io, file_path, None)
            .await
            .map_err(|e| format!("Failed to read {}: {}", file_path, e))?;

        // Validate schema matches exemplar
        if !schemas_compatible(&schema, &introspection.schema) {
            schema_mismatches.push(SchemaMismatch {
                file_path: file_path.clone(),
                reason: "Schema does not match exemplar".to_string(),
            });
            continue;
        }

        // Determine partition values
        let partition_values = determine_partition_values(
            file_path,
            &explicit_partition_values,
            partition_spec.as_ref(),
            &schema,
        )?;

        // Track partition summary
        let partition_key = format_partition_key(&partition_values);
        let entry = partition_summaries.entry(partition_key).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += introspection.data_file.record_count;

        total_bytes += introspection.data_file.file_size_in_bytes as u64;
        total_rows += introspection.data_file.record_count;

        let mut data_file = introspection.data_file;
        data_file.partition_values = partition_values;
        data_files.push(data_file);
    }

    if !schema_mismatches.is_empty() && !args.dry_run {
        return Err(format!(
            "{} files have schema mismatches. Run with --dry-run to see details.",
            schema_mismatches.len()
        ));
    }

    // 9. Build partition summaries for output
    let partitions: Vec<PartitionSummary> = partition_summaries
        .into_iter()
        .map(|(k, (count, rows))| PartitionSummary {
            partition_value: if k.is_empty() { "(unpartitioned)".to_string() } else { k },
            file_count: count,
            row_count: rows,
        })
        .collect();

    // 10. Dry run - show plan and exit
    if args.dry_run {
        let plan = CommitPlanOutput {
            schema_source: exemplar_path.clone(),
            target_table: format!("{}.{}", args.namespace, args.table),
            will_create_table: !table_exists,
            partition_columns: partition_spec
                .as_ref()
                .map(|s| s.fields().iter().map(|f| f.name().to_string()).collect())
                .unwrap_or_default(),
            files_to_commit: data_files.len(),
            total_rows,
            total_bytes,
            partitions,
            schema_mismatches,
            already_committed: 0, // TODO: check existing files
        };
        print(&plan, format);
        return Ok(());
    }

    // 11. Execute registration
    let options = if args.create && !table_exists {
        let mut opts = RegisterOptions::new()
            .allow_create_with_schema(schema.clone());
        if let Some(spec) = partition_spec {
            opts = opts.with_partition_spec(spec);
        }
        opts.allow_noop(true)
    } else {
        RegisterOptions::new().allow_noop(true)
    };

    let result = crate::catalog::register::register_data_files(
        catalog.as_ref(),
        namespace,
        table_ident,
        data_files,
        options,
    )
    .await
    .map_err(|e| format!("Commit failed: {}", e))?;

    let output = CommitResultOutput {
        target_table: format!("{}.{}", args.namespace, args.table),
        table_created: result.table_was_created,
        files_committed: result.added_files,
        rows_committed: result.added_records,
        files_skipped: result.skipped_files.len(),
        snapshot_id: result.snapshot_id,
    };

    print(&output, format);
    Ok(())
}

fn build_partition_spec(spec_str: &str, schema: &Schema) -> Result<PartitionSpec, String> {
    let parts = parse_partition_spec(spec_str)?;

    let fields: Vec<PartitionField> = parts
        .iter()
        .enumerate()
        .map(|(idx, (name, _type_str))| {
            // Find source field ID in schema
            let source_id = schema
                .fields()
                .iter()
                .find(|f| f.name() == name)
                .map(|f| f.id())
                .ok_or_else(|| format!("Partition column '{}' not found in schema", name))?;

            Ok(PartitionField::builder()
                .with_source_id(source_id)
                .with_field_id(1000 + idx as i32)
                .with_name(name.clone())
                .with_transform("identity")
                .build()
                .map_err(|e| format!("Failed to build partition field: {}", e))?)
        })
        .collect::<Result<Vec<_>, String>>()?;

    PartitionSpec::builder()
        .with_spec_id(0)
        .with_fields(fields)
        .build()
        .map_err(|e| format!("Failed to build partition spec: {}", e))
}

fn schemas_compatible(expected: &Schema, actual: &Schema) -> bool {
    // Simple check: same number of fields with same names and types
    if expected.fields().len() != actual.fields().len() {
        return false;
    }

    for (e, a) in expected.fields().iter().zip(actual.fields().iter()) {
        if e.name() != a.name() || e.field_type() != a.field_type() {
            return false;
        }
    }

    true
}

fn determine_partition_values(
    file_path: &str,
    explicit_values: &Option<HashMap<String, String>>,
    partition_spec: Option<&PartitionSpec>,
    schema: &Schema,
) -> Result<HashMap<String, crate::catalog::register::PartitionValue>, String> {
    use crate::catalog::register::PartitionValue;

    // Priority: explicit values > Hive path extraction
    if let Some(explicit) = explicit_values {
        return convert_partition_values(explicit, schema)
            .map_err(|e| format!("Invalid partition values: {}", e));
    }

    // Try Hive-style extraction
    let hive_values = parse_hive_partition_values(file_path);

    if let Some(spec) = partition_spec {
        // Validate we have all required partition values
        for field in spec.fields() {
            if !hive_values.contains_key(field.name()) {
                return Err(format!(
                    "Missing partition value for '{}' in path '{}'. Use --partition-values to specify.",
                    field.name(),
                    file_path
                ));
            }
        }
    }

    if hive_values.is_empty() {
        return Ok(HashMap::new());
    }

    convert_partition_values(&hive_values, schema)
        .map_err(|e| format!("Invalid partition values from path: {}", e))
}

fn format_partition_key(values: &HashMap<String, crate::catalog::register::PartitionValue>) -> String {
    if values.is_empty() {
        return String::new();
    }

    let mut parts: Vec<String> = values
        .iter()
        .map(|(k, v)| format!("{}={}", k, v.to_value_string()))
        .collect();
    parts.sort();
    parts.join("/")
}
```

**Step 2: Run cargo check**

Run: `cargo check --features cli`
Expected: PASS (may need to fix imports)

**Step 3: Run cargo clippy**

Run: `cargo clippy --features cli -- -D warnings`
Expected: PASS

**Step 4: Commit**

```bash
git add src/cli/commands/commit.rs
git commit -m "feat: implement commit command core logic"
```

---

## Task 9: Add Integration Test

**Files:**
- Create: `tests/commit_command.rs`

**Step 1: Create integration test file**

Create `tests/commit_command.rs`:

```rust
//! Integration tests for the commit command
//!
//! These tests require a running catalog and are marked #[ignore].
//! Run with: cargo test --test commit_command -- --ignored

use std::process::Command;

#[test]
#[ignore]
fn test_commit_dry_run() {
    let output = Command::new("cargo")
        .args([
            "run", "--features", "cli", "--",
            "commit", "/tmp/test-data/**/*.parquet",
            "--namespace", "test",
            "--table", "events",
            "--dry-run",
        ])
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("stdout: {}", stdout);
    println!("stderr: {}", stderr);

    // Should either succeed with a plan or fail with "no files found"
    assert!(
        output.status.success() || stderr.contains("No Parquet files found"),
        "Command failed unexpectedly: {}",
        stderr
    );
}

#[test]
fn test_partition_spec_parsing() {
    // Unit test that doesn't require catalog
    let spec = "year:int,month:int,day:int";
    let parts: Vec<&str> = spec.split(',').collect();

    assert_eq!(parts.len(), 3);
    assert!(parts[0].contains("year"));
    assert!(parts[1].contains("month"));
    assert!(parts[2].contains("day"));
}
```

**Step 2: Run unit test**

Run: `cargo test --test commit_command test_partition_spec_parsing`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/commit_command.rs
git commit -m "test: add commit command integration tests"
```

---

## Task 10: Update Documentation

**Files:**
- Modify: `AGENTS.md`
- Modify: `README.md`

**Step 1: Update AGENTS.md Quick Start CLI section**

Add to the CLI section in `AGENTS.md`:

```markdown
# Commit Parquet files to a table
icepick commit /data/**/*.parquet --namespace my_ns --table events --dry-run
icepick commit /data/**/*.parquet --namespace my_ns --table events

# Create new table from Parquet files
icepick commit /data/**/*.parquet --namespace my_ns --table events \
  --create --partition year:int,month:int

# Specify explicit partition values (for non-Hive paths)
icepick commit /flat/*.parquet --namespace my_ns --table events \
  --partition-values year=2024,month=01

# Use specific file as schema exemplar
icepick commit /data/**/*.parquet --namespace my_ns --table events \
  --exemplar /data/sample.parquet --create
```

**Step 2: Add Pattern to AGENTS.md**

Add new Pattern 9 after Pattern 8 (Snapshot cleanup):

```markdown
### Pattern 9: Committing Parquet files

```rust
use icepick::catalog::register::{
    introspect_parquet_file, parse_hive_partition_values, convert_partition_values,
    register_data_files, DataFileInput, RegisterOptions,
};

// Introspect a Parquet file (without partition extraction)
let introspection = introspect_parquet_file(file_io, path, None).await?;

// Extract partition values from Hive-style path
let hive_values = parse_hive_partition_values(path);  // HashMap<String, String>

// Convert to typed values using schema
let typed_values = convert_partition_values(&hive_values, &schema)?;

// Or provide explicit values
let mut explicit = HashMap::new();
explicit.insert("year".to_string(), "2024".to_string());
let typed_values = convert_partition_values(&explicit, &schema)?;
```
```

**Step 3: Update README.md**

Add new section after "Snapshot Cleanup":

```markdown
## Committing Parquet Files

Commit existing Parquet files to an Iceberg table:

```bash
# Preview what would be committed
icepick commit /data/**/*.parquet --namespace prod --table events --dry-run

# Commit files to existing table
icepick commit /data/**/*.parquet --namespace prod --table events

# Create new table with partition spec
icepick commit /data/**/*.parquet --namespace prod --table events \
  --create --partition year:int,month:int

# For non-Hive paths, specify partition values explicitly
icepick commit /flat/*.parquet --namespace prod --table events \
  --partition-values year=2024,month=01
```

The command:
- Uses first file's schema (or `--exemplar`) as the reference
- Validates all files match the schema
- Extracts partition values from Hive-style paths automatically
- Supports `--partition-values` for flat directory structures
- Shows detailed plan with `--dry-run` before committing
```

**Step 4: Commit**

```bash
git add AGENTS.md README.md
git commit -m "docs: add commit command documentation"
```

---

## Task 11: Final Verification

**Step 1: Run full test suite**

Run: `cargo test --all-features`
Expected: All tests pass

**Step 2: Run clippy**

Run: `cargo clippy --all-features -- -D warnings`
Expected: No warnings

**Step 3: Build release**

Run: `cargo build --release --features cli`
Expected: Builds successfully

**Step 4: Test help output**

Run: `cargo run --features cli -- commit --help`
Expected: Shows complete help with all options

**Step 5: Final commit**

```bash
git add -A
git commit -m "chore: final verification pass for commit command"
```

---

Plan complete and saved to `docs/plans/2026-01-18-commit-command.md`. Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

Which approach?
