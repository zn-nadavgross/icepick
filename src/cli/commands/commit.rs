//! Commit Parquet files to an Iceberg table

use std::collections::HashMap;

use clap::Args;
use serde::Serialize;

use crate::catalog::register::{
    convert_partition_values, introspect_parquet_file, parse_hive_partition_values, DataFileInput,
    PartitionValue, RegisterOptions,
};
use crate::cli::catalog::CatalogConfig;
use crate::cli::output::{format_bytes, format_number, print, OutputFormat, Outputable};
use crate::spec::{
    NamespaceIdent, PartitionField, PartitionSpec, PrimitiveType, Schema, TableIdent, Type,
};

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
    #[arg(long, requires = "create")]
    pub partition: Option<String>,

    /// Explicit partition values for all files (e.g., year=2024,month=01)
    #[arg(long)]
    pub partition_values: Option<String>,

    /// Show plan without committing
    #[arg(long)]
    pub dry_run: bool,
}

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
            lines.push(format!(
                "  Partitioned by: {}",
                self.partition_columns.join(", ")
            ));
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
            lines.push(format!(
                "Schema mismatches: {}",
                self.schema_mismatches.len()
            ));
            for mismatch in &self.schema_mismatches {
                lines.push(format!("  {}: {}", mismatch.file_path, mismatch.reason));
            }
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
        lines.push(format!(
            "  Rows: {}",
            format_number(self.rows_committed as u64)
        ));
        lines.push(format!("  Snapshot: {}", self.snapshot_id));

        lines.join("\n")
    }
}

/// Parse a type string into a PrimitiveType
pub fn parse_type_str(type_str: &str) -> Result<PrimitiveType, String> {
    match type_str.to_lowercase().as_str() {
        "boolean" | "bool" => Ok(PrimitiveType::Boolean),
        "int" | "integer" => Ok(PrimitiveType::Int),
        "long" | "bigint" => Ok(PrimitiveType::Long),
        "float" => Ok(PrimitiveType::Float),
        "double" => Ok(PrimitiveType::Double),
        "date" => Ok(PrimitiveType::Date),
        "time" => Ok(PrimitiveType::Time),
        "timestamp" => Ok(PrimitiveType::Timestamp),
        "timestamptz" => Ok(PrimitiveType::Timestamptz),
        "string" => Ok(PrimitiveType::String),
        "uuid" => Ok(PrimitiveType::Uuid),
        "binary" => Ok(PrimitiveType::Binary),
        _ => Err(format!(
            "Unknown type '{}'. Valid types: boolean, int, long, float, double, date, time, timestamp, timestamptz, string, uuid, binary",
            type_str
        )),
    }
}

/// Parse partition spec like "year:int,month:int" into vec of (name, type)
pub fn parse_partition_spec(spec: &str) -> Result<Vec<(String, PrimitiveType)>, String> {
    spec.split(',')
        .map(|part| {
            let parts: Vec<&str> = part.trim().splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(format!(
                    "Invalid partition spec '{}'. Expected format: name:type",
                    part
                ));
            }
            let parsed_type = parse_type_str(parts[1])?;
            Ok((parts[0].to_string(), parsed_type))
        })
        .collect()
}

/// Parse partition values like "year=2024,month=01" into HashMap
pub fn parse_partition_values_arg(values: &str) -> Result<HashMap<String, String>, String> {
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
        .map_err(|e| format!("Invalid glob pattern '{}': {}", pattern, e))?
        .collect();

    let paths = paths.map_err(|e| format!("Error reading files matching '{}': {}", pattern, e))?;

    let parquet_files: Vec<String> = paths
        .into_iter()
        .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
        .map(|p| p.to_string_lossy().to_string())
        .collect();

    if parquet_files.is_empty() {
        return Err(format!(
            "No Parquet files found matching pattern: {}",
            pattern
        ));
    }

    Ok(parquet_files)
}

/// Build a partition spec from a spec string and schema
pub fn build_partition_spec(spec_str: &str, schema: &Schema) -> Result<PartitionSpec, String> {
    let parts = parse_partition_spec(spec_str)?;

    let fields: Vec<PartitionField> = parts
        .iter()
        .enumerate()
        .map(|(idx, (name, expected_type))| {
            // Find source field in schema
            let field = schema
                .fields()
                .iter()
                .find(|f| f.name() == name)
                .ok_or_else(|| format!("Partition column '{}' not found in schema", name))?;

            // Validate type matches schema
            match field.field_type() {
                Type::Primitive(actual_type) => {
                    if actual_type != expected_type {
                        return Err(format!(
                            "Partition column '{}' type mismatch: specified {:?} but schema has {:?}",
                            name, expected_type, actual_type
                        ));
                    }
                }
                other => {
                    return Err(format!(
                        "Partition column '{}' must be a primitive type, got {:?}",
                        name, other
                    ));
                }
            }

            Ok(PartitionField::new(
                1000 + idx as i32, // field_id
                field.id(),        // source_id
                "identity",        // transform
                name.clone(),      // name
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(PartitionSpec::new(0, fields))
}

/// Check if two schemas are compatible for registration.
/// Returns Ok(()) if compatible, or Err with details about the mismatch.
pub fn check_schema_compatibility(expected: &Schema, actual: &Schema) -> Result<(), String> {
    // Check field count
    if expected.fields().len() != actual.fields().len() {
        return Err(format!(
            "field count mismatch: expected {} fields, got {}",
            expected.fields().len(),
            actual.fields().len()
        ));
    }

    // Check each field
    for (e, a) in expected.fields().iter().zip(actual.fields().iter()) {
        if e.name() != a.name() {
            return Err(format!(
                "field name mismatch at position: expected '{}', got '{}'",
                e.name(),
                a.name()
            ));
        }
        if e.field_type() != a.field_type() {
            return Err(format!(
                "field '{}' type mismatch: expected {:?}, got {:?}",
                e.name(),
                e.field_type(),
                a.field_type()
            ));
        }
    }

    Ok(())
}

/// Determine partition values for a file
fn determine_partition_values(
    file_path: &str,
    explicit_values: &Option<HashMap<String, String>>,
    partition_spec: Option<&PartitionSpec>,
    schema: &Schema,
) -> Result<HashMap<String, PartitionValue>, String> {
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

/// Format partition values as a key string for grouping
fn format_partition_key(values: &HashMap<String, PartitionValue>) -> String {
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

/// Execute the commit command
pub async fn execute(
    args: CommitArgs,
    config: &CatalogConfig,
    format: OutputFormat,
) -> Result<(), String> {
    let files = expand_glob(&args.pattern)?;
    println!("Found {} Parquet files", files.len());

    let catalog = config.create_catalog().await?;
    let file_io = catalog.file_io();
    let exemplar_path = args.exemplar.as_ref().unwrap_or(&files[0]);
    let exemplar = introspect_parquet_file(file_io, exemplar_path, None)
        .await
        .map_err(|e| format!("Failed to read exemplar file {}: {}", exemplar_path, e))?;
    let schema = exemplar.schema.clone();
    println!("Schema from: {}", exemplar_path);

    let partition_spec = args
        .partition
        .as_ref()
        .map(|s| build_partition_spec(s, &schema))
        .transpose()?;
    let explicit_partition_values = args
        .partition_values
        .as_ref()
        .map(|pv| parse_partition_values_arg(pv))
        .transpose()?;

    if args.namespace.is_empty() {
        return Err("Namespace cannot be empty".to_string());
    }
    if args.table.is_empty() {
        return Err("Table name cannot be empty".to_string());
    }
    let namespace = NamespaceIdent::from_strs(&[args.namespace.as_str()]);
    let table_ident = TableIdent::from_strs(&[args.namespace.as_str()], &args.table);

    let table_exists = catalog
        .table_exists(&table_ident)
        .await
        .map_err(|e| format!("Failed to check if table exists: {}", e))?;

    if !table_exists && !args.create {
        return Err(format!(
            "Table {}.{} does not exist. Use --create to create it.",
            args.namespace, args.table
        ));
    }

    let mut data_files: Vec<DataFileInput> = Vec::new();
    let mut schema_mismatches = Vec::new();
    let mut partition_summaries: HashMap<String, (usize, i64)> = HashMap::new();
    let mut total_bytes = 0u64;
    let mut total_rows = 0i64;

    for file_path in &files {
        let introspection = introspect_parquet_file(file_io, file_path, None)
            .await
            .map_err(|e| format!("Failed to read {}: {}", file_path, e))?;

        if let Err(mismatch_reason) = check_schema_compatibility(&schema, &introspection.schema) {
            schema_mismatches.push(SchemaMismatch {
                file_path: file_path.clone(),
                reason: mismatch_reason,
            });
            continue;
        }

        let partition_values = determine_partition_values(
            file_path,
            &explicit_partition_values,
            partition_spec.as_ref(),
            &schema,
        )?;

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

    let partitions: Vec<PartitionSummary> = partition_summaries
        .into_iter()
        .map(|(k, (count, rows))| PartitionSummary {
            partition_value: if k.is_empty() {
                "(unpartitioned)".to_string()
            } else {
                k
            },
            file_count: count,
            row_count: rows,
        })
        .collect();

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
        };
        print(&plan, format);
        return Ok(());
    }

    let options = if args.create && !table_exists {
        let mut opts = RegisterOptions::new().allow_create_with_schema(schema.clone());
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
