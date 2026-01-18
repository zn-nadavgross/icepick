//! Commit Parquet files to an Iceberg table

use std::collections::HashMap;

use clap::Args;
use serde::Serialize;

use crate::cli::catalog::CatalogConfig;
use crate::cli::output::{format_bytes, format_number, OutputFormat, Outputable};

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

        if self.already_committed > 0 {
            lines.push(format!(
                "Already committed (will skip): {}",
                self.already_committed
            ));
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

/// Parse partition spec like "year:int,month:int" into vec of (name, type)
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
        return Err(format!(
            "No Parquet files found matching pattern: {}",
            pattern
        ));
    }

    Ok(parquet_files)
}

/// Execute the commit command
pub async fn execute(
    _args: CommitArgs,
    _config: &CatalogConfig,
    _format: OutputFormat,
) -> Result<(), String> {
    Err("Not implemented yet".to_string())
}

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
