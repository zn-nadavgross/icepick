//! Commit Parquet files to an Iceberg table

use std::collections::HashMap;

use clap::Args;

use crate::cli::catalog::CatalogConfig;
use crate::cli::output::OutputFormat;

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
