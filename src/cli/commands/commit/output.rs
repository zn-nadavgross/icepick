//! Output types for the commit command

use serde::Serialize;

use crate::cli::output::{format_bytes, format_number, Outputable};

/// Commit plan output (dry run)
#[derive(Debug, Serialize)]
pub struct CommitPlanOutput {
    pub schema_source: String,
    pub target_table: String,
    pub will_create_table: bool,
    pub partition_columns: Vec<String>,
    pub files_to_commit: usize,
    pub files_to_upload: usize,
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

        if self.files_to_upload > 0 {
            lines.push(format!(
                "Files to upload: {} local files",
                self.files_to_upload
            ));
        }

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
