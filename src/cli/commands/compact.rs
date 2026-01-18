//! Compact command

use crate::cli::catalog::CatalogConfig;
use crate::cli::output::{
    format_bytes, format_number, format_percentage, print, OutputFormat, Outputable,
};
use crate::cli::util::parse_table_ident;
use crate::compact::{execute_compaction, plan_compaction, CompactOptions, CompactionPlan};
use clap::Args;
use serde::Serialize;

/// Compact command arguments
#[derive(Debug, Args)]
pub struct CompactArgs {
    /// Table identifier (namespace.table)
    pub table: String,

    /// Target size for output files in bytes (default: 256MB)
    #[arg(long, default_value = "268435456")]
    pub target_size: u64,

    /// Maximum input file size to consider for compaction in bytes (default: 128MB)
    #[arg(long, default_value = "134217728")]
    pub max_input_size: u64,

    /// Minimum files per group to trigger compaction (default: 3)
    #[arg(long, default_value = "3")]
    pub min_files: usize,

    /// Only compact this partition
    #[arg(long, short)]
    pub partition: Option<String>,

    /// Show plan without executing
    #[arg(long)]
    pub dry_run: bool,
}

/// Compaction plan output (dry run)
#[derive(Debug, Serialize)]
pub struct CompactionPlanOutput {
    pub table: String,
    pub partitions: Vec<PartitionPlanOutput>,
    pub total_input_files: usize,
    pub estimated_output_files: usize,
    pub total_input_bytes: u64,
    pub dry_run: bool,
}

#[derive(Debug, Serialize)]
pub struct PartitionPlanOutput {
    pub partition: Option<String>,
    pub input_files: usize,
    pub input_bytes: u64,
    pub estimated_output_files: usize,
    pub avg_file_size: u64,
}

impl Outputable for CompactionPlanOutput {
    fn to_text(&self) -> String {
        let mut lines = vec![format!("Compaction Plan for {}", self.table), String::new()];

        for part in &self.partitions {
            let partition_name = part
                .partition
                .as_ref()
                .map(|s| format!("Partition: {}", s))
                .unwrap_or_else(|| "Partition: (unpartitioned)".to_string());
            lines.push(partition_name);
            lines.push(format!(
                "  Input:  {} files, {} (avg {} /file)",
                part.input_files,
                format_bytes(part.input_bytes),
                format_bytes(part.avg_file_size)
            ));
            lines.push(format!(
                "  Output: ~{} files (target {})",
                part.estimated_output_files,
                format_bytes(self.total_input_bytes / self.estimated_output_files.max(1) as u64)
            ));
            lines.push(String::new());
        }

        let reduction = if self.total_input_files > 0 {
            let reduction_pct = 100.0
                - (self.estimated_output_files as f64 / self.total_input_files as f64 * 100.0);
            format!("{:.0}% reduction", reduction_pct)
        } else {
            "0% reduction".to_string()
        };

        lines.push("Summary".to_string());
        lines.push(format!(
            "  Files:   {} -> ~{} ({})",
            self.total_input_files, self.estimated_output_files, reduction
        ));
        lines.push(format!(
            "  Bytes:   {} -> ~{}",
            format_bytes(self.total_input_bytes),
            format_bytes(self.total_input_bytes) // Size doesn't change much
        ));

        if self.dry_run {
            lines.push(String::new());
            lines.push("Dry run complete. Remove --dry-run to execute.".to_string());
        }

        lines.join("\n")
    }
}

/// Compaction result output
#[derive(Debug, Serialize)]
pub struct CompactionResultOutput {
    pub table: String,
    pub partitions_compacted: usize,
    pub partitions_failed: usize,
    pub files_removed: usize,
    pub files_added: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub records_processed: u64,
    pub errors: Vec<String>,
}

impl Outputable for CompactionResultOutput {
    fn to_text(&self) -> String {
        let mut lines = vec![format!("Compacted {}", self.table), String::new()];

        lines.push("Complete".to_string());
        lines.push(format!("  Partitions: {}", self.partitions_compacted));
        if self.partitions_failed > 0 {
            lines.push(format!("  Failed:     {}", self.partitions_failed));
        }

        let file_reduction = if self.files_removed > self.files_added {
            format_percentage(
                (self.files_removed - self.files_added) as u64,
                self.files_removed as u64,
            )
        } else {
            "0%".to_string()
        };
        lines.push(format!(
            "  Files:      {} -> {} ({} reduction)",
            self.files_removed, self.files_added, file_reduction
        ));

        let bytes_savings = if self.bytes_before > self.bytes_after {
            format_percentage(self.bytes_before - self.bytes_after, self.bytes_before)
        } else {
            "0%".to_string()
        };
        lines.push(format!(
            "  Bytes:      {} -> {} ({} savings)",
            format_bytes(self.bytes_before),
            format_bytes(self.bytes_after),
            bytes_savings
        ));

        lines.push(format!(
            "  Records:    {}",
            format_number(self.records_processed)
        ));

        if !self.errors.is_empty() {
            lines.push(String::new());
            lines.push("Errors:".to_string());
            for err in &self.errors {
                lines.push(format!("  - {}", err));
            }
        }

        lines.join("\n")
    }
}

/// Execute the compact command
pub async fn execute(
    args: CompactArgs,
    config: &CatalogConfig,
    format: OutputFormat,
) -> Result<(), String> {
    let catalog = config.create_catalog().await?;
    let table_ident = parse_table_ident(&args.table)?;

    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| format!("Failed to load table: {}", e))?;

    // Build compaction options
    let mut options = CompactOptions::new()
        .with_target_file_size(args.target_size)
        .map_err(|e| format!("Invalid target size: {}", e))?
        .with_max_input_file_size(args.max_input_size)
        .map_err(|e| format!("Invalid max input size: {}", e))?
        .with_min_files_per_group(args.min_files)
        .map_err(|e| format!("Invalid min files: {}", e))?
        .with_dry_run(args.dry_run);

    if let Some(partition) = args.partition {
        options = options.with_partition_filter(partition);
    }

    // Create compaction plan
    let plan = plan_compaction(&table, &options)
        .await
        .map_err(|e| format!("Failed to create compaction plan: {}", e))?;

    if plan.is_empty() {
        println!("No files need compaction.");
        return Ok(());
    }

    if args.dry_run {
        // Output plan
        let plan_output = build_plan_output(&args.table, &plan, &options);
        print(&plan_output, format);
        return Ok(());
    }

    // Execute compaction
    println!("Compacting {}...", args.table);

    let result = execute_compaction(plan, &table, catalog.as_ref(), &options)
        .await
        .map_err(|e| format!("Compaction failed: {}", e))?;

    let output = CompactionResultOutput {
        table: args.table,
        partitions_compacted: result.partitions_compacted,
        partitions_failed: result.partitions_failed,
        files_removed: result.files_removed,
        files_added: result.files_added,
        bytes_before: result.bytes_before,
        bytes_after: result.bytes_after,
        records_processed: result.records_processed,
        errors: result
            .errors
            .iter()
            .map(|e| {
                format!(
                    "{}: {}",
                    e.partition.as_deref().unwrap_or("(unpartitioned)"),
                    e.error
                )
            })
            .collect(),
    };

    print(&output, format);
    Ok(())
}

fn build_plan_output(
    table: &str,
    plan: &CompactionPlan,
    options: &CompactOptions,
) -> CompactionPlanOutput {
    let partitions: Vec<PartitionPlanOutput> = plan
        .partitions
        .iter()
        .map(|p| {
            let avg_size = if p.total_input_files > 0 {
                p.total_input_bytes / p.total_input_files as u64
            } else {
                0
            };
            PartitionPlanOutput {
                partition: p.partition_value.clone(),
                input_files: p.total_input_files,
                input_bytes: p.total_input_bytes,
                estimated_output_files: p.estimated_output_files(options.target_file_size()),
                avg_file_size: avg_size,
            }
        })
        .collect();

    CompactionPlanOutput {
        table: table.to_string(),
        partitions,
        total_input_files: plan.total_input_files(),
        estimated_output_files: plan.estimated_output_files(options.target_file_size()),
        total_input_bytes: plan.total_input_bytes(),
        dry_run: options.dry_run(),
    }
}
