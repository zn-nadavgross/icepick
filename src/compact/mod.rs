//! Compaction module for Iceberg tables
//!
//! This module provides bin-pack compaction for Iceberg tables. Compaction
//! merges small files into larger ones to improve query performance and
//! reduce metadata overhead.
//!
//! # Example
//!
//! ```no_run
//! use icepick::compact::{CompactOptions, CompactionPlan, execute_compaction};
//! use icepick::catalog::Catalog;
//!
//! # async fn example(table: &icepick::Table, catalog: &dyn Catalog) -> Result<(), Box<dyn std::error::Error>> {
//! // Create compaction options
//! let options = CompactOptions::new()
//!     .with_target_file_size(256 * 1024 * 1024)?  // 256 MB
//!     .with_min_files_per_group(3)?;
//!
//! // Create a compaction plan
//! let plan = CompactionPlan::create(table, &options).await?;
//!
//! if !plan.is_empty() {
//!     println!("Found {} partitions to compact", plan.partition_count());
//!
//!     // Execute the plan
//!     let result = execute_compaction(plan, table, catalog, &options).await?;
//!     println!("Compacted {} files into {}", result.files_removed, result.files_added);
//! }
//! # Ok(())
//! # }
//! ```

pub mod execute;
pub mod options;
pub mod plan;

pub use execute::{execute_compaction, CompactionResult, PartitionError};
pub use options::CompactOptions;
pub use plan::{CompactionGroup, CompactionPlan, PartitionPlan};

use crate::catalog::Catalog;
use crate::error::Result;
use crate::table::Table;

/// Plan compaction for a table (does not execute)
pub async fn plan_compaction(table: &Table, options: &CompactOptions) -> Result<CompactionPlan> {
    CompactionPlan::create(table, options).await
}

/// Execute compaction on a table
///
/// This is a convenience function that creates a plan and executes it.
pub async fn compact_table(
    table: &Table,
    catalog: &dyn Catalog,
    options: &CompactOptions,
) -> Result<CompactionResult> {
    let plan = plan_compaction(table, options).await?;

    if plan.is_empty() {
        return Ok(CompactionResult::default());
    }

    execute_compaction(plan, table, catalog, options).await
}
