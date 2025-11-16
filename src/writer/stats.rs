//! Statistics collection for Parquet files

use crate::error::Result;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

/// Statistics collected from Arrow batches
#[derive(Debug, Clone)]
pub struct FileStats {
    pub record_count: i64,
    pub column_sizes: HashMap<i32, i64>,
    pub value_counts: HashMap<i32, i64>,
    pub null_value_counts: HashMap<i32, i64>,
}

/// Collector for file statistics
pub struct StatsCollector {
    record_count: i64,
    value_counts: HashMap<i32, i64>,
    null_value_counts: HashMap<i32, i64>,
}

impl StatsCollector {
    /// Create a new stats collector
    pub fn new() -> Self {
        Self {
            record_count: 0,
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
        }
    }

    /// Collect statistics from a record batch
    pub fn collect(&mut self, batch: &RecordBatch) -> Result<()> {
        self.record_count += batch.num_rows() as i64;

        for (col_idx, column) in batch.columns().iter().enumerate() {
            let field_id = col_idx as i32; // Simple mapping for now

            // Count non-null values
            let non_null_count = column.len() - column.null_count();
            *self.value_counts.entry(field_id).or_insert(0) += non_null_count as i64;

            // Count null values
            let null_count = column.null_count();
            if null_count > 0 {
                *self.null_value_counts.entry(field_id).or_insert(0) += null_count as i64;
            }
        }

        Ok(())
    }

    /// Finalize and return statistics
    pub fn finalize(self) -> FileStats {
        FileStats {
            record_count: self.record_count,
            column_sizes: HashMap::new(), // Not tracking byte sizes for MVP
            value_counts: self.value_counts,
            null_value_counts: self.null_value_counts,
        }
    }
}

impl Default for StatsCollector {
    fn default() -> Self {
        Self::new()
    }
}
