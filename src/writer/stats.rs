//! Statistics collection for Parquet files

use crate::error::Result;
use crate::spec::Schema;
use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

mod bounds;

use bounds::{compute_bounds, BoundState};

/// Statistics collected from Arrow batches
#[derive(Debug, Clone)]
pub struct FileStats {
    pub record_count: i64,
    pub column_sizes: HashMap<i32, i64>,
    pub value_counts: HashMap<i32, i64>,
    pub null_value_counts: HashMap<i32, i64>,
    pub lower_bounds: HashMap<i32, Vec<u8>>,
    pub upper_bounds: HashMap<i32, Vec<u8>>,
}

/// Collector for file statistics
pub struct StatsCollector {
    record_count: i64,
    column_sizes: HashMap<i32, i64>,
    value_counts: HashMap<i32, i64>,
    null_value_counts: HashMap<i32, i64>,
    field_ids_by_name: HashMap<String, i32>,
    bounds: BoundState,
}

impl StatsCollector {
    /// Create a new stats collector
    pub fn new(schema: &Schema) -> Self {
        let field_ids_by_name = schema
            .fields()
            .iter()
            .map(|field| (field.name().to_string(), field.id()))
            .collect();

        Self {
            record_count: 0,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            field_ids_by_name,
            bounds: BoundState::new(),
        }
    }

    /// Collect statistics from a record batch
    pub fn collect(&mut self, batch: &RecordBatch) -> Result<()> {
        self.record_count += batch.num_rows() as i64;

        let schema = batch.schema();
        for (col_idx, column) in batch.columns().iter().enumerate() {
            let field = schema.field(col_idx);
            let field_id = self
                .field_ids_by_name
                .get(field.name())
                .copied()
                .unwrap_or(col_idx as i32);

            // Count non-null values
            let non_null_count = column.len() - column.null_count();
            *self.value_counts.entry(field_id).or_insert(0) += non_null_count as i64;

            // Count null values
            let null_count = column.null_count();
            if null_count > 0 {
                *self.null_value_counts.entry(field_id).or_insert(0) += null_count as i64;
            }

            // Track column memory usage
            *self.column_sizes.entry(field_id).or_insert(0) +=
                column.get_array_memory_size() as i64;

            self.update_bounds(field_id, field.data_type(), column)?;
        }

        Ok(())
    }

    fn update_bounds(
        &mut self,
        field_id: i32,
        data_type: &DataType,
        column: &dyn Array,
    ) -> Result<()> {
        if let Some((lower, upper)) = compute_bounds(data_type, column)? {
            self.bounds.merge(field_id, lower, upper);
        }

        Ok(())
    }

    /// Finalize and return statistics
    pub fn finalize(self) -> FileStats {
        let (lower_bounds, upper_bounds) = self.bounds.into_encoded();
        FileStats {
            record_count: self.record_count,
            column_sizes: self.column_sizes,
            value_counts: self.value_counts,
            null_value_counts: self.null_value_counts,
            lower_bounds,
            upper_bounds,
        }
    }
}
