//! Table scanning and reading

use crate::error::{Error, Result};
use crate::expr::{evaluate_bounds, evaluate_partition, project_to_partition, Predicate};
use crate::reader::{DataFileEntry, DataFileStats};
use crate::table::Table;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use std::pin::Pin;
use std::vec::IntoIter;

/// A stream of Arrow RecordBatches
/// On WASM, we don't require Send since WASM is single-threaded
#[cfg(not(target_arch = "wasm32"))]
pub type ArrowRecordBatchStream = Pin<Box<dyn futures::Stream<Item = Result<RecordBatch>> + Send>>;

#[cfg(target_arch = "wasm32")]
pub type ArrowRecordBatchStream = Pin<Box<dyn futures::Stream<Item = Result<RecordBatch>>>>;

/// Builder for creating table scans
pub struct TableScanBuilder<'a> {
    table: &'a Table,
    predicate: Option<Predicate>,
}

impl<'a> TableScanBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            predicate: None,
        }
    }

    /// Add a filter predicate to the scan
    ///
    /// The predicate will be used for partition pruning and column statistics
    /// filtering to skip files that cannot contain matching rows.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use icepick::expr::{Predicate, Datum};
    ///
    /// let scan = table.scan()
    ///     .filter(Predicate::gt_eq("date", Datum::Date(19724)))
    ///     .build()?;
    /// ```
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Build the table scan
    pub fn build(self) -> Result<TableScan<'a>> {
        Ok(TableScan {
            table: self.table,
            predicate: self.predicate,
        })
    }
}

/// A table scan for reading data
pub struct TableScan<'a> {
    table: &'a Table,
    predicate: Option<Predicate>,
}

impl<'a> TableScan<'a> {
    /// Filter files based on the predicate using partition and bounds pruning
    ///
    /// Returns the filtered files as DataFileStats (which can be converted to DataFileEntry).
    async fn filter_files(&self) -> Result<Vec<DataFileStats>> {
        let Some(ref predicate) = self.predicate else {
            // No predicate - return all files as stats
            let files = self.table.files().await?;
            return Ok(files
                .into_iter()
                .map(|f| DataFileStats {
                    file_path: f.file_path,
                    record_count: f.record_count,
                    file_size_in_bytes: f.file_size_in_bytes,
                    file_format: f.file_format,
                    partition: Default::default(),
                    lower_bounds: Default::default(),
                    upper_bounds: Default::default(),
                    null_value_counts: Default::default(),
                    value_counts: Default::default(),
                })
                .collect());
        };

        let files_with_stats = self.table.files_with_stats().await?;
        let schema = self.table.schema()?;
        let partition_fields = self.table.partition_fields();

        // Project predicate to partition columns
        let partition_predicate = if let Some(spec) = self.table.current_partition_spec() {
            project_to_partition(predicate, schema, spec)
        } else {
            Predicate::AlwaysTrue
        };

        // Filter files using partition and bounds pruning
        Ok(files_with_stats
            .into_iter()
            .filter(|file| {
                // Partition pruning
                let partition_match = evaluate_partition(
                    &partition_predicate,
                    &file.partition,
                    partition_fields,
                    schema,
                );

                if !partition_match {
                    return false;
                }

                // Bounds pruning
                evaluate_bounds(
                    predicate,
                    schema,
                    &file.lower_bounds,
                    &file.upper_bounds,
                    &file.null_value_counts,
                    file.record_count,
                )
            })
            .collect())
    }

    /// Convert the scan into an Arrow RecordBatch stream
    ///
    /// When a predicate is set, files are filtered using:
    /// 1. Partition pruning - skip files whose partition values don't match
    /// 2. Bounds pruning - skip files whose min/max statistics prove no match
    ///
    /// Files that pass filtering are read sequentially and streamed as RecordBatches.
    pub async fn to_arrow(&self) -> Result<ArrowRecordBatchStream> {
        let file_io = self.table.file_io().clone();

        // Get filtered files and convert to DataFileEntry
        let files: Vec<DataFileEntry> = self
            .filter_files()
            .await?
            .into_iter()
            .map(|f| DataFileEntry {
                file_path: f.file_path,
                record_count: f.record_count,
                file_size_in_bytes: f.file_size_in_bytes,
                file_format: f.file_format,
                snapshot_id: None,
                file_sequence_number: None,
            })
            .collect();

        let state = ScanState {
            files: files.into_iter(),
            current_reader: None,
            file_io,
        };

        let stream = futures::stream::try_unfold(state, move |mut state| async move {
            loop {
                if let Some((ref path, ref mut reader)) = state.current_reader {
                    match reader.next() {
                        Some(Ok(batch)) => return Ok(Some((batch, state))),
                        Some(Err(e)) => {
                            return Err(Error::invalid_input(format!(
                                "Failed to read batches from {}: {}",
                                path, e
                            )))
                        }
                        None => {
                            state.current_reader = None;
                            continue;
                        }
                    }
                }

                match state.files.next() {
                    Some(file_entry) => {
                        let (path, reader) =
                            read_parquet_reader(&state.file_io, file_entry).await?;
                        state.current_reader = Some((path, reader));
                    }
                    None => return Ok(None),
                }
            }
        });

        Ok(Box::pin(stream))
    }

    /// Get the number of files that would be scanned
    ///
    /// This is useful for understanding the effect of predicate pushdown.
    /// Returns (files_after_filtering, total_files).
    pub async fn file_count(&self) -> Result<(usize, usize)> {
        let total_files = self.table.files().await?.len();
        let filtered_files = self.filter_files().await?.len();
        Ok((filtered_files, total_files))
    }
}

struct ScanState {
    files: IntoIter<DataFileEntry>,
    current_reader: Option<(String, ParquetRecordBatchReader)>,
    file_io: crate::io::FileIO,
}

/// Read a single Parquet file and return a reader for streaming record batches
async fn read_parquet_reader(
    file_io: &crate::io::FileIO,
    file_entry: DataFileEntry,
) -> Result<(String, ParquetRecordBatchReader)> {
    // Read file bytes from storage
    let bytes: Bytes = file_io.read(&file_entry.file_path).await?.into();

    // Build Parquet reader using Bytes
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|e| {
        Error::invalid_input(format!(
            "Failed to create Parquet reader for {}: {}",
            file_entry.file_path, e
        ))
    })?;

    let reader = builder.build().map_err(|e| {
        Error::invalid_input(format!(
            "Failed to build Parquet reader for {}: {}",
            file_entry.file_path, e
        ))
    })?;

    Ok((file_entry.file_path, reader))
}
