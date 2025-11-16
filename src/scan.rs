//! Table scanning and reading

use crate::error::{Error, Result};
use crate::reader::DataFileEntry;
use crate::table::Table;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// A stream of Arrow RecordBatches
pub type ArrowRecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

/// Builder for creating table scans
pub struct TableScanBuilder<'a> {
    table: &'a Table,
}

impl<'a> TableScanBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Build the table scan
    pub fn build(self) -> Result<TableScan<'a>> {
        Ok(TableScan { table: self.table })
    }
}

/// A table scan for reading data
pub struct TableScan<'a> {
    table: &'a Table,
}

impl<'a> TableScan<'a> {
    /// Convert the scan into an Arrow RecordBatch stream
    ///
    /// This reads all data files sequentially and streams RecordBatches.
    /// No filtering or projection is applied in this MVP version.
    pub async fn to_arrow(&self) -> Result<ArrowRecordBatchStream> {
        // Get all data files
        let files = self.table.files().await?;

        // Clone what we need for the async closure
        let file_io = self.table.file_io().clone();

        // Create a stream that processes files sequentially
        let stream = futures::stream::iter(files)
            .then(move |file_entry| {
                let file_io = file_io.clone();
                async move { read_parquet_file(&file_io, &file_entry).await }
            })
            .flat_map(|result| {
                match result {
                    Ok(batches) => {
                        // Convert Vec<RecordBatch> into a stream
                        futures::stream::iter(batches.into_iter().map(Ok)).boxed()
                    }
                    Err(e) => {
                        // Single error
                        futures::stream::iter(vec![Err(e)]).boxed()
                    }
                }
            });

        Ok(Box::pin(stream))
    }
}

/// Read a single Parquet file and return all RecordBatches
async fn read_parquet_file(
    file_io: &crate::io::FileIO,
    file_entry: &DataFileEntry,
) -> Result<Vec<RecordBatch>> {
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

    // Collect all batches
    let batches: std::result::Result<Vec<_>, _> = reader.collect();
    batches.map_err(|e| {
        Error::invalid_input(format!(
            "Failed to read batches from {}: {}",
            file_entry.file_path, e
        ))
    })
}
