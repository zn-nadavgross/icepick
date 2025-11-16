//! Table scanning and reading

use crate::error::{Error, Result};
use crate::reader::DataFileEntry;
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
