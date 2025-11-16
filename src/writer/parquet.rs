//! Parquet file writer

use crate::arrow_convert::schema_to_arrow;
use crate::error::{Error, Result};
use crate::io::FileIO;
use crate::spec::{DataFile, Schema};
use crate::writer::stats::StatsCollector;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Parquet file writer
pub struct ParquetWriter {
    #[allow(dead_code)]
    schema: Schema,
    parquet_writer: ArrowWriter<Vec<u8>>,
    stats_collector: StatsCollector,
}

impl ParquetWriter {
    /// Create a new Parquet writer
    pub fn new(schema: Schema) -> Result<Self> {
        let arrow_schema = schema_to_arrow(&schema)?;

        let buffer = Vec::new();
        let props = WriterProperties::builder().build();

        let parquet_writer = ArrowWriter::try_new(buffer, arrow_schema.into(), Some(props))
            .map_err(|e| Error::invalid_input(format!("Failed to create Parquet writer: {}", e)))?;

        Ok(Self {
            schema,
            parquet_writer,
            stats_collector: StatsCollector::new(),
        })
    }

    /// Write an Arrow RecordBatch
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.stats_collector.collect(batch)?;

        self.parquet_writer
            .write(batch)
            .map_err(|e| Error::invalid_input(format!("Failed to write batch: {}", e)))?;

        Ok(())
    }

    /// Finish writing and upload to storage, returning DataFile
    pub async fn finish(mut self, file_io: &FileIO, path: String) -> Result<DataFile> {
        // Flush the writer to ensure all data is written
        self.parquet_writer
            .flush()
            .map_err(|e| Error::invalid_input(format!("Failed to flush writer: {}", e)))?;

        // Close and get the buffer in one go
        let parquet_bytes = self
            .parquet_writer
            .into_inner()
            .map_err(|e| Error::invalid_input(format!("Failed to get buffer: {}", e)))?;

        let file_size = parquet_bytes.len() as i64;

        // Upload to storage
        file_io.write(&path, parquet_bytes).await?;

        // Build DataFile with statistics
        let stats = self.stats_collector.finalize();

        DataFile::builder()
            .with_file_path(&path)
            .with_file_format("PARQUET")
            .with_record_count(stats.record_count)
            .with_file_size_in_bytes(file_size)
            .with_value_counts(stats.value_counts)
            .with_null_value_counts(stats.null_value_counts)
            .build()
    }
}
