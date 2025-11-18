//! Direct Arrow to Parquet writer for S3
//!
//! This module provides a simple API for writing Arrow RecordBatch directly to S3 as Parquet files,
//! bypassing Iceberg metadata entirely. Use this when you need to write data for external systems
//! (Spark, DuckDB, etc.) that don't require Iceberg metadata.

use crate::error::{Error, Result};
use crate::io::FileIO;
use crate::spec::{DataFile, Schema};
use crate::writer::stats::StatsCollector;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::future::Future;
use std::pin::Pin;

#[cfg(not(target_arch = "wasm32"))]
type BuilderFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

#[cfg(target_arch = "wasm32")]
type BuilderFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + 'a>>;

/// Builder for writing Arrow RecordBatch to Parquet on S3
///
/// Created by the `arrow_to_parquet()` function. Use the builder pattern to configure
/// compression, then await the builder to execute the write.
///
/// # Examples
///
/// ```no_run
/// use icepick::{arrow_to_parquet, FileIO};
/// use arrow::record_batch::RecordBatch;
/// use parquet::basic::Compression;
///
/// # async fn example(batch: RecordBatch, file_io: FileIO) -> Result<(), Box<dyn std::error::Error>> {
/// // Simple write with defaults
/// arrow_to_parquet(&batch, "s3://bucket/data.parquet", &file_io).await?;
///
/// // With compression
/// arrow_to_parquet(&batch, "s3://bucket/data.parquet", &file_io)
///     .with_compression(Compression::ZSTD(parquet::basic::ZstdLevel::default()))
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ArrowParquetBuilder<'a> {
    batch: &'a RecordBatch,
    path: String,
    file_io: &'a FileIO,
    compression: Compression,
    stats_collector: Option<StatsCollector>,
}

impl<'a> ArrowParquetBuilder<'a> {
    /// Create a new builder with default compression (Snappy)
    pub(crate) fn new(batch: &'a RecordBatch, path: String, file_io: &'a FileIO) -> Self {
        Self {
            batch,
            path,
            file_io,
            compression: Compression::SNAPPY,
            stats_collector: None,
        }
    }

    /// Set the Parquet compression codec
    ///
    /// Default is `Compression::SNAPPY`. Other options include:
    /// - `Compression::UNCOMPRESSED`
    /// - `Compression::GZIP(GzipLevel::default())`
    /// - `Compression::ZSTD(ZstdLevel::default())`
    /// - `Compression::LZ4`
    /// - etc.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Enable Iceberg statistics collection so the builder can return a `DataFile`.
    ///
    /// This requires the Iceberg table schema so field IDs can be recorded correctly.
    /// Use [`finish_data_file`](Self::finish_data_file) to write the file and obtain
    /// a fully populated `DataFile`.
    pub fn with_iceberg_schema(mut self, schema: &Schema) -> Self {
        self.stats_collector = Some(StatsCollector::new(schema));
        self
    }

    /// Execute the write operation
    ///
    /// Writes the RecordBatch to an in-memory Parquet file, then uploads to S3.
    /// The entire file is buffered in memory before upload.
    pub async fn finish(self) -> Result<()> {
        self.write_parquet().await?;
        Ok(())
    }

    /// Finish the write, returning a `DataFile` populated with Iceberg statistics.
    ///
    /// Call [`with_iceberg_schema`](Self::with_iceberg_schema) before using this method
    /// so statistics can be tracked for manifest entries.
    pub async fn finish_data_file(mut self) -> Result<DataFile> {
        let mut stats_collector = self.stats_collector.take().ok_or_else(|| {
            Error::invalid_input(
                "finish_data_file() requires with_iceberg_schema() to be called first",
            )
        })?;
        stats_collector.collect(self.batch)?;
        let stats = stats_collector.finalize();

        let (path, file_size) = self.write_parquet().await?;

        DataFile::builder()
            .with_file_path(&path)
            .with_file_format("PARQUET")
            .with_record_count(stats.record_count)
            .with_file_size_in_bytes(file_size)
            .with_column_sizes(stats.column_sizes)
            .with_value_counts(stats.value_counts)
            .with_null_value_counts(stats.null_value_counts)
            .with_lower_bounds(stats.lower_bounds)
            .with_upper_bounds(stats.upper_bounds)
            .build()
    }

    async fn write_parquet(self) -> Result<(String, i64)> {
        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .build();

        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, self.batch.schema(), Some(props))
                .map_err(|e| {
                    Error::InvalidInput(format!("Failed to create Parquet writer: {}", e))
                })?;

            writer.write(self.batch).map_err(|e| {
                Error::InvalidInput(format!("Failed to write batch to Parquet: {}", e))
            })?;

            writer.close().map_err(|e| {
                Error::InvalidInput(format!("Failed to close Parquet writer: {}", e))
            })?;
        }

        let file_size = buffer.len() as i64;
        let path = self.path;
        self.file_io.write(&path, buffer).await?;
        Ok((path, file_size))
    }
}

/// Implement IntoFuture to allow direct .await on the builder
impl<'a> std::future::IntoFuture for ArrowParquetBuilder<'a> {
    type Output = Result<()>;
    type IntoFuture = BuilderFuture<'a>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.finish())
    }
}

/// Write an Arrow RecordBatch directly to S3 as a Parquet file
///
/// This function bypasses Iceberg metadata entirely and writes a standalone Parquet file.
/// Use this when you need to write data for external systems (Spark, DuckDB, etc.) that
/// don't require Iceberg metadata.
///
/// For writing to Iceberg tables, use the `Transaction` API instead.
///
/// # Arguments
///
/// * `batch` - Arrow RecordBatch to write
/// * `path` - S3 path where the Parquet file will be written (e.g., "s3://bucket/data.parquet")
/// * `file_io` - FileIO instance with S3 credentials/configuration
///
/// # Returns
///
/// Returns an `ArrowParquetBuilder` that can be configured with compression options,
/// then awaited to execute the write.
///
/// # Memory Usage
///
/// The entire Parquet file is buffered in memory before upload. For large batches,
/// ensure sufficient memory is available.
///
/// # Examples
///
/// ```no_run
/// use icepick::{arrow_to_parquet, FileIO};
/// use arrow::array::{Int32Array, StringArray};
/// use arrow::datatypes::{DataType, Field, Schema};
/// use arrow::record_batch::RecordBatch;
/// use parquet::basic::Compression;
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Setup FileIO with S3 credentials
/// let file_io = FileIO::from_aws_credentials(
///     icepick::io::AwsCredentials {
///         access_key_id: "your-key".to_string(),
///         secret_access_key: "your-secret".to_string(),
///         session_token: None,
///     },
///     "us-west-2".to_string()
/// );
///
/// // Create sample Arrow data
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
///     Field::new("name", DataType::Utf8, false),
/// ]));
///
/// let batch = RecordBatch::try_new(
///     schema,
///     vec![
///         Arc::new(Int32Array::from(vec![1, 2, 3])),
///         Arc::new(StringArray::from(vec!["a", "b", "c"])),
///     ],
/// )?;
///
/// // Simple write with defaults
/// arrow_to_parquet(&batch, "s3://my-bucket/output.parquet", &file_io).await?;
///
/// // With compression
/// arrow_to_parquet(&batch, "s3://my-bucket/compressed.parquet", &file_io)
///     .with_compression(Compression::ZSTD(parquet::basic::ZstdLevel::default()))
///     .await?;
///
/// // Manual partition paths
/// let date = "2025-01-15";
/// let path = format!("s3://my-bucket/data/date={}/data.parquet", date);
/// arrow_to_parquet(&batch, &path, &file_io).await?;
///
/// # Ok(())
/// # }
/// ```
pub fn arrow_to_parquet<'a>(
    batch: &'a RecordBatch,
    path: impl Into<String>,
    file_io: &'a FileIO,
) -> ArrowParquetBuilder<'a> {
    ArrowParquetBuilder::new(batch, path.into(), file_io)
}
