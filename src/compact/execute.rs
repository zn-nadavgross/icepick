//! Compaction execution

use crate::catalog::Catalog;
use crate::compact::options::CompactOptions;
use crate::compact::plan::{CompactionGroup, CompactionPlan, PartitionPlan};
use crate::error::{Error, Result};
use crate::io::FileIO;
use crate::spec::DataFile;
use crate::table::Table;
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Result of a compaction operation
#[derive(Debug, Clone, Default)]
pub struct CompactionResult {
    /// Number of partitions successfully compacted
    pub partitions_compacted: usize,
    /// Number of partitions that failed
    pub partitions_failed: usize,
    /// Total files removed
    pub files_removed: usize,
    /// Total files added
    pub files_added: usize,
    /// Total bytes before compaction
    pub bytes_before: u64,
    /// Total bytes after compaction
    pub bytes_after: u64,
    /// Total records processed
    pub records_processed: u64,
    /// Errors encountered during compaction
    pub errors: Vec<PartitionError>,
}

/// Error from compacting a single partition
#[derive(Debug, Clone)]
pub struct PartitionError {
    /// Partition value (None for unpartitioned)
    pub partition: Option<String>,
    /// Error message
    pub error: String,
}

/// Execute a compaction plan
pub async fn execute_compaction(
    plan: CompactionPlan,
    table: &Table,
    catalog: &dyn Catalog,
    options: &CompactOptions,
) -> Result<CompactionResult> {
    if options.dry_run {
        return Err(Error::InvalidInput(
            "Cannot execute compaction in dry-run mode".to_string(),
        ));
    }

    let mut result = CompactionResult::default();

    for (idx, partition_plan) in plan.partitions.iter().enumerate() {
        info!(
            "[{}/{}] Compacting partition: {:?}",
            idx + 1,
            plan.partition_count(),
            partition_plan.partition_value
        );

        match execute_partition_compaction(partition_plan, table, catalog).await {
            Ok((files_removed, files_added, bytes_before, bytes_after, records)) => {
                result.partitions_compacted += 1;
                result.files_removed += files_removed;
                result.files_added += files_added;
                result.bytes_before += bytes_before;
                result.bytes_after += bytes_after;
                result.records_processed += records;
            }
            Err(e) => {
                warn!(
                    "Failed to compact partition {:?}: {}",
                    partition_plan.partition_value, e
                );
                result.partitions_failed += 1;
                result.errors.push(PartitionError {
                    partition: partition_plan.partition_value.clone(),
                    error: e.to_string(),
                });
            }
        }
    }

    Ok(result)
}

/// Execute compaction for a single partition
async fn execute_partition_compaction(
    partition_plan: &PartitionPlan,
    table: &Table,
    catalog: &dyn Catalog,
) -> Result<(usize, usize, u64, u64, u64)> {
    let file_io = table.file_io();

    let mut all_files_to_delete: Vec<DataFile> = Vec::new();
    let mut all_files_to_add: Vec<DataFile> = Vec::new();
    let mut total_bytes_before: u64 = 0;
    let mut total_bytes_after: u64 = 0;
    let mut total_records: u64 = 0;

    for group in &partition_plan.groups {
        let (new_files, bytes_before, bytes_after, records) =
            compact_group(group, table, file_io).await?;

        all_files_to_delete.extend(group.input_files.clone());
        all_files_to_add.extend(new_files);
        total_bytes_before += bytes_before;
        total_bytes_after += bytes_after;
        total_records += records;
    }

    // Commit the transaction for this partition
    let files_removed = all_files_to_delete.len();
    let files_added = all_files_to_add.len();

    // Reload table to get latest metadata before commit
    let fresh_table = catalog.load_table(table.identifier()).await?;

    let timestamp_ms = chrono::Utc::now().timestamp_millis();
    fresh_table
        .transaction()
        .rewrite(all_files_to_delete, all_files_to_add)
        .commit(catalog, timestamp_ms)
        .await?;

    Ok((
        files_removed,
        files_added,
        total_bytes_before,
        total_bytes_after,
        total_records,
    ))
}

/// Compact a single group of files
async fn compact_group(
    group: &CompactionGroup,
    table: &Table,
    file_io: &FileIO,
) -> Result<(Vec<DataFile>, u64, u64, u64)> {
    debug!(
        "Compacting group with {} files ({} bytes)",
        group.input_files.len(),
        group.input_bytes
    );

    // Read all input files and collect batches
    let mut all_batches: Vec<RecordBatch> = Vec::new();

    for file in &group.input_files {
        let batches = read_parquet_file(file_io, file.file_path()).await?;
        all_batches.extend(batches);
    }

    if all_batches.is_empty() {
        return Ok((Vec::new(), group.input_bytes, 0, 0));
    }

    // Get the schema from the first batch
    let schema = all_batches[0].schema();

    // Concatenate all batches
    let combined_batch = concat_batches(&schema, &all_batches)
        .map_err(|e| Error::invalid_input(format!("Failed to concatenate batches: {}", e)))?;

    let total_records = combined_batch.num_rows() as u64;

    // Generate output path
    let partition_path = if let Some(first_file) = group.input_files.first() {
        // Extract partition path from first input file
        extract_partition_path(first_file.file_path())
    } else {
        "data".to_string()
    };

    let uuid = Uuid::new_v4().to_string().replace('-', "");
    let output_path = format!(
        "{}/{}/compacted_{}_from_{}_files.parquet",
        table.location(),
        partition_path,
        uuid,
        group.input_files.len()
    );

    // Write compacted file
    let new_file = write_compacted_parquet(file_io, &output_path, combined_batch).await?;
    let bytes_after = new_file.file_size_in_bytes() as u64;

    Ok((
        vec![new_file],
        group.input_bytes,
        bytes_after,
        total_records,
    ))
}

/// Read all record batches from a Parquet file
async fn read_parquet_file(file_io: &FileIO, path: &str) -> Result<Vec<RecordBatch>> {
    let bytes: Bytes = file_io.read(path).await?.into();

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|e| {
        Error::invalid_input(format!(
            "Failed to create Parquet reader for {}: {}",
            path, e
        ))
    })?;

    let reader = builder.build().map_err(|e| {
        Error::invalid_input(format!(
            "Failed to build Parquet reader for {}: {}",
            path, e
        ))
    })?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            Error::invalid_input(format!("Failed to read batch from {}: {}", path, e))
        })?;
        batches.push(batch);
    }

    Ok(batches)
}

/// Write a compacted Parquet file
async fn write_compacted_parquet(
    file_io: &FileIO,
    path: &str,
    batch: RecordBatch,
) -> Result<DataFile> {
    let schema = batch.schema();
    let record_count = batch.num_rows() as i64;

    let buffer = Vec::new();
    let props = WriterProperties::builder().build();

    let mut writer = ArrowWriter::try_new(buffer, schema, Some(props))
        .map_err(|e| Error::invalid_input(format!("Failed to create Parquet writer: {}", e)))?;

    writer
        .write(&batch)
        .map_err(|e| Error::invalid_input(format!("Failed to write batch: {}", e)))?;

    writer
        .flush()
        .map_err(|e| Error::invalid_input(format!("Failed to flush writer: {}", e)))?;

    let parquet_bytes = writer
        .into_inner()
        .map_err(|e| Error::invalid_input(format!("Failed to get buffer: {}", e)))?;

    let file_size = parquet_bytes.len() as i64;

    file_io.write(path, parquet_bytes).await?;

    DataFile::builder()
        .with_file_path(path)
        .with_file_format("PARQUET")
        .with_record_count(record_count)
        .with_file_size_in_bytes(file_size)
        .build()
}

/// Extract the partition path from a full file path
fn extract_partition_path(file_path: &str) -> String {
    // Find the "data" directory and extract everything up to the file name
    // e.g., s3://bucket/table/data/dt=2024-01-15/file.parquet -> data/dt=2024-01-15
    if let Some(data_pos) = file_path.find("/data/") {
        let after_data = &file_path[data_pos + 1..]; // Skip the leading /
        if let Some(last_slash) = after_data.rfind('/') {
            return after_data[..last_slash].to_string();
        }
        return "data".to_string();
    }
    "data".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_partition_path() {
        assert_eq!(
            extract_partition_path("s3://bucket/table/data/dt=2024-01-15/file.parquet"),
            "data/dt=2024-01-15"
        );
        assert_eq!(
            extract_partition_path("s3://bucket/table/data/file.parquet"),
            "data"
        );
        assert_eq!(
            extract_partition_path("s3://bucket/table/file.parquet"),
            "data"
        );
    }
}
