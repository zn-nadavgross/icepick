//! Compaction execution

use crate::catalog::Catalog;
use crate::compact::options::CompactOptions;
use crate::compact::plan::{CompactionGroup, CompactionPlan, PartitionPlan};
use crate::error::{Error, Result};
use crate::io::FileIO;
use crate::spec::DataFile;
use crate::table::Table;
use crate::arrow_convert::schema_to_arrow;
use arrow::array::{new_null_array, ArrayRef};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{Field, Schema as ArrowSchema, SchemaRef};
use crate::spec::Schema;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
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
///
/// # Atomicity Warning
///
/// **Each partition is committed in a separate transaction.** If compaction fails
/// mid-way through processing partitions, some partitions will be compacted while
/// others remain unchanged. This means the table may be left in a partially
/// compacted state.
///
/// To handle partial failures gracefully:
/// - Use `options.with_allow_partial_failure(true)` to continue compacting other
///   partitions even if one fails
/// - Check `CompactionResult.errors` to see which partitions failed
/// - Check `CompactionResult.partitions_failed` vs `partitions_compacted` for status
///
/// For fully atomic compaction, compact one partition at a time using
/// `options.with_partition_filter()`.
pub async fn execute_compaction(
    plan: CompactionPlan,
    table: &Table,
    catalog: &dyn Catalog,
    options: &CompactOptions,
) -> Result<CompactionResult> {
    if options.dry_run() {
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

    // Check if we should fail on partial failures
    if result.partitions_failed > 0 && !options.allow_partial_failure() {
        return Err(Error::InvalidInput(format!(
            "Compaction failed on {} of {} partitions. Use --allow-partial-failure to continue on errors.\n\nErrors:\n{}",
            result.partitions_failed,
            plan.partition_count(),
            result
                .errors
                .iter()
                .map(|e| format!(
                    "  - {}: {}",
                    e.partition.as_deref().unwrap_or("(unpartitioned)"),
                    e.error
                ))
                .collect::<Vec<_>>()
                .join("\n")
        )));
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

        all_files_to_delete.extend(group.files().iter().cloned());
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
        group.files().len(),
        group.total_bytes()
    );

    // Read all input files and collect raw batches
    let mut raw_batches: Vec<RecordBatch> = Vec::new();

    for file in group.files() {
        let batches = read_parquet_file(file_io, file.file_path()).await?;
        raw_batches.extend(batches);
    }

    if raw_batches.is_empty() {
        return Err(Error::InvalidInput(format!(
            "Compaction group produced no data from {} input files (total {} bytes). All files may be empty or failed to read.",
            group.files().len(),
            group.total_bytes()
        )));
    }

    // Files written at different times can diverge in physical type (a column
    // stored as Int64 in old files vs Date32 in newer ones) AND in column set
    // (columns added or dropped via schema evolution). Resolve every file against
    // the table schema: cast columns that are present, null-fill columns that are
    // absent. Partition columns aren't materialized in data files, so they are
    // excluded by keying the target off the columns that actually appear.
    let target_schema = build_target_schema(&raw_batches, table.schema()?)?;

    let mut all_batches: Vec<RecordBatch> = Vec::with_capacity(raw_batches.len());
    for batch in &raw_batches {
        all_batches.push(normalize_batch(batch, &target_schema)?);
    }

    // All batches now share the table's target schema
    let combined_batch = concat_batches(&target_schema, &all_batches)
        .map_err(|e| Error::invalid_input(format!("Failed to concatenate batches: {}", e)))?;

    let total_records = combined_batch.num_rows() as u64;

    // Generate output path
    let partition_path = if let Some(first_file) = group.files().first() {
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
        group.files().len()
    );

    // Extract partition data from the first input file (if any)
    let partition = group.files().first().map(|f| f.partition());

    // Write compacted file
    let new_file =
        write_compacted_parquet(file_io, &output_path, combined_batch, partition).await?;
    let bytes_after = new_file.file_size_in_bytes() as u64;

    Ok((
        vec![new_file],
        group.total_bytes(),
        bytes_after,
        total_records,
    ))
}

/// Build the target Arrow schema for concatenation. It is the set of table
/// schema columns that appear in at least one data file, in table-schema order
/// with canonical types and field-id metadata. Partition columns never appear in
/// the data files, so they fall out naturally; columns dropped from the table
/// schema are not included.
fn build_target_schema(batches: &[RecordBatch], table_schema: &Schema) -> Result<SchemaRef> {
    let table_arrow = schema_to_arrow(table_schema)?;

    let mut present: HashSet<String> = HashSet::new();
    for batch in batches {
        for field in batch.schema().fields() {
            present.insert(field.name().clone());
        }
    }

    let fields: Vec<Arc<Field>> = table_arrow
        .fields()
        .iter()
        .filter(|f| present.contains(f.name()))
        .cloned()
        .collect();

    if fields.is_empty() {
        return Err(Error::invalid_input(
            "No data-file columns matched the table schema during compaction".to_string(),
        ));
    }

    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// Resolve a batch against the target schema: cast columns that are present,
/// null-fill columns that are absent (added by later schema evolution). This lets
/// files with divergent physical types or column sets be concatenated.
fn normalize_batch(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());

    for field in target.fields() {
        match batch.column_by_name(field.name()) {
            Some(col) if col.data_type() == field.data_type() => columns.push(col.clone()),
            Some(col) => {
                let casted = cast(col, field.data_type()).map_err(|e| {
                    Error::invalid_input(format!(
                        "Failed to cast column '{}' from {:?} to {:?}: {}",
                        field.name(),
                        col.data_type(),
                        field.data_type(),
                        e
                    ))
                })?;
                columns.push(casted);
            }
            None => columns.push(new_null_array(field.data_type(), num_rows)),
        }
    }

    RecordBatch::try_new(target.clone(), columns).map_err(|e| {
        Error::invalid_input(format!("Failed to rebuild batch during compaction: {}", e))
    })
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
    partition: Option<&HashMap<String, String>>,
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

    let mut builder = DataFile::builder()
        .with_file_path(path)
        .with_file_format("PARQUET")
        .with_record_count(record_count)
        .with_file_size_in_bytes(file_size);

    if let Some(partition_data) = partition {
        builder = builder.with_partition(partition_data.clone());
    }

    builder.build()
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
