//! Orchestrate transaction commit with retries

use crate::commit::paths::{manifest_list_path, manifest_path, next_metadata_path};
use crate::error::{Error, Result};
use crate::manifest::rewriter::{rewrite_manifest, RewriteOutcome};
use crate::manifest::writer::{
    write_manifest_list, write_manifest_with_entries, FieldSummary, ManifestEntry,
    ManifestEntryStatus, ManifestListEntry,
};
use crate::reader::ManifestListReader;
use crate::spec::{DataFile, Snapshot, Summary};
use crate::transaction::{Transaction, TransactionOperation};
use std::collections::HashSet;
use tracing::debug;
use uuid::Uuid;

/// Generate a unique snapshot ID using UUID-based approach (like iceberg-rust)
/// Returns a positive i64 that's unique within the table
fn generate_snapshot_id(table: &crate::table::Table) -> i64 {
    let generate_random_id = || -> i64 {
        let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
        let snapshot_id = (lhs ^ rhs) as i64;
        if snapshot_id < 0 {
            -snapshot_id
        } else {
            snapshot_id
        }
    };

    let mut snapshot_id = generate_random_id();

    // Ensure uniqueness by checking against existing snapshots
    while table
        .metadata()
        .snapshots()
        .iter()
        .any(|s| s.snapshot_id() == snapshot_id)
    {
        snapshot_id = generate_random_id();
    }

    snapshot_id
}

/// Collected statistics from processing transaction operations
struct OperationStats {
    /// Files to add (from Append and Rewrite operations)
    files_to_add: Vec<DataFile>,
    /// Files to delete (from Rewrite operations)
    files_to_delete: Vec<DataFile>,
    /// Operation type for the snapshot summary
    operation_type: &'static str,
}

/// Process transaction operations and collect statistics
fn collect_operation_stats(transaction: &Transaction) -> Result<OperationStats> {
    let mut files_to_add = Vec::new();
    let mut files_to_delete = Vec::new();
    let mut has_rewrite = false;

    for op in transaction.operations() {
        match op {
            TransactionOperation::Append(files) => {
                files_to_add.extend(files.clone());
            }
            TransactionOperation::Rewrite {
                files_to_delete: delete,
                files_to_add: add,
            } => {
                files_to_delete.extend(delete.clone());
                files_to_add.extend(add.clone());
                has_rewrite = true;
            }
        }
    }

    if files_to_add.is_empty() && files_to_delete.is_empty() {
        return Err(Error::InvalidInput("No data files to commit".to_string()));
    }

    let operation_type = if has_rewrite { "replace" } else { "append" };

    Ok(OperationStats {
        files_to_add,
        files_to_delete,
        operation_type,
    })
}

/// Try to commit once (no retries)
pub async fn try_commit(
    transaction: &Transaction,
    catalog: &dyn crate::catalog::Catalog,
    timestamp_ms: i64,
) -> Result<()> {
    let table = transaction.table();
    let metadata = table.metadata();
    let file_io = table.file_io();
    let current_schema = metadata.current_schema()?;

    // Collect operation statistics
    let stats = collect_operation_stats(transaction)?;

    // Pick the current partition spec. icepick doesn't parse `default-spec-id`,
    // so fall back to the spec with the highest id (newest); for tables with a
    // single spec this is the only one. For unpartitioned tables, fields() is
    // empty and `partitions` stays []. This count MUST match readers like
    // Trino, which iterate by partition spec field index.
    let current_spec = metadata
        .partition_specs()
        .iter()
        .max_by_key(|s| s.spec_id())
        .cloned()
        .unwrap_or_else(|| crate::spec::PartitionSpec::new(0, Vec::new()));
    let current_spec_id = current_spec.spec_id();
    let current_spec_field_count = current_spec.fields().len();

    // Generate IDs
    let snapshot_id = generate_snapshot_id(table);
    let sequence_number = if metadata.snapshots().is_empty() {
        1
    } else {
        metadata
            .snapshots()
            .iter()
            .filter_map(|s| s.sequence_number())
            .max()
            .map(|max| max + 1)
            .unwrap_or(1)
    };
    debug!(
        "Generated snapshot_id: {}, sequence_number: {}",
        snapshot_id, sequence_number
    );
    let commit_uuid = Uuid::new_v4().to_string().replace('-', "");

    // Files we're removing in this commit. Used to drop the matching manifest
    // entries from rewritten parent manifests — we no longer write DELETE
    // entries (that approach left files referenced and broke orphan cleanup).
    let drop_set: HashSet<String> = stats
        .files_to_delete
        .iter()
        .map(|f| f.file_path().to_string())
        .collect();

    // 1. Write the new manifest holding only ADDED entries (the compaction
    // output, or whatever's being appended). If there's nothing to add we
    // skip writing a new manifest — the commit then consists purely of
    // dropping entries from rewritten parent manifests.
    let added_files_count = stats.files_to_add.len() as i32;
    let added_rows_count: i64 = stats.files_to_add.iter().map(|f| f.record_count()).sum();
    let deleted_files_count = stats.files_to_delete.len() as i32;
    let deleted_rows_count: i64 = stats.files_to_delete.iter().map(|f| f.record_count()).sum();

    let mut manifest_list_entries: Vec<ManifestListEntry> = Vec::new();
    let mut total_existing_files: i64 = 0;
    let mut total_existing_rows: i64 = 0;

    if !stats.files_to_add.is_empty() {
        let manifest_file_path = manifest_path(table.location(), &commit_uuid, 0);
        let manifest_entries_to_write: Vec<ManifestEntry> = stats
            .files_to_add
            .iter()
            .map(|file| ManifestEntry {
                data_file: file.clone(),
                status: ManifestEntryStatus::Added,
            })
            .collect();
        let manifest_bytes = write_manifest_with_entries(
            file_io,
            &manifest_file_path,
            &manifest_entries_to_write,
            snapshot_id,
            sequence_number,
            &current_spec,
            current_schema,
        )
        .await?;
        manifest_list_entries.push(ManifestListEntry {
            manifest_path: manifest_file_path,
            manifest_length: manifest_bytes,
            partition_spec_id: current_spec_id,
            content: 0,
            sequence_number,
            min_sequence_number: sequence_number,
            added_snapshot_id: snapshot_id,
            added_files_count,
            existing_files_count: 0,
            deleted_files_count: 0,
            added_rows_count,
            existing_rows_count: 0,
            deleted_rows_count: 0,
            partitions: vec![FieldSummary::default(); current_spec_field_count],
        });
    }

    // 2. Walk parent manifests. For each one, if any of the files we're
    // dropping live inside it, rewrite the manifest without those entries.
    // Otherwise carry the original manifest reference forward verbatim. This
    // mirrors Spark's RewriteFiles pattern — files we remove become genuinely
    // unreferenced after this commit, so orphan cleanup can reclaim their
    // storage. DELETE-status manifest entries are no longer written.
    if let Some(parent_snapshot) = table.current_snapshot() {
        debug!(
            "Reading parent manifest list from: {}",
            parent_snapshot.manifest_list()
        );
        let parent_manifest_infos =
            ManifestListReader::read_entries(file_io, parent_snapshot.manifest_list()).await?;

        for (idx, parent_info) in parent_manifest_infos.into_iter().enumerate() {
            let parent_total_files =
                parent_info.added_files_count + parent_info.existing_files_count;
            let parent_total_rows = parent_info.added_rows_count + parent_info.existing_rows_count;

            if drop_set.is_empty() {
                // Pure append commit — never rewrite parents.
                manifest_list_entries.push(carry_forward_entry(&parent_info, parent_total_files, parent_total_rows));
                total_existing_files += parent_total_files as i64;
                total_existing_rows += parent_total_rows;
                continue;
            }

            let rewrite_target = manifest_path(table.location(), &commit_uuid, idx + 1);
            match rewrite_manifest(
                file_io,
                &parent_info.manifest_path,
                &rewrite_target,
                &drop_set,
                &current_spec,
                current_schema,
                parent_info.added_snapshot_id,
                parent_info.sequence_number,
            )
            .await?
            {
                RewriteOutcome::Unchanged => {
                    manifest_list_entries.push(carry_forward_entry(&parent_info, parent_total_files, parent_total_rows));
                    total_existing_files += parent_total_files as i64;
                    total_existing_rows += parent_total_rows;
                }
                RewriteOutcome::Rewritten(result) => {
                    let rewritten_entry = ManifestListEntry {
                        manifest_path: result.target_path,
                        manifest_length: result.manifest_length,
                        partition_spec_id: parent_info.partition_spec_id,
                        content: parent_info.content,
                        sequence_number: parent_info.sequence_number,
                        min_sequence_number: parent_info.min_sequence_number,
                        added_snapshot_id: parent_info.added_snapshot_id,
                        added_files_count: 0,
                        existing_files_count: result.existing_files_count,
                        deleted_files_count: 0,
                        added_rows_count: 0,
                        existing_rows_count: result.existing_rows_count,
                        deleted_rows_count: 0,
                        partitions: parent_info.partitions,
                    };
                    total_existing_files += result.existing_files_count as i64;
                    total_existing_rows += result.existing_rows_count;
                    manifest_list_entries.push(rewritten_entry);
                }
                RewriteOutcome::EmptyAfterDrop => {
                    debug!(
                        "Parent manifest {} fully drained by drop set; omitting from new manifest list",
                        parent_info.manifest_path
                    );
                }
            }
        }
    }

    if manifest_list_entries.is_empty() {
        return Err(Error::InvalidInput(
            "Commit produced no manifest list entries — refusing to write empty snapshot"
                .to_string(),
        ));
    }

    let manifest_list_file_path = manifest_list_path(table.location(), snapshot_id, &commit_uuid);

    debug!(
        "Writing manifest list with {} entries total",
        manifest_list_entries.len()
    );

    // 2c. Write manifest list
    write_manifest_list(file_io, &manifest_list_file_path, manifest_list_entries).await?;

    // 3. Create snapshot summary
    // Calculate totals: existing + added - deleted
    let total_data_files =
        total_existing_files + added_files_count as i64 - deleted_files_count as i64;
    let total_records = total_existing_rows + added_rows_count - deleted_rows_count;

    let mut summary_builder = Summary::builder()
        .set("operation", stats.operation_type)
        .set("added-data-files", &added_files_count.to_string())
        .set("added-records", &added_rows_count.to_string())
        .set("total-data-files", &total_data_files.to_string())
        .set("total-records", &total_records.to_string());

    // Add deleted file stats for rewrite operations
    if deleted_files_count > 0 {
        summary_builder = summary_builder
            .set("deleted-data-files", &deleted_files_count.to_string())
            .set("deleted-records", &deleted_rows_count.to_string());
    }

    let summary = summary_builder.build();

    // Handle parent snapshot ID
    let current_snap_id = metadata.current_snapshot_id();
    debug!("Current snapshot ID from metadata: {:?}", current_snap_id);
    let schema_id = current_schema.schema_id();
    debug!("Building snapshot with schema_id: {}", schema_id);

    let mut snapshot_builder = Snapshot::builder().with_snapshot_id(snapshot_id);

    if let Some(parent_id) = current_snap_id {
        if parent_id != -1 {
            debug!("Setting parent_snapshot_id: {}", parent_id);
            snapshot_builder = snapshot_builder.with_parent_snapshot_id(parent_id);
        } else {
            debug!("No parent snapshot (current_snapshot_id = -1)");
        }
    }

    let snapshot = snapshot_builder
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(timestamp_ms)
        .with_manifest_list(&manifest_list_file_path)
        .with_summary(summary)
        .with_schema_id(schema_id)
        .build()?;

    debug!(
        "Built snapshot - parent: {:?}, schema: {:?}",
        snapshot.parent_snapshot_id(),
        snapshot.schema_id()
    );

    // 4. Update metadata
    let new_metadata = metadata.add_snapshot(snapshot.clone(), timestamp_ms);

    if let Some(last_snapshot) = new_metadata.snapshots().last() {
        debug!(
            "Snapshot in new_metadata before serialization - parent: {:?}, schema: {:?}",
            last_snapshot.parent_snapshot_id(),
            last_snapshot.schema_id()
        );
    }

    // 5. Write new metadata file
    let old_metadata_path = table.metadata_location();
    let new_metadata_path = next_metadata_path(table.location(), old_metadata_path, &commit_uuid);
    let metadata_json = serde_json::to_vec_pretty(&new_metadata)?;

    if let Ok(json_str) = std::str::from_utf8(&metadata_json) {
        if let Some(snapshot_section) = json_str.rfind("\"snapshot-id\"") {
            let snippet = &json_str[snapshot_section.saturating_sub(200)
                ..std::cmp::min(snapshot_section + 500, json_str.len())];
            debug!("Serialized snapshot snippet:\n{}", snippet);
        }
    }

    debug!("Writing metadata to: {}", new_metadata_path);
    file_io.write(&new_metadata_path, metadata_json).await?;

    // 6. Update catalog to point to new metadata
    catalog
        .update_table_metadata(table.identifier(), old_metadata_path, &new_metadata_path)
        .await?;

    Ok(())
}

/// Commit a transaction with automatic retry on concurrent modification
pub async fn commit_transaction(
    transaction: Transaction,
    catalog: &dyn crate::catalog::Catalog,
    timestamp_ms: i64,
) -> Result<()> {
    const MAX_RETRIES: u32 = 3;

    let mut transaction = transaction;

    for attempt in 0..MAX_RETRIES {
        match try_commit(&transaction, catalog, timestamp_ms).await {
            Ok(()) => return Ok(()),
            Err(e @ Error::ConcurrentModification { .. }) => {
                if attempt == MAX_RETRIES - 1 {
                    return Err(e);
                }

                let refreshed_table = catalog.load_table(transaction.table().identifier()).await?;
                transaction = transaction.rebind_table(refreshed_table);
            }
            Err(e) => return Err(e),
        }
    }

    unreachable!("Loop should always return within MAX_RETRIES iterations")
}

/// Reference a parent manifest unchanged in the new manifest list, flipping
/// its ADDED counts into EXISTING since from this commit's perspective those
/// files were added by a prior snapshot.
fn carry_forward_entry(
    parent_info: &crate::reader::manifest::ManifestFileInfo,
    parent_total_files: i32,
    parent_total_rows: i64,
) -> ManifestListEntry {
    ManifestListEntry {
        manifest_path: parent_info.manifest_path.clone(),
        manifest_length: parent_info.manifest_length,
        partition_spec_id: parent_info.partition_spec_id,
        content: parent_info.content,
        sequence_number: parent_info.sequence_number,
        min_sequence_number: parent_info.min_sequence_number,
        added_snapshot_id: parent_info.added_snapshot_id,
        added_files_count: 0,
        existing_files_count: parent_total_files,
        deleted_files_count: parent_info.deleted_files_count,
        added_rows_count: 0,
        existing_rows_count: parent_total_rows,
        deleted_rows_count: parent_info.deleted_rows_count,
        partitions: parent_info.partitions.clone(),
    }
}
