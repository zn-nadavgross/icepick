//! Orchestrate transaction commit with retries

use crate::commit::paths::{manifest_list_path, manifest_path, next_metadata_path};
use crate::error::{Error, Result};
use crate::manifest::writer::{write_manifest, write_manifest_list, ManifestListEntry};
use crate::reader::ManifestListReader;
use crate::spec::{Snapshot, Summary};
use crate::transaction::{Transaction, TransactionOperation};
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

    // Generate IDs
    let snapshot_id = generate_snapshot_id(table);
    // Sequence number should be based on last_sequence_number from metadata
    // For now, we'll compute it: if there are snapshots, max sequence + 1, otherwise 1
    let sequence_number = if metadata.snapshots().is_empty() {
        1 // First snapshot gets sequence number 1
    } else {
        // Find max sequence number from existing snapshots and add 1
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

    // Extract data files from operations
    let mut all_data_files = Vec::new();
    for op in transaction.operations() {
        let TransactionOperation::Append(files) = op;
        all_data_files.extend(files.clone());
    }

    if all_data_files.is_empty() {
        return Err(Error::InvalidInput("No data files to commit".to_string()));
    }

    // 1. Write manifest file
    let manifest_file_path = manifest_path(table.location(), &commit_uuid, 0);
    let manifest_bytes = write_manifest(
        file_io,
        &manifest_file_path,
        &all_data_files,
        snapshot_id,
        sequence_number,
    )
    .await?;

    // 2. Build manifest list entries
    let manifest_list_file_path = manifest_list_path(table.location(), snapshot_id, &commit_uuid);
    let added_files_count = all_data_files.len() as i32;
    let added_rows_count: i64 = all_data_files.iter().map(|f| f.record_count()).sum();

    let mut manifest_entries = Vec::new();

    // 2a. Carry forward manifests from parent snapshot (if exists)
    if let Some(parent_snapshot) = table.current_snapshot() {
        debug!(
            "Reading parent manifest list from: {}",
            parent_snapshot.manifest_list()
        );
        let parent_manifest_infos =
            ManifestListReader::read_entries(file_io, parent_snapshot.manifest_list()).await?;

        for parent_info in parent_manifest_infos {
            // Convert parent manifests to "existing" entries
            // Move counts from "added" to "existing" since these files now exist from a previous snapshot
            let existing_entry = ManifestListEntry {
                manifest_path: parent_info.manifest_path,
                manifest_length: parent_info.manifest_length,
                partition_spec_id: parent_info.partition_spec_id,
                content: parent_info.content,
                sequence_number: parent_info.sequence_number,
                min_sequence_number: parent_info.min_sequence_number,
                added_snapshot_id: parent_info.added_snapshot_id,
                added_files_count: 0, // No new files from this old manifest
                existing_files_count: parent_info.added_files_count
                    + parent_info.existing_files_count, // All files are now existing
                deleted_files_count: parent_info.deleted_files_count,
                added_rows_count: 0, // No new rows from this old manifest
                existing_rows_count: parent_info.added_rows_count + parent_info.existing_rows_count, // All rows are now existing
                deleted_rows_count: parent_info.deleted_rows_count,
            };
            manifest_entries.push(existing_entry);
        }

        debug!(
            "Carried forward {} manifests from parent snapshot",
            manifest_entries.len()
        );
    }

    // 2b. Add the new manifest entry
    let new_manifest_entry = ManifestListEntry {
        manifest_path: manifest_file_path.clone(),
        manifest_length: manifest_bytes,
        // TODO: Support partitioned tables
        // Currently hardcoded to 0 (unpartitioned). When partition support is added,
        // this should use the actual partition spec ID from the table metadata.
        partition_spec_id: 0,
        content: 0, // 0 = DATA
        sequence_number,
        min_sequence_number: sequence_number,
        added_snapshot_id: snapshot_id,
        added_files_count,
        existing_files_count: 0,
        deleted_files_count: 0,
        added_rows_count,
        existing_rows_count: 0,
        deleted_rows_count: 0,
    };
    manifest_entries.push(new_manifest_entry);

    debug!(
        "Writing manifest list with {} entries total",
        manifest_entries.len()
    );

    // 2c. Write manifest list
    write_manifest_list(file_io, &manifest_list_file_path, manifest_entries).await?;

    // 3. Create snapshot
    let summary = Summary::builder()
        .set("operation", "append")
        .set("added-data-files", &added_files_count.to_string())
        .set("added-records", &added_rows_count.to_string())
        .set("total-data-files", &added_files_count.to_string())
        .set("total-records", &added_rows_count.to_string())
        .build();

    // Handle parent snapshot ID: -1 means no parent (first snapshot)
    let current_snap_id = metadata.current_snapshot_id();
    debug!("Current snapshot ID from metadata: {:?}", current_snap_id);
    let schema_id = current_schema.schema_id();
    debug!("Building snapshot with schema_id: {}", schema_id);

    let mut snapshot_builder = Snapshot::builder().with_snapshot_id(snapshot_id);

    // Only set parent if there is a valid parent (not -1)
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

    // Debug: Check the snapshot in new_metadata before serialization
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

    // Debug: Print a snippet of the serialized JSON to see if parent-snapshot-id is there
    if let Ok(json_str) = std::str::from_utf8(&metadata_json) {
        if let Some(snapshot_section) = json_str.rfind("\"snapshot-id\"") {
            let snippet = &json_str[snapshot_section.saturating_sub(200)
                ..std::cmp::min(snapshot_section + 500, json_str.len())];
            debug!("Serialized snapshot snippet:\n{}", snippet);
        }
    }

    // Write metadata file
    debug!("Writing metadata to: {}", new_metadata_path);
    // Note: This will fail with 412 if file exists, which is fine for testing
    // In production, we should handle the exists check properly
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
