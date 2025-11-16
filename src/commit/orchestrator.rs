//! Orchestrate transaction commit with retries

use crate::commit::paths::{manifest_list_path, manifest_path, metadata_path};
use crate::error::{Error, Result};
use crate::manifest::writer::{write_manifest, write_manifest_list};
use crate::spec::{Snapshot, Summary};
use crate::transaction::{Transaction, TransactionOperation};
use uuid::Uuid;

/// Try to commit once (no retries)
#[allow(dead_code)]
pub async fn try_commit(transaction: &Transaction<'_>) -> Result<()> {
    let table = transaction.table();
    let metadata = table.metadata();
    let file_io = table.file_io();

    // Generate IDs
    let snapshot_id = metadata.current_snapshot_id().map(|id| id + 1).unwrap_or(1);
    let sequence_number = snapshot_id;
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

    // 2. Write manifest list
    let manifest_list_file_path = manifest_list_path(table.location(), snapshot_id, &commit_uuid);
    let added_files_count = all_data_files.len() as i32;
    let added_rows_count: i64 = all_data_files.iter().map(|f| f.record_count()).sum();

    write_manifest_list(
        file_io,
        &manifest_list_file_path,
        &manifest_file_path,
        manifest_bytes,
        snapshot_id,
        sequence_number,
        added_files_count,
        added_rows_count,
    )
    .await?;

    // 3. Create snapshot
    let summary = Summary::builder()
        .set("operation", "append")
        .set("added-data-files", &added_files_count.to_string())
        .set("added-records", &added_rows_count.to_string())
        .set("total-data-files", &added_files_count.to_string())
        .set("total-records", &added_rows_count.to_string())
        .build();

    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(metadata.current_snapshot_id().unwrap_or(0))
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        )
        .with_manifest_list(&manifest_list_file_path)
        .with_summary(summary)
        .with_schema_id(metadata.current_schema().schema_id())
        .build()?;

    // 4. Update metadata
    let new_metadata = metadata.add_snapshot(snapshot);

    // 5. Write new metadata file
    let new_version = metadata.snapshots().len() + 1;
    let new_metadata_path = metadata_path(table.location(), new_version);
    let metadata_json = serde_json::to_vec_pretty(&new_metadata)?;
    file_io.write(&new_metadata_path, metadata_json).await?;

    // TODO: Update catalog pointer (Phase 7)

    Ok(())
}

/// Commit a transaction with automatic retry on concurrent modification
#[allow(dead_code)]
pub async fn commit_transaction(_transaction: Transaction<'_>) -> Result<()> {
    todo!("Implement commit orchestration")
}
