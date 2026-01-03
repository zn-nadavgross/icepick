use crate::error::Result;
use crate::reader::{ManifestListReader, ManifestReader};
use crate::spec::{Snapshot, TableMetadata};
use futures::future::{BoxFuture, FutureExt};
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{HashMap, HashSet};

use super::plan::ExpirationPlan;

#[derive(Default)]
pub(crate) struct CleanupPlan {
    pub(crate) orphan_data_files: Vec<String>,
    pub(crate) orphan_manifest_files: Vec<String>,
    pub(crate) orphan_manifest_lists: Vec<String>,
}

pub(crate) async fn plan_orphan_files(
    file_io: &crate::io::FileIO,
    metadata: &TableMetadata,
    plan: &ExpirationPlan,
) -> Result<CleanupPlan> {
    let snapshot_map: HashMap<i64, &Snapshot> = metadata
        .snapshots()
        .iter()
        .map(|snapshot| (snapshot.snapshot_id(), snapshot))
        .collect();

    let (kept_manifest_files, kept_data_files) = collect_snapshot_files_for_ids(
        file_io,
        &plan.kept_snapshot_ids,
        &snapshot_map,
        plan.manifest_scan_concurrency,
        "kept",
    )
    .await?;

    let mut orphan_manifest_lists = HashSet::new();
    let (expired_manifest_files, expired_data_files) = collect_snapshot_files_for_ids(
        file_io,
        &plan.expired_snapshot_ids,
        &snapshot_map,
        plan.manifest_scan_concurrency,
        "expired",
    )
    .await?;

    for snapshot_id in &plan.expired_snapshot_ids {
        if let Some(snapshot) = snapshot_map.get(snapshot_id) {
            orphan_manifest_lists.insert(snapshot.manifest_list().to_string());
        }
    }

    let orphan_manifest_files: Vec<String> = expired_manifest_files
        .difference(&kept_manifest_files)
        .cloned()
        .collect();
    let orphan_data_files: Vec<String> = expired_data_files
        .difference(&kept_data_files)
        .cloned()
        .collect();

    Ok(CleanupPlan {
        orphan_data_files,
        orphan_manifest_files,
        orphan_manifest_lists: orphan_manifest_lists.into_iter().collect(),
    })
}

async fn collect_snapshot_files(
    file_io: &crate::io::FileIO,
    snapshot: &Snapshot,
) -> Result<SnapshotFiles> {
    let manifest_infos =
        ManifestListReader::read_entries(file_io, snapshot.manifest_list()).await?;
    let mut manifest_files = HashSet::new();
    let mut data_files = HashSet::new();

    for info in manifest_infos {
        manifest_files.insert(info.manifest_path.clone());
        let entries = ManifestReader::read(file_io, &info.manifest_path).await?;
        for entry in entries {
            data_files.insert(entry.file_path);
        }
    }

    Ok(SnapshotFiles {
        manifest_files,
        data_files,
    })
}

async fn collect_snapshot_files_for_ids(
    file_io: &crate::io::FileIO,
    snapshot_ids: &[i64],
    snapshot_map: &HashMap<i64, &Snapshot>,
    concurrency: usize,
    label: &'static str,
) -> Result<(HashSet<String>, HashSet<String>)> {
    let concurrency = concurrency.max(1);
    let mut snapshots = Vec::new();
    for snapshot_id in snapshot_ids {
        if let Some(snapshot) = snapshot_map.get(snapshot_id) {
            snapshots.push((*snapshot_id, (*snapshot).clone()));
        }
    }

    let mut manifest_files = HashSet::new();
    let mut data_files = HashSet::new();
    let mut in_flight: FuturesUnordered<BoxFuture<'static, Result<SnapshotFiles>>> =
        FuturesUnordered::new();
    let mut iter = snapshots.into_iter();

    for _ in 0..concurrency {
        if let Some((snapshot_id, snapshot)) = iter.next() {
            let file_io = file_io.clone();
            in_flight.push(
                async move {
                    tracing::debug!(
                        target: "icepick::maintenance",
                        snapshot_id,
                        manifest_list = %snapshot.manifest_list(),
                        label,
                        "Scanning snapshot manifest list"
                    );
                    collect_snapshot_files(&file_io, &snapshot).await
                }
                .boxed(),
            );
        }
    }

    while let Some(result) = in_flight.next().await {
        let files = result?;
        manifest_files.extend(files.manifest_files);
        data_files.extend(files.data_files);

        if let Some((snapshot_id, snapshot)) = iter.next() {
            let file_io = file_io.clone();
            in_flight.push(
                async move {
                    tracing::debug!(
                        target: "icepick::maintenance",
                        snapshot_id,
                        manifest_list = %snapshot.manifest_list(),
                        label,
                        "Scanning snapshot manifest list"
                    );
                    collect_snapshot_files(&file_io, &snapshot).await
                }
                .boxed(),
            );
        }
    }

    Ok((manifest_files, data_files))
}

struct SnapshotFiles {
    manifest_files: HashSet<String>,
    data_files: HashSet<String>,
}

pub(crate) async fn cleanup_orphan_files(
    file_io: &crate::io::FileIO,
    plan: &CleanupPlan,
    concurrency: usize,
) -> Result<()> {
    let concurrency = concurrency.max(1);
    tracing::debug!(
        target: "icepick::maintenance",
        data_files = plan.orphan_data_files.len(),
        manifest_files = plan.orphan_manifest_files.len(),
        manifest_lists = plan.orphan_manifest_lists.len(),
        concurrency,
        "Starting orphan file cleanup"
    );
    delete_paths(file_io, &plan.orphan_data_files, concurrency).await?;
    delete_paths(file_io, &plan.orphan_manifest_files, concurrency).await?;
    delete_paths(file_io, &plan.orphan_manifest_lists, concurrency).await?;
    Ok(())
}

async fn delete_paths(
    file_io: &crate::io::FileIO,
    paths: &[String],
    concurrency: usize,
) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }

    tracing::debug!(
        target: "icepick::maintenance",
        path_count = paths.len(),
        concurrency,
        "Deleting orphaned files"
    );

    let mut in_flight: FuturesUnordered<BoxFuture<'static, Result<()>>> = FuturesUnordered::new();
    let mut iter = paths.iter();

    for _ in 0..concurrency {
        if let Some(path) = iter.next() {
            let file_io = file_io.clone();
            let path = path.clone();
            in_flight.push(
                async move {
                    tracing::debug!(
                        target: "icepick::maintenance",
                        path = %path,
                        "Deleting orphan file"
                    );
                    file_io.delete(&path).await
                }
                .boxed(),
            );
        }
    }

    while let Some(result) = in_flight.next().await {
        result?;
        if let Some(path) = iter.next() {
            let file_io = file_io.clone();
            let path = path.clone();
            in_flight.push(
                async move {
                    tracing::debug!(
                        target: "icepick::maintenance",
                        path = %path,
                        "Deleting orphan file"
                    );
                    file_io.delete(&path).await
                }
                .boxed(),
            );
        }
    }

    Ok(())
}
