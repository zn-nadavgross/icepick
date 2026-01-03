use crate::error::Result;
use crate::spec::{Snapshot, TableMetadata};
use std::collections::{HashMap, HashSet};

use super::options::ResolvedExpireOptions;

#[derive(Debug)]
pub(crate) struct ExpirationPlan {
    pub(crate) expired_snapshot_ids: Vec<i64>,
    pub(crate) kept_snapshot_ids: Vec<i64>,
    pub(crate) manifest_scan_concurrency: usize,
}

pub(crate) fn plan_expiration(
    metadata: &TableMetadata,
    options: &ResolvedExpireOptions,
) -> Result<ExpirationPlan> {
    let snapshots = metadata.snapshots();
    if snapshots.is_empty() {
        return Ok(ExpirationPlan {
            expired_snapshot_ids: Vec::new(),
            kept_snapshot_ids: Vec::new(),
            manifest_scan_concurrency: options.manifest_scan_concurrency,
        });
    }

    let snapshot_ids: HashSet<i64> = snapshots.iter().map(Snapshot::snapshot_id).collect();
    let ordered_ids = ordered_snapshot_ids(metadata, &snapshot_ids);

    let mut keep = HashSet::new();
    if let Some(current_id) = normalize_snapshot_id(metadata.current_snapshot_id()) {
        keep.insert(current_id);
    }

    for reference in metadata.refs().values() {
        keep.insert(reference.snapshot_id());
    }

    if options.retain_last > 0 {
        for id in ordered_ids.iter().rev().take(options.retain_last as usize) {
            keep.insert(*id);
        }
    }

    let snapshot_map: HashMap<i64, &Snapshot> = snapshots
        .iter()
        .map(|snapshot| (snapshot.snapshot_id(), snapshot))
        .collect();

    let mut expired = Vec::new();
    for snapshot_id in ordered_ids {
        let snapshot = match snapshot_map.get(&snapshot_id) {
            Some(snapshot) => *snapshot,
            None => continue,
        };
        if keep.contains(&snapshot_id) {
            continue;
        }
        if let Some(cutoff) = options.older_than_ms {
            if snapshot.timestamp_ms() >= cutoff {
                continue;
            }
        }
        expired.push(snapshot_id);
        if expired.len() >= options.max_snapshots_per_run {
            break;
        }
    }

    Ok(ExpirationPlan {
        expired_snapshot_ids: expired,
        kept_snapshot_ids: keep.into_iter().collect(),
        manifest_scan_concurrency: options.manifest_scan_concurrency,
    })
}

fn ordered_snapshot_ids(metadata: &TableMetadata, snapshot_ids: &HashSet<i64>) -> Vec<i64> {
    if !metadata.snapshot_log().is_empty() {
        metadata
            .snapshot_log()
            .iter()
            .map(|entry| entry.snapshot_id())
            .filter(|id| snapshot_ids.contains(id))
            .collect()
    } else {
        let mut snapshots = metadata.snapshots().to_vec();
        snapshots.sort_by_key(|snapshot| snapshot.timestamp_ms());
        snapshots
            .into_iter()
            .map(|snapshot| snapshot.snapshot_id())
            .collect()
    }
}

pub(crate) fn normalize_snapshot_id(id: Option<i64>) -> Option<i64> {
    match id {
        Some(-1) | None => None,
        Some(value) => Some(value),
    }
}
