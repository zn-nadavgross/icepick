use crate::error::{Error, Result};
use crate::spec::TableMetadata;
use chrono::Utc;
use std::collections::HashMap;

use super::ExpireSnapshotsOptions;

const PROP_MAX_SNAPSHOT_AGE_MS: &str = "history.expire.max-snapshot-age-ms";
const PROP_MIN_SNAPSHOTS_TO_KEEP: &str = "history.expire.min-snapshots-to-keep";

#[derive(Debug, Clone)]
pub(crate) struct ResolvedExpireOptions {
    pub(crate) older_than_ms: Option<i64>,
    pub(crate) retain_last: i32,
    pub(crate) delete_orphan_data: bool,
    pub(crate) delete_orphan_manifests: bool,
    pub(crate) max_snapshots_per_run: usize,
    pub(crate) manifest_scan_concurrency: usize,
    pub(crate) cleanup_concurrency: usize,
    pub(crate) dry_run: bool,
}

pub(crate) fn resolve_options(
    metadata: &TableMetadata,
    options: ExpireSnapshotsOptions,
) -> Result<ResolvedExpireOptions> {
    let properties = metadata.properties();
    let older_than_ms = options
        .older_than_ms
        .or_else(|| derive_older_than_ms(properties));
    let retain_last = options
        .retain_last
        .or_else(|| derive_retain_last(properties))
        .unwrap_or(0);

    if older_than_ms.is_none() && retain_last <= 0 {
        return Err(Error::invalid_input(
            "ExpireSnapshots requires older_than_ms or a positive retain_last",
        ));
    }

    let max_snapshots_per_run = options.max_snapshots_per_run.unwrap_or(100);
    if max_snapshots_per_run == 0 {
        return Err(Error::invalid_input(
            "max_snapshots_per_run must be greater than zero",
        ));
    }

    Ok(ResolvedExpireOptions {
        older_than_ms,
        retain_last,
        delete_orphan_data: options.delete_orphan_data,
        delete_orphan_manifests: options.delete_orphan_manifests,
        max_snapshots_per_run,
        manifest_scan_concurrency: options.manifest_scan_concurrency.unwrap_or(4),
        cleanup_concurrency: options.cleanup_concurrency.unwrap_or(1),
        dry_run: options.dry_run,
    })
}

fn derive_older_than_ms(properties: &HashMap<String, String>) -> Option<i64> {
    let max_age_ms = properties
        .get(PROP_MAX_SNAPSHOT_AGE_MS)
        .and_then(|value| value.parse::<i64>().ok());
    max_age_ms.map(|age| Utc::now().timestamp_millis() - age)
}

fn derive_retain_last(properties: &HashMap<String, String>) -> Option<i32> {
    properties
        .get(PROP_MIN_SNAPSHOTS_TO_KEEP)
        .and_then(|value| value.parse::<i32>().ok())
}
