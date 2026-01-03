//! Table maintenance utilities (snapshot expiration, cleanup, etc.)

mod cleanup;
mod options;
mod plan;

#[cfg(test)]
mod tests;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::spec::TableIdent;
use crate::table::Table;
use async_trait::async_trait;

use cleanup::{cleanup_orphan_files, plan_orphan_files, CleanupPlan};
use options::resolve_options;
use plan::{normalize_snapshot_id, plan_expiration};

/// Maintenance hooks for catalog implementations.
///
/// This trait is implemented by REST-backed catalogs that can submit Iceberg
/// commit updates, such as `remove-snapshots`.
///
/// # Examples
///
/// ```no_run
/// use icepick::maintenance::CatalogMaintenance;
/// use icepick::catalog::Catalog;
/// use icepick::R2Catalog;
/// use icepick::spec::TableIdent;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let catalog = R2Catalog::new("catalog", "account", "bucket", "token").await?;
/// let table_id = TableIdent::from_strs(&["namespace"], "table");
/// let table = catalog.load_table(&table_id).await?;
/// let _maintenance: &dyn CatalogMaintenance = &catalog;
/// let _ = table.metadata().table_uuid();
/// # Ok(())
/// # }
/// ```
#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
pub trait CatalogMaintenance: Catalog {
    /// Remove snapshots from a table using the catalog's commit API.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog does not support snapshot removal
    /// or if the commit fails due to concurrent modification.
    async fn remove_snapshots(
        &self,
        identifier: &TableIdent,
        table_uuid: &str,
        current_snapshot_id: Option<i64>,
        snapshot_ids: Vec<i64>,
    ) -> Result<()> {
        let _ = (identifier, table_uuid, current_snapshot_id, snapshot_ids);
        Err(Error::invalid_input(
            "Snapshot expiration is not supported by this catalog implementation",
        ))
    }
}

/// Options for expiring snapshots.
///
/// # Examples
///
/// ```no_run
/// use icepick::maintenance::ExpireSnapshotsOptions;
///
/// let options = ExpireSnapshotsOptions {
///     older_than_ms: Some(1_700_000_000_000),
///     retain_last: Some(1),
///     delete_orphan_data: false,
///     delete_orphan_manifests: false,
///     max_snapshots_per_run: Some(100),
///     manifest_scan_concurrency: Some(4),
///     cleanup_concurrency: Some(4),
///     dry_run: true,
/// };
/// # let _ = options;
/// ```
#[derive(Debug, Clone, Default)]
pub struct ExpireSnapshotsOptions {
    /// Expire snapshots older than this timestamp (milliseconds since epoch).
    pub older_than_ms: Option<i64>,
    /// Keep at least this many most recent snapshots.
    pub retain_last: Option<i32>,
    /// Delete data files that are no longer referenced after expiration.
    pub delete_orphan_data: bool,
    /// Delete manifest and manifest list files that are no longer referenced.
    pub delete_orphan_manifests: bool,
    /// Limit the number of snapshots expired in a single run.
    pub max_snapshots_per_run: Option<usize>,
    /// Maximum number of concurrent manifest list scans.
    pub manifest_scan_concurrency: Option<usize>,
    /// Maximum number of concurrent delete operations during cleanup.
    pub cleanup_concurrency: Option<usize>,
    /// Only plan the expiration; do not commit changes or delete files.
    pub dry_run: bool,
}

/// Result of an expire snapshots operation.
///
/// # Examples
///
/// ```no_run
/// use icepick::maintenance::ExpireSnapshotsResult;
///
/// let result = ExpireSnapshotsResult::default();
/// println!("Expired: {}", result.expired_snapshot_ids.len());
/// ```
#[derive(Debug, Clone, Default)]
pub struct ExpireSnapshotsResult {
    /// Snapshot IDs that were expired (or would be expired in dry-run).
    pub expired_snapshot_ids: Vec<i64>,
    /// Data files deleted as orphaned.
    pub deleted_data_files: Vec<String>,
    /// Manifest files deleted as orphaned.
    pub deleted_manifest_files: Vec<String>,
    /// Manifest list files deleted as orphaned.
    pub deleted_manifest_lists: Vec<String>,
}

/// Expire old snapshots based on the provided options and catalog capabilities.
///
/// This removes snapshots from table metadata and optionally deletes orphaned files.
///
/// # Errors
///
/// Returns an error if no retention policy is specified or if the catalog
/// cannot commit snapshot removal updates.
///
/// # Examples
///
/// ```no_run
/// use icepick::maintenance::{expire_snapshots, ExpireSnapshotsOptions};
/// use icepick::catalog::Catalog;
/// use icepick::R2Catalog;
/// use icepick::spec::TableIdent;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let catalog = R2Catalog::new("catalog", "account", "bucket", "token").await?;
/// let table_id = TableIdent::from_strs(&["namespace"], "table");
/// let table = catalog.load_table(&table_id).await?;
/// let options = ExpireSnapshotsOptions {
///     older_than_ms: Some(chrono::Utc::now().timestamp_millis() - 86_400_000),
///     retain_last: Some(1),
///     ..Default::default()
/// };
///
/// let result = expire_snapshots(&table, &catalog, options).await?;
/// println!("Expired snapshots: {}", result.expired_snapshot_ids.len());
/// # Ok(())
/// # }
/// ```
pub async fn expire_snapshots(
    table: &Table,
    catalog: &dyn CatalogMaintenance,
    options: ExpireSnapshotsOptions,
) -> Result<ExpireSnapshotsResult> {
    let fresh_table = catalog.load_table(table.identifier()).await?;
    let metadata = fresh_table.metadata();
    let resolved = resolve_options(metadata, options)?;
    let plan = plan_expiration(metadata, &resolved)?;
    let expired_snapshot_ids = plan.expired_snapshot_ids.clone();

    if expired_snapshot_ids.is_empty() {
        return Ok(ExpireSnapshotsResult::default());
    }

    let cleanup_requested = resolved.delete_orphan_data || resolved.delete_orphan_manifests;
    let mut cleanup_plan = if cleanup_requested {
        plan_orphan_files(fresh_table.file_io(), metadata, &plan).await?
    } else {
        CleanupPlan::default()
    };

    if !resolved.delete_orphan_data {
        cleanup_plan.orphan_data_files.clear();
    }
    if !resolved.delete_orphan_manifests {
        cleanup_plan.orphan_manifest_files.clear();
        cleanup_plan.orphan_manifest_lists.clear();
    }

    if !resolved.dry_run {
        catalog
            .remove_snapshots(
                fresh_table.identifier(),
                metadata.table_uuid(),
                normalize_snapshot_id(metadata.current_snapshot_id()),
                expired_snapshot_ids.clone(),
            )
            .await?;

        if cleanup_requested {
            cleanup_orphan_files(
                fresh_table.file_io(),
                &cleanup_plan,
                resolved.cleanup_concurrency,
            )
            .await?;
        }
    }

    Ok(ExpireSnapshotsResult {
        expired_snapshot_ids,
        deleted_data_files: cleanup_plan.orphan_data_files,
        deleted_manifest_files: cleanup_plan.orphan_manifest_files,
        deleted_manifest_lists: cleanup_plan.orphan_manifest_lists,
    })
}
