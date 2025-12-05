//! Register existing Parquet files without rewriting data.

mod introspect;
mod types;
mod validate;

use std::collections::{HashMap, HashSet};

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::spec::{NamespaceIdent, TableCreation, TableIdent};
use crate::table::Table;
#[cfg(not(target_family = "wasm"))]
use chrono::Utc;
use validate::validate_schema;

pub use introspect::{
    infer_partition_values_from_path, introspect_parquet_file, ParquetIntrospection,
};
pub use types::{
    DataFileFormat, DataFileInput, DataFileRegistrar, EncryptionMetadata, FileMetrics,
    PartitionValue, RegisterOptions, RegisterResult, SkippedFile, SkippedReason,
};

/// Register pre-existing files against an Iceberg table.
pub async fn register_data_files<C: Catalog>(
    catalog: &C,
    namespace: NamespaceIdent,
    table: TableIdent,
    files: Vec<DataFileInput>,
    options: RegisterOptions,
) -> Result<RegisterResult> {
    if files.is_empty() {
        return Err(Error::invalid_input("No data files provided"));
    }

    let timestamp_ms = resolve_timestamp(options.timestamp_ms)?;

    let mut table_was_created = false;
    let target_table = match catalog.load_table(&table).await {
        Ok(table) => table,
        Err(Error::NotFound { .. }) if !options.fail_if_missing => {
            let schema = options.table_schema.clone().ok_or_else(|| {
                Error::invalid_input(
                    "Table is missing and no schema provided. Supply RegisterOptions::allow_create_with_schema.",
                )
            })?;
            ensure_namespace_exists(catalog, &namespace).await?;
            let mut creation_builder = TableCreation::builder()
                .with_name(table.name())
                .with_schema(schema);

            if let Some(partition_spec) = options.partition_spec.clone() {
                creation_builder = creation_builder.with_partition_spec(partition_spec);
            }

            let creation = creation_builder.build()?;
            let created = catalog.create_table(&namespace, creation).await?;
            table_was_created = true;
            created
        }
        Err(err) => return Err(err),
    };

    let partition_spec = target_table.metadata().partition_specs().first();
    let table_schema = target_table.schema()?.clone();

    validate_schema(&target_table, &options, &files)?;

    let existing_files = current_file_paths(&target_table).await?;
    let mut skipped_files = Vec::new();
    let mut data_files = Vec::new();

    for input in files {
        if existing_files.contains(&input.file_path) {
            skipped_files.push(SkippedFile {
                file_path: input.file_path,
                reason: SkippedReason::AlreadyCommitted,
            });
            continue;
        }

        let data_file = input.into_data_file(partition_spec, &table_schema)?;
        data_files.push(data_file);
    }

    if data_files.is_empty() {
        if options.allow_noop {
            let snapshot_id = target_table
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .unwrap_or_default();
            return Ok(RegisterResult {
                snapshot_id,
                added_files: 0,
                added_records: 0,
                table_was_created,
                skipped_files,
            });
        } else {
            return Err(Error::noop_registration(
                "All provided files were already present",
            ));
        }
    }

    let added_files = data_files.len();
    let added_records: i64 = data_files.iter().map(|f| f.record_count()).sum();

    target_table
        .transaction()
        .append(data_files)
        .commit(catalog, timestamp_ms)
        .await?;

    let refreshed_table = catalog.load_table(&table).await?;
    let snapshot = refreshed_table
        .current_snapshot()
        .ok_or_else(|| Error::unexpected("Commit succeeded but table has no snapshot"))?;

    Ok(RegisterResult {
        snapshot_id: snapshot.snapshot_id(),
        added_files: snapshot
            .summary()
            .get("added-data-files")
            .and_then(|v| v.parse().ok())
            .unwrap_or(added_files as i32) as usize,
        added_records,
        table_was_created,
        skipped_files,
    })
}

async fn ensure_namespace_exists<C: Catalog>(
    catalog: &C,
    namespace: &NamespaceIdent,
) -> Result<()> {
    if !catalog.namespace_exists(namespace).await? {
        catalog
            .create_namespace(namespace, HashMap::new())
            .await
            .map_err(|e| {
                if matches!(e, Error::NotFound { .. }) {
                    Error::invalid_input(format!("Namespace {} does not exist", namespace))
                } else {
                    e
                }
            })?;
    }
    Ok(())
}

fn resolve_timestamp(explicit: Option<i64>) -> Result<i64> {
    match explicit {
        Some(ts) => Ok(ts),
        None => {
            #[cfg(target_family = "wasm")]
            {
                Err(Error::invalid_input(
                    "timestamp_ms is required on WASM targets",
                ))
            }
            #[cfg(not(target_family = "wasm"))]
            {
                Ok(Utc::now().timestamp_millis())
            }
        }
    }
}

async fn current_file_paths(table: &Table) -> Result<HashSet<String>> {
    if table.current_snapshot().is_none() {
        return Ok(HashSet::new());
    }

    let entries = table.files().await?;
    Ok(entries.into_iter().map(|e| e.file_path).collect())
}
