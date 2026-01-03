use super::*;
use crate::catalog::Catalog;
use crate::manifest::writer::{write_manifest, write_manifest_list, ManifestListEntry};
use crate::spec::{
    DataFile, NamespaceIdent, NestedField, PrimitiveType, Schema, Snapshot, TableIdent,
    TableMetadata, Type,
};
use crate::table::Table;
use async_trait::async_trait;
use chrono::Utc;
use opendal::Operator;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct TestCatalog {
    table: Table,
    removed: Arc<Mutex<Vec<i64>>>,
}

#[async_trait]
impl Catalog for TestCatalog {
    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Ok(())
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> Result<bool> {
        Ok(true)
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        Ok(vec![self.table.identifier().clone()])
    }

    async fn table_exists(&self, _identifier: &TableIdent) -> Result<bool> {
        Ok(true)
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: crate::spec::TableCreation,
    ) -> Result<Table> {
        Ok(self.table.clone())
    }

    async fn load_table(&self, _identifier: &TableIdent) -> Result<Table> {
        Ok(self.table.clone())
    }

    async fn drop_table(&self, _identifier: &TableIdent) -> Result<()> {
        Ok(())
    }

    async fn update_table_metadata(
        &self,
        _identifier: &TableIdent,
        _old_metadata_location: &str,
        _new_metadata_location: &str,
    ) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl CatalogMaintenance for TestCatalog {
    async fn remove_snapshots(
        &self,
        _identifier: &TableIdent,
        _table_uuid: &str,
        _current_snapshot_id: Option<i64>,
        snapshot_ids: Vec<i64>,
    ) -> Result<()> {
        let mut removed = self.removed.lock().unwrap();
        removed.extend(snapshot_ids);
        Ok(())
    }
}

fn build_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .unwrap()
}

fn build_data_file(path: &str, record_count: i64) -> DataFile {
    DataFile::builder()
        .with_file_path(path)
        .with_file_format("PARQUET")
        .with_record_count(record_count)
        .with_file_size_in_bytes(128)
        .build()
        .unwrap()
}

async fn write_snapshot_files(
    file_io: &crate::io::FileIO,
    table_location: &str,
    snapshot_id: i64,
    sequence_number: i64,
    data_files: &[DataFile],
) -> Result<String> {
    let manifest_path = format!(
        "{}/metadata/manifest-{}.avro",
        table_location.trim_end_matches('/'),
        snapshot_id
    );
    let manifest_list_path = format!(
        "{}/metadata/snap-{}-1-test.avro",
        table_location.trim_end_matches('/'),
        snapshot_id
    );

    let manifest_bytes = write_manifest(
        file_io,
        &manifest_path,
        data_files,
        snapshot_id,
        sequence_number,
    )
    .await?;
    let entry = ManifestListEntry {
        manifest_path: manifest_path.clone(),
        manifest_length: manifest_bytes,
        partition_spec_id: 0,
        content: 0,
        sequence_number,
        min_sequence_number: sequence_number,
        added_snapshot_id: snapshot_id,
        added_files_count: data_files.len() as i32,
        existing_files_count: 0,
        deleted_files_count: 0,
        added_rows_count: data_files.iter().map(|f| f.record_count()).sum(),
        existing_rows_count: 0,
        deleted_rows_count: 0,
    };

    write_manifest_list(file_io, &manifest_list_path, vec![entry]).await?;
    Ok(manifest_list_path)
}

async fn setup_table() -> Result<(Table, i64, i64)> {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = crate::io::FileIO::new(op);
    let table_location = "memory://table";

    let data_file_a = build_data_file("memory://table/data/file-a.parquet", 10);
    let data_file_b = build_data_file("memory://table/data/file-b.parquet", 20);

    file_io
        .write(data_file_a.file_path(), b"data-a".to_vec())
        .await?;
    file_io
        .write(data_file_b.file_path(), b"data-b".to_vec())
        .await?;

    let now_ms = Utc::now().timestamp_millis();
    let old_ts = now_ms - 2 * 24 * 60 * 60 * 1000;

    let manifest_list_old = write_snapshot_files(
        &file_io,
        table_location,
        1,
        1,
        std::slice::from_ref(&data_file_a),
    )
    .await?;
    let manifest_list_new = write_snapshot_files(
        &file_io,
        table_location,
        2,
        2,
        std::slice::from_ref(&data_file_b),
    )
    .await?;

    let snapshot_old = Snapshot::builder()
        .with_snapshot_id(1)
        .with_sequence_number(1)
        .with_timestamp_ms(old_ts)
        .with_manifest_list(&manifest_list_old)
        .with_summary(crate::spec::Summary::builder().build())
        .with_schema_id(0)
        .build()?;

    let snapshot_new = Snapshot::builder()
        .with_snapshot_id(2)
        .with_sequence_number(2)
        .with_timestamp_ms(now_ms)
        .with_manifest_list(&manifest_list_new)
        .with_summary(crate::spec::Summary::builder().build())
        .with_schema_id(0)
        .build()?;

    let metadata = TableMetadata::builder()
        .with_location(table_location)
        .with_current_schema(build_schema())
        .with_current_snapshot(snapshot_old)
        .with_current_snapshot(snapshot_new)
        .build()?;

    let table = Table::new(
        TableIdent::new(
            NamespaceIdent::new(vec!["default".to_string()]),
            "test".to_string(),
        ),
        metadata,
        "memory://table/metadata/v1.metadata.json".to_string(),
        file_io,
    );

    Ok((table, now_ms, old_ts))
}

#[tokio::test]
async fn expire_snapshots_dry_run_keeps_files() -> Result<()> {
    let (table, now_ms, _old_ts) = setup_table().await?;
    let catalog = TestCatalog {
        table: table.clone(),
        removed: Arc::new(Mutex::new(Vec::new())),
    };

    let options = ExpireSnapshotsOptions {
        older_than_ms: Some(now_ms - 24 * 60 * 60 * 1000),
        retain_last: Some(1),
        delete_orphan_data: true,
        delete_orphan_manifests: true,
        max_snapshots_per_run: Some(100),
        manifest_scan_concurrency: Some(4),
        cleanup_concurrency: Some(4),
        dry_run: true,
    };

    let result = expire_snapshots(&table, &catalog, options).await?;
    assert_eq!(result.expired_snapshot_ids, vec![1]);

    let file_io = table.file_io();
    assert!(file_io.exists("memory://table/data/file-a.parquet").await?);
    assert!(file_io.exists("memory://table/data/file-b.parquet").await?);
    assert!(
        file_io
            .exists("memory://table/metadata/manifest-1.avro")
            .await?
    );
    assert!(
        file_io
            .exists("memory://table/metadata/manifest-2.avro")
            .await?
    );
    assert!(
        file_io
            .exists("memory://table/metadata/snap-1-1-test.avro")
            .await?
    );
    assert!(
        file_io
            .exists("memory://table/metadata/snap-2-1-test.avro")
            .await?
    );

    let expected_data: HashSet<String> = ["memory://table/data/file-a.parquet".to_string()]
        .into_iter()
        .collect();
    let expected_manifests: HashSet<String> =
        ["memory://table/metadata/manifest-1.avro".to_string()]
            .into_iter()
            .collect();
    let expected_lists: HashSet<String> =
        ["memory://table/metadata/snap-1-1-test.avro".to_string()]
            .into_iter()
            .collect();

    assert_eq!(
        result
            .deleted_data_files
            .into_iter()
            .collect::<HashSet<_>>(),
        expected_data
    );
    assert_eq!(
        result
            .deleted_manifest_files
            .into_iter()
            .collect::<HashSet<_>>(),
        expected_manifests
    );
    assert_eq!(
        result
            .deleted_manifest_lists
            .into_iter()
            .collect::<HashSet<_>>(),
        expected_lists
    );

    Ok(())
}

#[tokio::test]
async fn expire_snapshots_deletes_orphans() -> Result<()> {
    let (table, now_ms, _old_ts) = setup_table().await?;
    let removed = Arc::new(Mutex::new(Vec::new()));
    let catalog = TestCatalog {
        table: table.clone(),
        removed: removed.clone(),
    };

    let options = ExpireSnapshotsOptions {
        older_than_ms: Some(now_ms - 24 * 60 * 60 * 1000),
        retain_last: Some(1),
        delete_orphan_data: true,
        delete_orphan_manifests: true,
        max_snapshots_per_run: Some(100),
        manifest_scan_concurrency: Some(4),
        cleanup_concurrency: Some(4),
        dry_run: false,
    };

    let result = expire_snapshots(&table, &catalog, options).await?;
    assert_eq!(result.expired_snapshot_ids, vec![1]);

    let removed_ids = removed.lock().unwrap().clone();
    assert_eq!(removed_ids, vec![1]);

    let file_io = table.file_io();
    assert!(!file_io.exists("memory://table/data/file-a.parquet").await?);
    assert!(file_io.exists("memory://table/data/file-b.parquet").await?);
    assert!(
        !file_io
            .exists("memory://table/metadata/manifest-1.avro")
            .await?
    );
    assert!(
        file_io
            .exists("memory://table/metadata/manifest-2.avro")
            .await?
    );
    assert!(
        !file_io
            .exists("memory://table/metadata/snap-1-1-test.avro")
            .await?
    );
    assert!(
        file_io
            .exists("memory://table/metadata/snap-2-1-test.avro")
            .await?
    );

    Ok(())
}
