use icepick::catalog::Catalog;
use icepick::{
    register_data_files, DataContentType, DataFileFormat, DataFileInput, Error, FileIO,
    NamespaceIdent, RegisterOptions, TableIdent, TableMetadata,
};
use opendal::Operator;
use std::collections::HashMap;
use tokio::sync::RwLock;

struct RefreshingCatalog {
    table: RwLock<icepick::table::Table>,
}

impl RefreshingCatalog {
    fn new(table: icepick::table::Table) -> Self {
        Self {
            table: RwLock::new(table),
        }
    }
}

#[async_trait::async_trait]
impl icepick::catalog::Catalog for RefreshingCatalog {
    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> icepick::error::Result<()> {
        Ok(())
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> icepick::error::Result<bool> {
        Ok(true)
    }

    async fn list_tables(
        &self,
        _namespace: &NamespaceIdent,
    ) -> icepick::error::Result<Vec<TableIdent>> {
        Ok(vec![])
    }

    async fn table_exists(&self, _identifier: &TableIdent) -> icepick::error::Result<bool> {
        Ok(true)
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: icepick::spec::TableCreation,
    ) -> icepick::error::Result<icepick::table::Table> {
        Err(icepick::Error::invalid_request(
            "RefreshingCatalog does not support create_table in tests",
        ))
    }

    async fn load_table(
        &self,
        _identifier: &TableIdent,
    ) -> icepick::error::Result<icepick::table::Table> {
        let table = self.table.read().await;
        Ok(table.clone())
    }

    async fn drop_table(&self, _identifier: &TableIdent) -> icepick::error::Result<()> {
        Ok(())
    }

    async fn update_table_metadata(
        &self,
        _identifier: &TableIdent,
        _old_metadata_location: &str,
        new_metadata_location: &str,
    ) -> icepick::error::Result<()> {
        let table = self.table.read().await;
        let bytes = table.file_io().read(new_metadata_location).await?;
        let metadata: TableMetadata = serde_json::from_slice(&bytes)
            .map_err(|e| Error::invalid_input(format!("failed to parse metadata: {e}")))?;

        let new_table = icepick::table::Table::new(
            table.identifier().clone(),
            metadata,
            new_metadata_location.to_string(),
            table.file_io().clone(),
        );

        drop(table);
        let mut table_guard = self.table.write().await;
        *table_guard = new_table;

        Ok(())
    }
}

fn build_table(file_io: &FileIO) -> icepick::table::Table {
    let schema = icepick::spec::Schema::builder()
        .with_fields(vec![icepick::spec::NestedField::required_field(
            1,
            "id".to_string(),
            icepick::spec::Type::Primitive(icepick::spec::PrimitiveType::Long),
        )])
        .build()
        .unwrap();

    let metadata = TableMetadata::builder()
        .with_location("memory://warehouse/db/table")
        .with_current_schema(schema)
        .build()
        .unwrap();

    let ident = TableIdent::new(
        NamespaceIdent::new(vec!["db".to_string()]),
        "table".to_string(),
    );

    icepick::table::Table::new(
        ident,
        metadata,
        "memory://warehouse/db/table/metadata/v0.metadata.json".to_string(),
        file_io.clone(),
    )
}

fn sample_input(path: &str) -> DataFileInput {
    DataFileInput {
        file_path: path.to_string(),
        file_format: DataFileFormat::Parquet,
        file_size_in_bytes: 128,
        record_count: 4,
        partition_values: HashMap::new(),
        metrics: None,
        content_type: DataContentType::Data,
        split_offsets: None,
        encryption: None,
        source_schema: None,
    }
}

#[tokio::test]
async fn register_adds_snapshot() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);
    let table = build_table(&file_io);
    let catalog = RefreshingCatalog::new(table.clone());

    let namespace = table.identifier().namespace().clone();
    let table_ident = table.identifier().clone();

    let result = register_data_files(
        &catalog,
        namespace,
        table_ident.clone(),
        vec![sample_input(
            "memory://warehouse/db/table/data/part-1.parquet",
        )],
        RegisterOptions::new().with_timestamp_ms(123),
    )
    .await
    .expect("registration should succeed");

    assert_eq!(result.added_files, 1);
    assert_eq!(result.added_records, 4);
    assert!(!result.table_was_created);
    assert!(result.skipped_files.is_empty());

    let refreshed = catalog.load_table(&table_ident).await.unwrap();
    assert!(refreshed.current_snapshot().is_some());
}

#[tokio::test]
async fn register_is_idempotent() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);
    let table = build_table(&file_io);
    let catalog = RefreshingCatalog::new(table.clone());

    let namespace = table.identifier().namespace().clone();
    let table_ident = table.identifier().clone();
    let file_path = "memory://warehouse/db/table/data/part-1.parquet";

    register_data_files(
        &catalog,
        namespace.clone(),
        table_ident.clone(),
        vec![sample_input(file_path)],
        RegisterOptions::new().with_timestamp_ms(123),
    )
    .await
    .expect("first registration should succeed");

    let err = register_data_files(
        &catalog,
        namespace,
        table_ident,
        vec![sample_input(file_path)],
        RegisterOptions::new().with_timestamp_ms(124),
    )
    .await
    .expect_err("second registration should be a noop");

    match err {
        Error::NoopRegistration { .. } => {}
        other => panic!("expected NoopRegistration, got {other:?}"),
    }
}

#[tokio::test]
async fn register_allow_noop_when_all_skipped() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);
    let table = build_table(&file_io);
    let catalog = RefreshingCatalog::new(table.clone());

    let namespace = table.identifier().namespace().clone();
    let table_ident = table.identifier().clone();
    let file_path = "memory://warehouse/db/table/data/part-1.parquet";

    register_data_files(
        &catalog,
        namespace.clone(),
        table_ident.clone(),
        vec![sample_input(file_path)],
        RegisterOptions::new().with_timestamp_ms(123),
    )
    .await
    .expect("first registration should succeed");

    let result = register_data_files(
        &catalog,
        namespace,
        table_ident,
        vec![sample_input(file_path)],
        RegisterOptions::new()
            .with_timestamp_ms(124)
            .allow_noop(true),
    )
    .await
    .expect("second registration should be treated as noop");

    assert_eq!(result.added_files, 0);
    assert_eq!(result.added_records, 0);
    assert_eq!(result.skipped_files.len(), 1);
}

#[tokio::test]
async fn register_rejects_relative_paths() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);
    let table = build_table(&file_io);
    let catalog = RefreshingCatalog::new(table.clone());

    let namespace = table.identifier().namespace().clone();
    let table_ident = table.identifier().clone();

    let err = register_data_files(
        &catalog,
        namespace,
        table_ident,
        vec![sample_input("logs/part-1.parquet")],
        RegisterOptions::new().with_timestamp_ms(123),
    )
    .await
    .expect_err("relative paths should be rejected");

    match err {
        Error::InvalidInput(msg) => {
            assert!(msg.contains("absolute URI"), "unexpected message: {}", msg);
        }
        other => panic!("expected InvalidInput, got {other:?}"),
    }
}
