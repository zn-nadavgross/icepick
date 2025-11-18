use std::collections::HashMap;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use icepick::arrow_convert::PARQUET_FIELD_ID_METADATA_KEY;
use icepick::catalog::Catalog;
use icepick::error::{Error, Result};
use icepick::io::FileIO;
use icepick::spec::{NamespaceIdent, TableCreation, TableIdent, TableMetadata};
use icepick::table::Table;
use icepick::{
    AppendOnlyTableWriter, PartitionFieldConfig, PartitionTransform, TableWriterOptions,
};
use opendal::Operator;
use tokio::sync::RwLock;

struct SimpleCatalog {
    file_io: FileIO,
    table: RwLock<Option<Table>>,
}

impl SimpleCatalog {
    fn new(file_io: FileIO) -> Self {
        Self {
            file_io,
            table: RwLock::new(None),
        }
    }

    fn default_location(namespace: &NamespaceIdent, name: &str) -> String {
        format!("memory://warehouse/{}/{}", namespace, name)
    }
}

#[async_trait::async_trait]
impl Catalog for SimpleCatalog {
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

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let guard = self.table.read().await;
        if let Some(table) = guard.as_ref() {
            if table.identifier().namespace() == namespace {
                return Ok(vec![table.identifier().clone()]);
            }
        }
        Ok(vec![])
    }

    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool> {
        let guard = self.table.read().await;
        Ok(guard
            .as_ref()
            .map(|table| table.identifier() == identifier)
            .unwrap_or(false))
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let location = Self::default_location(namespace, creation.name());
        let mut metadata_builder = TableMetadata::builder()
            .with_location(&location)
            .with_current_schema(creation.schema().clone());
        if let Some(spec) = creation.partition_spec() {
            metadata_builder = metadata_builder.with_partition_specs(vec![spec.clone()]);
        }
        let metadata = metadata_builder.build()?;
        let ident = TableIdent::new(namespace.clone(), creation.name().to_string());
        let metadata_location = format!("{}/metadata/v0.metadata.json", location);
        let table = Table::new(ident, metadata, metadata_location, self.file_io.clone());
        *self.table.write().await = Some(table.clone());
        Ok(table)
    }

    async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
        let guard = self.table.read().await;
        guard
            .as_ref()
            .filter(|table| table.identifier() == identifier)
            .cloned()
            .ok_or_else(|| Error::not_found(identifier.to_string()))
    }

    async fn drop_table(&self, _identifier: &TableIdent) -> Result<()> {
        *self.table.write().await = None;
        Ok(())
    }

    async fn update_table_metadata(
        &self,
        identifier: &TableIdent,
        old_metadata_location: &str,
        new_metadata_location: &str,
    ) -> Result<()> {
        let mut guard = self.table.write().await;
        let current = guard
            .as_ref()
            .filter(|table| table.identifier() == identifier)
            .ok_or_else(|| Error::not_found(identifier.to_string()))?
            .clone();

        if current.metadata_location() != old_metadata_location {
            return Err(Error::concurrent_modification(format!(
                "stale metadata pointer, expected {}",
                current.metadata_location()
            )));
        }

        let bytes = self.file_io.read(new_metadata_location).await?;
        let metadata: TableMetadata = serde_json::from_slice(&bytes)?;

        let updated = Table::new(
            current.identifier().clone(),
            metadata,
            new_metadata_location.to_string(),
            current.file_io().clone(),
        );

        *guard = Some(updated);
        Ok(())
    }
}

fn sample_batch() -> RecordBatch {
    let schema = ArrowSchema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("name", DataType::Utf8, true, 2),
    ]);

    RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![
            std::sync::Arc::new(Int64Array::from(vec![1, 2, 3])),
            std::sync::Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap()
}

fn different_batch() -> RecordBatch {
    let schema = ArrowSchema::new(vec![field_with_id("value", DataType::Int64, false, 10)]);
    RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![std::sync::Arc::new(Int64Array::from(vec![42]))],
    )
    .unwrap()
}

fn field_with_id(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
    let mut field = Field::new(name, data_type, nullable);
    field.set_metadata(HashMap::from([(
        PARQUET_FIELD_ID_METADATA_KEY.to_string(),
        id.to_string(),
    )]));
    field
}

#[tokio::test]
async fn test_writer_creates_table_and_appends() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);
    let catalog = SimpleCatalog::new(file_io.clone());
    let namespace = NamespaceIdent::new(vec!["default".to_string()]);

    let writer = AppendOnlyTableWriter::new(&catalog, namespace.clone(), "demo");
    writer.append_batch(sample_batch()).await.unwrap();

    let table_ident = TableIdent::new(namespace, "demo".to_string());
    let table = catalog.load_table(&table_ident).await.unwrap();
    assert!(table.metadata().current_snapshot().is_some());
}

#[tokio::test]
async fn test_writer_validates_schema() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);
    let catalog = SimpleCatalog::new(file_io.clone());
    let namespace = NamespaceIdent::new(vec!["analytics".to_string()]);

    let writer = AppendOnlyTableWriter::new(&catalog, namespace.clone(), "events");
    writer.append_batch(sample_batch()).await.unwrap();

    let err = writer.append_batch(different_batch()).await.unwrap_err();
    assert!(err
        .to_string()
        .contains("RecordBatch schema does not match"),);
}

#[tokio::test]
async fn test_writer_applies_partition_spec() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);
    let catalog = SimpleCatalog::new(file_io.clone());
    let namespace = NamespaceIdent::new(vec!["warehouse".to_string()]);

    let options = TableWriterOptions::new().with_partition_field(
        PartitionFieldConfig::new("id", PartitionTransform::Identity).with_name("id_bucket"),
    );

    let writer = AppendOnlyTableWriter::new(&catalog, namespace.clone(), "partitioned")
        .with_options(options);
    writer.append_batch(sample_batch()).await.unwrap();

    let table_ident = TableIdent::new(namespace, "partitioned".to_string());
    let table = catalog.load_table(&table_ident).await.unwrap();
    let specs = table.metadata().partition_specs();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].fields().len(), 1);
    assert_eq!(specs[0].fields()[0].name(), "id_bucket");
}
