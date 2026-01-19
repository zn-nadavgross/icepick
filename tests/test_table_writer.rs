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
    fn file_io(&self) -> &icepick::io::FileIO {
        &self.file_io
    }

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

    async fn update_table_schema(
        &self,
        identifier: &TableIdent,
        new_schema: icepick::spec::Schema,
    ) -> Result<Table> {
        use icepick::spec::TableMetadata;

        let mut guard = self.table.write().await;
        let current = guard
            .as_ref()
            .filter(|table| table.identifier() == identifier)
            .ok_or_else(|| Error::not_found(identifier.to_string()))?
            .clone();

        // Create new metadata with updated schema
        let old_metadata = current.metadata();
        let new_metadata = TableMetadata::builder()
            .with_location(old_metadata.location())
            .with_table_uuid(old_metadata.table_uuid().to_string())
            .with_current_schema(new_schema)
            .with_partition_specs(old_metadata.partition_specs().to_vec())
            .build()?;

        let new_metadata_location = format!(
            "{}/metadata/v{}.metadata.json",
            current.location(),
            chrono::Utc::now().timestamp_millis()
        );

        // Write new metadata
        let metadata_bytes = serde_json::to_vec(&new_metadata)?;
        self.file_io
            .write(&new_metadata_location, metadata_bytes)
            .await?;

        // Create updated table
        let updated = Table::new(
            current.identifier().clone(),
            new_metadata,
            new_metadata_location,
            current.file_io().clone(),
        );

        *guard = Some(updated.clone());
        Ok(updated)
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

fn single_partition_batch() -> RecordBatch {
    let schema = ArrowSchema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("name", DataType::Utf8, true, 2),
    ]);

    // All rows have same id value for single partition
    RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![
            std::sync::Arc::new(Int64Array::from(vec![1, 1, 1])),
            std::sync::Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
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
    writer.append_batch(single_partition_batch()).await.unwrap();

    let table_ident = TableIdent::new(namespace, "partitioned".to_string());
    let table = catalog.load_table(&table_ident).await.unwrap();
    let specs = table.metadata().partition_specs();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].fields().len(), 1);
    assert_eq!(specs[0].fields()[0].name(), "id_bucket");
}

#[tokio::test]
async fn test_writer_extracts_partition_values() {
    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);
    let catalog = SimpleCatalog::new(file_io.clone());
    let namespace = NamespaceIdent::new(vec!["analytics".to_string()]);

    let options = TableWriterOptions::new().with_partition_field(
        PartitionFieldConfig::new("id", PartitionTransform::Identity).with_name("id_partition"),
    );

    let writer = AppendOnlyTableWriter::new(&catalog, namespace.clone(), "partitioned_data")
        .with_options(options);

    let result = writer.append_batch(single_partition_batch()).await.unwrap();

    // Verify partition values are extracted in AppendResult
    let data_file = result.data_file();
    let partition = data_file.partition();
    assert!(
        !partition.is_empty(),
        "Partition values should be extracted"
    );
    assert_eq!(partition.get("id_partition"), Some(&"1".to_string()));
}

#[tokio::test]
async fn test_writer_validates_single_partition() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Schema as ArrowSchema};

    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);
    let catalog = SimpleCatalog::new(file_io.clone());
    let namespace = NamespaceIdent::new(vec!["test".to_string()]);

    let options = TableWriterOptions::new().with_partition_field(PartitionFieldConfig::new(
        "id",
        PartitionTransform::Identity,
    ));

    let writer = AppendOnlyTableWriter::new(&catalog, namespace.clone(), "multi_partition")
        .with_options(options);

    // Create batch with different partition values (id varies across rows)
    let schema = ArrowSchema::new(vec![field_with_id("id", DataType::Int64, false, 1)]);
    let multi_partition_batch = RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![std::sync::Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    // This should fail because batch contains multiple partition values
    let err = writer
        .append_batch(multi_partition_batch)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("multiple partition"),
        "Expected error about multiple partitions, got: {}",
        err
    );
}

#[tokio::test]
async fn test_table_created_vs_appended() {
    use icepick::AppendResult;

    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);
    let catalog = SimpleCatalog::new(file_io.clone());
    let namespace = NamespaceIdent::new(vec!["events".to_string()]);

    let writer = AppendOnlyTableWriter::new(&catalog, namespace.clone(), "logs");

    // First append to non-existent table should return TableCreated
    let result = writer.append_batch(sample_batch()).await.unwrap();
    match result {
        AppendResult::TableCreated { data_file, schema } => {
            assert_eq!(schema.fields().len(), 2);
            assert!(data_file.record_count() > 0);
        }
        _ => panic!("Expected TableCreated variant on first append"),
    }

    // Second append to existing table should return Appended
    let result2 = writer.append_batch(sample_batch()).await.unwrap();
    match result2 {
        AppendResult::Appended { data_file } => {
            assert!(data_file.record_count() > 0);
        }
        _ => panic!("Expected Appended variant on subsequent append"),
    }
}

#[tokio::test]
async fn test_append_batches_returns_correct_variants() {
    use icepick::AppendResult;

    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);
    let catalog = SimpleCatalog::new(file_io.clone());
    let namespace = NamespaceIdent::new(vec!["warehouse".to_string()]);

    let writer = AppendOnlyTableWriter::new(&catalog, namespace, "multi_batch");

    // Append multiple batches at once - first should be TableCreated, rest Appended
    let results = writer
        .append_batches(vec![sample_batch(), sample_batch(), sample_batch()])
        .await
        .unwrap();

    assert_eq!(results.len(), 3);

    // First result should be TableCreated
    match &results[0] {
        AppendResult::TableCreated { .. } => {}
        _ => panic!("Expected TableCreated for first batch"),
    }

    // Subsequent results should be Appended
    match &results[1] {
        AppendResult::Appended { .. } => {}
        _ => panic!("Expected Appended for second batch"),
    }

    match &results[2] {
        AppendResult::Appended { .. } => {}
        _ => panic!("Expected Appended for third batch"),
    }
}

#[tokio::test]
async fn test_schema_evolution_with_add_fields() {
    use icepick::{AppendResult, SchemaEvolutionPolicy};

    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);
    let catalog = SimpleCatalog::new(file_io.clone());
    let namespace = NamespaceIdent::new(vec!["analytics".to_string()]);

    // Create writer with AddFields evolution policy
    let options = TableWriterOptions::new().with_schema_evolution(SchemaEvolutionPolicy::AddFields);

    let writer = AppendOnlyTableWriter::new(&catalog, namespace.clone(), "evolving_table")
        .with_options(options);

    // First append with initial schema (id, name)
    let result1 = writer.append_batch(single_partition_batch()).await.unwrap();
    match &result1 {
        AppendResult::TableCreated { schema, .. } => {
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.fields()[0].name(), "id");
            assert_eq!(schema.fields()[1].name(), "name");
        }
        _ => panic!("Expected TableCreated for first append"),
    }

    // Second append with new field added (id, name, age)
    let evolved_schema = ArrowSchema::new(vec![
        field_with_id("id", DataType::Int64, false, 1),
        field_with_id("name", DataType::Utf8, true, 2),
        field_with_id("age", DataType::Int64, true, 10), // New field
    ]);

    let evolved_batch = RecordBatch::try_new(
        std::sync::Arc::new(evolved_schema),
        vec![
            std::sync::Arc::new(Int64Array::from(vec![1, 1, 1])),
            std::sync::Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            std::sync::Arc::new(Int64Array::from(vec![Some(25), None, Some(30)])),
        ],
    )
    .unwrap();

    let result2 = writer.append_batch(evolved_batch.clone()).await.unwrap();
    match &result2 {
        AppendResult::SchemaEvolved {
            old_schema,
            new_schema,
            ..
        } => {
            // Old schema should have 2 fields
            assert_eq!(old_schema.fields().len(), 2);
            // New schema should have 3 fields
            assert_eq!(new_schema.fields().len(), 3);
            assert_eq!(new_schema.fields()[2].name(), "age");
            // New field should have new ID (max old ID + 1 = 2 + 1 = 3)
            assert_eq!(new_schema.fields()[2].id(), 3);
        }
        _ => panic!(
            "Expected SchemaEvolved for second append, got: {:?}",
            result2
        ),
    }

    // Third append with same evolved schema should return Appended
    let result3 = writer.append_batch(evolved_batch.clone()).await.unwrap();
    match &result3 {
        AppendResult::Appended { .. } => {}
        _ => panic!("Expected Appended for third append with same schema"),
    }
}

#[tokio::test]
async fn test_schema_evolution_rejects_type_changes() {
    use icepick::SchemaEvolutionPolicy;

    let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
    let file_io = FileIO::new(op);
    let catalog = SimpleCatalog::new(file_io.clone());
    let namespace = NamespaceIdent::new(vec!["test".to_string()]);

    let options = TableWriterOptions::new().with_schema_evolution(SchemaEvolutionPolicy::AddFields);

    let writer = AppendOnlyTableWriter::new(&catalog, namespace.clone(), "type_change_table")
        .with_options(options);

    // First append with Int64 field
    let initial_batch = single_partition_batch();
    writer.append_batch(initial_batch).await.unwrap();

    // Try to append with same field name but different type (String instead of Int64)
    let type_changed_schema = ArrowSchema::new(vec![
        field_with_id("id", DataType::Utf8, false, 1), // Changed from Int64 to Utf8
        field_with_id("name", DataType::Utf8, true, 2),
    ]);

    let type_changed_batch = RecordBatch::try_new(
        std::sync::Arc::new(type_changed_schema),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["1", "2", "3"])),
            std::sync::Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
        ],
    )
    .unwrap();

    // This should fail
    let err = writer.append_batch(type_changed_batch).await.unwrap_err();
    assert!(
        err.to_string().contains("incompatible"),
        "Expected incompatible schema error, got: {}",
        err
    );
}
