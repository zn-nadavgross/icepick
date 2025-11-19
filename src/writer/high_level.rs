//! High level append-only writer that accepts Arrow RecordBatch inputs.
//!
//! This utility hides namespace/table creation, schema inference, and transaction
//! handling so callers can focus on providing batches.

use crate::arrow_convert::arrow_schema_to_iceberg;
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::spec::{
    NamespaceIdent, PartitionField, PartitionSpec, Schema, TableCreation, TableIdent,
};
use crate::table::Table;
use crate::writer::arrow_to_parquet;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use uuid::Uuid;

/// Options for the append-only writer.
#[derive(Debug, Clone, Default)]
pub struct TableWriterOptions {
    partition_fields: Vec<PartitionFieldConfig>,
}

impl TableWriterOptions {
    /// Create a new options instance with no partitioning.
    pub fn new() -> Self {
        Self::default()
    }

    /// Partition fields that should be applied when the table is created.
    pub fn partition_fields(&self) -> &[PartitionFieldConfig] {
        &self.partition_fields
    }

    /// Add a partition field configuration.
    pub fn with_partition_field(mut self, field: PartitionFieldConfig) -> Self {
        self.partition_fields.push(field);
        self
    }
}

/// Partition field configuration used to build Iceberg partition specs.
#[derive(Debug, Clone)]
pub struct PartitionFieldConfig {
    column: String,
    transform: PartitionTransform,
    name: Option<String>,
}

impl PartitionFieldConfig {
    /// Create a new configuration for the given column and transform.
    pub fn new(column: impl Into<String>, transform: PartitionTransform) -> Self {
        Self {
            column: column.into(),
            transform,
            name: None,
        }
    }

    /// Override the partition field name used in the Iceberg spec.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Source column used for partitioning.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Partition transform to apply.
    pub fn transform(&self) -> &PartitionTransform {
        &self.transform
    }

    /// Optional explicit partition field name.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

/// Supported partition transforms for the high level writer.
#[derive(Debug, Clone)]
pub enum PartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket(i32),
    Truncate(i32),
}

impl PartitionTransform {
    fn as_iceberg_expression(&self) -> String {
        match self {
            PartitionTransform::Identity => "identity".to_string(),
            PartitionTransform::Year => "year".to_string(),
            PartitionTransform::Month => "month".to_string(),
            PartitionTransform::Day => "day".to_string(),
            PartitionTransform::Hour => "hour".to_string(),
            PartitionTransform::Bucket(buckets) => format!("bucket[{buckets}]"),
            PartitionTransform::Truncate(width) => format!("truncate[{width}]"),
        }
    }
}

/// Append-only writer that handles namespace/table creation and commits automatically.
pub struct AppendOnlyTableWriter<'a> {
    catalog: &'a dyn Catalog,
    namespace: NamespaceIdent,
    table_name: String,
    options: TableWriterOptions,
}

impl<'a> AppendOnlyTableWriter<'a> {
    /// Create a new writer for the provided namespace/table combination.
    pub fn new(
        catalog: &'a dyn Catalog,
        namespace: NamespaceIdent,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            catalog,
            namespace,
            table_name: table_name.into(),
            options: TableWriterOptions::default(),
        }
    }

    /// Override default options.
    pub fn with_options(mut self, options: TableWriterOptions) -> Self {
        self.options = options;
        self
    }

    /// Append a single RecordBatch to the table.
    pub async fn append_batch(&self, batch: RecordBatch) -> Result<()> {
        self.append_batches(vec![batch]).await
    }

    /// Append multiple RecordBatches to the table in sequence.
    pub async fn append_batches(&self, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Err(Error::invalid_input(
                "append_batches requires at least one RecordBatch",
            ));
        }

        let iceberg_schema = self.derive_schema(&batches)?;
        self.ensure_namespace().await?;

        let table_ident = self.table_ident();
        let mut table = self.load_or_create_table(&iceberg_schema).await?;

        for batch in batches {
            table = self.append_single_batch(&table_ident, table, batch).await?;
        }

        Ok(())
    }

    fn table_ident(&self) -> TableIdent {
        TableIdent::new(self.namespace.clone(), self.table_name.clone())
    }

    fn derive_schema(&self, batches: &[RecordBatch]) -> Result<Schema> {
        let first = batches.first().ok_or_else(|| {
            Error::invalid_input("append_batches requires at least one RecordBatch")
        })?;
        for batch in batches.iter().skip(1) {
            if batch.schema() != first.schema() {
                return Err(Error::invalid_input(
                    "All RecordBatch instances must share the same schema",
                ));
            }
        }
        arrow_schema_to_iceberg(first.schema().as_ref())
    }

    async fn ensure_namespace(&self) -> Result<()> {
        match self.catalog.namespace_exists(&self.namespace).await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(err) => return Err(err),
        }

        match self
            .catalog
            .create_namespace(&self.namespace, HashMap::new())
            .await
        {
            Ok(_) => Ok(()),
            Err(Error::Conflict { .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn load_or_create_table(&self, schema: &Schema) -> Result<Table> {
        let table_ident = self.table_ident();
        match self.catalog.load_table(&table_ident).await {
            Ok(table) => {
                self.validate_table_schema(table.schema()?, schema)?;
                Ok(table)
            }
            Err(Error::NotFound { .. }) => self.create_table(schema).await,
            Err(err) => Err(err),
        }
    }

    async fn create_table(&self, schema: &Schema) -> Result<Table> {
        let partition_spec = self.build_partition_spec(schema)?;
        let mut builder = TableCreation::builder()
            .with_name(self.table_name.clone())
            .with_schema(schema.clone());
        if let Some(spec) = partition_spec {
            builder = builder.with_partition_spec(spec);
        }
        let creation = builder.build()?;
        self.catalog.create_table(&self.namespace, creation).await
    }

    fn build_partition_spec(&self, schema: &Schema) -> Result<Option<PartitionSpec>> {
        if self.options.partition_fields.is_empty() {
            return Ok(None);
        }

        let mut fields = Vec::with_capacity(self.options.partition_fields.len());
        let mut next_id = 1000;

        for config in self.options.partition_fields.iter() {
            let source_field = schema
                .fields()
                .iter()
                .find(|field| field.name() == config.column())
                .ok_or_else(|| {
                    Error::invalid_input(format!(
                        "Partition column '{}' not found in schema",
                        config.column()
                    ))
                })?;

            let field_name = config
                .name()
                .map(|s| s.to_string())
                .unwrap_or_else(|| source_field.name().to_string());

            fields.push(PartitionField::new(
                next_id,
                source_field.id(),
                config.transform().as_iceberg_expression(),
                field_name,
            ));
            next_id += 1;
        }

        Ok(Some(PartitionSpec::new(0, fields)))
    }

    fn validate_table_schema(&self, existing: &Schema, incoming: &Schema) -> Result<()> {
        if existing.fields() != incoming.fields() {
            return Err(Error::invalid_input(
                "RecordBatch schema does not match existing Iceberg schema",
            ));
        }
        Ok(())
    }

    async fn append_single_batch(
        &self,
        table_ident: &TableIdent,
        table: Table,
        batch: RecordBatch,
    ) -> Result<Table> {
        let file_path = Self::build_data_file_path(table.location());
        let data_file = arrow_to_parquet(&batch, file_path.clone(), table.file_io())
            .finish_data_file()
            .await?;

        table
            .transaction()
            .append(vec![data_file])
            .commit(self.catalog)
            .await?;

        self.catalog.load_table(table_ident).await
    }

    fn build_data_file_path(location: &str) -> String {
        let trimmed = location.trim_end_matches('/');
        format!("{}/data/{}.parquet", trimmed, Uuid::new_v4())
    }
}
