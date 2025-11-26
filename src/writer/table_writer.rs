//! Append-only table writer that accepts Arrow RecordBatch inputs.
//!
//! This module provides the `AppendOnlyTableWriter` which handles table creation,
//! schema evolution, partition extraction, and transaction management automatically.

use crate::arrow_convert::arrow_schema_to_iceberg;
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::spec::data_file::DataFile;
use crate::spec::{
    NamespaceIdent, PartitionField, PartitionSpec, Schema, TableCreation, TableIdent,
};
use crate::table::Table;
use crate::writer::arrow_to_parquet;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use uuid::Uuid;

/// Result of appending data to a table
#[derive(Debug, Clone)]
pub enum AppendResult {
    /// Table was created for the first time
    TableCreated { data_file: DataFile, schema: Schema },
    /// Schema was evolved to accommodate new fields
    SchemaEvolved {
        data_file: DataFile,
        old_schema: Schema,
        new_schema: Schema,
    },
    /// Data appended to existing table without schema changes
    Appended { data_file: DataFile },
}

impl AppendResult {
    /// Get the DataFile regardless of variant
    pub fn data_file(&self) -> &DataFile {
        match self {
            Self::TableCreated { data_file, .. } => data_file,
            Self::SchemaEvolved { data_file, .. } => data_file,
            Self::Appended { data_file } => data_file,
        }
    }
}

/// Schema evolution policy for handling schema changes
#[derive(Debug, Clone, Copy, Default)]
pub enum SchemaEvolutionPolicy {
    /// Reject batches with different schemas (safe default)
    #[default]
    Reject,
    /// Automatically add new fields from incoming batch
    AddFields,
}

/// Options for the append-only writer.
#[derive(Debug, Clone, Default)]
pub struct TableWriterOptions {
    partition_fields: Vec<PartitionFieldConfig>,
    schema_evolution: SchemaEvolutionPolicy,
    timestamp_ms: Option<i64>,
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

    /// Set the schema evolution policy.
    pub fn with_schema_evolution(mut self, policy: SchemaEvolutionPolicy) -> Self {
        self.schema_evolution = policy;
        self
    }

    /// Get the schema evolution policy.
    pub fn schema_evolution(&self) -> SchemaEvolutionPolicy {
        self.schema_evolution
    }

    /// Set an explicit timestamp for commits (required for WASM compatibility).
    /// If not set, callers must provide timestamp via append_batch_with_timestamp.
    pub fn with_timestamp_ms(mut self, timestamp_ms: i64) -> Self {
        self.timestamp_ms = Some(timestamp_ms);
        self
    }

    /// Get the timestamp if set.
    pub fn timestamp_ms(&self) -> Option<i64> {
        self.timestamp_ms
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
    pub async fn append_batch(&self, batch: RecordBatch) -> Result<AppendResult> {
        let mut results = self.append_batches(vec![batch]).await?;
        Ok(results.remove(0))
    }

    /// Append multiple RecordBatches to the table in sequence.
    pub async fn append_batches(&self, batches: Vec<RecordBatch>) -> Result<Vec<AppendResult>> {
        if batches.is_empty() {
            return Err(Error::invalid_input(
                "append_batches requires at least one RecordBatch",
            ));
        }

        let iceberg_schema = self.derive_schema(&batches)?;
        self.ensure_namespace().await?;

        let table_ident = self.table_ident();
        let (mut table, table_was_created, old_schema) =
            self.load_or_create_table(&iceberg_schema).await?;
        let mut results = Vec::new();

        for (i, batch) in batches.into_iter().enumerate() {
            // First batch on a newly created table should return TableCreated
            // First batch on a schema-evolved table should return SchemaEvolved
            let is_first_batch_on_new_table = table_was_created && i == 0;
            let is_first_batch_on_evolved_table = old_schema.is_some() && i == 0;

            let (updated_table, result) = self
                .append_single_batch(
                    &table_ident,
                    table,
                    batch,
                    is_first_batch_on_new_table,
                    is_first_batch_on_evolved_table,
                    old_schema.clone(),
                )
                .await?;
            table = updated_table;
            results.push(result);
        }

        Ok(results)
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

    async fn load_or_create_table(&self, schema: &Schema) -> Result<(Table, bool, Option<Schema>)> {
        let table_ident = self.table_ident();
        match self.catalog.load_table(&table_ident).await {
            Ok(table) => {
                let existing_schema = table.schema()?.clone();

                // Check if schema evolution is needed
                match self.options.schema_evolution {
                    SchemaEvolutionPolicy::Reject => {
                        // Strict validation - reject any differences
                        self.validate_table_schema(&existing_schema, schema)?;
                        Ok((table, false, None))
                    }
                    SchemaEvolutionPolicy::AddFields => {
                        // Check if schemas are compatible
                        if !crate::spec::schema_evolution::schemas_compatible(
                            &existing_schema,
                            schema,
                        ) {
                            return Err(Error::invalid_input(
                                "Incoming schema is incompatible with existing schema (type mismatch)"
                            ));
                        }

                        // Check if there are new fields
                        if crate::spec::schema_evolution::has_new_fields(&existing_schema, schema) {
                            // Merge schemas
                            let merged_schema = crate::spec::schema_evolution::merge_schemas(
                                &existing_schema,
                                schema,
                            )?;

                            // Update table schema via catalog
                            let updated_table = self
                                .catalog
                                .update_table_schema(&table_ident, merged_schema.clone())
                                .await?;

                            // Return table, not_created=false, schema_evolved=Some(old_schema)
                            Ok((updated_table, false, Some(existing_schema)))
                        } else {
                            // Schemas match, no evolution needed
                            Ok((table, false, None))
                        }
                    }
                }
            }
            Err(Error::NotFound { .. }) => {
                let table = self.create_table(schema).await?;
                Ok((table, true, None))
            }
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
        is_first_batch_on_new_table: bool,
        is_first_batch_on_evolved_table: bool,
        old_schema: Option<Schema>,
    ) -> Result<(Table, AppendResult)> {
        let file_path = Self::build_data_file_path(table.location());
        let schema = table.schema()?.clone();

        // Extract partition values if table has partition spec
        let partition_values = if let Some(partition_spec) =
            table.metadata().partition_specs().first()
        {
            if !partition_spec.fields().is_empty() {
                // Validate all rows belong to same partition
                super::partition_extract::validate_single_partition(
                    &batch,
                    partition_spec,
                    &schema,
                )?;

                // Extract partition values
                super::partition_extract::extract_partition_values(&batch, partition_spec, &schema)?
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };

        // Write parquet file and collect stats
        let mut data_file = arrow_to_parquet(&batch, file_path.clone(), table.file_io())
            .finish_data_file()
            .await?;

        // Add partition values to data file if present
        if !partition_values.is_empty() {
            data_file = DataFile::builder()
                .with_file_path(data_file.file_path())
                .with_file_format(data_file.file_format())
                .with_record_count(data_file.record_count())
                .with_file_size_in_bytes(data_file.file_size_in_bytes())
                .with_partition(partition_values)
                .with_column_sizes(data_file.column_sizes().cloned().unwrap_or_default())
                .with_value_counts(data_file.value_counts().cloned().unwrap_or_default())
                .with_null_value_counts(data_file.null_value_counts().cloned().unwrap_or_default())
                .with_lower_bounds(data_file.lower_bounds().cloned().unwrap_or_default())
                .with_upper_bounds(data_file.upper_bounds().cloned().unwrap_or_default())
                .build()?;
        }

        // Use provided timestamp or generate one on native platforms
        let timestamp_ms = self.options.timestamp_ms.unwrap_or_else(|| {
            #[cfg(not(target_family = "wasm"))]
            {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64
            }
            #[cfg(target_family = "wasm")]
            {
                panic!("timestamp_ms must be provided via TableWriterOptions::with_timestamp_ms() on WASM platforms")
            }
        });

        table
            .transaction()
            .append(vec![data_file.clone()])
            .commit(self.catalog, timestamp_ms)
            .await?;

        let reloaded_table = self.catalog.load_table(table_ident).await?;

        // Determine which variant to return
        let result = if is_first_batch_on_new_table {
            AppendResult::TableCreated {
                data_file,
                schema: schema.clone(),
            }
        } else if is_first_batch_on_evolved_table {
            AppendResult::SchemaEvolved {
                data_file,
                old_schema: old_schema
                    .expect("old_schema must be Some when is_first_batch_on_evolved_table"),
                new_schema: schema.clone(),
            }
        } else {
            AppendResult::Appended { data_file }
        };

        Ok((reloaded_table, result))
    }

    fn build_data_file_path(location: &str) -> String {
        let trimmed = location.trim_end_matches('/');
        format!("{}/data/{}.parquet", trimmed, Uuid::new_v4())
    }
}
