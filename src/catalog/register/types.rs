use std::collections::HashMap;

use super::validate::validate_partitions;
use crate::catalog::Catalog;
use crate::error::Result;
use crate::spec::{DataContentType, DataFile, PartitionSpec, Schema};
use crate::writer::SchemaEvolutionPolicy;

/// Supported data file formats for registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataFileFormat {
    Parquet,
}

impl DataFileFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            DataFileFormat::Parquet => "PARQUET",
        }
    }
}

/// Partition value provided by the caller (already resolved).
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionValue {
    String(String),
    Int(i32),
    Long(i64),
    Bool(bool),
}

impl PartitionValue {
    pub fn to_value_string(&self) -> String {
        match self {
            PartitionValue::String(v) => v.clone(),
            PartitionValue::Int(v) => v.to_string(),
            PartitionValue::Long(v) => v.to_string(),
            PartitionValue::Bool(v) => v.to_string(),
        }
    }
}

/// Optional column metrics derived from file metadata.
#[derive(Debug, Clone, Default)]
pub struct FileMetrics {
    pub column_sizes: HashMap<i32, i64>,
    pub value_counts: HashMap<i32, i64>,
    pub null_value_counts: HashMap<i32, i64>,
    pub lower_bounds: HashMap<i32, Vec<u8>>,
    pub upper_bounds: HashMap<i32, Vec<u8>>,
}

/// Optional encryption metadata.
#[derive(Debug, Clone)]
pub struct EncryptionMetadata {
    pub key_metadata: Vec<u8>,
}

/// Caller-supplied information about a data file.
#[derive(Debug, Clone)]
pub struct DataFileInput {
    pub file_path: String,
    pub file_format: DataFileFormat,
    pub file_size_in_bytes: i64,
    pub record_count: i64,
    pub partition_values: HashMap<String, PartitionValue>,
    pub metrics: Option<FileMetrics>,
    pub content_type: DataContentType,
    pub split_offsets: Option<Vec<i64>>,
    pub encryption: Option<EncryptionMetadata>,
    /// Optional schema derived from the Parquet footer, required when auto-creating tables.
    pub source_schema: Option<Schema>,
}

impl DataFileInput {
    /// Convert to `DataFile`, validating that partition values match the table partition spec.
    pub fn into_data_file(
        self,
        partition_spec: Option<&PartitionSpec>,
        table_schema: &Schema,
    ) -> Result<DataFile> {
        validate_partitions(&self.partition_values, partition_spec, table_schema)?;

        let mut builder = DataFile::builder()
            .with_file_path(&self.file_path)
            .with_file_format(self.file_format.as_str())
            .with_record_count(self.record_count)
            .with_file_size_in_bytes(self.file_size_in_bytes)
            .with_partition(
                self.partition_values
                    .into_iter()
                    .map(|(k, v)| (k, v.to_value_string()))
                    .collect(),
            )
            .with_content_type(self.content_type);

        if let Some(metrics) = self.metrics {
            if !metrics.column_sizes.is_empty() {
                builder = builder.with_column_sizes(metrics.column_sizes);
            }
            if !metrics.value_counts.is_empty() {
                builder = builder.with_value_counts(metrics.value_counts);
            }
            if !metrics.null_value_counts.is_empty() {
                builder = builder.with_null_value_counts(metrics.null_value_counts);
            }
            if !metrics.lower_bounds.is_empty() {
                builder = builder.with_lower_bounds(metrics.lower_bounds);
            }
            if !metrics.upper_bounds.is_empty() {
                builder = builder.with_upper_bounds(metrics.upper_bounds);
            }
        }

        if let Some(split_offsets) = self.split_offsets {
            if !split_offsets.is_empty() {
                builder = builder.with_split_offsets(split_offsets);
            }
        }

        if let Some(encryption) = self.encryption {
            builder = builder.with_key_metadata(encryption.key_metadata);
        }

        builder.build()
    }
}

/// Options that control registration behavior.
#[derive(Debug, Clone)]
pub struct RegisterOptions {
    pub timestamp_ms: Option<i64>,
    pub fail_if_missing: bool,
    pub schema_evolution: SchemaEvolutionPolicy,
    pub table_schema: Option<Schema>,
    pub partition_spec: Option<PartitionSpec>,
    pub allow_noop: bool,
}

impl Default for RegisterOptions {
    fn default() -> Self {
        Self {
            timestamp_ms: None,
            fail_if_missing: true,
            schema_evolution: SchemaEvolutionPolicy::Reject,
            table_schema: None,
            partition_spec: None,
            allow_noop: false,
        }
    }
}

impl RegisterOptions {
    /// Create a new options struct.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set explicit timestamp in milliseconds.
    pub fn with_timestamp_ms(mut self, timestamp_ms: i64) -> Self {
        self.timestamp_ms = Some(timestamp_ms);
        self
    }

    /// Allow creating missing namespace/table with the provided schema.
    pub fn allow_create_with_schema(mut self, schema: Schema) -> Self {
        self.table_schema = Some(schema);
        self.fail_if_missing = false;
        self
    }

    /// Provide a partition spec for table creation.
    pub fn with_partition_spec(mut self, partition_spec: PartitionSpec) -> Self {
        self.partition_spec = Some(partition_spec);
        self
    }

    /// Control schema evolution policy for registration.
    pub fn with_schema_evolution(mut self, policy: SchemaEvolutionPolicy) -> Self {
        self.schema_evolution = policy;
        self
    }

    /// Require missing tables/namespaces to raise a NotFound error.
    pub fn with_fail_if_missing(mut self, fail_if_missing: bool) -> Self {
        self.fail_if_missing = fail_if_missing;
        self
    }

    /// Allow registration to succeed even if all files are already committed.
    pub fn allow_noop(mut self, allow: bool) -> Self {
        self.allow_noop = allow;
        self
    }
}

/// Reason a file was skipped during registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkippedReason {
    AlreadyCommitted,
}

/// A file that was skipped (idempotency reporting).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedFile {
    pub file_path: String,
    pub reason: SkippedReason,
}

/// Result of a register operation.
#[derive(Debug, Clone)]
pub struct RegisterResult {
    pub snapshot_id: i64,
    pub added_files: usize,
    pub added_records: i64,
    pub table_was_created: bool,
    pub skipped_files: Vec<SkippedFile>,
}

/// Register pre-existing data files by adding a new Iceberg snapshot.
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
pub trait DataFileRegistrar {
    async fn register_data_files(
        &self,
        namespace: crate::spec::NamespaceIdent,
        table: crate::spec::TableIdent,
        files: Vec<DataFileInput>,
        options: RegisterOptions,
    ) -> Result<RegisterResult>;
}

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl<T: Catalog + Sync + Send> DataFileRegistrar for T {
    async fn register_data_files(
        &self,
        namespace: crate::spec::NamespaceIdent,
        table: crate::spec::TableIdent,
        files: Vec<DataFileInput>,
        options: RegisterOptions,
    ) -> Result<RegisterResult> {
        super::register_data_files(self, namespace, table, files, options).await
    }
}
