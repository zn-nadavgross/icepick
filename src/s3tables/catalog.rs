use crate::s3tables::client::S3TablesClient;
use crate::s3tables::error::{Result as S3Result, S3TablesError};
use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error as IcebergError, ErrorKind, Namespace, NamespaceIdent, Result as IcebergResult,
    TableCommit, TableCreation, TableIdent,
};
use std::collections::HashMap;

/// Catalog implementation for AWS S3 Tables
///
/// Wraps S3TablesClient to implement the iceberg::Catalog trait,
/// allowing it to be used as a drop-in replacement for RestCatalog.
#[derive(Debug)]
#[allow(dead_code)]
pub struct S3TablesCatalog {
    pub client: S3TablesClient,
    file_io: FileIO,
    name: String,
}

impl S3TablesCatalog {
    /// Create a new S3 Tables catalog from an ARN
    pub async fn from_arn(name: String, arn: &str) -> S3Result<Self> {
        let client = S3TablesClient::from_arn(arn).await?;

        // Create FileIO for S3 access with default configuration
        // The actual S3 paths will come from table metadata
        let file_io = FileIO::from_path("s3://")
            .map_err(|e| S3TablesError::Unexpected(format!("Failed to create FileIO: {}", e)))?
            .build()
            .map_err(|e| S3TablesError::Unexpected(format!("Failed to build FileIO: {}", e)))?;

        Ok(Self {
            client,
            file_io,
            name,
        })
    }
}

/// Convert S3TablesError to IcebergError
fn to_iceberg_error(e: S3TablesError) -> IcebergError {
    match e {
        S3TablesError::NotFound(msg) => IcebergError::new(ErrorKind::DataInvalid, msg),
        S3TablesError::Conflict(msg) => IcebergError::new(ErrorKind::DataInvalid, msg),
        S3TablesError::InvalidRequest(msg) => IcebergError::new(ErrorKind::DataInvalid, msg),
        S3TablesError::AuthError(msg) => IcebergError::new(ErrorKind::Unexpected, msg),
        S3TablesError::HttpError(msg) => IcebergError::new(ErrorKind::Unexpected, msg),
        S3TablesError::InvalidArn(msg) => IcebergError::new(ErrorKind::DataInvalid, msg),
        S3TablesError::Unexpected(msg) => IcebergError::new(ErrorKind::Unexpected, msg),
    }
}

#[async_trait]
impl Catalog for S3TablesCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        // S3 Tables doesn't support listing namespaces via REST API
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "S3 Tables does not support listing namespaces",
        ))
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        let namespace_name = namespace.to_string();
        self.client
            .create_namespace(&namespace_name, properties.clone())
            .await
            .map_err(to_iceberg_error)?;

        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "S3 Tables does not support getting namespace properties",
        ))
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> IcebergResult<bool> {
        // Cannot determine without list support
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "S3 Tables does not support checking namespace existence",
        ))
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "S3 Tables does not support updating namespaces",
        ))
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<()> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "S3 Tables does not support dropping namespaces",
        ))
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "S3 Tables does not support listing tables",
        ))
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        let namespace_name = namespace.to_string();
        let metadata = self
            .client
            .create_table(&namespace_name, &creation.name, creation.schema)
            .await
            .map_err(to_iceberg_error)?;

        // Build Table from metadata
        let table_ident = TableIdent::new(namespace.clone(), creation.name);
        self.build_table(table_ident, metadata)
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        let namespace_name = table.namespace.to_string();
        let metadata = self
            .client
            .load_table(&namespace_name, &table.name)
            .await
            .map_err(to_iceberg_error)?;

        self.build_table(table.clone(), metadata)
    }

    async fn drop_table(&self, _table: &TableIdent) -> IcebergResult<()> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "S3 Tables does not support dropping tables",
        ))
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        match self.load_table(table).await {
            Ok(_) => Ok(true),
            Err(e) if e.to_string().contains("not found") => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "S3 Tables does not support renaming tables",
        ))
    }

    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> IcebergResult<Table> {
        Err(IcebergError::new(
            ErrorKind::FeatureUnsupported,
            "S3 Tables does not support registering tables",
        ))
    }

    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<Table> {
        let namespace_name = commit.identifier().namespace.to_string();
        let table_name = commit.identifier().name.clone();
        let table_ident = commit.identifier().clone();

        let requirements = commit.take_requirements();
        let updates = commit.take_updates();

        let metadata = self
            .client
            .update_table(&namespace_name, &table_name, requirements, updates)
            .await
            .map_err(to_iceberg_error)?;

        self.build_table(table_ident, metadata)
    }
}

impl S3TablesCatalog {
    /// Helper to build a Table from TableMetadata
    fn build_table(&self, ident: TableIdent, metadata: TableMetadata) -> IcebergResult<Table> {
        let metadata_location = format!(
            "{}/metadata/00000-initial.metadata.json",
            metadata.location()
        );

        Table::builder()
            .identifier(ident)
            .metadata(metadata)
            .metadata_location(metadata_location)
            .file_io(self.file_io.clone())
            .build()
    }
}
