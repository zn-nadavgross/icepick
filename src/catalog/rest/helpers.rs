use crate::catalog::CatalogError;
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::{Error as IcebergError, ErrorKind, Result as IcebergResult, TableIdent};

pub fn to_iceberg_error(e: CatalogError) -> IcebergError {
    match e {
        CatalogError::NotFound(msg) => IcebergError::new(ErrorKind::DataInvalid, msg),
        CatalogError::Conflict(msg) => IcebergError::new(ErrorKind::DataInvalid, msg),
        CatalogError::InvalidRequest(msg) => IcebergError::new(ErrorKind::DataInvalid, msg),
        CatalogError::AuthError(msg) => IcebergError::new(ErrorKind::Unexpected, msg),
        CatalogError::HttpError(msg) => IcebergError::new(ErrorKind::Unexpected, msg),
        CatalogError::InvalidArn(msg) => IcebergError::new(ErrorKind::DataInvalid, msg),
        CatalogError::InvalidConfig(msg) => IcebergError::new(ErrorKind::DataInvalid, msg),
        CatalogError::Unexpected(msg) => IcebergError::new(ErrorKind::Unexpected, msg),
    }
}

pub fn build_table(
    ident: TableIdent,
    metadata: TableMetadata,
    file_io: iceberg::io::FileIO,
) -> IcebergResult<Table> {
    let metadata_location = format!(
        "{}/metadata/00000-initial.metadata.json",
        metadata.location()
    );

    Table::builder()
        .identifier(ident)
        .metadata(metadata)
        .metadata_location(metadata_location)
        .file_io(file_io)
        .build()
}
