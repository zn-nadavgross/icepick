use crate::catalog::CatalogError;
use crate::error::{Error, Result};
use crate::spec::{TableIdent, TableMetadata};
use crate::table::Table;

pub fn from_catalog_error(e: CatalogError) -> Error {
    match e {
        CatalogError::Transient(msg) => Error::unexpected(format!("Transient error: {}", msg)),
        CatalogError::Permanent(msg) => Error::unexpected(format!("Permanent error: {}", msg)),
        CatalogError::Timeout(duration) => {
            Error::unexpected(format!("Timeout after {:?}", duration))
        }
        CatalogError::NotFound(msg) => Error::not_found(msg),
        CatalogError::Conflict(msg) => Error::concurrent_modification(msg),
        CatalogError::InvalidRequest(msg) => Error::invalid_input(msg),
        CatalogError::AuthError(msg) => Error::unauthorized(msg),
        CatalogError::HttpError(msg) => Error::io_error(msg),
        CatalogError::ServerError { status, message } => Error::server_error(status, message),
        CatalogError::Network(err) => Error::NetworkError { source: err },
        #[cfg(not(target_family = "wasm"))]
        CatalogError::InvalidArn(msg) => Error::invalid_input(msg),
        CatalogError::Unexpected(msg) => Error::unexpected(msg),
    }
}

pub fn build_table(
    ident: TableIdent,
    metadata: TableMetadata,
    metadata_location: String,
    file_io: crate::io::FileIO,
) -> Result<Table> {
    Ok(Table::new(ident, metadata, metadata_location, file_io))
}
