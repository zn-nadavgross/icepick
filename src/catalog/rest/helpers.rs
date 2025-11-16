use crate::catalog::CatalogError;
use crate::error::{Error, Result};
use crate::spec::{TableIdent, TableMetadata};
use crate::table::Table;

pub fn from_catalog_error(e: CatalogError) -> Error {
    match e {
        CatalogError::NotFound(msg) => Error::not_found(msg),
        CatalogError::Conflict(msg) => Error::concurrent_modification(msg),
        CatalogError::InvalidRequest(msg) => Error::invalid_input(msg),
        CatalogError::AuthError(msg) => Error::unexpected(msg),
        CatalogError::HttpError(msg) => Error::io_error(msg),
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
