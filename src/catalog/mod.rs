//! Iceberg REST catalog implementation with pluggable authentication

mod auth;
mod options;
pub mod r2;
pub(crate) mod rest;
pub mod rest_catalog;

#[cfg(not(target_family = "wasm"))]
pub mod s3_tables;

// New trait-based API
mod catalog_trait;

pub use catalog_trait::Catalog;
pub use options::{CatalogOptions, HttpClientConfig};
pub use rest_catalog::{RestAuthProvider, RestCatalog, RestCatalogBuilder};

// Make auth providers internal - not part of public API
pub use auth::BearerTokenAuthProvider;

#[cfg(not(target_family = "wasm"))]
pub use auth::SigV4AuthProvider;

use crate::error::Error;
use async_trait::async_trait;

/// Result type for catalog operations (internal)
pub(crate) type Result<T> = std::result::Result<T, CatalogError>;

/// Error types for catalog operations (internal)
#[derive(Debug, thiserror::Error)]
pub(crate) enum CatalogError {
    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Authentication failed: {0}")]
    AuthError(String),

    #[error("HTTP error: {0}")]
    HttpError(String),

    #[error("Server error {status}: {message}")]
    ServerError { status: u16, message: String },

    #[error("Network error: {0}")]
    Network(reqwest::Error),

    #[cfg(not(target_family = "wasm"))]
    #[error("Invalid ARN: {0}")]
    InvalidArn(String),

    #[error("Unexpected error: {0}")]
    Unexpected(String),
}

/// Configuration for R2 Data Catalog (internal)
#[derive(Debug, Clone)]
pub(crate) struct R2Config {
    pub account_id: String,
    pub bucket_name: String,
    pub api_token: String,
    pub endpoint_override: Option<String>,
}

/// Authentication provider trait for signing/authenticating requests (internal)
#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
pub(crate) trait AuthProvider: Send + Sync + std::fmt::Debug {
    /// Sign or authenticate an HTTP request
    async fn sign_request(&self, request: reqwest::Request) -> Result<reqwest::Request>;
}

pub(crate) fn map_catalog_error(err: CatalogError) -> Error {
    match err {
        CatalogError::NotFound(resource) => Error::not_found(resource),
        CatalogError::Conflict(message) => Error::conflict(message),
        CatalogError::InvalidRequest(message) => Error::invalid_request(message),
        CatalogError::AuthError(provider) => Error::unauthorized(provider),
        CatalogError::HttpError(message) => Error::unexpected(message),
        CatalogError::ServerError { status, message } => Error::server_error(status, message),
        CatalogError::Network(source) => Error::NetworkError { source },
        #[cfg(not(target_family = "wasm"))]
        CatalogError::InvalidArn(arn) => Error::invalid_arn(arn),
        CatalogError::Unexpected(message) => Error::unexpected(message),
    }
}
