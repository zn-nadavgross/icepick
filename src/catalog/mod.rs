//! Iceberg REST catalog implementation with pluggable authentication

mod auth;
pub mod r2;
pub(crate) mod rest;

#[cfg(not(target_family = "wasm"))]
pub mod s3_tables;

// New trait-based API
mod catalog_trait;

pub use catalog_trait::Catalog;

// Make auth providers internal - not part of public API
pub(crate) use auth::BearerTokenAuthProvider;

#[cfg(not(target_family = "wasm"))]
pub(crate) use auth::SigV4AuthProvider;

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
#[async_trait]
pub(crate) trait AuthProvider: Send + Sync + std::fmt::Debug {
    /// Sign or authenticate an HTTP request
    async fn sign_request(&self, request: reqwest::Request) -> Result<reqwest::Request>;
}
