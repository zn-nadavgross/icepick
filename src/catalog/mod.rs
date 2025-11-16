//! Iceberg REST catalog implementation with pluggable authentication

mod auth;
pub mod rest;

pub use auth::BearerTokenAuthProvider;

#[cfg(not(target_family = "wasm"))]
pub use auth::SigV4AuthProvider;

pub use rest::IcebergRestCatalog;

use async_trait::async_trait;

/// Result type for catalog operations
pub type Result<T> = std::result::Result<T, CatalogError>;

/// Error types for catalog operations
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
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

    #[error("Invalid ARN: {0}")]
    InvalidArn(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Unexpected error: {0}")]
    Unexpected(String),
}

/// Configuration for R2 Data Catalog
#[derive(Debug, Clone)]
pub struct R2Config {
    pub account_id: String,
    pub bucket_name: String,
    pub api_token: String,
    pub endpoint_override: Option<String>,
}

/// Authentication provider trait for signing/authenticating requests
#[async_trait]
pub trait AuthProvider: Send + Sync + std::fmt::Debug {
    /// Sign or authenticate an HTTP request
    async fn sign_request(&self, request: reqwest::Request) -> Result<reqwest::Request>;
}
