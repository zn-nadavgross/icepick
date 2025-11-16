//! Error types for icepick catalog operations

/// Result type for catalog operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for catalog operations
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Resource not found (HTTP 404)
    #[error("Resource not found: {resource}")]
    NotFound { resource: String },

    /// Authentication failed (HTTP 401)
    #[error("Unauthorized: {provider}")]
    Unauthorized { provider: String },

    /// Access forbidden (HTTP 403)
    #[error("Forbidden: {resource}")]
    Forbidden { resource: String },

    /// Conflict with existing resource (HTTP 409)
    #[error("Conflict: {message}")]
    Conflict { message: String },

    /// Invalid request (HTTP 400)
    #[error("Invalid request: {message}")]
    InvalidRequest { message: String },

    /// Server error (HTTP 5xx)
    #[error("Server error {status}: {message}")]
    ServerError { status: u16, message: String },

    /// Network/transport error
    #[error("Network error: {source}")]
    NetworkError {
        #[from]
        source: reqwest::Error,
    },

    /// Invalid ARN format (native platforms only)
    #[cfg(not(target_family = "wasm"))]
    #[error("Invalid ARN: {arn}")]
    InvalidArn { arn: String },

    /// Invalid configuration
    #[error("Invalid configuration: {message}")]
    InvalidConfig { message: String },

    /// JSON parsing error
    #[error("JSON parse error: {source}")]
    JsonError {
        #[from]
        source: serde_json::Error,
    },

    /// Unexpected error
    #[error("Unexpected error: {message}")]
    Unexpected { message: String },

    /// Invalid input (validation error)
    #[error("Invalid input: {0}")]
    InvalidInput(String),

    /// I/O error
    #[error("I/O error: {0}")]
    IoError(String),

    /// Concurrent modification detected (optimistic locking failure)
    #[error("Concurrent modification detected: {message}")]
    ConcurrentModification { message: String },
}

impl Error {
    /// Create a NotFound error
    pub fn not_found(resource: impl Into<String>) -> Self {
        Self::NotFound {
            resource: resource.into(),
        }
    }

    /// Create an Unauthorized error
    pub fn unauthorized(provider: impl Into<String>) -> Self {
        Self::Unauthorized {
            provider: provider.into(),
        }
    }

    /// Create a Forbidden error
    pub fn forbidden(resource: impl Into<String>) -> Self {
        Self::Forbidden {
            resource: resource.into(),
        }
    }

    /// Create a Conflict error
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    /// Create an InvalidRequest error
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            message: message.into(),
        }
    }

    /// Create a ServerError
    pub fn server_error(status: u16, message: impl Into<String>) -> Self {
        Self::ServerError {
            status,
            message: message.into(),
        }
    }

    /// Create an InvalidArn error (native platforms only)
    #[cfg(not(target_family = "wasm"))]
    pub fn invalid_arn(arn: impl Into<String>) -> Self {
        Self::InvalidArn { arn: arn.into() }
    }

    /// Create an InvalidConfig error
    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::InvalidConfig {
            message: message.into(),
        }
    }

    /// Create an Unexpected error
    pub fn unexpected(message: impl Into<String>) -> Self {
        Self::Unexpected {
            message: message.into(),
        }
    }

    /// Create a ConcurrentModification error
    pub fn concurrent_modification(message: impl Into<String>) -> Self {
        Self::ConcurrentModification {
            message: message.into(),
        }
    }

    /// Create an InvalidInput error
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput(message.into())
    }

    /// Create an IoError
    pub fn io_error(message: impl Into<String>) -> Self {
        Self::IoError(message.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_constructors() {
        let err = Error::not_found("table1");
        assert!(matches!(err, Error::NotFound { .. }));
        assert_eq!(err.to_string(), "Resource not found: table1");

        let err = Error::unauthorized("AWS");
        assert!(matches!(err, Error::Unauthorized { .. }));

        let err = Error::forbidden("namespace1");
        assert!(matches!(err, Error::Forbidden { .. }));

        #[cfg(not(target_family = "wasm"))]
        {
            let err = Error::invalid_arn("bad-arn");
            assert!(matches!(err, Error::InvalidArn { .. }));
        }
    }

    #[test]
    fn test_error_display() {
        let err = Error::server_error(500, "Internal error");
        assert_eq!(err.to_string(), "Server error 500: Internal error");

        let err = Error::conflict("Resource already exists");
        assert_eq!(err.to_string(), "Conflict: Resource already exists");
    }

    #[test]
    fn test_concurrent_modification_error() {
        let err = Error::concurrent_modification("expected v2, found v3");
        assert!(matches!(err, Error::ConcurrentModification { .. }));
        assert_eq!(
            err.to_string(),
            "Concurrent modification detected: expected v2, found v3"
        );
    }
}
