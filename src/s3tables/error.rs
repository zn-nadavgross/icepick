use std::fmt;

#[derive(Debug)]
pub enum S3TablesError {
    InvalidArn(String),
    HttpError(String),
    AuthError(String),
    NotFound(String),
    Conflict(String),
    InvalidRequest(String),
    Unexpected(String),
}

impl fmt::Display for S3TablesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArn(msg) => write!(f, "Invalid S3 Tables ARN: {}", msg),
            Self::HttpError(msg) => write!(f, "HTTP error: {}", msg),
            Self::AuthError(msg) => write!(f, "Authentication failed: {}", msg),
            Self::NotFound(msg) => write!(f, "Not found: {}", msg),
            Self::Conflict(msg) => write!(f, "Conflict: {}", msg),
            Self::InvalidRequest(msg) => write!(f, "Invalid request: {}", msg),
            Self::Unexpected(msg) => write!(f, "Unexpected error: {}", msg),
        }
    }
}

impl std::error::Error for S3TablesError {}

pub type Result<T> = std::result::Result<T, S3TablesError>;
