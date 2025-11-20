//! Shared configuration types for catalog clients.

use crate::catalog::retry::RetryConfig;
use std::time::Duration;

/// Configuration for the underlying HTTP client.
///
/// This struct lets callers configure request timeouts, retry behaviour,
/// and the user-agent header used for all catalog requests.
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    max_retries: usize,
    retry_backoff: Duration,
    user_agent: Option<String>,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            timeout: Some(Duration::from_secs(30)),
            connect_timeout: Some(Duration::from_secs(10)),
            max_retries: 2,
            retry_backoff: Duration::from_millis(250),
            user_agent: Some(default_user_agent()),
        }
    }
}

impl HttpClientConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the global request timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Disable the global request timeout.
    pub fn without_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    /// Override the connect timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Disable the connect timeout.
    pub fn without_connect_timeout(mut self) -> Self {
        self.connect_timeout = None;
        self
    }

    /// Override the maximum number of retries for failed requests.
    pub fn with_max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    /// Override the backoff used between retries.
    pub fn with_retry_backoff(mut self, backoff: Duration) -> Self {
        self.retry_backoff = backoff;
        self
    }

    /// Override the default user-agent header.
    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    /// Remove the custom user-agent header.
    pub fn without_user_agent(mut self) -> Self {
        self.user_agent = None;
        self
    }

    /// Request timeout.
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Connect timeout.
    pub fn connect_timeout(&self) -> Option<Duration> {
        self.connect_timeout
    }

    /// Maximum number of retries.
    pub fn max_retries(&self) -> usize {
        self.max_retries
    }

    /// Backoff applied between retries.
    pub fn retry_backoff(&self) -> Duration {
        self.retry_backoff
    }

    /// Optional user-agent header value.
    pub fn user_agent(&self) -> Option<&str> {
        self.user_agent.as_deref()
    }
}

fn default_user_agent() -> String {
    format!("icepick/{}", env!("CARGO_PKG_VERSION"))
}

/// High-level catalog options shared between R2 and S3 Tables.
#[derive(Debug, Clone)]
pub struct CatalogOptions {
    http: HttpClientConfig,
    reference: String,
    retry: Option<RetryConfig>,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self {
            http: HttpClientConfig::default(),
            reference: "main".to_string(),
            retry: None,
        }
    }
}

impl CatalogOptions {
    /// Create a new options struct with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the catalog reference/branch to operate on.
    pub fn with_reference(mut self, reference: impl Into<String>) -> Self {
        self.reference = reference.into();
        self
    }

    /// Override the HTTP configuration.
    pub fn with_http_config(mut self, http: HttpClientConfig) -> Self {
        self.http = http;
        self
    }

    /// Configure retry behavior for catalog operations.
    ///
    /// This controls application-level retries based on error types (Transient/Permanent/Timeout).
    /// If not set, catalog operations will not retry failed requests.
    pub fn with_retry_config(mut self, retry: RetryConfig) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Access the configured HTTP behaviour.
    pub fn http(&self) -> &HttpClientConfig {
        &self.http
    }

    /// Get the reference/branch that commits and table loads target.
    pub fn reference(&self) -> &str {
        &self.reference
    }

    /// Get the retry configuration if set.
    pub fn retry(&self) -> Option<&RetryConfig> {
        self.retry.as_ref()
    }
}
