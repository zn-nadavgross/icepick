#[cfg(not(target_family = "wasm"))]
mod arn;
mod catalog_impl;
mod catalog_trait;
mod client;
pub mod commit_types;
mod helpers;
mod types;

use crate::catalog::{AuthProvider, CatalogError, CatalogOptions, Result};
use crate::io::FileIO;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use reqwest::{Client, Response, StatusCode};
use std::time::{Duration, Instant};
use tracing::{debug, trace, Level};

#[cfg(target_family = "wasm")]
use gloo_timers::future::TimeoutFuture;
#[cfg(not(target_family = "wasm"))]
use tokio::time::sleep;

const HTTP_TRACE_TARGET: &str = "icepick::http";

/// Shared Iceberg REST catalog implementation
pub struct IcebergRestCatalog {
    endpoint: String,
    prefix: String,
    http_client: Client,
    auth_provider: Box<dyn AuthProvider>,
    file_io: FileIO,
    name: String,
    options: CatalogOptions,
}

impl std::fmt::Debug for IcebergRestCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergRestCatalog")
            .field("endpoint", &self.endpoint)
            .field("prefix", &self.prefix)
            .field("reference", &self.options.reference())
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

fn should_retry_status(status: StatusCode) -> bool {
    status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS
}

#[cfg(not(target_family = "wasm"))]
fn should_retry_error(err: &reqwest::Error) -> bool {
    err.is_connect() || err.is_timeout()
}

#[cfg(target_family = "wasm")]
fn should_retry_error(err: &reqwest::Error) -> bool {
    err.is_timeout()
}

async fn backoff_sleep(duration: Duration) {
    if duration.is_zero() {
        return;
    }

    sleep_for(duration).await;
}

#[cfg(not(target_family = "wasm"))]
async fn sleep_for(duration: Duration) {
    sleep(duration).await;
}

#[cfg(target_family = "wasm")]
async fn sleep_for(duration: Duration) {
    let millis = duration.as_millis().min(u128::from(u32::MAX)) as u32;
    TimeoutFuture::new(millis).await;
}

impl IcebergRestCatalog {
    /// Construct API endpoint URL with proper prefix handling
    /// Follows PyIceberg pattern: {uri}/v1/{prefix}/{endpoint}
    /// If prefix is empty, produces: {uri}/v1/{endpoint}
    fn url(&self, endpoint: &str) -> String {
        let mut url = self.endpoint.clone();

        // Add /v1/
        url = if url.ends_with('/') {
            format!("{}v1/", url)
        } else {
            format!("{}/v1/", url)
        };

        // Add prefix if not empty
        if !self.prefix.is_empty() {
            url = if url.ends_with('/') {
                format!("{}{}/", url, self.prefix)
            } else {
                format!("{}/{}/", url, self.prefix)
            };
        }

        // Add endpoint
        format!("{}{}", url, endpoint)
    }

    fn table_url(&self, namespace: &str, table_name: &str, include_ref: bool) -> String {
        let endpoint = format!("namespaces/{}/tables/{}", namespace, table_name);
        if include_ref {
            self.append_reference(self.url(&endpoint))
        } else {
            self.url(&endpoint)
        }
    }

    fn append_reference(&self, url: String) -> String {
        if self.options.reference().is_empty() {
            return url;
        }

        let encoded = utf8_percent_encode(self.options.reference(), NON_ALPHANUMERIC).to_string();
        let separator = if url.contains('?') { '&' } else { '?' };
        format!("{url}{separator}ref={encoded}")
    }

    async fn send_request(&self, req: reqwest::Request) -> Result<Response> {
        let http_config = self.options.http();
        let max_attempts = http_config.max_retries().saturating_add(1);
        let mut attempt = 0;

        loop {
            let cloned_request = req.try_clone().ok_or_else(|| {
                CatalogError::HttpError("Request body cannot be cloned for retry".to_string())
            })?;
            let signed_req = self.auth_provider.sign_request(cloned_request).await?;
            if tracing::enabled!(Level::DEBUG) {
                debug!(
                    target: HTTP_TRACE_TARGET,
                    method = %signed_req.method(),
                    url = %signed_req.url(),
                    attempt = attempt + 1,
                    "Sending HTTP request"
                );
            }

            match self.http_client.execute(signed_req).await {
                Ok(response) => {
                    let status = response.status();
                    if attempt + 1 < max_attempts && should_retry_status(status) {
                        if tracing::enabled!(Level::DEBUG) {
                            debug!(
                                target: HTTP_TRACE_TARGET,
                                status = status.as_u16(),
                                attempt = attempt + 1,
                                "Retrying HTTP request after server response"
                            );
                        }
                        // Drain the response body so the connection can be reused
                        let _ = response.bytes().await;
                        backoff_sleep(http_config.retry_backoff()).await;
                        attempt += 1;
                        continue;
                    }

                    return Ok(response);
                }
                Err(err) => {
                    if attempt + 1 < max_attempts && should_retry_error(&err) {
                        if tracing::enabled!(Level::DEBUG) {
                            debug!(
                                target: HTTP_TRACE_TARGET,
                                attempt = attempt + 1,
                                error = %err,
                                "Retrying HTTP request after transport error"
                            );
                        }
                        backoff_sleep(http_config.retry_backoff()).await;
                        attempt += 1;
                        continue;
                    }

                    return Err(CatalogError::Network(err));
                }
            }
        }
    }

    async fn handle_response(&self, response: Response) -> Result<serde_json::Value> {
        let status = response.status();
        if tracing::enabled!(Level::DEBUG) {
            debug!(
                target: HTTP_TRACE_TARGET,
                status = status.as_u16(),
                "Received HTTP response"
            );
        }

        match status.as_u16() {
            200..=299 => {
                // Debug: log the response body before parsing
                let body_text = response.text().await.map_err(|e| {
                    CatalogError::HttpError(format!("Failed to read response body: {}", e))
                })?;
                if tracing::enabled!(Level::TRACE) {
                    trace!(
                        target: HTTP_TRACE_TARGET,
                        status = status.as_u16(),
                        body = body_text,
                        "Response body"
                    );
                }

                // Handle empty responses (204 No Content or empty body)
                if body_text.is_empty() || status.as_u16() == 204 {
                    return Ok(serde_json::Value::Object(serde_json::Map::new()));
                }

                serde_json::from_str(&body_text).map_err(|e| {
                    CatalogError::HttpError(format!("Failed to parse response: {}", e))
                })
            }

            403 => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unable to read response".to_string());
                if tracing::enabled!(Level::TRACE) {
                    trace!(
                        target: HTTP_TRACE_TARGET,
                        status = status.as_u16(),
                        body = body,
                        "Error response body"
                    );
                }
                Err(CatalogError::AuthError(format!(
                    "Authentication failed: {}",
                    body
                )))
            }

            404 => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Resource not found".to_string());
                if tracing::enabled!(Level::TRACE) {
                    trace!(
                        target: HTTP_TRACE_TARGET,
                        status = status.as_u16(),
                        body = body,
                        "Error response body"
                    );
                }
                Err(CatalogError::NotFound(body))
            }

            409 => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Conflict".to_string());
                if tracing::enabled!(Level::TRACE) {
                    trace!(
                        target: HTTP_TRACE_TARGET,
                        status = status.as_u16(),
                        body = body,
                        "Error response body"
                    );
                }
                Err(CatalogError::Conflict(format!(
                    "Requirements not met: {}",
                    body
                )))
            }

            400 => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Bad request".to_string());
                if tracing::enabled!(Level::TRACE) {
                    trace!(
                        target: HTTP_TRACE_TARGET,
                        status = status.as_u16(),
                        body = body,
                        "Error response body"
                    );
                }
                Err(CatalogError::InvalidRequest(body))
            }

            429 => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Too many requests".to_string());
                if tracing::enabled!(Level::TRACE) {
                    trace!(
                        target: HTTP_TRACE_TARGET,
                        status = status.as_u16(),
                        body = body,
                        "Error response body"
                    );
                }
                Err(CatalogError::ServerError {
                    status: status.as_u16(),
                    message: body,
                })
            }

            500..=599 => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Server error".to_string());
                if tracing::enabled!(Level::TRACE) {
                    trace!(
                        target: HTTP_TRACE_TARGET,
                        status = status.as_u16(),
                        body = body,
                        "Error response body"
                    );
                }
                Err(CatalogError::ServerError {
                    status: status.as_u16(),
                    message: body,
                })
            }

            _ => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                if tracing::enabled!(Level::TRACE) {
                    trace!(
                        target: HTTP_TRACE_TARGET,
                        status = status.as_u16(),
                        body = body,
                        "Error response body"
                    );
                }
                Err(CatalogError::Unexpected(format!(
                    "HTTP {}: {}",
                    status, body
                )))
            }
        }
    }

    /// Return a reference to the underlying FileIO.
    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Wrap a catalog operation with application-level retry logic based on error type.
    ///
    /// This implements retry behavior configured via `RetryConfig` for transient errors
    /// like network failures, server errors, and rate limits.
    pub(super) async fn with_retry<F, Fut, T>(&self, operation: F) -> crate::error::Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = crate::error::Result<T>>,
    {
        let retry_config = match self.options.retry() {
            Some(config) => config,
            None => {
                // No retry configured, execute once
                return operation().await;
            }
        };

        let start_time = Instant::now();
        let max_retries = retry_config.max_retries();
        let mut attempt = 0;

        loop {
            // Check if we've exceeded max elapsed time
            if let Some(max_elapsed) = retry_config.max_elapsed_time() {
                if start_time.elapsed() >= max_elapsed {
                    if tracing::enabled!(Level::DEBUG) {
                        debug!("Retry timeout: exceeded max elapsed time {:?}", max_elapsed);
                    }
                    return Err(crate::error::Error::unexpected(format!(
                        "Operation timed out after {:?}",
                        max_elapsed
                    )));
                }
            }

            match operation().await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    // Check if error is retryable (network errors, server errors)
                    let is_retryable = matches!(
                        err,
                        crate::error::Error::NetworkError { .. }
                            | crate::error::Error::ServerError { .. }
                            | crate::error::Error::IoError { .. }
                    );

                    if !is_retryable {
                        if tracing::enabled!(Level::DEBUG) {
                            debug!("Non-retryable error encountered: {:?}", err);
                        }
                        return Err(err);
                    }

                    // Check if we have retries left
                    if attempt >= max_retries {
                        if tracing::enabled!(Level::DEBUG) {
                            debug!("Max retries ({}) exceeded, returning error", max_retries);
                        }
                        return Err(err);
                    }

                    // Calculate backoff delay
                    let delay = retry_config.delay_for_attempt(attempt);

                    if tracing::enabled!(Level::DEBUG) {
                        debug!(
                            "Retryable error on attempt {}/{}: {:?}. Waiting {:?} before retry",
                            attempt + 1,
                            max_retries,
                            err,
                            delay
                        );
                    }

                    // Sleep before retry
                    backoff_sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }
}
