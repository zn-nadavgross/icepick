#[cfg(not(target_family = "wasm"))]
mod arn;
mod catalog_impl;
mod catalog_trait;
mod client;
pub mod commit_types;
mod helpers;
mod types;

use crate::catalog::{AuthProvider, CatalogError, Result};
use crate::io::FileIO;
use reqwest::{Client, Response};
use tracing::{debug, trace, Level};

const HTTP_TRACE_TARGET: &str = "icepick::http";

/// Shared Iceberg REST catalog implementation
pub struct IcebergRestCatalog {
    endpoint: String,
    prefix: String,
    http_client: Client,
    auth_provider: Box<dyn AuthProvider>,
    file_io: FileIO,
    name: String,
}

impl std::fmt::Debug for IcebergRestCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergRestCatalog")
            .field("endpoint", &self.endpoint)
            .field("prefix", &self.prefix)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
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

    async fn send_request(&self, req: reqwest::Request) -> Result<Response> {
        let signed_req = self.auth_provider.sign_request(req).await?;
        if tracing::enabled!(Level::DEBUG) {
            debug!(
                target: HTTP_TRACE_TARGET,
                method = %signed_req.method(),
                url = %signed_req.url(),
                "Sending HTTP request"
            );
        }

        let response = self
            .http_client
            .execute(signed_req)
            .await
            .map_err(|e| CatalogError::HttpError(format!("Request failed: {}", e)))?;

        Ok(response)
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
}
