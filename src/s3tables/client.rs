use crate::s3tables::error::{Result, S3TablesError};
use iceberg::spec::{Schema, TableMetadata};
use reqsign::{Context, OsEnv, Signer};
use reqsign_aws_v4::{Credential, DefaultCredentialProvider, RequestSigner};
use reqsign_file_read_tokio::TokioFileRead;
use reqsign_http_send_reqwest::ReqwestHttpSend;
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Parse S3 Tables ARN and extract region and bucket name
/// ARN format: arn:aws:s3tables:region:account:bucket/name
fn parse_s3tables_arn(arn: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = arn.split(':').collect();

    if parts.len() != 6 {
        return Err(S3TablesError::InvalidArn(format!(
            "Expected 6 parts, got {}",
            parts.len()
        )));
    }

    if parts[0] != "arn" {
        return Err(S3TablesError::InvalidArn(
            "Must start with 'arn'".to_string(),
        ));
    }

    if parts[2] != "s3tables" {
        return Err(S3TablesError::InvalidArn(format!(
            "Not an S3 Tables ARN: {}",
            parts[2]
        )));
    }

    let region = parts[3].to_string();
    let bucket_name = parts[5]
        .strip_prefix("bucket/")
        .ok_or_else(|| S3TablesError::InvalidArn("Missing 'bucket/' prefix".to_string()))?
        .to_string();

    Ok((region, bucket_name))
}

#[derive(Serialize)]
struct CreateNamespaceRequest {
    namespace: Vec<String>,
    properties: HashMap<String, String>,
}

#[derive(Deserialize)]
struct CreateNamespaceResponse {
    namespace: Vec<String>,
    properties: HashMap<String, String>,
}

#[derive(Serialize)]
struct CreateTableRequest {
    name: String,
    schema: Schema,
    location: Option<String>,
    #[serde(rename = "partition-spec")]
    partition_spec: serde_json::Value,
    #[serde(rename = "write-order")]
    write_order: serde_json::Value,
    properties: HashMap<String, String>,
}

#[derive(Deserialize)]
struct CreateTableResponse {
    metadata: TableMetadata,
    #[serde(rename = "metadata-location")]
    metadata_location: String,
}

pub struct S3TablesClient {
    endpoint: String,
    warehouse: String,
    region: String,
    http_client: Client,
    signer: Signer<Credential>,
}

impl S3TablesClient {
    pub async fn from_arn(arn: &str) -> Result<Self> {
        let (region, _bucket_name) = parse_s3tables_arn(arn)?;
        let endpoint = format!("https://s3tables.{}.amazonaws.com/iceberg", region);

        let http_client = Client::new();

        // Build context for AWS credential loading and HTTP requests
        let ctx = Context::new()
            .with_file_read(TokioFileRead)
            .with_http_send(ReqwestHttpSend::default())
            .with_env(OsEnv);

        // Create credential provider (loads from environment/config files)
        let credential_provider = DefaultCredentialProvider::new();

        // Create request signer for s3tables service
        let request_signer = RequestSigner::new("s3tables", &region);

        // Assemble the signer
        let signer = Signer::new(ctx, credential_provider, request_signer);

        Ok(Self {
            endpoint,
            warehouse: arn.to_string(),
            region,
            http_client,
            signer,
        })
    }

    pub async fn create_namespace(
        &self,
        namespace: &str,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let url = format!("{}/v1/namespaces", self.endpoint);

        let body = CreateNamespaceRequest {
            namespace: vec![namespace.to_string()],
            properties,
        };

        let req = self.http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .build()
            .map_err(|e| S3TablesError::HttpError(format!("Failed to build request: {}", e)))?;

        let response = self.send_signed_request(req).await?;
        let _json_value = self.handle_response(response).await?;

        // Namespace created successfully
        Ok(())
    }

    pub async fn create_table(
        &self,
        namespace: &str,
        table_name: &str,
        schema: Schema,
    ) -> Result<TableMetadata> {
        let url = format!("{}/v1/namespaces/{}/tables", self.endpoint, namespace);

        let body = CreateTableRequest {
            name: table_name.to_string(),
            schema,
            location: None,  // S3 Tables auto-assigns
            partition_spec: serde_json::json!({
                "spec-id": 0,
                "fields": []
            }),
            write_order: serde_json::json!({
                "order-id": 0,
                "fields": []
            }),
            properties: HashMap::new(),
        };

        let req = self.http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .build()
            .map_err(|e| S3TablesError::HttpError(format!("Failed to build request: {}", e)))?;

        let response = self.send_signed_request(req).await?;
        let json_value = self.handle_response(response).await?;

        let table_response: CreateTableResponse = serde_json::from_value(json_value)
            .map_err(|e| S3TablesError::Unexpected(format!("Failed to parse table response: {}", e)))?;

        Ok(table_response.metadata)
    }

    async fn send_signed_request(&self, req: reqwest::Request) -> Result<Response> {
        // Extract request components
        let url = req.url().clone();
        let method = req.method().clone();
        let headers = req.headers().clone();
        let body_bytes = req
            .body()
            .and_then(|b| b.as_bytes())
            .map(|b| b.to_vec())
            .unwrap_or_default();

        // Build http::Request for signing
        let mut http_req = http::Request::builder()
            .method(method.as_str())
            .uri(url.as_str())
            .body(())
            .map_err(|e| {
                S3TablesError::Unexpected(format!("Failed to build HTTP request: {}", e))
            })?;

        // Copy headers
        for (name, value) in headers.iter() {
            http_req.headers_mut().insert(name.clone(), value.clone());
        }

        // Sign the request
        let (mut parts, _) = http_req.into_parts();
        self.signer
            .sign(&mut parts, None)
            .await
            .map_err(|e| S3TablesError::AuthError(format!("Failed to sign request: {}", e)))?;

        // Build signed reqwest::Request
        let mut signed_req = self
            .http_client
            .request(method, url)
            .body(body_bytes)
            .build()
            .map_err(|e| S3TablesError::HttpError(format!("Failed to build request: {}", e)))?;

        // Copy signed headers
        *signed_req.headers_mut() = parts.headers;

        // Execute signed request
        let response = self
            .http_client
            .execute(signed_req)
            .await
            .map_err(|e| S3TablesError::HttpError(format!("Request failed: {}", e)))?;

        Ok(response)
    }

    async fn handle_response(&self, response: Response) -> Result<serde_json::Value> {
        let status = response.status();

        match status.as_u16() {
            200..=299 => {
                response.json().await
                    .map_err(|e| S3TablesError::HttpError(
                        format!("Failed to parse JSON response: {}", e)
                    ))
            }

            403 => {
                let body = response.text().await
                    .unwrap_or_else(|_| "Unable to read response".to_string());
                Err(S3TablesError::AuthError(
                    format!("Authentication failed: {}", body)
                ))
            }

            404 => {
                Err(S3TablesError::NotFound("Resource not found".to_string()))
            }

            409 => {
                let body = response.text().await
                    .unwrap_or_else(|_| "Conflict".to_string());
                Err(S3TablesError::Conflict(
                    format!("Requirements not met: {}", body)
                ))
            }

            400 => {
                let body = response.text().await
                    .unwrap_or_else(|_| "Bad request".to_string());
                Err(S3TablesError::InvalidRequest(body))
            }

            _ => {
                let body = response.text().await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                Err(S3TablesError::Unexpected(
                    format!("HTTP {}: {}", status, body)
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_arn_valid() {
        let arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_ok());
        let (region, bucket) = result.unwrap();
        assert_eq!(region, "us-west-2");
        assert_eq!(bucket, "my-bucket");
    }

    #[test]
    fn test_parse_arn_invalid_format() {
        let arn = "invalid-arn";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_arn_wrong_service() {
        let arn = "arn:aws:s3:us-west-2:123456789012:bucket/my-bucket";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_arn_missing_bucket_prefix() {
        let arn = "arn:aws:s3tables:us-west-2:123456789012:my-bucket";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_from_arn_creates_client() {
        let arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/test-bucket";
        let result = S3TablesClient::from_arn(arn).await;
        assert!(result.is_ok());
        let client = result.unwrap();
        assert_eq!(client.region, "us-west-2");
        assert_eq!(client.warehouse, arn);
        assert_eq!(
            client.endpoint,
            "https://s3tables.us-west-2.amazonaws.com/iceberg"
        );
    }
}
