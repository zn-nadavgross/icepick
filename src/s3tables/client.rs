use crate::s3tables::arn::{parse_s3tables_arn, ARN_ENCODE_SET};
use crate::s3tables::error::{Result, S3TablesError};
use crate::s3tables::types::*;
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;
use http::Request as HttpRequest;
use iceberg::spec::{Schema, TableMetadata};
use iceberg::{TableRequirement, TableUpdate};
use percent_encoding::utf8_percent_encode;
use reqwest::{Client, Response};
use std::collections::HashMap;
use std::time::SystemTime;

pub struct S3TablesClient {
    endpoint: String,
    warehouse: String,
    warehouse_prefix: String, // URL-encoded ARN for path prefix
    region: String,
    http_client: Client,
    credentials: aws_credential_types::Credentials,
}

impl std::fmt::Debug for S3TablesClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3TablesClient")
            .field("endpoint", &self.endpoint)
            .field("warehouse", &self.warehouse)
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

impl S3TablesClient {
    pub async fn from_arn(arn: &str) -> Result<Self> {
        let (region, _bucket_name) = parse_s3tables_arn(arn)?;
        let endpoint = format!("https://s3tables.{}.amazonaws.com/iceberg", region);

        // URL-encode the ARN for use in path
        let warehouse_prefix = utf8_percent_encode(arn, ARN_ENCODE_SET).to_string();
        eprintln!("DEBUG: Encoded ARN for path: {}", warehouse_prefix);

        let http_client = Client::new();

        // Load AWS credentials using the SDK
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let credentials = config
            .credentials_provider()
            .ok_or_else(|| S3TablesError::AuthError("No credentials provider found".to_string()))?
            .provide_credentials()
            .await
            .map_err(|e| S3TablesError::AuthError(format!("Failed to load credentials: {}", e)))?;

        eprintln!("DEBUG: Loaded AWS credentials");

        Ok(Self {
            endpoint,
            warehouse: arn.to_string(),
            warehouse_prefix,
            region,
            http_client,
            credentials,
        })
    }

    pub async fn create_namespace(
        &self,
        namespace: &str,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let url = format!("{}/v1/{}/namespaces", self.endpoint, self.warehouse_prefix);

        let body = CreateNamespaceRequest {
            namespace: vec![namespace.to_string()],
            properties,
        };

        let req = self
            .http_client
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
        let url = format!(
            "{}/v1/{}/namespaces/{}/tables",
            self.endpoint, self.warehouse_prefix, namespace
        );
        eprintln!("DEBUG: Creating table at URL: {}", url);

        let body = CreateTableRequest {
            name: table_name.to_string(),
            schema,
            location: None, // S3 Tables auto-assigns
            partition_spec: serde_json::json!({
                "spec-id": 0,
                "fields": []
            }),
            write_order: serde_json::json!({
                "order-id": 0,
                "fields": []
            }),
            properties: HashMap::new(),
            stage_create: false, // Create table immediately, not staged
        };

        eprintln!(
            "DEBUG: Request body: {}",
            serde_json::to_string_pretty(&body).unwrap_or_default()
        );

        let req = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .build()
            .map_err(|e| S3TablesError::HttpError(format!("Failed to build request: {}", e)))?;

        let response = self.send_signed_request(req).await?;
        let json_value = self.handle_response(response).await?;

        let table_response: CreateTableResponse =
            serde_json::from_value(json_value).map_err(|e| {
                S3TablesError::Unexpected(format!("Failed to parse table response: {}", e))
            })?;

        Ok(table_response.metadata)
    }

    pub async fn load_table(&self, namespace: &str, table_name: &str) -> Result<TableMetadata> {
        let url = format!(
            "{}/v1/{}/namespaces/{}/tables/{}",
            self.endpoint, self.warehouse_prefix, namespace, table_name
        );

        let req = self
            .http_client
            .get(&url)
            .header("Accept", "application/json")
            .build()
            .map_err(|e| S3TablesError::HttpError(format!("Failed to build request: {}", e)))?;

        let response = self.send_signed_request(req).await?;
        let json_value = self.handle_response(response).await?;

        let table_response: LoadTableResponse =
            serde_json::from_value(json_value).map_err(|e| {
                S3TablesError::Unexpected(format!("Failed to parse table response: {}", e))
            })?;

        Ok(table_response.metadata)
    }

    pub async fn update_table(
        &self,
        namespace: &str,
        table_name: &str,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<TableMetadata> {
        let url = format!(
            "{}/v1/{}/namespaces/{}/tables/{}",
            self.endpoint, self.warehouse_prefix, namespace, table_name
        );

        let body = UpdateTableRequest {
            requirements,
            updates,
        };

        let req = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .build()
            .map_err(|e| S3TablesError::HttpError(format!("Failed to build request: {}", e)))?;

        let response = self.send_signed_request(req).await?;
        let json_value = self.handle_response(response).await?;

        let table_response: UpdateTableResponse =
            serde_json::from_value(json_value).map_err(|e| {
                S3TablesError::Unexpected(format!("Failed to parse table response: {}", e))
            })?;

        Ok(table_response.metadata)
    }

    async fn send_signed_request(&self, req: reqwest::Request) -> Result<Response> {
        let url = req.url().clone();
        let method = req.method().clone();
        let headers = req.headers().clone();
        let body_bytes = req
            .body()
            .and_then(|b| b.as_bytes())
            .map(|b| b.to_vec())
            .unwrap_or_default();

        eprintln!("\n=== REQUEST ===");
        eprintln!("Method: {}", method);
        eprintln!("URL: {}", url);
        eprintln!("Body length: {} bytes", body_bytes.len());

        // Build http::Request for signing
        let mut http_req = HttpRequest::builder()
            .method(method.as_str())
            .uri(url.as_str())
            .body(&body_bytes[..])
            .map_err(|e| {
                S3TablesError::Unexpected(format!("Failed to build HTTP request: {}", e))
            })?;

        // Copy original headers
        for (name, value) in headers.iter() {
            http_req.headers_mut().insert(name.clone(), value.clone());
        }

        // Convert credentials to Identity for signing
        let identity = self.credentials.clone().into();

        // Configure SigV4 signing
        let signing_settings = SigningSettings::default();
        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name("s3tables")
            .time(SystemTime::now())
            .settings(signing_settings)
            .build()
            .expect("signing params are valid")
            .into();

        // Sign the request
        let signable_request = SignableRequest::new(
            http_req.method().as_str(),
            url.as_str(),
            std::iter::empty::<(&str, &str)>(),
            SignableBody::Bytes(&body_bytes),
        )
        .expect("signable request");

        let (signing_instructions, _signature) =
            aws_sigv4::http_request::sign(signable_request, &signing_params)
                .map_err(|e| S3TablesError::AuthError(format!("Failed to sign request: {}", e)))?
                .into_parts();

        // Apply signing instructions to headers
        signing_instructions.apply_to_request_http1x(&mut http_req);

        eprintln!("\n=== SIGNED HEADERS ===");
        for (name, value) in http_req.headers().iter() {
            eprintln!("  {}: {:?}", name, value);
        }

        // Build final reqwest::Request with signed headers
        let mut signed_req = self
            .http_client
            .request(method, url)
            .body(body_bytes.clone())
            .build()
            .map_err(|e| S3TablesError::HttpError(format!("Failed to build request: {}", e)))?;

        // Copy all headers from signed http::Request
        *signed_req.headers_mut() = http_req.headers().clone();

        // Execute request
        let response = self
            .http_client
            .execute(signed_req)
            .await
            .map_err(|e| S3TablesError::HttpError(format!("Request failed: {}", e)))?;

        eprintln!("\n=== RESPONSE ===");
        eprintln!("Status: {}", response.status());

        Ok(response)
    }

    async fn handle_response(&self, response: Response) -> Result<serde_json::Value> {
        let status = response.status();

        match status.as_u16() {
            200..=299 => response.json().await.map_err(|e| {
                S3TablesError::HttpError(format!("Failed to parse JSON response: {}", e))
            }),

            403 => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unable to read response".to_string());
                Err(S3TablesError::AuthError(format!(
                    "Authentication failed: {}",
                    body
                )))
            }

            404 => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Resource not found".to_string());
                Err(S3TablesError::NotFound(body))
            }

            409 => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Conflict".to_string());
                Err(S3TablesError::Conflict(format!(
                    "Requirements not met: {}",
                    body
                )))
            }

            400 => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Bad request".to_string());
                Err(S3TablesError::InvalidRequest(body))
            }

            _ => {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                Err(S3TablesError::Unexpected(format!(
                    "HTTP {}: {}",
                    status, body
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
