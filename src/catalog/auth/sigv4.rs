use crate::catalog::{AuthProvider, CatalogError, Result};
use async_trait::async_trait;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;
use http::Request as HttpRequest;
use std::time::SystemTime;

/// AWS SigV4 authentication provider for S3 Tables
#[derive(Debug)]
pub struct SigV4AuthProvider {
    region: String,
    service: String,
    credentials: aws_credential_types::Credentials,
}

impl SigV4AuthProvider {
    pub fn new(
        region: String,
        service: String,
        credentials: aws_credential_types::Credentials,
    ) -> Self {
        Self {
            region,
            service,
            credentials,
        }
    }
}

#[async_trait]
impl AuthProvider for SigV4AuthProvider {
    async fn sign_request(&self, req: reqwest::Request) -> Result<reqwest::Request> {
        let url = req.url().clone();
        let method = req.method().clone();
        let headers = req.headers().clone();
        let body_bytes = req
            .body()
            .and_then(|b| b.as_bytes())
            .map(|b| b.to_vec())
            .unwrap_or_default();

        // Build http::Request for signing
        let mut http_req = HttpRequest::builder()
            .method(method.as_str())
            .uri(url.as_str())
            .body(&body_bytes[..])
            .map_err(|e| {
                CatalogError::Unexpected(format!("Failed to build HTTP request: {}", e))
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
            .name(&self.service)
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
                .map_err(|e| CatalogError::AuthError(format!("Failed to sign request: {}", e)))?
                .into_parts();

        // Apply signing instructions to headers
        signing_instructions.apply_to_request_http1x(&mut http_req);

        // Build final reqwest::Request with signed headers
        let http_client = reqwest::Client::new();
        let mut signed_req = http_client
            .request(method, url)
            .body(body_bytes.clone())
            .build()
            .map_err(|e| CatalogError::HttpError(format!("Failed to build request: {}", e)))?;

        // Copy all headers from signed http::Request
        *signed_req.headers_mut() = http_req.headers().clone();

        Ok(signed_req)
    }
}
