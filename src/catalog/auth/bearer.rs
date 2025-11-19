use crate::catalog::{AuthProvider, Result};
use async_trait::async_trait;

/// Bearer token authentication for R2 Data Catalog
#[derive(Debug, Clone)]
pub struct BearerTokenAuthProvider {
    token: String,
}

impl BearerTokenAuthProvider {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
impl AuthProvider for BearerTokenAuthProvider {
    async fn sign_request(&self, mut request: reqwest::Request) -> Result<reqwest::Request> {
        // Add Authorization header with Bearer token
        request.headers_mut().insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", self.token).parse().map_err(|e| {
                crate::catalog::CatalogError::AuthError(format!(
                    "Failed to create auth header: {}",
                    e
                ))
            })?,
        );
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bearer_token_adds_auth_header() {
        let provider = BearerTokenAuthProvider::new("test-token-123");

        let req = reqwest::Client::new()
            .get("https://example.com")
            .build()
            .unwrap();

        let signed_req = provider.sign_request(req).await.unwrap();

        let auth_header = signed_req
            .headers()
            .get(reqwest::header::AUTHORIZATION)
            .expect("Authorization header should be present");

        assert_eq!(auth_header, "Bearer test-token-123");
    }

    #[tokio::test]
    async fn test_bearer_token_with_different_tokens() {
        let test_cases = vec![
            "simple-token",
            "token-with-dashes",
            "CamelCaseToken123",
            "token_with_underscores",
            "VeryLongTokenStringThatMightBeUsedInProduction123456789",
        ];

        for token in test_cases {
            let provider = BearerTokenAuthProvider::new(token);

            let req = reqwest::Client::new()
                .get("https://example.com")
                .build()
                .unwrap();

            let signed_req = provider.sign_request(req).await.unwrap();

            let auth_header = signed_req
                .headers()
                .get(reqwest::header::AUTHORIZATION)
                .expect("Authorization header should be present")
                .to_str()
                .unwrap();

            assert_eq!(auth_header, format!("Bearer {}", token));
        }
    }

    #[tokio::test]
    async fn test_bearer_token_preserves_existing_headers() {
        let provider = BearerTokenAuthProvider::new("my-token");

        let req = reqwest::Client::new()
            .post("https://example.com")
            .header("Content-Type", "application/json")
            .header("X-Custom-Header", "custom-value")
            .build()
            .unwrap();

        let signed_req = provider.sign_request(req).await.unwrap();

        // Check original headers are preserved
        assert_eq!(
            signed_req.headers().get("Content-Type").unwrap(),
            "application/json"
        );
        assert_eq!(
            signed_req.headers().get("X-Custom-Header").unwrap(),
            "custom-value"
        );

        // Check auth header is added
        assert_eq!(
            signed_req
                .headers()
                .get(reqwest::header::AUTHORIZATION)
                .unwrap(),
            "Bearer my-token"
        );
    }

    #[tokio::test]
    async fn test_bearer_token_with_request_body() {
        let provider = BearerTokenAuthProvider::new("token-123");

        let req = reqwest::Client::new()
            .post("https://example.com")
            .body("request body content")
            .build()
            .unwrap();

        let signed_req = provider.sign_request(req).await.unwrap();

        // Auth header should be present
        let auth_header = signed_req
            .headers()
            .get(reqwest::header::AUTHORIZATION)
            .unwrap();
        assert_eq!(auth_header, "Bearer token-123");

        // Body should be preserved
        let body = signed_req.body().unwrap().as_bytes().unwrap();
        assert_eq!(body, b"request body content");
    }

    #[test]
    fn test_bearer_token_provider_debug() {
        let provider = BearerTokenAuthProvider::new("secret-token");
        let debug_str = format!("{:?}", provider);
        assert!(debug_str.contains("BearerTokenAuthProvider"));
    }

    #[test]
    fn test_bearer_token_provider_clone() {
        let provider1 = BearerTokenAuthProvider::new("token-abc");
        let provider2 = provider1.clone();

        assert_eq!(provider1.token, provider2.token);
    }
}
