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

#[async_trait]
impl AuthProvider for BearerTokenAuthProvider {
    async fn sign_request(&self, mut request: reqwest::Request) -> Result<reqwest::Request> {
        // Add Authorization header with Bearer token
        request
            .headers_mut()
            .insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.token)
                    .parse()
                    .map_err(|e| {
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
}
