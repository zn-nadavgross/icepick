//! Vended credential provider for REST catalogs
use crate::io::VendedCredentialProvider;
use reqwest::Client;

/// Credential provider that fetches vended credentials from Iceberg REST catalog
#[derive(Debug)]
#[allow(dead_code)] // TODO: Implement full vended credential fetching
pub(crate) struct RestCredentialProvider {
    pub(crate) endpoint: String,
    pub(crate) prefix: String,
    pub(crate) token: String,
    pub(crate) http_client: Client,
    pub(crate) s3_endpoint: Option<String>,
}

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl VendedCredentialProvider for RestCredentialProvider {
    async fn get_credentials(
        &self,
        _path: &str,
    ) -> std::result::Result<crate::io::VendedCredentials, crate::error::Error> {
        // TODO: Parse table from path and fetch credentials from /v1/prefix/namespaces/ns/tables/t/credentials
        // For now, return error as path->table mapping is not trivial for R2 Data Catalog
        Err(crate::error::Error::IoError(
            "Table-scoped credentials not yet implemented. Use table.load_credentials() instead."
                .to_string(),
        ))
    }

    fn s3_endpoint(&self) -> Option<&str> {
        self.s3_endpoint.as_deref()
    }
}
