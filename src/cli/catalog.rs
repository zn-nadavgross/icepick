//! Catalog connection utilities

use crate::catalog::Catalog;
use crate::{R2Catalog, S3TablesCatalog};
use std::sync::Arc;

/// Configuration for connecting to a catalog
#[derive(Debug, Clone)]
pub struct CatalogConfig {
    /// S3 Tables ARN
    pub arn: Option<String>,
    /// R2 Account ID
    pub r2_account: Option<String>,
    /// R2 Bucket
    pub r2_bucket: Option<String>,
    /// API Token (R2/REST)
    pub token: Option<String>,
    /// REST catalog endpoint (reserved for future use)
    pub endpoint: Option<String>,
}

impl CatalogConfig {
    /// Create a catalog from the configuration
    ///
    /// Priority order:
    /// 1. `--arn` -> S3TablesCatalog
    /// 2. `--r2-account` + `--r2-bucket` + `--token` -> R2Catalog
    pub async fn create_catalog(&self) -> Result<Arc<dyn Catalog>, String> {
        // Priority 1: S3 Tables ARN
        if let Some(ref arn) = self.arn {
            let catalog = S3TablesCatalog::from_arn("icepick", arn)
                .await
                .map_err(|e| format!("Failed to create S3 Tables catalog: {}", e))?;
            return Ok(Arc::new(catalog));
        }

        // Priority 2: R2 Catalog
        if let (Some(ref account), Some(ref bucket)) = (&self.r2_account, &self.r2_bucket) {
            let token = self.token.as_ref()
                .ok_or_else(|| "R2 catalog requires --token or ICEPICK_TOKEN".to_string())?;

            let catalog = R2Catalog::new("icepick", account, bucket, token)
                .await
                .map_err(|e| format!("Failed to create R2 catalog: {}", e))?;
            return Ok(Arc::new(catalog));
        }

        // REST catalog support is reserved for future implementation
        if self.endpoint.is_some() {
            return Err("REST catalog endpoint support is not yet implemented. Use --arn for S3 Tables or --r2-account/--r2-bucket for R2.".to_string());
        }

        Err("No catalog configuration specified. Use --arn for S3 Tables or --r2-account/--r2-bucket for Cloudflare R2.".to_string())
    }

    /// Get a description of the catalog type
    pub fn catalog_type(&self) -> &'static str {
        if self.arn.is_some() {
            "S3 Tables"
        } else if self.r2_account.is_some() && self.r2_bucket.is_some() {
            "Cloudflare R2"
        } else if self.endpoint.is_some() {
            "REST (not yet supported)"
        } else {
            "Unknown"
        }
    }
}
