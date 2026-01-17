//! Catalog commands

use crate::cli::catalog::CatalogConfig;
use crate::cli::output::{print, OutputFormat, Outputable};
use clap::Subcommand;
use serde::Serialize;

/// Catalog commands
#[derive(Debug, Subcommand)]
pub enum CatalogCommand {
    /// Show catalog information
    Info,
}

/// Catalog info output
#[derive(Debug, Serialize)]
pub struct CatalogInfo {
    pub catalog_type: String,
    pub arn: Option<String>,
    pub r2_account: Option<String>,
    pub r2_bucket: Option<String>,
    pub endpoint: Option<String>,
    pub status: String,
}

impl Outputable for CatalogInfo {
    fn to_text(&self) -> String {
        let mut lines = vec![
            format!("Catalog Type:  {}", self.catalog_type),
        ];

        if let Some(ref arn) = self.arn {
            lines.push(format!("ARN:           {}", arn));
            // Extract region from ARN
            if let Some(region) = extract_region_from_arn(arn) {
                lines.push(format!("Region:        {}", region));
            }
        }

        if let Some(ref account) = self.r2_account {
            lines.push(format!("Account ID:    {}", account));
        }

        if let Some(ref bucket) = self.r2_bucket {
            lines.push(format!("Bucket:        {}", bucket));
        }

        if let Some(ref endpoint) = self.endpoint {
            lines.push(format!("Endpoint:      {}", endpoint));
        }

        lines.push(format!("Status:        {}", self.status));

        lines.join("\n")
    }
}

fn extract_region_from_arn(arn: &str) -> Option<String> {
    // arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() >= 4 {
        Some(parts[3].to_string())
    } else {
        None
    }
}

/// Execute a catalog command
pub async fn execute(
    command: CatalogCommand,
    config: &CatalogConfig,
    format: OutputFormat,
) -> Result<(), String> {
    match command {
        CatalogCommand::Info => {
            // Try to connect to verify the catalog works
            let status = match config.create_catalog().await {
                Ok(_) => "Connected".to_string(),
                Err(e) => format!("Error: {}", e),
            };

            let info = CatalogInfo {
                catalog_type: config.catalog_type().to_string(),
                arn: config.arn.clone(),
                r2_account: config.r2_account.clone(),
                r2_bucket: config.r2_bucket.clone(),
                endpoint: config.endpoint.clone(),
                status,
            };

            print(&info, format);
            Ok(())
        }
    }
}
