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
    pub catalog_url: Option<String>,
    pub status: String,
}

impl Outputable for CatalogInfo {
    fn to_text(&self) -> String {
        let mut lines = vec![
            format!("Catalog Type:  {}", self.catalog_type),
        ];

        if let Some(ref url) = self.catalog_url {
            lines.push(format!("Catalog URL:   {}", url));
        }

        lines.push(format!("Status:        {}", self.status));

        lines.join("\n")
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
                catalog_url: config.catalog_url.clone(),
                status,
            };

            print(&info, format);
            Ok(())
        }
    }
}
