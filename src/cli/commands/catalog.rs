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
}

impl Outputable for CatalogInfo {
    fn to_text(&self) -> String {
        let mut lines = vec![format!("Catalog Type:  {}", self.catalog_type)];

        if let Some(ref url) = self.catalog_url {
            lines.push(format!("Catalog URL:   {}", url));
        }

        lines.push("Status:        Connected".to_string());

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
            config
                .create_catalog()
                .await
                .map_err(|e| format!("Failed to connect to catalog: {}", e))?;

            let info = CatalogInfo {
                catalog_type: config.catalog_type().to_string(),
                catalog_url: config.catalog_url.clone(),
            };

            print(&info, format);
            Ok(())
        }
    }
}
