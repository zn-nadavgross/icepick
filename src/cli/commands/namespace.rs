//! Namespace commands

use crate::cli::catalog::CatalogConfig;
use crate::cli::output::{print, OutputFormat, Outputable};
use crate::spec::NamespaceIdent;
use clap::Subcommand;
use serde::Serialize;
use std::collections::HashMap;

/// Namespace commands
#[derive(Debug, Subcommand)]
pub enum NamespaceCommand {
    /// List all namespaces (not supported by all catalogs)
    List,

    /// Create a namespace
    Create {
        /// Namespace name
        name: String,
    },
}

/// Namespace list output
#[derive(Debug, Serialize)]
pub struct NamespaceList {
    pub namespaces: Vec<String>,
}

impl Outputable for NamespaceList {
    fn to_text(&self) -> String {
        if self.namespaces.is_empty() {
            return "No namespaces found.".to_string();
        }

        let mut lines = vec!["Namespaces:".to_string()];
        for ns in &self.namespaces {
            lines.push(format!("  {}", ns));
        }
        lines.join("\n")
    }
}

/// Namespace create result
#[derive(Debug, Serialize)]
pub struct NamespaceCreateResult {
    pub namespace: String,
    pub created: bool,
}

impl Outputable for NamespaceCreateResult {
    fn to_text(&self) -> String {
        if self.created {
            format!("Namespace '{}' created successfully.", self.namespace)
        } else {
            format!("Namespace '{}' already exists.", self.namespace)
        }
    }
}

/// Execute a namespace command
pub async fn execute(
    command: NamespaceCommand,
    config: &CatalogConfig,
    format: OutputFormat,
) -> Result<(), String> {
    let catalog = config.create_catalog().await?;

    match command {
        NamespaceCommand::List => {
            // Note: Most Iceberg catalogs don't have a list_namespaces method
            // This is a placeholder that will need to be implemented per catalog
            let result = NamespaceList {
                namespaces: vec!["(namespace listing not supported - use table list)".to_string()],
            };
            print(&result, format);
            Ok(())
        }

        NamespaceCommand::Create { name } => {
            let namespace = NamespaceIdent::new(vec![name.clone()]);

            // Check if namespace already exists
            let exists = catalog
                .namespace_exists(&namespace)
                .await
                .map_err(|e| format!("Failed to check namespace: {}", e))?;

            if !exists {
                catalog
                    .create_namespace(&namespace, HashMap::new())
                    .await
                    .map_err(|e| format!("Failed to create namespace: {}", e))?;
            }

            let result = NamespaceCreateResult {
                namespace: name,
                created: !exists,
            };
            print(&result, format);
            Ok(())
        }
    }
}
