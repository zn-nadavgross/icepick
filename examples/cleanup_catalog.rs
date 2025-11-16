use anyhow::{Context, Result};
use icepick::catalog::Catalog;
use icepick::S3TablesCatalog;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <s3-tables-arn>", args[0]);
        std::process::exit(1);
    }

    let arn = &args[1];

    info!("🔌 Connecting to S3 Tables catalog: {}", arn);
    let catalog = S3TablesCatalog::from_arn("cleanup", arn)
        .await
        .context("Failed to create S3 Tables catalog")?;

    info!("✓ Connected to catalog");

    // Note: S3 Tables doesn't support listing all namespaces via the REST API
    // We need to try common namespace names or have them provided
    warn!("⚠️  S3 Tables REST API doesn't support listing namespaces");
    warn!("⚠️  Attempting cleanup of common namespace names...");

    // Try common namespace names
    let common_namespaces = vec![
        "default", "test", "dev", "prod", "staging", "sandbox", "demo", "example",
    ];

    let mut total_tables_deleted = 0;
    let mut total_namespaces_deleted = 0;

    for ns_name in &common_namespaces {
        let namespace = icepick::spec::NamespaceIdent::new(vec![ns_name.to_string()]);

        // Check if namespace exists
        match catalog.namespace_exists(&namespace).await {
            Ok(true) => {
                info!("📁 Found namespace: {}", ns_name);

                // List tables in this namespace
                match catalog.list_tables(&namespace).await {
                    Ok(tables) => {
                        info!("  Found {} table(s) in {}", tables.len(), ns_name);

                        let table_count = tables.len();

                        // Delete each table
                        for table_id in &tables {
                            info!("  🗑️  Deleting table: {}", table_id);
                            match catalog.drop_table(table_id).await {
                                Ok(_) => {
                                    info!("  ✓ Deleted table: {}", table_id);
                                    total_tables_deleted += 1;
                                }
                                Err(e) => {
                                    warn!("  ✗ Failed to delete table {}: {}", table_id, e);
                                }
                            }
                        }

                        // Note: S3 Tables doesn't support deleting namespaces via REST API
                        // We can only delete tables
                        warn!(
                            "  ℹ️  Namespace '{}' cannot be deleted via REST API (S3 Tables limitation)",
                            ns_name
                        );
                        // Count it anyway since we cleaned its tables
                        if table_count > 0 {
                            total_namespaces_deleted += 1;
                        }
                    }
                    Err(e) => {
                        warn!("  ✗ Failed to list tables in {}: {}", ns_name, e);
                    }
                }
            }
            Ok(false) => {
                // Namespace doesn't exist, skip silently
            }
            Err(e) => {
                warn!("✗ Error checking namespace {}: {}", ns_name, e);
            }
        }
    }

    info!("");
    info!("🎯 Cleanup Summary:");
    info!("   Tables deleted: {}", total_tables_deleted);
    info!("   Namespaces processed: {}", total_namespaces_deleted);

    if total_tables_deleted == 0 {
        warn!("⚠️  No tables found. The catalog may be empty or use non-standard namespace names.");
    }

    Ok(())
}
