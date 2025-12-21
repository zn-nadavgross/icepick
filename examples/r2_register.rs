use anyhow::{Context, Result};
use icepick::catalog::{BackoffStrategy, Catalog, CatalogOptions, HttpClientConfig, RetryConfig};
use icepick::spec::{NamespaceIdent, TableIdent};
use icepick::{introspect_parquet_file, DataFileRegistrar, R2Catalog, RegisterOptions};
use std::time::Duration;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Create R2 Data Catalog with Bearer token authentication
async fn create_r2_catalog_from_env() -> Result<R2Catalog> {
    dotenvy::dotenv().ok();

    let account_id = std::env::var("CLOUDFLARE_ACCOUNT_ID")
        .context("CLOUDFLARE_ACCOUNT_ID not found in environment")?;
    let bucket_name = std::env::var("CLOUDFLARE_BUCKET_NAME")
        .context("CLOUDFLARE_BUCKET_NAME not found in environment")?;
    let api_token = std::env::var("CLOUDFLARE_API_TOKEN")
        .context("CLOUDFLARE_API_TOKEN not found in environment")?;

    let http_config = HttpClientConfig::new()
        .with_timeout(Duration::from_secs(60))
        .with_connect_timeout(Duration::from_secs(10));

    let retry_config = RetryConfig::new(
        3,
        BackoffStrategy::Exponential {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
        },
    )
    .with_max_elapsed_time(Duration::from_secs(120));

    let options = CatalogOptions::new()
        .with_http_config(http_config)
        .with_retry_config(retry_config);

    let catalog = R2Catalog::with_options("r2", account_id, bucket_name, api_token, options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create R2 catalog: {}", e))?;

    Ok(catalog)
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    init_tracing();

    let args: Vec<String> = std::env::args().collect();

    // Usage: r2_register <namespace> <table> <file_path1> [file_path2 ...]
    if args.len() < 4 {
        eprintln!(
            "Usage: {} <namespace> <table> <s3://bucket/path/to/file.parquet> [more files...]",
            args[0]
        );
        eprintln!();
        eprintln!(
            "Register pre-existing Parquet files into an Iceberg table without rewriting data."
        );
        eprintln!();
        eprintln!("Environment variables:");
        eprintln!("  CLOUDFLARE_ACCOUNT_ID  - Cloudflare account ID");
        eprintln!("  CLOUDFLARE_BUCKET_NAME - R2 bucket name");
        eprintln!("  CLOUDFLARE_API_TOKEN   - Cloudflare API token");
        std::process::exit(1);
    }

    let namespace_name = &args[1];
    let table_name = &args[2];
    let file_paths: Vec<&str> = args[3..].iter().map(|s| s.as_str()).collect();

    info!(
        "Registering {} file(s) to {}.{}",
        file_paths.len(),
        namespace_name,
        table_name
    );

    let catalog = create_r2_catalog_from_env()
        .await
        .context("Failed to connect to R2 Data Catalog")?;

    info!("Connected to R2 Data Catalog");

    let namespace = NamespaceIdent::new(vec![namespace_name.clone()]);
    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());

    // Get FileIO from catalog for introspection
    let file_io = catalog.file_io();

    // Introspect each file to get metadata (schema, row count, size, partition values)
    let mut data_file_inputs = Vec::new();
    let mut first_schema = None;

    for path in &file_paths {
        info!("Introspecting: {}", path);

        // Load the table to get partition spec (if table exists)
        let partition_spec = match catalog.load_table(&table_ident).await {
            Ok(table) => table.metadata().partition_specs().first().cloned(),
            Err(_) => None,
        };

        let introspection = introspect_parquet_file(file_io, path, partition_spec.as_ref())
            .await
            .with_context(|| format!("Failed to introspect {}", path))?;

        info!(
            "  Schema: {} fields, {} rows, {} bytes",
            introspection.schema.fields().len(),
            introspection.data_file.record_count,
            introspection.data_file.file_size_in_bytes
        );

        if !introspection.data_file.partition_values.is_empty() {
            info!(
                "  Partitions: {:?}",
                introspection.data_file.partition_values
            );
        }

        if first_schema.is_none() {
            first_schema = Some(introspection.schema.clone());
        }

        data_file_inputs.push(introspection.data_file);
    }

    // Build registration options
    // If table doesn't exist, allow creation with schema from first file
    let options = if let Some(schema) = first_schema {
        RegisterOptions::new()
            .allow_create_with_schema(schema)
            .allow_noop(true) // Don't error if files already registered
    } else {
        RegisterOptions::new().allow_noop(true)
    };

    // Register the files
    info!("Registering files...");
    let result = catalog
        .register_data_files(namespace, table_ident, data_file_inputs, options)
        .await
        .context("Failed to register data files")?;

    // Report results
    info!("Registration complete:");
    info!("  Snapshot ID: {}", result.snapshot_id);
    info!("  Added files: {}", result.added_files);
    info!("  Added records: {}", result.added_records);
    if result.table_was_created {
        info!("  Table was created");
    }
    if !result.skipped_files.is_empty() {
        info!(
            "  Skipped {} file(s) (already committed)",
            result.skipped_files.len()
        );
        for skipped in &result.skipped_files {
            info!("    - {}", skipped.file_path);
        }
    }

    Ok(())
}
