use anyhow::{Context, Result};
use icepick::catalog::{BackoffStrategy, Catalog, CatalogOptions, HttpClientConfig, RetryConfig};
use icepick::spec::{NamespaceIdent, NestedField, PrimitiveType, Schema, TableIdent, Type};
use icepick::{AppendOnlyTableWriter, R2Catalog, TableWriterOptions};
use std::time::Duration;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Create R2 Data Catalog with Bearer token authentication, timeout, and retry configuration
async fn create_r2_catalog_from_env() -> Result<R2Catalog> {
    // Load .env file
    dotenvy::dotenv().ok();

    let account_id = std::env::var("CLOUDFLARE_ACCOUNT_ID")
        .context("CLOUDFLARE_ACCOUNT_ID not found in environment")?;
    let bucket_name = std::env::var("CLOUDFLARE_BUCKET_NAME")
        .context("CLOUDFLARE_BUCKET_NAME not found in environment")?;
    let api_token = std::env::var("CLOUDFLARE_API_TOKEN")
        .context("CLOUDFLARE_API_TOKEN not found in environment")?;

    // Configure HTTP client with timeouts
    let http_config = HttpClientConfig::new()
        .with_timeout(Duration::from_secs(60))
        .with_connect_timeout(Duration::from_secs(10));

    // Configure retry behavior with exponential backoff
    let retry_config = RetryConfig::new(
        3,
        BackoffStrategy::Exponential {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
        },
    )
    .with_max_elapsed_time(Duration::from_secs(120));

    // Create catalog with options
    let options = CatalogOptions::new()
        .with_http_config(http_config)
        .with_retry_config(retry_config);

    let catalog = R2Catalog::with_options("r2", account_id, bucket_name, api_token, options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create R2 catalog: {}", e))?;

    Ok(catalog)
}

/// Build simple schema: { id: i64 }
fn build_schema() -> Result<Schema> {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required_field(
            1,
            "id".to_string(),
            Type::Primitive(PrimitiveType::Long),
        )])
        .build()
        .context("Failed to build schema")?;

    Ok(schema)
}

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use icepick::arrow_convert::schema_to_arrow;
use std::sync::Arc;

/// Create sample data: [1, 2, 3]
/// Uses Iceberg schema converted to Arrow to ensure field IDs are present
fn create_sample_data(iceberg_schema: &Schema) -> Result<RecordBatch> {
    let id_array = Int64Array::from(vec![1, 2, 3]);

    // Convert Iceberg schema to Arrow schema - this adds PARQUET:field_id metadata
    let arrow_schema = schema_to_arrow(iceberg_schema)
        .context("Failed to convert Iceberg schema to Arrow schema")?;

    let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![Arc::new(id_array)])
        .context("Failed to create record batch")?;

    Ok(batch)
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
    // Load .env file early for all environment variables
    dotenvy::dotenv().ok();
    init_tracing();

    let args: Vec<String> = std::env::args().collect();
    let (namespace_name, table_name) = if args.len() == 3 {
        // Command line arguments provided
        (args[1].clone(), args[2].clone())
    } else {
        // Use defaults or environment variables
        let namespace =
            std::env::var("CLOUDFLARE_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let table = std::env::var("CLOUDFLARE_TABLE").unwrap_or_else(|_| "test_table".to_string());
        info!("Usage: {} <namespace> <table-name>", args[0]);
        info!("Using defaults: namespace={}, table={}", namespace, table);
        (namespace, table)
    };

    let catalog = create_r2_catalog_from_env()
        .await
        .context("Failed to connect to R2 Data Catalog")?;

    info!("✓ Connected to R2 Data Catalog");

    let namespace = NamespaceIdent::new(vec![namespace_name.clone()]);
    let schema = build_schema()?;

    let batch = create_sample_data(&schema)?;

    // Set timestamp explicitly for WASM compatibility
    // Note: On WASM, you would get the timestamp from JavaScript via wasm_bindgen
    #[cfg(not(target_family = "wasm"))]
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    #[cfg(target_family = "wasm")]
    let timestamp_ms = {
        // On WASM, get timestamp from JavaScript: js_sys::Date::now() as i64
        panic!("WASM timestamp must be provided from JavaScript")
    };

    let writer = AppendOnlyTableWriter::new(&catalog, namespace.clone(), table_name.clone())
        .with_options(TableWriterOptions::new().with_timestamp_ms(timestamp_ms));
    writer
        .append_batch(batch.clone())
        .await
        .context("Failed to append batch")?;

    info!(
        "✓ Wrote {} rows via AppendOnlyTableWriter",
        batch.num_rows()
    );

    // Read data back
    info!("--- Reading data back ---");

    // Reload table to get latest metadata
    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());
    let table = catalog.load_table(&table_ident).await?;

    // List data files
    let files = table.files().await?;
    info!("✓ Found {} data file(s)", files.len());
    for file in &files {
        info!(
            "Data file {} ({} records, {} bytes)",
            file.file_path, file.record_count, file.file_size_in_bytes
        );
    }

    // Scan and read data
    let scan = table.scan().build()?;
    let mut stream = scan.to_arrow().await?;

    use futures::StreamExt;

    let mut total_rows = 0;
    info!("Reading batches:");
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        total_rows += batch.num_rows();
        info!("Batch: {} rows", batch.num_rows());

        // Print the first batch as a sample
        if total_rows == batch.num_rows() {
            use arrow::util::pretty::print_batches;
            info!("Sample data:");
            print_batches(&[batch])?;
        }
    }

    info!("✓ Read {} total rows", total_rows);

    Ok(())
}
