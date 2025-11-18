use anyhow::{ensure, Context, Result};
use icepick::catalog::Catalog;
use icepick::spec::{NamespaceIdent, NestedField, PrimitiveType, Schema, TableIdent, Type};
use icepick::{
    AppendOnlyTableWriter, PartitionFieldConfig, PartitionTransform, S3TablesCatalog,
    TableWriterOptions,
};
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Create S3 Tables catalog with SigV4 authentication
async fn create_s3_tables_catalog(arn: &str) -> Result<S3TablesCatalog> {
    let catalog = S3TablesCatalog::from_arn("s3tables", arn)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create S3 Tables catalog: {}", e))?;

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
    let args: Vec<String> = std::env::args().collect();
    init_tracing();
    ensure!(
        args.len() == 4,
        "Usage: {} <s3-tables-arn> <namespace> <table-name>",
        args[0]
    );

    let arn = &args[1];
    let namespace_name = &args[2];
    let table_name = &args[3];

    let catalog = create_s3_tables_catalog(arn)
        .await
        .context("Failed to connect to S3 Tables catalog")?;

    info!("✓ Connected to S3 Tables catalog");

    let namespace = NamespaceIdent::new(vec![namespace_name.clone()]);
    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());

    let schema = build_schema()?;
    let batch = create_sample_data(&schema)?;

    // Configure optional partitioning (identity on id for this example)
    let writer_options = TableWriterOptions::new().with_partition_field(PartitionFieldConfig::new(
        "id",
        PartitionTransform::Identity,
    ));

    let writer = AppendOnlyTableWriter::new(&catalog, namespace.clone(), table_name.clone())
        .with_options(writer_options);
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
    let table = catalog
        .load_table(&table_ident)
        .await
        .context("Failed to load table for reading")?;

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
