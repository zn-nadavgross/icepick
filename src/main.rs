use anyhow::{Context, Result, ensure};
use iceberg::{Catalog, CatalogBuilder, TableCreation};
use iceberg::spec::{Schema, NestedField, PrimitiveType, Type, DataFileFormat};
use iceberg::NamespaceIdent;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use futures::stream::StreamExt;

/// Parse S3 Tables ARN and extract region and bucket name
/// ARN format: arn:aws:s3tables:region:account:bucket/name
fn parse_s3_tables_arn(arn: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = arn.split(':').collect();
    ensure!(parts.len() == 6, "Invalid S3 Tables ARN format: expected 6 parts");
    ensure!(parts[0] == "arn", "ARN must start with 'arn'");
    ensure!(parts[2] == "s3tables", "Not an S3 Tables ARN");

    let region = parts[3].to_string();
    let bucket_name = parts[5]
        .strip_prefix("bucket/")
        .context("ARN must contain 'bucket/' prefix")?
        .to_string();

    Ok((region, bucket_name))
}

/// Create REST catalog configured for S3 Tables
///
/// NOTE: This currently fails with 403 authentication errors because:
/// - AWS S3 Tables requires SigV4 signing on all REST API requests
/// - rust-iceberg REST catalog (v0.7.0) does not support SigV4 signing
/// - RestCatalogBuilder.with_client() accepts only reqwest::Client (no middleware support)
/// - reqwest::Client does not have request interceptor hooks
///
/// To make this work, rust-iceberg would need to:
/// 1. Accept ClientWithMiddleware from reqwest-middleware, OR
/// 2. Add built-in SigV4 support like Java's RESTSigV4Signer, OR
/// 3. Add request interceptor hooks to the REST catalog
async fn create_s3_tables_catalog(arn: &str, region: &str) -> Result<RestCatalog> {
    let rest_uri = format!("https://s3tables.{}.amazonaws.com/iceberg", region);

    let mut props = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), rest_uri);
    props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), arn.to_string());

    let catalog = RestCatalogBuilder::default()
        .load("s3tables", props)
        .await
        .context("Failed to create REST catalog")?;

    Ok(catalog)
}

/// Build simple schema: { id: i64 }
fn build_schema() -> Result<Schema> {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))
                .into()
        ])
        .build()
        .context("Failed to build schema")?;

    Ok(schema)
}

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Create sample data: [1, 2, 3]
fn create_sample_data() -> Result<RecordBatch> {
    let id_array = Int64Array::from(vec![1, 2, 3]);

    let arrow_schema = ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false)
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(id_array)]
    )
    .context("Failed to create record batch")?;

    Ok(batch)
}

use arrow::util::pretty::print_batches;

/// Print Arrow RecordBatch in pretty table format
fn print_batch(batch: &RecordBatch) -> Result<()> {
    print_batches(&[batch.clone()])
        .context("Failed to print batch")?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    ensure!(
        args.len() == 4,
        "Usage: {} <s3-tables-arn> <namespace> <table-name>",
        args[0]
    );

    let arn = &args[1];
    let namespace_name = &args[2];
    let table_name = &args[3];

    let (region, _bucket) = parse_s3_tables_arn(arn)?;

    let catalog = create_s3_tables_catalog(arn, &region)
        .await
        .context("Failed to connect to S3 Tables catalog")?;

    println!("✓ Connected to S3 Tables catalog");

    let namespace = NamespaceIdent::new(namespace_name.clone());

    // Try to create namespace (may already exist)
    match catalog.create_namespace(&namespace, HashMap::new()).await {
        Ok(_) => println!("✓ Created namespace: {}", namespace_name),
        Err(e) if e.to_string().contains("already exists") => {
            println!("✓ Namespace already exists: {}", namespace_name)
        }
        Err(e) => return Err(e).context("Failed to create namespace")?,
    }

    let schema = build_schema()?;

    let table_creation = TableCreation::builder()
        .name(table_name.clone())
        .schema(schema)
        .build();

    let table = catalog
        .create_table(&namespace, table_creation)
        .await
        .context(format!("Failed to create table '{}'", table_name))?;

    println!("✓ Created table: {}.{}", namespace_name, table_name);

    let batch = create_sample_data()?;

    // Set up location and file name generators
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .context("Failed to create location generator")?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "data".to_string(),
        None,
        DataFileFormat::Parquet,
    );

    // Create Parquet writer builder
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        None,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );

    // Create data file writer
    let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);
    let mut data_file_writer = data_file_writer_builder.build().await
        .context("Failed to create data file writer")?;

    // Write data
    data_file_writer.write(batch.clone()).await
        .context("Failed to write data")?;

    // Close writer and retrieve data files
    let data_files = data_file_writer.close().await
        .context("Failed to close writer")?;

    println!("✓ Wrote {} rows to {} data files", batch.num_rows(), data_files.len());

    let scan = table
        .scan()
        .build()
        .context("Failed to create table scan")?;

    let mut stream = scan
        .to_arrow()
        .await
        .context("Failed to create arrow stream")?;

    let mut read_batches = Vec::new();
    while let Some(batch_result) = stream.next().await {
        let read_batch = batch_result.context("Failed to read batch")?;
        read_batches.push(read_batch);
    }

    println!("✓ Read {} batches", read_batches.len());

    println!("\nWritten data:");
    print_batch(&batch)?;

    println!("\nRead data:");
    for read_batch in &read_batches {
        print_batch(read_batch)?;
    }

    Ok(())
}
