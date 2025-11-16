use anyhow::{Context, Result};
use futures::stream::StreamExt;
use hello_world_iceberg::catalog::IcebergRestCatalog;
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::NamespaceIdent;
use iceberg::{Catalog, TableCreation};
use parquet::file::properties::WriterProperties;

/// Create R2 Data Catalog with Bearer token authentication from .env
async fn create_r2_catalog_from_env() -> Result<IcebergRestCatalog> {
    // Load .env file
    dotenvy::dotenv().ok();

    let catalog_uri = std::env::var("CLOUDFLARE_CATALOG_URI")
        .context("CLOUDFLARE_CATALOG_URI not found in environment")?;
    let warehouse_name = std::env::var("CLOUDFLARE_WAREHOUSE_NAME")
        .context("CLOUDFLARE_WAREHOUSE_NAME not found in environment")?;
    let api_token = std::env::var("CLOUDFLARE_API_TOKEN")
        .context("CLOUDFLARE_API_TOKEN not found in environment")?;

    let catalog = IcebergRestCatalog::from_catalog_uri(
        "r2".to_string(),
        catalog_uri,
        warehouse_name,
        api_token,
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create R2 catalog: {}", e))?;

    Ok(catalog)
}

/// Build simple schema: { id: i64 }
fn build_schema() -> Result<Schema> {
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )
        .into()])
        .build()
        .context("Failed to build schema")?;

    Ok(schema)
}

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use iceberg::arrow::schema_to_arrow_schema;
use std::sync::Arc;

/// Create sample data: [1, 2, 3]
/// Uses Iceberg schema converted to Arrow to ensure field IDs are present
fn create_sample_data(iceberg_schema: &Schema) -> Result<RecordBatch> {
    let id_array = Int64Array::from(vec![1, 2, 3]);

    // Convert Iceberg schema to Arrow schema - this adds PARQUET:field_id metadata
    let arrow_schema = schema_to_arrow_schema(iceberg_schema)
        .context("Failed to convert Iceberg schema to Arrow schema")?;

    let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![Arc::new(id_array)])
        .context("Failed to create record batch")?;

    Ok(batch)
}

use arrow::util::pretty::print_batches;

/// Print Arrow RecordBatch in pretty table format
fn print_batch(batch: &RecordBatch) -> Result<()> {
    print_batches(std::slice::from_ref(batch)).context("Failed to print batch")?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file early for all environment variables
    dotenvy::dotenv().ok();

    let args: Vec<String> = std::env::args().collect();
    let (namespace_name, table_name) = if args.len() == 3 {
        // Command line arguments provided
        (args[1].clone(), args[2].clone())
    } else {
        // Use defaults or environment variables
        let namespace =
            std::env::var("CLOUDFLARE_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let table = std::env::var("CLOUDFLARE_TABLE").unwrap_or_else(|_| "test_table".to_string());
        println!("Usage: {} <namespace> <table-name>", args[0]);
        println!("Using defaults: namespace={}, table={}", namespace, table);
        (namespace, table)
    };

    let catalog = create_r2_catalog_from_env()
        .await
        .context("Failed to connect to R2 Data Catalog")?;

    println!("✓ Connected to R2 Data Catalog");

    let namespace = NamespaceIdent::new(namespace_name.clone());

    // Cloudflare R2 requires explicit namespace creation
    // Try to create the namespace
    println!("Creating namespace: {}", namespace_name);
    match catalog
        .create_namespace(&namespace, Default::default())
        .await
    {
        Ok(_) => println!("✓ Created namespace: {}", namespace_name),
        Err(e) if e.to_string().contains("lready exists") || e.to_string().contains("Conflict") => {
            println!("ℹ Namespace already exists: {}", namespace_name)
        }
        Err(e) => {
            eprintln!("Warning: Failed to create namespace: {}", e);
            println!("ℹ Attempting to continue with existing namespace");
        }
    }

    let schema = build_schema()?;

    // Try to load the table first, create if it doesn't exist
    let table_ident = iceberg::TableIdent::new(namespace.clone(), table_name.clone());

    println!(
        "Attempting to load table: {}.{}",
        namespace_name, table_name
    );
    let table = match catalog.load_table(&table_ident).await {
        Ok(table) => {
            println!("✓ Loaded existing table: {}.{}", namespace_name, table_name);
            table
        }
        Err(load_err) => {
            println!("Table not found, attempting to create: {}", load_err);
            let table_creation = TableCreation::builder()
                .name(table_name.clone())
                .schema(schema.clone())
                .build();

            let table = catalog
                .create_table(&namespace, table_creation)
                .await
                .context(format!(
                    "Failed to create table '{}'. Load error: {}. Create error",
                    table_name, load_err
                ))?;

            println!("✓ Created table: {}.{}", namespace_name, table_name);
            table
        }
    };

    let batch = create_sample_data(&schema)?;

    // Set up location and file name generators
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .context("Failed to create location generator")?;

    // Use UUID suffix to ensure unique file names on each run
    let unique_suffix = uuid::Uuid::new_v4().to_string();
    let file_name_generator = DefaultFileNameGenerator::new(
        "data".to_string(),
        Some(unique_suffix),
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
    let mut data_file_writer = data_file_writer_builder
        .build()
        .await
        .context("Failed to create data file writer")?;

    // Write data
    data_file_writer
        .write(batch.clone())
        .await
        .context("Failed to write data")?;

    // Close writer and retrieve data files
    let data_files = data_file_writer
        .close()
        .await
        .context("Failed to close writer")?;

    println!(
        "✓ Wrote {} rows to {} data files",
        batch.num_rows(),
        data_files.len()
    );

    // Commit data files to table via transaction
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action
        .apply(tx)
        .context("Failed to apply transaction action")?;
    let table = tx
        .commit(&catalog)
        .await
        .context("Failed to commit transaction")?;

    println!("✓ Committed snapshot to table");

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
