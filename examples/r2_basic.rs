use anyhow::{Context, Result};
use icepick::catalog::Catalog;
use icepick::spec::{
    NamespaceIdent, NestedField, PrimitiveType, Schema, TableCreation, TableIdent, Type,
};
use icepick::writer::ParquetWriter;
use icepick::R2Catalog;

/// Create R2 Data Catalog with Bearer token authentication from .env
async fn create_r2_catalog_from_env() -> Result<R2Catalog> {
    // Load .env file
    dotenvy::dotenv().ok();

    let account_id = std::env::var("CLOUDFLARE_ACCOUNT_ID")
        .context("CLOUDFLARE_ACCOUNT_ID not found in environment")?;
    let bucket_name = std::env::var("CLOUDFLARE_BUCKET_NAME")
        .context("CLOUDFLARE_BUCKET_NAME not found in environment")?;
    let api_token = std::env::var("CLOUDFLARE_API_TOKEN")
        .context("CLOUDFLARE_API_TOKEN not found in environment")?;

    let catalog = R2Catalog::new("r2", account_id, bucket_name, api_token)
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

    let namespace = NamespaceIdent::new(vec![namespace_name.clone()]);

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
    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());

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
                .with_name(table_name.clone())
                .with_schema(schema.clone())
                .build()
                .context("Failed to build table creation")?;

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

    // Create simple icepick ParquetWriter
    let mut writer = ParquetWriter::new(table.metadata().current_schema().clone())
        .context("Failed to create Parquet writer")?;

    writer
        .write_batch(&batch)
        .context("Failed to write batch")?;

    let file_path = format!(
        "{}/data/file-{}.parquet",
        table.location(),
        uuid::Uuid::new_v4()
    );

    let data_file = writer
        .finish(table.file_io(), file_path.clone())
        .await
        .context("Failed to finish writing Parquet file")?;

    println!("✓ Wrote {} rows to {}", batch.num_rows(), file_path);

    // Commit using icepick transaction
    table
        .transaction()
        .append(vec![data_file])
        .commit(&catalog)
        .await
        .context("Failed to commit transaction")?;

    println!("✓ Committed snapshot to table");
    println!("\nNote: Reading data back is not yet supported in icepick");

    Ok(())
}
