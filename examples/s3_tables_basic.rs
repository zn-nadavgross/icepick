use anyhow::{ensure, Context, Result};
use icepick::catalog::Catalog;
use icepick::spec::{
    NamespaceIdent, NestedField, PrimitiveType, Schema, TableCreation, TableIdent, Type,
};
use icepick::writer::ParquetWriter;
use icepick::S3TablesCatalog;

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

    let catalog = create_s3_tables_catalog(arn)
        .await
        .context("Failed to connect to S3 Tables catalog")?;

    println!("✓ Connected to S3 Tables catalog");

    let namespace = NamespaceIdent::new(vec![namespace_name.clone()]);

    // Note: S3 Tables may not support namespace creation via REST API
    // Namespaces might need to be created in AWS console
    println!("ℹ Using namespace: {} (assuming it exists)", namespace_name);

    let schema = build_schema()?;

    // Try to load the table first, create if it doesn't exist
    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());
    let table = match catalog.load_table(&table_ident).await {
        Ok(table) => {
            println!("✓ Loaded existing table: {}.{}", namespace_name, table_name);
            table
        }
        Err(_) => {
            let table_creation = TableCreation::builder()
                .with_name(table_name.clone())
                .with_schema(schema.clone())
                .build()
                .context("Failed to build table creation")?;

            let table = catalog
                .create_table(&namespace, table_creation)
                .await
                .context(format!("Failed to create table '{}'", table_name))?;

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
        .commit()
        .await
        .context("Failed to commit transaction")?;

    println!("✓ Committed snapshot to table");
    println!("\nNote: Reading data back is not yet supported in icepick");

    Ok(())
}
