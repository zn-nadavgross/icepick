use anyhow::{Context, Result, ensure};
use iceberg::{Catalog, CatalogBuilder};
use iceberg::spec::{Schema, NestedField, PrimitiveType, Type};
use iceberg::NamespaceIdent;
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
use std::collections::HashMap;

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
async fn create_s3_tables_catalog(arn: &str, region: &str) -> Result<RestCatalog> {
    let rest_uri = format!("https://s3tables.{}.amazonaws.com/iceberg", region);

    let mut props = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), rest_uri);
    props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), arn.to_string());
    props.insert("rest.sigv4-enabled".to_string(), "true".to_string());
    props.insert("rest.signing-name".to_string(), "s3tables".to_string());
    props.insert("rest.signing-region".to_string(), region.to_string());

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
    let _table_name = &args[3];

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
    println!("✓ Created schema with {} fields", schema.as_struct().fields().len());

    Ok(())
}
