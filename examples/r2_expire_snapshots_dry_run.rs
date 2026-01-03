#![cfg_attr(not(feature = "maintenance"), allow(dead_code, unused_imports))]

#[cfg(feature = "maintenance")]
use anyhow::{Context, Result};
#[cfg(feature = "maintenance")]
use icepick::catalog::{Catalog, CatalogOptions, HttpClientConfig};
#[cfg(feature = "maintenance")]
use icepick::maintenance::ExpireSnapshotsOptions;
#[cfg(feature = "maintenance")]
use icepick::spec::TableIdent;
#[cfg(feature = "maintenance")]
use icepick::R2Catalog;
#[cfg(feature = "maintenance")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(feature = "maintenance")]
use tracing_subscriber::EnvFilter;

#[cfg(feature = "maintenance")]
async fn create_r2_catalog_from_env() -> Result<R2Catalog> {
    dotenvy::dotenv().ok();

    let account_id = std::env::var("CLOUDFLARE_ACCOUNT_ID")
        .context("CLOUDFLARE_ACCOUNT_ID not found in environment")?;
    let bucket_name = std::env::var("CLOUDFLARE_BUCKET_NAME")
        .context("CLOUDFLARE_BUCKET_NAME not found in environment")?;
    let api_token = std::env::var("CLOUDFLARE_API_TOKEN")
        .context("CLOUDFLARE_API_TOKEN not found in environment")?;

    let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").ok();
    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();

    let http_config = HttpClientConfig::new()
        .with_timeout(Duration::from_secs(30))
        .with_connect_timeout(Duration::from_secs(10));
    let options = CatalogOptions::new().with_http_config(http_config);

    let catalog = if let (Some(access_key_id), Some(secret_access_key)) =
        (access_key_id, secret_access_key)
    {
        println!("Using R2 credentials for FileIO access.");
        R2Catalog::with_credentials(
            "r2",
            account_id,
            bucket_name,
            api_token,
            access_key_id,
            secret_access_key,
            options,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create R2 catalog: {}", e))?
    } else {
        println!("Using catalog token only (no FileIO credentials).");
        R2Catalog::with_options("r2", account_id, bucket_name, api_token, options)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create R2 catalog: {}", e))?
    };

    Ok(catalog)
}

#[cfg(feature = "maintenance")]
#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "icepick::http=debug,icepick::catalog::rest=debug,icepick::maintenance=debug,opendal=info",
        )
    });
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    println!("Connecting to R2 catalog...");
    let catalog = create_r2_catalog_from_env()
        .await
        .context("Failed to connect to R2 Data Catalog")?;
    println!("Catalog connected, loading table...");

    let args: Vec<String> = std::env::args().collect();
    let (namespace, table) = if args.len() == 3 {
        (args[1].clone(), args[2].clone())
    } else {
        let namespace =
            std::env::var("CLOUDFLARE_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let table = std::env::var("CLOUDFLARE_TABLE").unwrap_or_else(|_| "logs".to_string());
        println!("Usage: {} <namespace> <table-name>", args[0]);
        println!("Using defaults: namespace={}, table={}", namespace, table);
        (namespace, table)
    };

    // TODO: enable use commit table env for this mode

    let table_id = TableIdent::from_strs(&[&namespace], &table);
    let table = catalog.load_table(&table_id).await?;
    println!("Table loaded, planning snapshot expiration...");

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis() as i64;
    let one_hour_ms = 60 * 60 * 1000;
    let options = ExpireSnapshotsOptions {
        older_than_ms: Some(now_ms - one_hour_ms),
        retain_last: Some(1),
        delete_orphan_data: true,
        delete_orphan_manifests: true,
        max_snapshots_per_run: Some(20),
        manifest_scan_concurrency: Some(8),
        cleanup_concurrency: Some(24),
        dry_run: false,
    };

    let dry_run = options.dry_run;
    let result = table.expire_snapshots(&catalog, options).await?;
    if dry_run {
        println!(
            "Dry run: would expire {} snapshot(s): {:?}",
            result.expired_snapshot_ids.len(),
            result.expired_snapshot_ids
        );
    } else {
        println!(
            "Expired {} snapshot(s): {:?}",
            result.expired_snapshot_ids.len(),
            result.expired_snapshot_ids
        );
    }

    Ok(())
}

#[cfg(not(feature = "maintenance"))]
fn main() {
    eprintln!(
        "This example requires the `maintenance` feature. Re-run with: \
         cargo run --example r2_expire_snapshots_dry_run --features maintenance"
    );
}
