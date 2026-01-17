//! icepick CLI - Iceberg table maintenance tool

use clap::{Parser, Subcommand};
use icepick::cli::commands::{
    catalog as catalog_cmd, compact as compact_cmd, namespace as namespace_cmd,
    table as table_cmd,
};
use icepick::cli::{CatalogConfig, OutputFormat};

/// Iceberg table maintenance CLI
#[derive(Debug, Parser)]
#[command(name = "icepick", about = "Iceberg table maintenance CLI")]
#[command(version, author)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// S3 Tables ARN
    #[arg(long, env = "ICEPICK_ARN", global = true)]
    arn: Option<String>,

    /// R2 Account ID
    #[arg(long, env = "ICEPICK_R2_ACCOUNT", global = true)]
    r2_account: Option<String>,

    /// R2 Bucket
    #[arg(long, env = "ICEPICK_R2_BUCKET", global = true)]
    r2_bucket: Option<String>,

    /// API Token (R2/REST)
    #[arg(long, env = "ICEPICK_TOKEN", global = true)]
    token: Option<String>,

    /// REST catalog endpoint
    #[arg(long, env = "ICEPICK_ENDPOINT", global = true)]
    endpoint: Option<String>,

    /// Output format
    #[arg(long, short, default_value = "text", global = true)]
    output: OutputFormat,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Catalog operations
    #[command(subcommand)]
    Catalog(catalog_cmd::CatalogCommand),

    /// Namespace operations
    #[command(subcommand)]
    Namespace(namespace_cmd::NamespaceCommand),

    /// Table operations
    #[command(subcommand)]
    Table(table_cmd::TableCommand),

    /// Compact a table
    Compact(compact_cmd::CompactArgs),
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::WARN.into()),
        )
        .init();

    let cli = Cli::parse();

    let config = CatalogConfig {
        arn: cli.arn,
        r2_account: cli.r2_account,
        r2_bucket: cli.r2_bucket,
        token: cli.token,
        endpoint: cli.endpoint,
    };

    let result = match cli.command {
        Commands::Catalog(cmd) => catalog_cmd::execute(cmd, &config, cli.output).await,
        Commands::Namespace(cmd) => namespace_cmd::execute(cmd, &config, cli.output).await,
        Commands::Table(cmd) => table_cmd::execute(cmd, &config, cli.output).await,
        Commands::Compact(args) => compact_cmd::execute(args, &config, cli.output).await,
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
