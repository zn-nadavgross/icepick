//! Commit Parquet files to an Iceberg table

mod helpers;
mod output;

use std::collections::HashMap;

use clap::Args;

use crate::catalog::register::{
    introspect_local_parquet_file, introspect_parquet_file, DataFileInput, RegisterOptions,
};
use crate::cli::catalog::CatalogConfig;
use crate::cli::output::{print, OutputFormat};
use crate::io::{get_filename, is_local_path};
use crate::spec::{NamespaceIdent, PartitionSpec, Schema, TableIdent};

use helpers::{
    determine_partition_values, expand_glob, format_partition_key, generate_upload_path,
    upload_local_file,
};
use output::{CommitPlanOutput, CommitResultOutput, PartitionSummary, SchemaMismatch};

// Re-export for tests
pub use helpers::{
    build_partition_spec, check_schema_compatibility, parse_partition_spec,
    parse_partition_values_arg, parse_type_str,
};

/// Commit Parquet files to an Iceberg table
#[derive(Debug, Args)]
pub struct CommitArgs {
    /// Glob pattern for Parquet files (e.g., /data/**/*.parquet)
    pub pattern: String,

    /// Target namespace
    #[arg(long, short)]
    pub namespace: String,

    /// Target table name
    #[arg(long, short)]
    pub table: String,

    /// Parquet file to use as schema exemplar (default: first file from glob)
    #[arg(long)]
    pub exemplar: Option<String>,

    /// Create table if it doesn't exist
    #[arg(long)]
    pub create: bool,

    /// Partition columns for new table (e.g., year:int,month:int)
    #[arg(long, requires = "create")]
    pub partition: Option<String>,

    /// Explicit partition values for all files (e.g., year=2024,month=01)
    #[arg(long)]
    pub partition_values: Option<String>,

    /// Show plan without committing
    #[arg(long)]
    pub dry_run: bool,
}

/// Result of resolving table location for local file uploads
struct TableLocationResult {
    location: String,
    table_was_pre_created: bool,
}

/// Context for resolving table location
struct TableContext<'a> {
    catalog: &'a dyn crate::catalog::Catalog,
    namespace: &'a NamespaceIdent,
    table_ident: &'a TableIdent,
    schema: &'a Schema,
    partition_spec: Option<&'a PartitionSpec>,
    ns_name: &'a str,
    table_name: &'a str,
}

/// Resolve table location for local file uploads.
async fn resolve_table_location(
    ctx: &TableContext<'_>,
    table_exists: bool,
    dry_run: bool,
) -> Result<TableLocationResult, String> {
    if table_exists {
        let table = ctx
            .catalog
            .load_table(ctx.table_ident)
            .await
            .map_err(|e| format!("Failed to load table: {}", e))?;
        return Ok(TableLocationResult {
            location: table.location().to_string(),
            table_was_pre_created: false,
        });
    }

    if dry_run {
        return Ok(TableLocationResult {
            location: format!("s3://<bucket>/{}/{}", ctx.ns_name, ctx.table_name),
            table_was_pre_created: false,
        });
    }

    let mut creation_builder = crate::spec::TableCreation::builder()
        .with_name(ctx.table_name.to_string())
        .with_schema(ctx.schema.clone());

    if let Some(spec) = ctx.partition_spec {
        creation_builder = creation_builder.with_partition_spec(spec.clone());
    }

    let creation = creation_builder
        .build()
        .map_err(|e| format!("Failed to build table creation: {}", e))?;

    let table = ctx
        .catalog
        .create_table(ctx.namespace, creation)
        .await
        .map_err(|e| format!("Failed to create table: {}", e))?;

    println!("Created table: {}.{}", ctx.ns_name, ctx.table_name);
    Ok(TableLocationResult {
        location: table.location().to_string(),
        table_was_pre_created: true,
    })
}

/// Result of processing all input files
struct ProcessedFiles {
    data_files: Vec<DataFileInput>,
    uploads: Vec<(String, String)>,
    schema_mismatches: Vec<SchemaMismatch>,
    partition_summaries: HashMap<String, (usize, i64)>,
    total_bytes: u64,
    total_rows: i64,
}

/// Process all input files: introspect, validate schema, extract partitions
async fn process_input_files(
    files: &[String],
    file_io: &crate::io::FileIO,
    schema: &Schema,
    explicit_partition_values: &Option<HashMap<String, String>>,
    partition_spec: Option<&PartitionSpec>,
    table_location: &str,
) -> Result<ProcessedFiles, String> {
    let mut data_files: Vec<DataFileInput> = Vec::new();
    let mut schema_mismatches = Vec::new();
    let mut partition_summaries: HashMap<String, (usize, i64)> = HashMap::new();
    let mut total_bytes = 0u64;
    let mut total_rows = 0i64;
    let mut uploads: Vec<(String, String)> = Vec::new();

    for file_path in files {
        let introspection = if is_local_path(file_path) {
            introspect_local_parquet_file(file_path, None)
                .await
                .map_err(|e| format!("Failed to read {}: {}", file_path, e))?
        } else {
            introspect_parquet_file(file_io, file_path, None)
                .await
                .map_err(|e| format!("Failed to read {}: {}", file_path, e))?
        };

        if let Err(mismatch_reason) = check_schema_compatibility(schema, &introspection.schema) {
            schema_mismatches.push(SchemaMismatch {
                file_path: file_path.clone(),
                reason: mismatch_reason,
            });
            continue;
        }

        let partition_values = determine_partition_values(
            file_path,
            explicit_partition_values,
            partition_spec,
            schema,
        )?;

        let partition_key = format_partition_key(&partition_values);
        let entry = partition_summaries.entry(partition_key).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += introspection.data_file.record_count;

        total_bytes += introspection.data_file.file_size_in_bytes as u64;
        total_rows += introspection.data_file.record_count;

        let mut data_file = introspection.data_file;
        data_file.partition_values = partition_values;

        if is_local_path(file_path) {
            let remote_path = generate_upload_path(table_location, file_path);
            uploads.push((file_path.clone(), remote_path.clone()));
            data_file.file_path = remote_path;
        }

        data_files.push(data_file);
    }

    Ok(ProcessedFiles {
        data_files,
        uploads,
        schema_mismatches,
        partition_summaries,
        total_bytes,
        total_rows,
    })
}

/// Upload local files to remote storage
async fn execute_uploads(
    uploads: &[(String, String)],
    file_io: &crate::io::FileIO,
) -> Result<(), String> {
    if uploads.is_empty() {
        return Ok(());
    }

    println!("Uploading {} local files...", uploads.len());
    for (local_path, remote_path) in uploads {
        println!("  {} -> {}", get_filename(local_path), remote_path);
        upload_local_file(local_path, remote_path, file_io).await?;
    }
    println!("Upload complete");
    Ok(())
}

/// Build partition summaries for output
fn build_partition_summaries(
    partition_summaries: HashMap<String, (usize, i64)>,
) -> Vec<PartitionSummary> {
    partition_summaries
        .into_iter()
        .map(|(k, (count, rows))| PartitionSummary {
            partition_value: if k.is_empty() {
                "(unpartitioned)".to_string()
            } else {
                k
            },
            file_count: count,
            row_count: rows,
        })
        .collect()
}

/// Execute the commit command
pub async fn execute(
    args: CommitArgs,
    config: &CatalogConfig,
    format: OutputFormat,
) -> Result<(), String> {
    let files = expand_glob(&args.pattern)?;
    println!("Found {} Parquet files", files.len());

    let has_local_files = files.iter().any(|f| is_local_path(f));
    if has_local_files {
        println!("Detected local files - will upload to table storage");
    }

    let catalog = config.create_catalog().await?;
    let file_io = catalog.file_io();

    let exemplar_path = args.exemplar.as_ref().unwrap_or(&files[0]);
    let exemplar = if is_local_path(exemplar_path) {
        introspect_local_parquet_file(exemplar_path, None)
            .await
            .map_err(|e| format!("Failed to read exemplar file {}: {}", exemplar_path, e))?
    } else {
        introspect_parquet_file(file_io, exemplar_path, None)
            .await
            .map_err(|e| format!("Failed to read exemplar file {}: {}", exemplar_path, e))?
    };
    let schema = exemplar.schema.clone();
    println!("Schema from: {}", exemplar_path);

    let partition_spec = args
        .partition
        .as_ref()
        .map(|s| build_partition_spec(s, &schema))
        .transpose()?;
    let explicit_partition_values = args
        .partition_values
        .as_ref()
        .map(|pv| parse_partition_values_arg(pv))
        .transpose()?;

    if args.namespace.is_empty() {
        return Err("Namespace cannot be empty".to_string());
    }
    if args.table.is_empty() {
        return Err("Table name cannot be empty".to_string());
    }
    let namespace = NamespaceIdent::from_strs(&[args.namespace.as_str()]);
    let table_ident = TableIdent::from_strs(&[args.namespace.as_str()], &args.table);

    let table_exists = catalog
        .table_exists(&table_ident)
        .await
        .map_err(|e| format!("Failed to check if table exists: {}", e))?;

    if !table_exists && !args.create {
        return Err(format!(
            "Table {}.{} does not exist. Use --create to create it.",
            args.namespace, args.table
        ));
    }

    let (table_location, table_was_pre_created) = if has_local_files {
        let ctx = TableContext {
            catalog: catalog.as_ref(),
            namespace: &namespace,
            table_ident: &table_ident,
            schema: &schema,
            partition_spec: partition_spec.as_ref(),
            ns_name: &args.namespace,
            table_name: &args.table,
        };
        let result = resolve_table_location(&ctx, table_exists, args.dry_run).await?;
        (result.location, result.table_was_pre_created)
    } else {
        (String::new(), false)
    };

    let processed = process_input_files(
        &files,
        file_io,
        &schema,
        &explicit_partition_values,
        partition_spec.as_ref(),
        &table_location,
    )
    .await?;

    if !processed.schema_mismatches.is_empty() && !args.dry_run {
        return Err(format!(
            "{} files have schema mismatches. Run with --dry-run to see details.",
            processed.schema_mismatches.len()
        ));
    }

    let partitions = build_partition_summaries(processed.partition_summaries);

    if args.dry_run {
        let plan = CommitPlanOutput {
            schema_source: exemplar_path.clone(),
            target_table: format!("{}.{}", args.namespace, args.table),
            will_create_table: !table_exists,
            partition_columns: partition_spec
                .as_ref()
                .map(|s| s.fields().iter().map(|f| f.name().to_string()).collect())
                .unwrap_or_default(),
            files_to_commit: processed.data_files.len(),
            files_to_upload: processed.uploads.len(),
            total_rows: processed.total_rows,
            total_bytes: processed.total_bytes,
            partitions,
            schema_mismatches: processed.schema_mismatches,
        };
        print(&plan, format);
        return Ok(());
    }

    execute_uploads(&processed.uploads, file_io).await?;

    let options = if args.create && !table_exists && !table_was_pre_created {
        let mut opts = RegisterOptions::new().allow_create_with_schema(schema.clone());
        if let Some(spec) = partition_spec {
            opts = opts.with_partition_spec(spec);
        }
        opts.allow_noop(true)
    } else {
        RegisterOptions::new().allow_noop(true)
    };

    // Clear source_schema to skip validation (catalog may assign different field IDs)
    let data_files: Vec<DataFileInput> = processed
        .data_files
        .into_iter()
        .map(|mut f| {
            f.source_schema = None;
            f
        })
        .collect();

    let result = crate::catalog::register::register_data_files(
        catalog.as_ref(),
        namespace,
        table_ident,
        data_files,
        options,
    )
    .await
    .map_err(|e| format!("Commit failed: {}", e))?;

    let output = CommitResultOutput {
        target_table: format!("{}.{}", args.namespace, args.table),
        table_created: result.table_was_created || table_was_pre_created,
        files_committed: result.added_files,
        rows_committed: result.added_records,
        files_skipped: result.skipped_files.len(),
        snapshot_id: result.snapshot_id,
    };

    print(&output, format);
    Ok(())
}
