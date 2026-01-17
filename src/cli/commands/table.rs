//! Table commands

use crate::cli::catalog::CatalogConfig;
use crate::cli::output::{format_bytes, format_number, print, OutputFormat, Outputable};
use crate::cli::util::parse_table_ident;
use crate::expr::parse_filter;
use crate::spec::NamespaceIdent;
use clap::Subcommand;
use comfy_table::{Row, Table as ComfyTable};
use serde::Serialize;

/// Table commands
#[derive(Debug, Subcommand)]
pub enum TableCommand {
    /// List tables in a namespace
    List {
        /// Namespace name
        #[arg(long, short)]
        namespace: String,
    },

    /// Show table information
    Info {
        /// Table identifier (namespace.table)
        table: String,
    },

    /// List data files in a table
    Files {
        /// Table identifier (namespace.table)
        table: String,

        /// Filter by partition value
        #[arg(long, short)]
        partition: Option<String>,
    },

    /// Scan table with optional filter (show file pruning stats)
    Scan {
        /// Table identifier (namespace.table)
        table: String,

        /// Filter expression (e.g., "date >= '2024-01-01' AND status = 'active'")
        #[arg(long, short)]
        filter: Option<String>,
    },
}

/// Table list output
#[derive(Debug, Serialize)]
pub struct TableList {
    pub namespace: String,
    pub tables: Vec<String>,
}

impl Outputable for TableList {
    fn to_text(&self) -> String {
        if self.tables.is_empty() {
            return format!("No tables found in namespace '{}'.", self.namespace);
        }

        let mut lines = vec![format!("Tables in '{}':", self.namespace)];
        for table in &self.tables {
            lines.push(format!("  {}", table));
        }
        lines.join("\n")
    }
}

/// Table info output
#[derive(Debug, Serialize)]
pub struct TableInfo {
    pub table: String,
    pub location: String,
    pub format_version: i32,
    pub current_snapshot_id: Option<i64>,
    pub schema_fields: Vec<SchemaField>,
    pub partition_specs: Vec<String>,
    pub snapshot_count: usize,
    pub data_file_count: usize,
    pub total_size_bytes: u64,
    pub total_records: u64,
}

#[derive(Debug, Serialize)]
pub struct SchemaField {
    pub id: i32,
    pub name: String,
    pub field_type: String,
    pub required: bool,
}

impl Outputable for TableInfo {
    fn to_text(&self) -> String {
        let mut lines = vec![
            format!("Table:            {}", self.table),
            format!("Location:         {}", self.location),
            format!("Format Version:   {}", self.format_version),
        ];

        if let Some(snap_id) = self.current_snapshot_id {
            lines.push(format!("Current Snapshot: {}", snap_id));
        } else {
            lines.push("Current Snapshot: (none)".to_string());
        }

        lines.push(String::new());
        lines.push("Schema:".to_string());

        let mut schema_table = ComfyTable::new();
        schema_table.set_header(Row::from(vec!["ID", "Name", "Type", "Required"]));
        for field in &self.schema_fields {
            schema_table.add_row(Row::from(vec![
                field.id.to_string(),
                field.name.clone(),
                field.field_type.clone(),
                if field.required { "yes" } else { "no" }.to_string(),
            ]));
        }
        lines.push(schema_table.to_string());

        if !self.partition_specs.is_empty() {
            lines.push(String::new());
            lines.push("Partitions:".to_string());
            for spec in &self.partition_specs {
                lines.push(format!("  {}", spec));
            }
        }

        lines.push(String::new());
        lines.push(format!("Snapshots:    {}", self.snapshot_count));
        lines.push(format!("Data Files:   {}", format_number(self.data_file_count as u64)));
        lines.push(format!("Total Size:   {}", format_bytes(self.total_size_bytes)));
        lines.push(format!("Total Records: {}", format_number(self.total_records)));

        lines.join("\n")
    }
}

/// Table files output
#[derive(Debug, Serialize)]
pub struct TableFiles {
    pub table: String,
    pub files: Vec<FileInfo>,
    pub total_count: usize,
    pub total_size_bytes: u64,
    pub total_records: u64,
}

#[derive(Debug, Serialize)]
pub struct FileInfo {
    pub path: String,
    pub size_bytes: i64,
    pub record_count: i64,
    pub format: String,
}

impl Outputable for TableFiles {
    fn to_text(&self) -> String {
        if self.files.is_empty() {
            return format!("No data files found in table '{}'.", self.table);
        }

        let mut lines = vec![format!("Data files in '{}':", self.table), String::new()];

        let mut table = ComfyTable::new();
        table.set_header(Row::from(vec!["Path", "Size", "Records", "Format"]));

        for file in &self.files {
            // Truncate path for display
            let display_path = if file.path.len() > 60 {
                format!("...{}", &file.path[file.path.len() - 57..])
            } else {
                file.path.clone()
            };

            table.add_row(Row::from(vec![
                display_path,
                format_bytes(file.size_bytes as u64),
                format_number(file.record_count as u64),
                file.format.clone(),
            ]));
        }
        lines.push(table.to_string());

        lines.push(String::new());
        lines.push(format!(
            "Total: {} files, {}, {} records",
            self.total_count,
            format_bytes(self.total_size_bytes),
            format_number(self.total_records)
        ));

        lines.join("\n")
    }
}

/// Scan result output
#[derive(Debug, Serialize)]
pub struct ScanResult {
    pub table: String,
    pub filter: Option<String>,
    pub total_files: usize,
    pub files_after_filter: usize,
    pub files_pruned: usize,
    pub pruning_percentage: f64,
}

impl Outputable for ScanResult {
    fn to_text(&self) -> String {
        let mut lines = vec![format!("Scan plan for '{}':", self.table)];

        if let Some(ref filter) = self.filter {
            lines.push(format!("Filter: {}", filter));
        } else {
            lines.push("Filter: (none)".to_string());
        }

        lines.push(String::new());
        lines.push(format!("Total files:        {}", format_number(self.total_files as u64)));
        lines.push(format!("Files after filter: {}", format_number(self.files_after_filter as u64)));
        lines.push(format!("Files pruned:       {}", format_number(self.files_pruned as u64)));
        lines.push(format!("Pruning:            {:.1}%", self.pruning_percentage));

        lines.join("\n")
    }
}

/// Execute a table command
pub async fn execute(
    command: TableCommand,
    config: &CatalogConfig,
    format: OutputFormat,
) -> Result<(), String> {
    let catalog = config.create_catalog().await?;

    match command {
        TableCommand::List { namespace } => {
            let ns = NamespaceIdent::new(vec![namespace.clone()]);
            let tables = catalog
                .list_tables(&ns)
                .await
                .map_err(|e| format!("Failed to list tables: {}", e))?;

            let result = TableList {
                namespace,
                tables: tables.iter().map(|t| t.name().to_string()).collect(),
            };
            print(&result, format);
            Ok(())
        }

        TableCommand::Info { table: table_str } => {
            let table_ident = parse_table_ident(&table_str)?;
            let table = catalog
                .load_table(&table_ident)
                .await
                .map_err(|e| format!("Failed to load table: {}", e))?;

            let metadata = table.metadata();
            let schema = metadata.current_schema().map_err(|e| e.to_string())?;

            // Collect schema fields
            let schema_fields: Vec<SchemaField> = schema
                .fields()
                .iter()
                .map(|f| SchemaField {
                    id: f.id(),
                    name: f.name().to_string(),
                    field_type: format!("{:?}", f.field_type()),
                    required: f.is_required(),
                })
                .collect();

            // Get file stats
            let (data_file_count, total_size_bytes, total_records) = if table.current_snapshot().is_some() {
                match table.files().await {
                    Ok(files) => {
                        let count = files.len();
                        let size: u64 = files.iter().map(|f| f.file_size_in_bytes as u64).sum();
                        let records: u64 = files.iter().map(|f| f.record_count as u64).sum();
                        (count, size, records)
                    }
                    Err(_) => (0, 0, 0),
                }
            } else {
                (0, 0, 0)
            };

            let info = TableInfo {
                table: table_str,
                location: table.location().to_string(),
                format_version: metadata.format_version(),
                current_snapshot_id: metadata.current_snapshot_id(),
                schema_fields,
                partition_specs: vec![], // TODO: Add partition spec parsing
                snapshot_count: metadata.snapshots().len(),
                data_file_count,
                total_size_bytes,
                total_records,
            };

            print(&info, format);
            Ok(())
        }

        TableCommand::Files { table: table_str, partition } => {
            let table_ident = parse_table_ident(&table_str)?;
            let table = catalog
                .load_table(&table_ident)
                .await
                .map_err(|e| format!("Failed to load table: {}", e))?;

            let files = table
                .files()
                .await
                .map_err(|e| format!("Failed to list files: {}", e))?;

            // Filter by partition if specified
            let filtered_files: Vec<_> = if let Some(ref part_filter) = partition {
                files
                    .into_iter()
                    .filter(|f| f.file_path.contains(part_filter))
                    .collect()
            } else {
                files
            };

            let file_infos: Vec<FileInfo> = filtered_files
                .iter()
                .map(|f| FileInfo {
                    path: f.file_path.clone(),
                    size_bytes: f.file_size_in_bytes,
                    record_count: f.record_count,
                    format: f.file_format.clone(),
                })
                .collect();

            let total_size: u64 = file_infos.iter().map(|f| f.size_bytes as u64).sum();
            let total_records: u64 = file_infos.iter().map(|f| f.record_count as u64).sum();

            let result = TableFiles {
                table: table_str,
                total_count: file_infos.len(),
                total_size_bytes: total_size,
                total_records,
                files: file_infos,
            };

            print(&result, format);
            Ok(())
        }

        TableCommand::Scan { table: table_str, filter } => {
            let table_ident = parse_table_ident(&table_str)?;
            let table = catalog
                .load_table(&table_ident)
                .await
                .map_err(|e| format!("Failed to load table: {}", e))?;

            // Parse the filter expression if provided
            let predicate = if let Some(ref filter_str) = filter {
                Some(parse_filter(filter_str).map_err(|e| format!("Failed to parse filter: {}", e))?)
            } else {
                None
            };

            // Build scan with optional filter
            let mut scan_builder = table.scan();
            if let Some(pred) = predicate {
                scan_builder = scan_builder.filter(pred);
            }
            let scan = scan_builder.build().map_err(|e| format!("Failed to build scan: {}", e))?;

            // Get file counts
            let (files_after_filter, total_files) = scan
                .file_count()
                .await
                .map_err(|e| format!("Failed to get file count: {}", e))?;

            let files_pruned = total_files.saturating_sub(files_after_filter);
            let pruning_percentage = if total_files > 0 {
                (files_pruned as f64 / total_files as f64) * 100.0
            } else {
                0.0
            };

            let result = ScanResult {
                table: table_str,
                filter,
                total_files,
                files_after_filter,
                files_pruned,
                pruning_percentage,
            };

            print(&result, format);
            Ok(())
        }
    }
}
