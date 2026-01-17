# icepick CLI Implementation Plan

This document outlines the plan to transform icepick into a CLI tool while maintaining its WASM library capabilities.

## Goals

- **Append-only commits** (existing)
- **Snapshot pruning** (existing branch to integrate)
- **Compaction** (new - bin-pack with partition scoping)
- **Metadata listing / catalog info** (new CLI commands)

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| CLI framework | clap | Most popular, derive macros, env var support |
| Output format | AWS CLI style | Human-readable default, `--output json` for scripting |
| Config | Env vars + CLI args | Simple to start, config file can be added later |
| Compaction strategy | Bin-pack | Simple, predictable, good for append-heavy workloads |
| Compaction scope | Partition-scoped | Never merge across partitions |
| Materialization | Full | Read all files in group to memory, then write |
| Commit granularity | One tx per partition | Partial progress saved, natural boundary |
| Compacted file names | `compacted_{uuid}_from_{n}_files.parquet` | Debuggable, reasonable length |

---

## Phase 1: Foundation - Transaction Rewrite Support

**Goal:** Enable atomic delete + add operations in a single commit

### 1.1 Extend Transaction API

**File:** `src/transaction.rs`

```rust
pub enum TransactionOperation {
    Append(Vec<DataFile>),
    Rewrite {
        files_to_delete: Vec<DataFile>,
        files_to_add: Vec<DataFile>,
    },
}

impl Transaction {
    /// Rewrite files: atomically delete old files and add new ones.
    /// Used for compaction, where we replace N small files with M larger files.
    pub fn rewrite(mut self, files_to_delete: Vec<DataFile>, files_to_add: Vec<DataFile>) -> Self {
        self.operations.push(TransactionOperation::Rewrite {
            files_to_delete,
            files_to_add,
        });
        self
    }
}
```

### 1.2 Update Manifest Writer

**File:** `src/manifest/writer.rs`

- Add `ManifestEntryStatus` enum:
  - `Existing = 0`
  - `Added = 1`
  - `Deleted = 2`
- Create `write_manifest_with_status()` that accepts `(file, status)` pairs
- Refactor existing `write_manifest()` to call new function with all `Added`

### 1.3 Update Commit Orchestrator

**File:** `src/commit/orchestrator.rs`

Changes to `try_commit()`:
- Handle `TransactionOperation::Rewrite`
- Write single manifest containing both deleted (status=2) and added (status=1) entries
- Update snapshot summary:
  - `"operation": "replace"` (instead of `"append"`)
  - Add `"deleted-data-files"`, `"deleted-records"` fields
  - Compute correct `"total-data-files"`, `"total-records"` (subtract deleted, add new)
- Carry forward non-deleted files from parent manifests

---

## Phase 2: Compaction Module

**Goal:** Bin-pack compaction with partition scoping and full materialization

### 2.1 Module Structure

```
src/compact/
├── mod.rs           # Public exports
├── options.rs       # CompactOptions struct
├── plan.rs          # CompactionPlan, bin-packing algorithm
└── execute.rs       # Read, merge, write, commit
```

### 2.2 Options

**File:** `src/compact/options.rs`

```rust
/// Options for bin-pack compaction
#[derive(Debug, Clone)]
pub struct CompactOptions {
    /// Target size for output files (default: 256MB)
    pub target_file_size: u64,

    /// Only compact files smaller than this (default: 128MB)
    pub max_input_file_size: u64,

    /// Minimum files in a group to trigger compaction (default: 3)
    pub min_files_per_group: usize,

    /// Only compact specific partition (None = all partitions)
    pub partition_filter: Option<String>,

    /// Show plan without executing
    pub dry_run: bool,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            target_file_size: 256 * 1024 * 1024,      // 256 MB
            max_input_file_size: 128 * 1024 * 1024,   // 128 MB
            min_files_per_group: 3,
            partition_filter: None,
            dry_run: false,
        }
    }
}
```

### 2.3 Planning

**File:** `src/compact/plan.rs`

```rust
pub struct CompactionPlan {
    pub partitions: Vec<PartitionPlan>,
}

pub struct PartitionPlan {
    pub partition_value: Option<String>,
    pub groups: Vec<CompactionGroup>,
    pub total_input_files: usize,
    pub total_input_bytes: u64,
}

pub struct CompactionGroup {
    pub input_files: Vec<DataFile>,
    pub input_bytes: u64,
    pub input_records: u64,
}

impl CompactionPlan {
    /// Analyze table and create compaction plan
    pub async fn create(table: &Table, options: &CompactOptions) -> Result<Self>;

    /// True if nothing to compact
    pub fn is_empty(&self) -> bool;

    /// Total files across all partitions
    pub fn total_input_files(&self) -> usize;

    /// Estimated output files
    pub fn estimated_output_files(&self, target_size: u64) -> usize;
}
```

**Bin-packing algorithm:**
1. List all data files from current snapshot
2. Group files by partition value
3. For each partition:
   - Filter to files where `size < max_input_file_size`
   - Sort by size ascending
   - Greedy bin-pack (first-fit) targeting `target_file_size`
   - Skip groups with fewer than `min_files_per_group`

### 2.4 Execution

**File:** `src/compact/execute.rs`

For each partition (one transaction per partition):

1. For each group in partition:
   - Read all input Parquet files → `Vec<RecordBatch>`
   - Concatenate batches (`arrow::compute::concat_batches`)
   - Write to `{table_location}/data/{partition_path}/compacted_{uuid}_from_{n}_files.parquet`
   - Collect new `DataFile` metadata

2. Build transaction:
   ```rust
   table.transaction()
       .rewrite(all_deleted_files, all_new_files)
       .commit(catalog, timestamp_ms)
       .await?;
   ```

3. Return partition result

```rust
pub struct CompactionResult {
    pub partitions_compacted: usize,
    pub partitions_failed: usize,
    pub files_removed: usize,
    pub files_added: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub records_processed: u64,
    pub errors: Vec<PartitionError>,
}

pub struct PartitionError {
    pub partition: Option<String>,
    pub error: String,
}
```

### 2.5 Public API

**File:** `src/compact/mod.rs`

```rust
pub use options::CompactOptions;
pub use plan::{CompactionPlan, PartitionPlan, CompactionGroup};
pub use execute::CompactionResult;

/// Plan compaction for a table (does not execute)
pub async fn plan_compaction(
    table: &Table,
    options: &CompactOptions,
) -> Result<CompactionPlan>;

/// Execute a compaction plan
pub async fn execute_compaction(
    plan: CompactionPlan,
    table: &Table,
    catalog: &dyn Catalog,
    options: &CompactOptions,
) -> Result<CompactionResult>;
```

### 2.6 Export from lib.rs

```rust
pub mod compact;
pub use compact::{CompactOptions, CompactionPlan, CompactionResult};
```

---

## Phase 3: CLI Infrastructure

**Goal:** clap-based CLI with AWS CLI-style output

### 3.1 Binary Target

**File:** `Cargo.toml`

```toml
[[bin]]
name = "icepick"
path = "src/bin/icepick.rs"

[dependencies]
clap = { version = "4", features = ["derive", "env"] }
comfy-table = "7"
bytesize = "1"
humantime = "2"
```

### 3.2 CLI Structure

```
src/bin/
└── icepick.rs       # Entry point
src/cli/
├── mod.rs           # Module exports
├── output.rs        # Text/JSON formatting
├── catalog.rs       # Catalog connection from args/env
└── commands/
    ├── mod.rs
    ├── catalog.rs   # catalog info
    ├── namespace.rs # namespace list, create
    ├── table.rs     # table list, info, files
    ├── snapshot.rs  # snapshot list, prune
    └── compact.rs   # compact
```

### 3.3 Global Options

```rust
#[derive(Parser)]
#[command(name = "icepick", about = "Iceberg table maintenance CLI")]
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

#[derive(ValueEnum, Clone)]
enum OutputFormat {
    Text,
    Json,
}
```

### 3.4 Catalog Resolution

Priority order:
1. `--arn` → S3TablesCatalog
2. `--r2-account` + `--r2-bucket` + `--token` → R2Catalog
3. `--endpoint` + `--token` → RestCatalog
4. Error if none specified

### 3.5 Output Formatting

**File:** `src/cli/output.rs`

```rust
pub trait Outputable: Serialize {
    fn to_text(&self) -> String;
}

pub fn print<T: Outputable>(item: &T, format: OutputFormat) {
    match format {
        OutputFormat::Text => println!("{}", item.to_text()),
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(item).unwrap()),
    }
}
```

---

## Phase 4: CLI Commands

### 4.1 Catalog Info

```
icepick catalog info
```

Output:
```
Catalog Type:  S3 Tables
ARN:           arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket
Region:        us-west-2
Status:        Connected
```

### 4.2 Namespace Commands

```
icepick namespace list
icepick namespace create <name>
```

### 4.3 Table Commands

```
icepick table list [--namespace <ns>]
icepick table info <namespace.table>
icepick table files <namespace.table> [--partition <value>]
```

Example `table info` output:
```
Table:          analytics.events
Location:       s3://bucket/warehouse/analytics/events
Format Version: 2
Current Snapshot: 1234567890

Schema:
  1  id          long      required
  2  timestamp   timestamp required
  3  event_type  string    optional
  4  payload     string    optional

Partitions:
  1000  dt  day(timestamp)

Snapshots: 15
Data Files: 234
Total Size: 12.4 GB
Total Records: 45,678,901
```

### 4.4 Snapshot Commands

```
icepick snapshot list <namespace.table>
icepick snapshot prune <namespace.table>
    --retain-last <n>        # Keep N most recent
    --older-than <duration>  # Remove older than (e.g., "7d", "24h")
    --dry-run
```

### 4.5 Compact Command

```
icepick compact <namespace.table>
    --target-size <bytes>      # Default: 268435456 (256MB)
    --max-input-size <bytes>   # Default: 134217728 (128MB)
    --min-files <n>            # Default: 3
    --partition <value>        # Only compact this partition
    --dry-run
```

Example dry-run output:
```
Compaction Plan for analytics.events

Partition: dt=2024-01-15
  Input:  23 files, 445 MB (avg 19 MB/file)
  Output: ~2 files (target 256 MB)

Partition: dt=2024-01-16
  Input:  18 files, 312 MB (avg 17 MB/file)
  Output: ~2 files (target 256 MB)

Summary
  Files:   41 → ~4 (90% reduction)
  Bytes:   757 MB → ~757 MB

Dry run complete. Remove --dry-run to execute.
```

Example execution output:
```
Compacting analytics.events...

[1/2] Partition dt=2024-01-15
      23 files (445 MB) → 2 files (443 MB)  ✓

[2/2] Partition dt=2024-01-16
      18 files (312 MB) → 2 files (310 MB)  ✓

Complete
  Partitions: 2
  Files:      41 → 4 (90% reduction)
  Bytes:      757 MB → 753 MB (0.5% savings)
  Records:    1,234,567
```

---

## Phase 5: Testing

### 5.1 Unit Tests

- `src/compact/plan.rs`: Bin-packing algorithm with edge cases
- `src/manifest/writer.rs`: Manifest entries with different statuses
- `src/cli/output.rs`: Text and JSON formatting

### 5.2 Integration Tests

- End-to-end compaction with in-memory FileIO
- Concurrent modification retry during compaction
- Partition filtering
- Transaction rewrite commit

### 5.3 Manual Testing Checklist

- [ ] Compact unpartitioned table
- [ ] Compact single partition with `--partition`
- [ ] Compact all partitions
- [ ] Verify `--dry-run` doesn't modify anything
- [ ] Verify `--output json` is valid JSON
- [ ] Error handling: missing table, invalid ARN, network errors
- [ ] Interrupt mid-compaction, verify partial progress saved

---

## Dependencies

```toml
[dependencies]
# CLI (new)
clap = { version = "4", features = ["derive", "env"] }
comfy-table = "7"
bytesize = "1"
humantime = "2"

# Existing (ensure present)
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

---

## Future Enhancements (Out of Scope)

- Config file support (TOML or YAML)
- Sort-order compaction (`--sort-by <columns>`)
- Z-order clustering (`--zorder <columns>`)
- Background/async compaction with progress file
- `icepick scan` for basic queries
- Streaming compaction (lower memory footprint)
