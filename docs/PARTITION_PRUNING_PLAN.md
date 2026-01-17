# Partition Pruning Implementation Plan

This document outlines the implementation plan for adding partition pruning support to icepick, enabling efficient filtering of data files based on partition values and column statistics.

## Current State

### What Exists
- `TableScan` reads **all** data files sequentially without filtering
- `DataFile` has partition data (`HashMap<String, String>`) and bounds (`lower_bounds`, `upper_bounds`)
- `ManifestReader` reads data files but **ignores partition data and bounds**
- `CompactOptions.partition_filter` does simple path substring matching (not true partition pruning)
- `PartitionSpec` and `PartitionField` types exist but aren't used for filtering

### Gaps
1. No predicate/expression API for scan filters
2. Manifest reader doesn't extract partition values or column bounds
3. No partition spec evaluation (transform application)
4. No bounds-based file skipping
5. TableScan has no way to accept filter predicates

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         TableScan                                    │
│  .filter(predicate) ─────────────────────────────────────────────►  │
└───────────────────────────────────────┬─────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Predicate Evaluator                              │
│  1. Partition pruning (eliminate files by partition value)           │
│  2. Stats pruning (eliminate files by min/max bounds)                │
└───────────────────────────────────────┬─────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Filtered File List                               │
│  Only read files that might contain matching rows                    │
└─────────────────────────────────────────────────────────────────────┘
```

## Implementation Phases

### Phase 1: Expression API

Create a simple expression/predicate API for representing filter conditions.

**Files to create:**
- `src/expr/mod.rs` - Module exports
- `src/expr/predicate.rs` - Predicate types

**Types:**

```rust
/// A scalar value for comparison
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Bool(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    Date(i32),           // days since epoch
    Timestamp(i64),      // microseconds since epoch
    Binary(Vec<u8>),
}

/// Binary comparison operators
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ComparisonOp {
    Eq,        // =
    NotEq,     // !=
    Lt,        // <
    LtEq,      // <=
    Gt,        // >
    GtEq,      // >=
}

/// A reference to a column by name or ID
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnRef {
    Named(String),
    Id(i32),
}

/// A predicate expression
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Always true
    AlwaysTrue,
    /// Always false
    AlwaysFalse,
    /// Column comparison: column op value
    Comparison {
        column: ColumnRef,
        op: ComparisonOp,
        value: Datum,
    },
    /// Column IS NULL
    IsNull(ColumnRef),
    /// Column IS NOT NULL
    IsNotNull(ColumnRef),
    /// Column IN (values...)
    In {
        column: ColumnRef,
        values: Vec<Datum>,
    },
    /// Logical AND of predicates
    And(Vec<Predicate>),
    /// Logical OR of predicates
    Or(Vec<Predicate>),
    /// Logical NOT of predicate
    Not(Box<Predicate>),
}
```

**Builder API:**

```rust
impl Predicate {
    pub fn and(predicates: impl IntoIterator<Item = Predicate>) -> Self;
    pub fn or(predicates: impl IntoIterator<Item = Predicate>) -> Self;
    pub fn not(predicate: Predicate) -> Self;

    // Convenience constructors
    pub fn eq(column: impl Into<ColumnRef>, value: impl Into<Datum>) -> Self;
    pub fn lt(column: impl Into<ColumnRef>, value: impl Into<Datum>) -> Self;
    pub fn gt(column: impl Into<ColumnRef>, value: impl Into<Datum>) -> Self;
    pub fn is_null(column: impl Into<ColumnRef>) -> Self;
    pub fn is_not_null(column: impl Into<ColumnRef>) -> Self;
}
```

### Phase 2: Manifest Reader Enhancement

Extend `ManifestReader` to extract partition data and column bounds from manifest entries.

**Changes to `src/reader/manifest.rs`:**

```rust
/// Enhanced data file entry with partition and bounds info
#[derive(Debug, Clone)]
pub struct DataFileEntry {
    pub file_path: String,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub file_format: String,
    // New fields:
    pub partition: HashMap<String, PartitionValue>,
    pub lower_bounds: HashMap<i32, Vec<u8>>,
    pub upper_bounds: HashMap<i32, Vec<u8>>,
    pub null_value_counts: HashMap<i32, i64>,
}

/// Partition value from manifest (before transform inversion)
#[derive(Debug, Clone)]
pub enum PartitionValue {
    Int(i32),
    Long(i64),
    String(String),
    Date(i32),
    Binary(Vec<u8>),
    Null,
}
```

**Manifest Avro parsing changes:**
- Parse `partition` field from manifest entry
- Parse `lower_bounds` and `upper_bounds` maps
- Parse `null_value_counts` for IS NULL pruning

### Phase 3: Partition Evaluator

Create logic to evaluate predicates against partition values.

**Files to create:**
- `src/expr/partition_eval.rs` - Partition predicate evaluation

**Key functions:**

```rust
/// Project a predicate onto partition columns
/// Returns a predicate that can be evaluated against partition values
pub fn project_to_partition(
    predicate: &Predicate,
    schema: &Schema,
    partition_spec: &PartitionSpec,
) -> Predicate;

/// Evaluate a projected predicate against partition values
/// Returns true if the partition MIGHT contain matching rows
pub fn evaluate_partition(
    predicate: &Predicate,
    partition_values: &HashMap<String, PartitionValue>,
) -> bool;
```

**Transform handling:**

For each partition field transform, we need inversion logic:

| Transform | Filter on source column | Rewritten to partition column |
|-----------|------------------------|------------------------------|
| identity  | `col = X` | `part_col = X` |
| identity  | `col < X` | `part_col < X` |
| year      | `col = '2024-01-15'` | `part_col = 54` (2024) |
| year      | `col >= '2024-01-01'` | `part_col >= 54` |
| month     | `col = '2024-03-15'` | `part_col = 650` (2024*12 + 3 - 1) |
| day       | `col = '2024-03-15'` | `part_col = 19797` (days since epoch) |
| hour      | Complex range logic | ... |
| bucket    | `col = X` | `part_col = hash(X) % N` |
| truncate  | `col = 'hello'` (W=3) | `part_col = 'hel'` |

**Initial scope:** Start with `identity`, `year`, `month`, `day` transforms. `bucket` and `truncate` are more complex.

### Phase 4: Bounds Evaluator

Create logic to evaluate predicates against column min/max bounds.

**Files to create:**
- `src/expr/bounds_eval.rs` - Column statistics evaluation

**Key function:**

```rust
/// Evaluate predicate against file column bounds
/// Returns true if the file MIGHT contain matching rows
pub fn evaluate_bounds(
    predicate: &Predicate,
    schema: &Schema,
    lower_bounds: &HashMap<i32, Vec<u8>>,
    upper_bounds: &HashMap<i32, Vec<u8>>,
    null_counts: &HashMap<i32, i64>,
    row_count: i64,
) -> bool;
```

**Evaluation rules:**

| Predicate | Condition to SKIP file |
|-----------|----------------------|
| `col = X` | `X < lower` OR `X > upper` |
| `col < X` | `lower >= X` |
| `col <= X` | `lower > X` |
| `col > X` | `upper <= X` |
| `col >= X` | `upper < X` |
| `col IS NULL` | `null_count = 0` |
| `col IS NOT NULL` | `null_count = row_count` |

**Binary serialization:**
- Iceberg stores bounds as binary (little-endian for numeric types)
- Need deserialization for each primitive type
- Date = i32 days, Timestamp = i64 microseconds

### Phase 5: TableScan Integration

Integrate the evaluators into `TableScan`.

**Changes to `src/scan.rs`:**

```rust
pub struct TableScanBuilder<'a> {
    table: &'a Table,
    predicate: Option<Predicate>,  // New
}

impl<'a> TableScanBuilder<'a> {
    /// Add a filter predicate
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    pub fn build(self) -> Result<TableScan<'a>> {
        Ok(TableScan {
            table: self.table,
            predicate: self.predicate,
        })
    }
}
```

**File filtering in `to_arrow()`:**

```rust
pub async fn to_arrow(&self) -> Result<ArrowRecordBatchStream> {
    let files = self.table.files_with_stats().await?;  // New method

    let filtered_files = if let Some(ref pred) = self.predicate {
        let schema = self.table.schema()?;
        let partition_spec = self.table.partition_spec()?;

        // Project predicate to partition columns
        let partition_pred = project_to_partition(pred, &schema, &partition_spec);

        files.into_iter().filter(|file| {
            // Partition pruning
            if !evaluate_partition(&partition_pred, &file.partition) {
                return false;
            }
            // Bounds pruning
            evaluate_bounds(pred, &schema, &file.lower_bounds, &file.upper_bounds,
                           &file.null_counts, file.record_count)
        }).collect()
    } else {
        files
    };

    // ... rest of streaming logic
}
```

### Phase 6: CLI Integration

Add filter support to CLI table scan command.

**Changes to `src/cli/commands/table.rs`:**

```rust
/// Scan command
Scan {
    /// Table identifier (namespace.table)
    table: String,

    /// Filter expression (e.g., "date >= '2024-01-01'")
    #[arg(long, short)]
    filter: Option<String>,

    /// Output limit
    #[arg(long, default_value = "100")]
    limit: usize,
}
```

**Expression parsing (simple grammar):**

```
filter     = comparison | and_expr | or_expr
comparison = column op value
op         = '=' | '!=' | '<' | '<=' | '>' | '>='
column     = identifier
value      = string_lit | number_lit | date_lit
```

Example: `--filter "date >= '2024-01-01' AND status = 'active'"`

## File Summary

| File | Action | Description |
|------|--------|-------------|
| `src/expr/mod.rs` | Create | Module exports |
| `src/expr/predicate.rs` | Create | Predicate/expression types |
| `src/expr/partition_eval.rs` | Create | Partition predicate evaluation |
| `src/expr/bounds_eval.rs` | Create | Column bounds evaluation |
| `src/expr/parser.rs` | Create | Simple expression parser for CLI |
| `src/reader/manifest.rs` | Modify | Extract partition/bounds from manifests |
| `src/scan.rs` | Modify | Add filter() method and pruning logic |
| `src/table.rs` | Modify | Add files_with_stats() method |
| `src/lib.rs` | Modify | Export expr module |
| `src/cli/commands/table.rs` | Modify | Add scan subcommand with filter |

## Testing Strategy

### Unit Tests

1. **Predicate construction**: Test builder API, AND/OR/NOT combinations
2. **Partition projection**: Test transform inversion for each supported transform
3. **Partition evaluation**: Test evaluation against various partition values
4. **Bounds evaluation**: Test each comparison operator with edge cases
5. **Binary deserialization**: Test decoding bounds for each primitive type

### Integration Tests

1. **End-to-end partition pruning**: Create table with partitions, verify only relevant files scanned
2. **End-to-end bounds pruning**: Create table with known min/max, verify file skipping
3. **Combined pruning**: Both partition and bounds filtering together
4. **CLI filter parsing**: Test various filter expressions

### Test Data

Create test fixtures with:
- Known partition values (e.g., `date=2024-01-15`)
- Known column bounds (e.g., `id` between 1-100)
- Various file counts to verify pruning effectiveness

## Implementation Order

1. **Phase 1: Expression API** - Foundation for all filtering
2. **Phase 2: Manifest Enhancement** - Get the data we need
3. **Phase 3: Partition Evaluator** - Most impactful pruning
4. **Phase 4: Bounds Evaluator** - Additional pruning
5. **Phase 5: TableScan Integration** - Wire it all together
6. **Phase 6: CLI Integration** - User-facing feature

## Out of Scope (Future Work)

- Row-level filtering (post-scan, in Arrow)
- Predicate pushdown to Parquet reader
- Complex transforms (bucket, truncate with all edge cases)
- Manifest-level pruning (skip entire manifests)
- Delete file handling with predicates
- Expression optimization/simplification

## Success Metrics

- Partition pruning reduces files scanned by N% for partition-filtered queries
- Bounds pruning provides additional reduction for range queries
- No regression in non-filtered scan performance
- CLI provides intuitive filter syntax
