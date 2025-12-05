# Icepick Feature: Register Existing Parquet Files

Enable the Cloudflare Workers cron to commit already-written Parquet files (produced by Durable Objects) into Iceberg without rewriting data. This unlocks the “write to R2, register later” path while keeping a single source of truth in the Iceberg catalog.

## Goals
- Append pre-existing Parquet files to a table in one atomic Iceberg commit without rewriting bytes.
- Keep the API wasm-friendly and compatible with both R2Catalog and S3TablesCatalog.
- Preserve catalog invariants: optimistic concurrency, type-safe identifiers, and no raw OpenDAL exposure.

## Non-goals
- Predicate pushdown, projection, or manifest filtering (scan-only concerns).
- Full schema evolution; only additive/backward-compatible validation is required for registration.

## Public API Surface
- Catalog entry point (new module under `catalog/register.rs`):
  ```rust
  pub async fn register_data_files<C: Catalog>(
      catalog: &C,
      namespace: NamespaceIdent,
      table: TableIdent,
      files: Vec<DataFileInput>,
      options: RegisterOptions,
  ) -> Result<RegisterResult>;
  ```
- `DataFileInput` (conversion layer to `spec::DataFile`):
  - `file_path: String` (fully-qualified; resolved by FileIO)
  - `file_format: DataFileFormat` (enum; start with Parquet)
  - `file_size_in_bytes: i64`
  - `record_count: i64`
  - `partition_values: HashMap<String, PartitionValue>` (already resolved; stringly partition map stays internal to `DataFile`)
  - `metrics: Option<FileMetrics>` (per-column counts, bounds, sizes)
  - `content_type: DataContentType` (default `Data`)
  - `split_offsets: Option<Vec<i64>>`
  - `encryption: Option<EncryptionMetadata>`
- `RegisterOptions`:
  - `timestamp_ms: Option<i64>` (required for wasm where `SystemTime` may be unavailable)
  - `fail_if_missing: bool` (true -> return NotFound; false -> create namespace/table)
  - `schema_evolution: SchemaEvolutionPolicy` (`Reject` by default; `AllowAdditive` allowed)
- `RegisterResult`:
  - `snapshot_id: i64`
  - `added_files: usize`
  - `added_records: i64`
  - `table_was_created: bool`
  - `skipped_files: Vec<SkippedFile>` (idempotency reporting; see below)

## Behavior
- Build a single manifest + snapshot that references all provided files; no Parquet re-encode.
- Namespace/table handling:
  - `fail_if_missing == true`: error with `Error::NotFound { resource }`.
  - `fail_if_missing == false`: create namespace and table if absent, deriving schema + partition spec from the first file’s footer (Arrow schema mapping). Persist derived schema as table schema 0; subsequent calls validate compatibility.
- Validation:
  - All files must share compatible schema with table (field ids + types). Reject additive changes unless `schema_evolution` permits adding nullable fields with generated ids.
  - Partition values must match the table’s `PartitionSpec` arity and types; no implicit inference in core API.
  - Reject empty `files` input.
- Concurrency:
  - Use existing optimistic commit path (`Transaction::append`) with metadata location compare-and-swap.
  - Surface `ConcurrentModification` / `Conflict` for retry; do not hide them.
- Idempotency:
  - If a file path is already present in the current snapshot, record a `SkippedFile { file_path, reason: SkippedReason::AlreadyCommitted }` and exclude it from the new manifest. If all files are skipped, return `Error::NoopRegistration`.
- Timestamp:
  - Use `options.timestamp_ms` when provided; otherwise, use `OffsetDateTime::now_utc()` on native targets. Store on the snapshot for deterministic testing.

## Helper: Parquet → DataFileInput
- Optional utility (e.g., `io::parquet::introspect_file`) that reads metadata via `FileIO` + Parquet footer:
  ```rust
  pub async fn introspect_parquet_file(
      file_io: &FileIO,
      path: &str,
      partition_spec: Option<&PartitionSpec>,
  ) -> Result<ParquetIntrospection>;
  ```
- `ParquetIntrospection`:
  - `data_file: DataFileInput` (metrics populated from footer)
  - `schema: Schema` (Arrow-to-Iceberg mapping with field ids)
  - `partition_values: Option<HashMap<String, PartitionValue>>` (if `partition_spec` provided and path parsing succeeds)
- Requirements:
  - Never expose raw OpenDAL `Operator`; only use `FileIO`.
  - Avoid buffering file body; read only footer/metadata.
  - Feature-gate to keep wasm binary slim; ensure wasm path uses async file size/stat where available.

## Writer Crate Plumbing (`otlp2parquet-writer`)
- Provide a thin wasm-friendly wrapper:
  ```rust
  pub async fn register_existing_files(
      catalog: &dyn Catalog,
      namespace: NamespaceIdent,
      table: TableIdent,
      files: Vec<DataFileInput>,
      timestamp_ms: Option<i64>,
  ) -> Result<RegisterResult>;
  ```
- Keep behind the existing feature flag used for Workers; avoid pulling native-only deps.

## Error Handling
- Use existing `Error`/`CatalogError` variants; add targeted ones only if necessary:
  - `NoopRegistration` for all-skipped input.
  - `SchemaMismatch` with field-level reasons when footer schema diverges from table schema.
  - `PartitionValidation` for missing/mismatched partition values.
- No panics; return structured errors with context.

## Testing Plan
- Unit: `DataFileInput` → `DataFile` conversion (content type, metrics, partition map, split offsets).
- Unit: schema compatibility checks (reject incompatible, allow additive when configured).
- Unit: idempotency behavior (skip already-committed file, error on all skipped).
- Integration (ignored): register against in-memory `FileIO` + synthetic Parquet footer; separate `#[ignore]` for S3Tables/R2 paths.
- WASM: compile-only guard for helper functions; ensure `register_data_files` remains available with `?Send` where appropriate.

## Rationale
- Avoids double writes and egress from Workers by registering existing Parquet artifacts.
- Reuses the transaction/manifest pipeline to stay consistent with Iceberg semantics.
- Keeps the API small, typed, and wasm-safe while preserving optimistic concurrency.
