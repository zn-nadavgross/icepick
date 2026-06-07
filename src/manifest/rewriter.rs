//! Rewrite an existing manifest file with selected entries dropped.
//!
//! Compaction needs to remove the manifest entries that pointed to the small
//! files we just merged. The classic "carry-forward parent manifest + append
//! a new DELETE manifest" pattern is fragile against engine differences —
//! tombstone matching depends on snapshot_id/sequence_number conventions that
//! icepick and Trino don't agree on, so deleted files keep showing as live
//! and orphan cleanup refuses to reclaim their storage.
//!
//! The Spark `RewriteFiles` pattern fixes this by physically rewriting each
//! parent manifest, omitting the entries for files we want gone. The file
//! simply disappears from the table's manifest set; no DELETE entry is
//! involved, no tombstone matching to fail.

use crate::error::{Error, Result};
use crate::io::FileIO;
use crate::manifest::schema::manifest_entry_schema_v2;
use crate::manifest::writer::set_manifest_user_metadata;
use crate::spec::{PartitionSpec, Schema as IcebergSchema};
use apache_avro::Reader as AvroReader;
use apache_avro::Writer;
use apache_avro::types::Value;
use std::collections::HashSet;

/// Outcome of attempting to rewrite a parent manifest.
pub enum RewriteOutcome {
    /// No entries matched the drop set — caller should keep the original
    /// manifest reference in the new manifest list, no rewrite needed.
    Unchanged,
    /// At least one entry was dropped and at least one survived — a new
    /// manifest was written at `target_path`, callers should reference it.
    Rewritten(RewriteResult),
    /// Every entry in the source manifest was dropped — the rewritten
    /// manifest would be empty so we don't write it. Caller should omit
    /// this manifest from the new manifest list entirely.
    EmptyAfterDrop,
}

/// Accounting for the rewritten manifest, used to populate manifest list
/// counters.
pub struct RewriteResult {
    pub target_path: String,
    pub manifest_length: i64,
    pub existing_files_count: i32,
    pub existing_rows_count: i64,
}

/// Read `source_path`, copy all entries whose data_file.file_path is NOT in
/// `drop_file_paths` into a freshly written manifest at `target_path`, and
/// report the result. Surviving entries are marked `EXISTING` (status=0)
/// regardless of their original status — they belong to a prior snapshot
/// from this commit's perspective.
pub async fn rewrite_manifest(
    file_io: &FileIO,
    source_path: &str,
    target_path: &str,
    drop_file_paths: &HashSet<String>,
    partition_spec: &PartitionSpec,
    iceberg_schema: &IcebergSchema,
) -> Result<RewriteOutcome> {
    let bytes = file_io.read(source_path).await?;
    let reader = AvroReader::new(&bytes[..]).map_err(|e| {
        Error::invalid_input(format!(
            "Failed to open source manifest {}: {}",
            source_path, e
        ))
    })?;

    let writer_schema = manifest_entry_schema_v2(partition_spec, iceberg_schema)?;
    let mut writer = Writer::new(&writer_schema, Vec::new());
    set_manifest_user_metadata(&mut writer, partition_spec, iceberg_schema)?;

    let mut existing_files_count: i32 = 0;
    let mut existing_rows_count: i64 = 0;
    let mut dropped: i32 = 0;

    for (idx, value) in reader.enumerate() {
        let value = value.map_err(|e| {
            Error::invalid_input(format!(
                "Failed to decode manifest entry {} from {}: {}",
                idx, source_path, e
            ))
        })?;

        let Value::Record(fields) = value else {
            continue;
        };

        let (file_path, record_count) = match extract_entry_meta(&fields) {
            Some(meta) => meta,
            None => continue,
        };

        if drop_file_paths.contains(&file_path) {
            dropped += 1;
            continue;
        }

        // Surviving entries become EXISTING in the rewritten manifest. The
        // data sequence/snapshot fields on the entry stay intact so readers
        // still see the file's original lineage; only its status changes.
        let new_fields: Vec<(String, Value)> = fields
            .into_iter()
            .map(|(name, val)| {
                if name == "status" {
                    (name, Value::Int(0))
                } else {
                    (name, val)
                }
            })
            .collect();

        writer.append(Value::Record(new_fields)).map_err(|e| {
            Error::invalid_input(format!("Failed to append rewritten entry: {}", e))
        })?;

        existing_files_count += 1;
        existing_rows_count += record_count;
    }

    if dropped == 0 {
        return Ok(RewriteOutcome::Unchanged);
    }
    if existing_files_count == 0 {
        return Ok(RewriteOutcome::EmptyAfterDrop);
    }

    let bytes_out = writer
        .into_inner()
        .map_err(|e| Error::invalid_input(format!("Failed to finalize rewritten manifest: {}", e)))?;
    let manifest_length = bytes_out.len() as i64;
    file_io.write(target_path, bytes_out).await?;

    Ok(RewriteOutcome::Rewritten(RewriteResult {
        target_path: target_path.to_string(),
        manifest_length,
        existing_files_count,
        existing_rows_count,
    }))
}

/// Pull the data_file's file_path and record_count out of a manifest entry
/// Avro Record. Returns None if either is missing, which would indicate a
/// malformed entry; the caller skips it.
fn extract_entry_meta(fields: &[(String, Value)]) -> Option<(String, i64)> {
    for (name, val) in fields {
        if name != "data_file" {
            continue;
        }
        let Value::Record(df_fields) = val else {
            return None;
        };
        let mut file_path: Option<String> = None;
        let mut record_count: i64 = 0;
        for (df_name, df_val) in df_fields {
            match df_name.as_str() {
                "file_path" => {
                    if let Value::String(s) = df_val {
                        file_path = Some(s.clone());
                    }
                }
                "record_count" => {
                    if let Value::Long(n) = df_val {
                        record_count = *n;
                    }
                }
                _ => {}
            }
        }
        return file_path.map(|p| (p, record_count));
    }
    None
}
