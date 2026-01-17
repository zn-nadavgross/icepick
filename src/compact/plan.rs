//! Compaction planning with bin-packing algorithm

use crate::compact::options::CompactOptions;
use crate::error::Result;
use crate::spec::DataFile;
use crate::table::Table;
use std::collections::HashMap;

/// A group of files to be compacted together
#[derive(Debug, Clone)]
pub struct CompactionGroup {
    /// Input files to compact
    pub input_files: Vec<DataFile>,
    /// Total size of input files in bytes
    pub input_bytes: u64,
    /// Total record count in input files
    pub input_records: u64,
}

/// Plan for compacting a single partition
#[derive(Debug, Clone)]
pub struct PartitionPlan {
    /// Partition value (None for unpartitioned tables)
    pub partition_value: Option<String>,
    /// Groups of files to compact
    pub groups: Vec<CompactionGroup>,
    /// Total number of input files
    pub total_input_files: usize,
    /// Total input bytes
    pub total_input_bytes: u64,
}

impl PartitionPlan {
    /// Estimate the number of output files based on target size
    pub fn estimated_output_files(&self, target_size: u64) -> usize {
        self.groups
            .iter()
            .map(|g| {
                let files = (g.input_bytes as f64 / target_size as f64).ceil() as usize;
                files.max(1)
            })
            .sum()
    }
}

/// Complete compaction plan for a table
#[derive(Debug, Clone)]
pub struct CompactionPlan {
    /// Plans for each partition
    pub partitions: Vec<PartitionPlan>,
}

impl CompactionPlan {
    /// Create a compaction plan for a table
    pub async fn create(table: &Table, options: &CompactOptions) -> Result<Self> {
        // Get all data files from current snapshot
        let files = match table.current_snapshot() {
            Some(_) => table.files().await?,
            None => {
                // No snapshot means no files to compact
                return Ok(Self {
                    partitions: Vec::new(),
                });
            }
        };

        // Convert DataFileEntry to DataFile for easier manipulation
        let data_files: Vec<DataFile> = files
            .into_iter()
            .map(|entry| {
                DataFile::builder()
                    .with_file_path(&entry.file_path)
                    .with_file_format(&entry.file_format)
                    .with_record_count(entry.record_count)
                    .with_file_size_in_bytes(entry.file_size_in_bytes)
                    .build()
            })
            .collect::<Result<Vec<_>>>()?;

        // Group files by partition value
        let mut partition_groups: HashMap<Option<String>, Vec<DataFile>> = HashMap::new();

        for file in data_files {
            // Extract partition value from file path or partition data
            let partition_key = extract_partition_value(file.file_path());

            // Apply partition filter if specified
            if let Some(ref filter) = options.partition_filter {
                if partition_key.as_ref() != Some(filter) {
                    continue;
                }
            }

            partition_groups
                .entry(partition_key)
                .or_default()
                .push(file);
        }

        // Build compaction plan for each partition
        let mut partitions = Vec::new();

        for (partition_value, mut files) in partition_groups {
            // Filter to files smaller than max_input_file_size
            files.retain(|f| (f.file_size_in_bytes() as u64) < options.max_input_file_size);

            if files.len() < options.min_files_per_group {
                // Not enough files to compact
                continue;
            }

            // Sort by size ascending for better bin-packing
            files.sort_by_key(|f| f.file_size_in_bytes());

            // Greedy bin-packing (first-fit decreasing)
            let groups =
                bin_pack_files(files, options.target_file_size, options.min_files_per_group);

            if groups.is_empty() {
                continue;
            }

            let total_input_files: usize = groups.iter().map(|g| g.input_files.len()).sum();
            let total_input_bytes: u64 = groups.iter().map(|g| g.input_bytes).sum();

            partitions.push(PartitionPlan {
                partition_value,
                groups,
                total_input_files,
                total_input_bytes,
            });
        }

        Ok(Self { partitions })
    }

    /// Check if there's nothing to compact
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    /// Total files across all partitions
    pub fn total_input_files(&self) -> usize {
        self.partitions.iter().map(|p| p.total_input_files).sum()
    }

    /// Total bytes across all partitions
    pub fn total_input_bytes(&self) -> u64 {
        self.partitions.iter().map(|p| p.total_input_bytes).sum()
    }

    /// Estimated output files across all partitions
    pub fn estimated_output_files(&self, target_size: u64) -> usize {
        self.partitions
            .iter()
            .map(|p| p.estimated_output_files(target_size))
            .sum()
    }

    /// Total number of partitions to compact
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
}

/// Extract partition value from file path (Hive-style partitioning)
fn extract_partition_value(file_path: &str) -> Option<String> {
    // Look for patterns like /key=value/ in the path
    // e.g., s3://bucket/table/data/dt=2024-01-15/file.parquet -> "dt=2024-01-15"
    for segment in file_path.split('/') {
        if segment.contains('=') && !segment.starts_with("s3://") && !segment.starts_with("http") {
            return Some(segment.to_string());
        }
    }
    None
}

/// Greedy bin-packing algorithm (first-fit decreasing)
fn bin_pack_files(
    files: Vec<DataFile>,
    target_size: u64,
    min_files_per_group: usize,
) -> Vec<CompactionGroup> {
    let mut groups: Vec<CompactionGroup> = Vec::new();

    for file in files {
        let file_size = file.file_size_in_bytes() as u64;
        let file_records = file.record_count();

        // Try to find an existing group that can fit this file
        let mut placed = false;
        for group in &mut groups {
            if group.input_bytes + file_size <= target_size {
                group.input_bytes += file_size;
                group.input_records += file_records as u64;
                group.input_files.push(file.clone());
                placed = true;
                break;
            }
        }

        // Create a new group if no existing group can fit the file
        if !placed {
            groups.push(CompactionGroup {
                input_files: vec![file],
                input_bytes: file_size,
                input_records: file_records as u64,
            });
        }
    }

    // Filter out groups that don't meet the minimum file count
    groups.retain(|g| g.input_files.len() >= min_files_per_group);

    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_partition_value() {
        assert_eq!(
            extract_partition_value("s3://bucket/table/data/dt=2024-01-15/file.parquet"),
            Some("dt=2024-01-15".to_string())
        );
        assert_eq!(
            extract_partition_value("s3://bucket/table/data/file.parquet"),
            None
        );
        assert_eq!(
            extract_partition_value("s3://bucket/table/data/year=2024/month=01/file.parquet"),
            Some("year=2024".to_string()) // Returns first partition
        );
    }

    #[test]
    fn test_bin_pack_empty() {
        let groups = bin_pack_files(vec![], 256 * 1024 * 1024, 3);
        assert!(groups.is_empty());
    }
}
