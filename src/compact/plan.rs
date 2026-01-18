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
    input_files: Vec<DataFile>,
    /// Total size of input files in bytes
    input_bytes: u64,
    /// Total record count in input files
    input_records: u64,
}

impl CompactionGroup {
    /// Create a new compaction group from input files
    ///
    /// Automatically computes total bytes and records from the files.
    ///
    /// # Errors
    ///
    /// Returns an error if `input_files` is empty
    pub fn new(input_files: Vec<DataFile>) -> Result<Self> {
        if input_files.is_empty() {
            return Err(crate::error::Error::invalid_input(
                "CompactionGroup cannot be created with empty input_files",
            ));
        }

        let input_bytes = input_files
            .iter()
            .map(|f| f.file_size_in_bytes() as u64)
            .sum();

        let input_records = input_files.iter().map(|f| f.record_count() as u64).sum();

        Ok(Self {
            input_files,
            input_bytes,
            input_records,
        })
    }

    /// Get the input files to compact
    pub fn files(&self) -> &[DataFile] {
        &self.input_files
    }

    /// Get the total size of input files in bytes
    pub fn total_bytes(&self) -> u64 {
        self.input_bytes
    }

    /// Get the total record count in input files
    pub fn total_records(&self) -> u64 {
        self.input_records
    }
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
                let files = (g.total_bytes() as f64 / target_size as f64).ceil() as usize;
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
            if let Some(filter) = options.partition_filter() {
                if partition_key.as_deref() != Some(filter) {
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
            files.retain(|f| (f.file_size_in_bytes() as u64) < options.max_input_file_size());

            if files.len() < options.min_files_per_group() {
                // Not enough files to compact
                continue;
            }

            // Sort by size ascending for better bin-packing
            files.sort_by_key(|f| f.file_size_in_bytes());

            // Greedy bin-packing (first-fit decreasing)
            let groups = bin_pack_files(
                files,
                options.target_file_size(),
                options.min_files_per_group(),
            );

            if groups.is_empty() {
                continue;
            }

            let total_input_files: usize = groups.iter().map(|g| g.files().len()).sum();
            let total_input_bytes: u64 = groups.iter().map(|g| g.total_bytes()).sum();

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
    // Supports multi-level partitions: /year=2024/month=01/ -> "year=2024/month=01"
    let partitions: Vec<&str> = file_path
        .split('/')
        .filter(|segment| {
            segment.contains('=') && !segment.starts_with("s3://") && !segment.starts_with("http")
        })
        .collect();

    if partitions.is_empty() {
        None
    } else {
        Some(partitions.join("/"))
    }
}

/// Greedy bin-packing algorithm (first-fit decreasing)
fn bin_pack_files(
    files: Vec<DataFile>,
    target_size: u64,
    min_files_per_group: usize,
) -> Vec<CompactionGroup> {
    // Track groups as Vec<Vec<DataFile>> during packing
    let mut group_files: Vec<Vec<DataFile>> = Vec::new();
    let mut group_sizes: Vec<u64> = Vec::new();

    for file in files {
        let file_size = file.file_size_in_bytes() as u64;

        // Try to find an existing group that can fit this file
        let mut placed = false;
        for (idx, current_size) in group_sizes.iter_mut().enumerate() {
            if *current_size + file_size <= target_size {
                *current_size += file_size;
                group_files[idx].push(file.clone());
                placed = true;
                break;
            }
        }

        // Create a new group if no existing group can fit the file
        if !placed {
            group_files.push(vec![file]);
            group_sizes.push(file_size);
        }
    }

    // Convert Vec<Vec<DataFile>> to Vec<CompactionGroup>
    // Filter out groups that don't meet the minimum file count
    group_files
        .into_iter()
        .filter(|files| files.len() >= min_files_per_group)
        .filter_map(|files| CompactionGroup::new(files).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_group_new_with_valid_files() {
        let file1 = DataFile::builder()
            .with_file_path("s3://bucket/file1.parquet")
            .with_file_format("PARQUET")
            .with_record_count(100)
            .with_file_size_in_bytes(1024)
            .build()
            .unwrap();

        let file2 = DataFile::builder()
            .with_file_path("s3://bucket/file2.parquet")
            .with_file_format("PARQUET")
            .with_record_count(200)
            .with_file_size_in_bytes(2048)
            .build()
            .unwrap();

        let group = CompactionGroup::new(vec![file1, file2]).unwrap();

        assert_eq!(group.files().len(), 2);
        assert_eq!(group.total_bytes(), 1024 + 2048);
        assert_eq!(group.total_records(), 100 + 200);
    }

    #[test]
    fn test_compaction_group_new_with_empty_files() {
        let result = CompactionGroup::new(vec![]);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(err
            .to_string()
            .contains("CompactionGroup cannot be created with empty input_files"));
    }

    #[test]
    fn test_compaction_group_getters() {
        let file = DataFile::builder()
            .with_file_path("s3://bucket/file.parquet")
            .with_file_format("PARQUET")
            .with_record_count(150)
            .with_file_size_in_bytes(3000)
            .build()
            .unwrap();

        let group = CompactionGroup::new(vec![file.clone()]).unwrap();

        // Test getter methods
        assert_eq!(group.files().len(), 1);
        assert_eq!(group.files()[0].file_path(), file.file_path());
        assert_eq!(group.total_bytes(), 3000);
        assert_eq!(group.total_records(), 150);
    }

    #[test]
    fn test_compaction_group_automatic_aggregates() {
        // Verify that aggregates are computed automatically and correctly
        let files: Vec<DataFile> = (0..5)
            .map(|i| {
                DataFile::builder()
                    .with_file_path(&format!("s3://bucket/file{}.parquet", i))
                    .with_file_format("PARQUET")
                    .with_record_count(100 + i as i64)
                    .with_file_size_in_bytes(1000 + i as i64)
                    .build()
                    .unwrap()
            })
            .collect();

        let expected_bytes: u64 = files.iter().map(|f| f.file_size_in_bytes() as u64).sum();
        let expected_records: u64 = files.iter().map(|f| f.record_count() as u64).sum();

        let group = CompactionGroup::new(files).unwrap();

        assert_eq!(group.total_bytes(), expected_bytes);
        assert_eq!(group.total_records(), expected_records);
    }

    #[test]
    fn test_extract_partition_value() {
        // Single partition
        assert_eq!(
            extract_partition_value("s3://bucket/table/data/dt=2024-01-15/file.parquet"),
            Some("dt=2024-01-15".to_string())
        );

        // No partition
        assert_eq!(
            extract_partition_value("s3://bucket/table/data/file.parquet"),
            None
        );

        // Multi-level partitions - should return all partition keys
        assert_eq!(
            extract_partition_value("s3://bucket/table/data/year=2024/month=01/file.parquet"),
            Some("year=2024/month=01".to_string())
        );

        // Three-level partitions
        assert_eq!(
            extract_partition_value(
                "s3://bucket/table/data/year=2024/month=01/day=15/file.parquet"
            ),
            Some("year=2024/month=01/day=15".to_string())
        );
    }

    #[test]
    fn test_bin_pack_empty() {
        let groups = bin_pack_files(vec![], 256 * 1024 * 1024, 3);
        assert!(groups.is_empty());
    }

    #[test]
    fn test_bin_pack_filters_small_groups() {
        // Create 2 files that are small enough to fit in target but below min_files_per_group
        let files: Vec<DataFile> = (0..2)
            .map(|i| {
                DataFile::builder()
                    .with_file_path(&format!("s3://bucket/file{}.parquet", i))
                    .with_file_format("PARQUET")
                    .with_record_count(100)
                    .with_file_size_in_bytes(1024)
                    .build()
                    .unwrap()
            })
            .collect();

        let groups = bin_pack_files(files, 256 * 1024 * 1024, 3);
        // Should be empty because group has only 2 files but min is 3
        assert!(groups.is_empty());
    }

    #[test]
    fn test_bin_pack_creates_valid_groups() {
        // Create enough files to form a valid group
        let files: Vec<DataFile> = (0..5)
            .map(|i| {
                DataFile::builder()
                    .with_file_path(&format!("s3://bucket/file{}.parquet", i))
                    .with_file_format("PARQUET")
                    .with_record_count(100)
                    .with_file_size_in_bytes(1024)
                    .build()
                    .unwrap()
            })
            .collect();

        let groups = bin_pack_files(files, 256 * 1024 * 1024, 3);
        // Should create one group with all 5 files
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].files().len(), 5);
    }
}
