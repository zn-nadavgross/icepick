//! Compaction options

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
            target_file_size: 256 * 1024 * 1024,    // 256 MB
            max_input_file_size: 128 * 1024 * 1024, // 128 MB
            min_files_per_group: 3,
            partition_filter: None,
            dry_run: false,
        }
    }
}

impl CompactOptions {
    /// Create new options with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set target file size for output files
    pub fn with_target_file_size(mut self, size: u64) -> Self {
        self.target_file_size = size;
        self
    }

    /// Set maximum input file size to consider for compaction
    pub fn with_max_input_file_size(mut self, size: u64) -> Self {
        self.max_input_file_size = size;
        self
    }

    /// Set minimum files per group to trigger compaction
    pub fn with_min_files_per_group(mut self, count: usize) -> Self {
        self.min_files_per_group = count;
        self
    }

    /// Set partition filter to only compact specific partition
    pub fn with_partition_filter(mut self, partition: String) -> Self {
        self.partition_filter = Some(partition);
        self
    }

    /// Enable dry run mode
    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }
}
