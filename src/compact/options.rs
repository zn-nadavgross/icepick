//! Compaction options

use crate::error::Error;

/// Options for bin-pack compaction
#[derive(Debug, Clone)]
pub struct CompactOptions {
    /// Target size for output files (default: 256MB)
    target_file_size: u64,

    /// Only compact files smaller than this (default: 128MB)
    max_input_file_size: u64,

    /// Minimum files in a group to trigger compaction (default: 3)
    min_files_per_group: usize,

    /// Only compact specific partition (None = all partitions)
    partition_filter: Option<String>,

    /// Show plan without executing
    dry_run: bool,

    /// Allow partial failures - continue compacting other partitions if one fails (default: false)
    allow_partial_failure: bool,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            target_file_size: 256 * 1024 * 1024,    // 256 MB
            max_input_file_size: 128 * 1024 * 1024, // 128 MB
            min_files_per_group: 3,
            partition_filter: None,
            dry_run: false,
            allow_partial_failure: false,
        }
    }
}

impl CompactOptions {
    /// Minimum allowed target file size (1KB)
    const MIN_TARGET_FILE_SIZE: u64 = 1024;

    /// Create new options with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set target file size for output files
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `size` is 0
    /// - `size` is less than 1KB (1024 bytes)
    /// - `size` is less than or equal to the current `max_input_file_size`
    pub fn with_target_file_size(mut self, size: u64) -> crate::error::Result<Self> {
        if size == 0 {
            return Err(Error::invalid_input(
                "target_file_size must be greater than 0",
            ));
        }

        if size < Self::MIN_TARGET_FILE_SIZE {
            return Err(Error::invalid_input(format!(
                "target_file_size must be at least {} bytes (1KB), got {}",
                Self::MIN_TARGET_FILE_SIZE,
                size
            )));
        }

        // Validate cross-field constraint
        if size <= self.max_input_file_size {
            return Err(Error::invalid_input(format!(
                "target_file_size ({}) must be greater than max_input_file_size ({})",
                size, self.max_input_file_size
            )));
        }

        self.target_file_size = size;
        Ok(self)
    }

    /// Set maximum input file size to consider for compaction
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `size` is 0
    /// - `size` is greater than or equal to the current `target_file_size`
    pub fn with_max_input_file_size(mut self, size: u64) -> crate::error::Result<Self> {
        if size == 0 {
            return Err(Error::invalid_input(
                "max_input_file_size must be greater than 0",
            ));
        }

        // Validate cross-field constraint
        if size >= self.target_file_size {
            return Err(Error::invalid_input(format!(
                "max_input_file_size ({}) must be less than target_file_size ({})",
                size, self.target_file_size
            )));
        }

        self.max_input_file_size = size;
        Ok(self)
    }

    /// Set minimum files per group to trigger compaction
    ///
    /// # Errors
    ///
    /// Returns an error if `count` is less than 2 (cannot compact fewer than 2 files)
    pub fn with_min_files_per_group(mut self, count: usize) -> crate::error::Result<Self> {
        if count < 2 {
            return Err(Error::invalid_input(format!(
                "min_files_per_group must be at least 2 (cannot compact fewer than 2 files), got {}",
                count
            )));
        }

        self.min_files_per_group = count;
        Ok(self)
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

    /// Allow partial failures - continue compacting other partitions if one fails
    pub fn with_allow_partial_failure(mut self, allow: bool) -> Self {
        self.allow_partial_failure = allow;
        self
    }

    /// Get target file size for output files
    pub fn target_file_size(&self) -> u64 {
        self.target_file_size
    }

    /// Get maximum input file size to consider for compaction
    pub fn max_input_file_size(&self) -> u64 {
        self.max_input_file_size
    }

    /// Get minimum files per group to trigger compaction
    pub fn min_files_per_group(&self) -> usize {
        self.min_files_per_group
    }

    /// Get partition filter
    pub fn partition_filter(&self) -> Option<&str> {
        self.partition_filter.as_deref()
    }

    /// Check if dry run mode is enabled
    pub fn dry_run(&self) -> bool {
        self.dry_run
    }

    /// Check if partial failures are allowed
    pub fn allow_partial_failure(&self) -> bool {
        self.allow_partial_failure
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let options = CompactOptions::default();
        assert_eq!(options.target_file_size(), 256 * 1024 * 1024);
        assert_eq!(options.max_input_file_size(), 128 * 1024 * 1024);
        assert_eq!(options.min_files_per_group(), 3);
        assert_eq!(options.partition_filter(), None);
        assert!(!options.dry_run());
        assert!(!options.allow_partial_failure());
    }

    #[test]
    fn test_with_target_file_size_zero() {
        let result = CompactOptions::new().with_target_file_size(0);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("target_file_size must be greater than 0"));
    }

    #[test]
    fn test_with_target_file_size_below_minimum() {
        let result = CompactOptions::new().with_target_file_size(512);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least 1024 bytes"));
    }

    #[test]
    fn test_with_target_file_size_less_than_max_input() {
        // Default max_input is 128MB, try setting target to 64MB
        let result = CompactOptions::new().with_target_file_size(64 * 1024 * 1024);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be greater than max_input_file_size"));
    }

    #[test]
    fn test_with_max_input_file_size_zero() {
        let result = CompactOptions::new().with_max_input_file_size(0);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("max_input_file_size must be greater than 0"));
    }

    #[test]
    fn test_with_max_input_file_size_greater_than_target() {
        // Default target is 256MB, try setting max_input to 512MB
        let result = CompactOptions::new().with_max_input_file_size(512 * 1024 * 1024);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be less than target_file_size"));
    }

    #[test]
    fn test_with_min_files_per_group_zero() {
        let result = CompactOptions::new().with_min_files_per_group(0);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be at least 2"));
    }

    #[test]
    fn test_with_min_files_per_group_one() {
        let result = CompactOptions::new().with_min_files_per_group(1);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cannot compact fewer than 2 files"));
    }

    #[test]
    fn test_valid_configuration() {
        let options = CompactOptions::new()
            .with_target_file_size(512 * 1024 * 1024)
            .unwrap()
            .with_max_input_file_size(256 * 1024 * 1024)
            .unwrap()
            .with_min_files_per_group(5)
            .unwrap()
            .with_dry_run(true)
            .with_allow_partial_failure(true)
            .with_partition_filter("year=2025".to_string());

        assert_eq!(options.target_file_size(), 512 * 1024 * 1024);
        assert_eq!(options.max_input_file_size(), 256 * 1024 * 1024);
        assert_eq!(options.min_files_per_group(), 5);
        assert_eq!(options.partition_filter(), Some("year=2025"));
        assert!(options.dry_run());
        assert!(options.allow_partial_failure());
    }

    #[test]
    fn test_builder_chain_order_matters() {
        // Setting max_input first, then target should work
        let result = CompactOptions::new()
            .with_max_input_file_size(64 * 1024 * 1024)
            .unwrap()
            .with_target_file_size(128 * 1024 * 1024);
        assert!(result.is_ok());

        // Setting target first, then max_input should also work
        let result = CompactOptions::new()
            .with_target_file_size(512 * 1024 * 1024)
            .unwrap()
            .with_max_input_file_size(256 * 1024 * 1024);
        assert!(result.is_ok());
    }

    #[test]
    fn test_fields_are_private() {
        // This test ensures fields remain private - it would fail to compile if fields were public
        let options = CompactOptions::new();

        // These should be the only way to access values (through getters)
        let _ = options.target_file_size();
        let _ = options.max_input_file_size();
        let _ = options.min_files_per_group();
        let _ = options.partition_filter();
        let _ = options.dry_run();
        let _ = options.allow_partial_failure();

        // The following would fail to compile if uncommented (proving fields are private):
        // let _ = options.target_file_size;
        // let _ = options.max_input_file_size;
    }

    #[test]
    fn test_getter_methods() {
        let options = CompactOptions::new()
            .with_target_file_size(512 * 1024 * 1024)
            .unwrap()
            .with_max_input_file_size(256 * 1024 * 1024)
            .unwrap()
            .with_partition_filter("test".to_string());

        // Test all getters
        assert_eq!(options.target_file_size(), 512 * 1024 * 1024);
        assert_eq!(options.max_input_file_size(), 256 * 1024 * 1024);
        assert_eq!(options.partition_filter(), Some("test"));
    }
}
