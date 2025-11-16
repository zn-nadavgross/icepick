//! Transaction commit orchestration
//!
//! This module implements the commit workflow for Iceberg tables:
//! 1. Write manifest files (Avro) containing data file metadata
//! 2. Write manifest list (Avro) referencing manifests
//! 3. Create snapshot with summary statistics
//! 4. Write new table metadata JSON
//! 5. Update catalog pointer atomically
//!
//! Follows PyIceberg file naming conventions and Iceberg v2 format.

mod orchestrator;
pub mod paths;

pub use orchestrator::{commit_transaction, try_commit};
