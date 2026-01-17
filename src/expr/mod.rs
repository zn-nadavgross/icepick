//! Expression and predicate types for filtering Iceberg tables
//!
//! This module provides types for building filter predicates that can be used
//! for partition pruning and column statistics-based file filtering.
//!
//! # Example
//!
//! ```
//! use icepick::expr::{Predicate, Datum};
//!
//! // Simple equality filter
//! let filter = Predicate::eq("status", "active");
//!
//! // Range filter
//! let filter = Predicate::and([
//!     Predicate::gt_eq("date", Datum::Date(19724)), // 2024-01-01
//!     Predicate::lt("date", Datum::Date(19755)),    // 2024-02-01
//! ]);
//!
//! // Complex filter with AND/OR
//! let filter = Predicate::or([
//!     Predicate::eq("region", "us-west"),
//!     Predicate::and([
//!         Predicate::eq("region", "eu-central"),
//!         Predicate::gt("priority", 5),
//!     ]),
//! ]);
//! ```

mod bounds_eval;
mod parser;
mod partition_eval;
mod predicate;

pub use bounds_eval::evaluate_bounds;
pub use parser::parse_filter;
pub use partition_eval::{
    build_partition_mapping, evaluate_partition, project_to_partition, PartitionMapping, Transform,
};
pub use predicate::{ColumnRef, ComparisonOp, Datum, Predicate};
