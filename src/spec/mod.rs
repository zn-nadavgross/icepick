//! Iceberg specification types
//!
//! Vendored from iceberg-rust v0.7.0
//! Copyright 2024 Apache Software Foundation
//! Licensed under Apache License 2.0

pub mod identifier;
pub mod metadata;
pub mod schema;
pub mod snapshot;
pub mod types;

pub use identifier::{NamespaceIdent, TableIdent};
pub use metadata::TableMetadata;
pub use schema::Schema;
pub use snapshot::{Snapshot, Summary};
pub use types::{NestedField, PrimitiveType, StructType, Type};
