//! Iceberg specification types
//!
//! Vendored from iceberg-rust v0.7.0
//! Copyright 2024 Apache Software Foundation
//! Licensed under Apache License 2.0

pub mod data_file;
pub mod identifier;
pub mod metadata;
pub mod schema;
pub mod schema_evolution;
pub mod snapshot;
pub mod table_creation;
pub mod types;

pub use data_file::{DataContentType, DataFile};
pub use identifier::{NamespaceIdent, TableIdent};
pub use metadata::{
    MetadataLogEntry, PartitionField, PartitionSpec, SnapshotLogEntry, SnapshotReference,
    SortField, SortOrder, TableMetadata,
};
pub use schema::Schema;
pub use snapshot::{Snapshot, Summary};
pub use table_creation::{TableCreation, TableCreationBuilder};
pub use types::{NestedField, PrimitiveType, StructType, Type};
