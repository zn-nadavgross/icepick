//! Iceberg specification types
//!
//! Vendored from iceberg-rust v0.7.0
//! Copyright 2024 Apache Software Foundation
//! Licensed under Apache License 2.0

pub mod identifier;
pub mod schema;
pub mod types;

pub use identifier::{NamespaceIdent, TableIdent};
pub use schema::Schema;
pub use types::{NestedField, PrimitiveType, StructType, Type};
