//! Minimal Iceberg REST catalog client for AWS S3 Tables

mod catalog;
mod client;
mod error;

pub use catalog::S3TablesCatalog;
pub use client::S3TablesClient;
pub use error::S3TablesError;
