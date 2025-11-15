//! Minimal Iceberg REST catalog client for AWS S3 Tables

mod client;
mod error;

pub use client::S3TablesClient;
pub use error::S3TablesError;
