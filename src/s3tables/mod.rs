//! Minimal Iceberg REST catalog client for AWS S3 Tables

mod arn;
mod catalog;
mod client;
mod error;
mod types;

pub use catalog::S3TablesCatalog;
