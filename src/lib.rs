//! Icepick: Production-ready cloud Iceberg catalogs
//!
//! Specialized Rust library for Apache Iceberg catalog operations on cloud providers.
//! Provides rock-solid implementations for AWS S3 Tables and Cloudflare R2 Data Catalog.
//!
//! # Quick Start
//!
//! ## AWS S3 Tables (native platforms only)
//!
//! ```no_run
//! use icepick::S3TablesCatalog;
//! use iceberg::Catalog;
//! use iceberg::TableIdent;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let catalog = S3TablesCatalog::from_arn(
//!     "my-catalog",
//!     "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
//! ).await?;
//!
//! let table_id = TableIdent::from_strs(["namespace", "table"])?;
//! let table = catalog.load_table(&table_id).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Cloudflare R2 (all platforms including WASM)
//!
//! ```no_run
//! use icepick::R2Catalog;
//! use iceberg::Catalog;
//! use iceberg::TableIdent;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let catalog = R2Catalog::new(
//!     "my-catalog",
//!     "account-id",
//!     "bucket-name",
//!     "api-token"
//! ).await?;
//!
//! let table_id = TableIdent::from_strs(["namespace", "table"])?;
//! let table = catalog.load_table(&table_id).await?;
//! # Ok(())
//! # }
//! ```

pub mod catalog;
pub mod error;

// Re-export common types
pub use error::{Error, Result};

// Re-export catalog types
pub use catalog::r2::R2Catalog;

#[cfg(not(target_family = "wasm"))]
pub use catalog::s3_tables::S3TablesCatalog;
