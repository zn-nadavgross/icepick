//! Parquet writer and statistics collection

pub mod arrow_parquet;
pub mod parquet;
pub mod stats;

pub use arrow_parquet::arrow_to_parquet;
pub use parquet::ParquetWriter;
