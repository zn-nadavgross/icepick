//! CLI module for icepick
//!
//! This module contains the command-line interface implementation.

pub mod catalog;
pub mod commands;
pub mod output;
pub mod util;

pub use catalog::CatalogConfig;
pub use output::OutputFormat;
pub use util::parse_table_ident;
