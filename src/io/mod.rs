//! I/O operations for Iceberg files
//! WASM-compatible via OpenDAL

mod file_io;

pub use file_io::{AwsCredentials, FileIO};
