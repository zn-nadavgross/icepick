//! I/O operations for Iceberg files
//! WASM-compatible via OpenDAL

mod file_io;
#[cfg(not(target_family = "wasm"))]
pub mod local;

pub use file_io::{AwsCredentials, FileIO, VendedCredentialProvider, VendedCredentials};
#[cfg(not(target_family = "wasm"))]
pub use local::{create_local_file_io, get_filename, is_local_path};
