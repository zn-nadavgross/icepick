//! I/O operations for Iceberg files
//! WASM-compatible via OpenDAL

mod file_io;
pub mod local;

pub use file_io::{AwsCredentials, FileIO, VendedCredentialProvider, VendedCredentials};
pub use local::{create_local_file_io, get_filename, is_local_path};
