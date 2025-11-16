mod bearer;
pub use bearer::BearerTokenAuthProvider;

#[cfg(not(target_family = "wasm"))]
mod sigv4;

#[cfg(not(target_family = "wasm"))]
pub use sigv4::SigV4AuthProvider;

pub use crate::catalog::AuthProvider;
