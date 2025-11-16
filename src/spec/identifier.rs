//! Namespace and table identifiers
//! Vendored from iceberg-rust v0.7.0

use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

/// Identifier for a namespace (database)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceIdent(Vec<String>);

impl NamespaceIdent {
    /// Create a new namespace identifier
    pub fn new(parts: Vec<String>) -> Self {
        assert!(!parts.is_empty(), "Namespace cannot be empty");
        Self(parts)
    }

    /// Create from a slice of strings
    pub fn from_strs(parts: &[&str]) -> Self {
        Self::new(parts.iter().map(|s| s.to_string()).collect())
    }

    /// Get namespace parts as a slice
    pub fn as_slice(&self) -> &[String] {
        &self.0
    }

    /// Convert to Vec
    pub fn into_vec(self) -> Vec<String> {
        self.0
    }
}

impl Deref for NamespaceIdent {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for NamespaceIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.join("."))
    }
}

impl From<Vec<String>> for NamespaceIdent {
    fn from(parts: Vec<String>) -> Self {
        Self::new(parts)
    }
}

impl<'a> From<&'a [&'a str]> for NamespaceIdent {
    fn from(parts: &'a [&'a str]) -> Self {
        Self::from_strs(parts)
    }
}
