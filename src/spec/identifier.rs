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

/// Identifier for a table
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableIdent {
    namespace: NamespaceIdent,
    name: String,
}

impl TableIdent {
    /// Create a new table identifier
    pub fn new(namespace: NamespaceIdent, name: String) -> Self {
        assert!(!name.is_empty(), "Table name cannot be empty");
        Self { namespace, name }
    }

    /// Create from string slices
    pub fn from_strs(namespace: &[&str], name: &str) -> Self {
        Self::new(NamespaceIdent::from_strs(namespace), name.to_string())
    }

    /// Get the namespace
    pub fn namespace(&self) -> &NamespaceIdent {
        &self.namespace
    }

    /// Get the table name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Consume and return namespace and name
    pub fn into_parts(self) -> (NamespaceIdent, String) {
        (self.namespace, self.name)
    }
}

impl fmt::Display for TableIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}
