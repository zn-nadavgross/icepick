//! CLI utility functions

use crate::spec::{NamespaceIdent, TableIdent};

/// Parse a table identifier (namespace.table)
pub fn parse_table_ident(s: &str) -> Result<TableIdent, String> {
    let parts: Vec<&str> = s.splitn(2, '.').collect();
    if parts.len() != 2 {
        return Err(format!(
            "Invalid table identifier '{}'. Expected format: namespace.table",
            s
        ));
    }
    let namespace = NamespaceIdent::new(vec![parts[0].to_string()]);
    Ok(TableIdent::new(namespace, parts[1].to_string()))
}
