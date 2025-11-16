//! Transaction API for writing to Iceberg tables

use crate::table::Table;

/// A transaction for modifying a table
pub struct Transaction<'a> {
    table: &'a Table,
}

impl<'a> Transaction<'a> {
    /// Create a new transaction
    pub(crate) fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Get the table this transaction operates on
    pub fn table(&self) -> &Table {
        self.table
    }
}
