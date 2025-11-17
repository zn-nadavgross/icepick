//! Transaction API for writing to Iceberg tables

use crate::spec::DataFile;
use crate::table::Table;

/// Operations that can be performed in a transaction
#[derive(Debug, Clone)]
pub enum TransactionOperation {
    /// Append data files
    Append(#[allow(dead_code)] Vec<DataFile>),
}

/// A transaction for modifying a table
pub struct Transaction {
    table: Table,
    operations: Vec<TransactionOperation>,
}

impl Transaction {
    /// Create a new transaction
    pub(crate) fn new(table: Table) -> Self {
        Self {
            table,
            operations: Vec::new(),
        }
    }

    /// Get the table this transaction operates on
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Append data files to the table
    pub fn append(mut self, data_files: Vec<DataFile>) -> Self {
        self.operations
            .push(TransactionOperation::Append(data_files));
        self
    }

    /// Check if transaction has any operations
    pub fn has_operations(&self) -> bool {
        !self.operations.is_empty()
    }

    /// Get the operations (for internal use)
    #[allow(dead_code)]
    pub(crate) fn operations(&self) -> &[TransactionOperation] {
        &self.operations
    }

    /// Replace the table metadata backing this transaction (for retries)
    pub(crate) fn rebind_table(self, table: Table) -> Self {
        Self {
            table,
            operations: self.operations,
        }
    }

    /// Commit the transaction, writing snapshots to the catalog
    pub async fn commit(self, catalog: &dyn crate::catalog::Catalog) -> crate::error::Result<()> {
        crate::commit::commit_transaction(self, catalog).await
    }
}
