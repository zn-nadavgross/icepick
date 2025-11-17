use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use icepick::catalog::Catalog;
use icepick::error::{Error, Result};
use icepick::spec::{NamespaceIdent, TableCreation, TableIdent};
use icepick::table::Table;
use tokio::sync::{Mutex, RwLock};

/// Simple in-memory catalog used by integration tests
pub struct TestCatalog {
    table: RwLock<Table>,
    updates: Mutex<Vec<(String, String)>>,
    fail_next_update: AtomicBool,
    load_calls: AtomicUsize,
}

impl TestCatalog {
    pub fn new(table: Table) -> Self {
        Self {
            table: RwLock::new(table),
            updates: Mutex::new(Vec::new()),
            fail_next_update: AtomicBool::new(false),
            load_calls: AtomicUsize::new(0),
        }
    }

    /// Record that the next update should fail with a concurrent modification error
    #[allow(dead_code)]
    pub fn fail_next_update(&self) {
        self.fail_next_update.store(true, Ordering::SeqCst);
    }

    /// Retrieve recorded update operations
    pub async fn recorded_updates(&self) -> Vec<(String, String)> {
        self.updates.lock().await.clone()
    }

    /// Number of times load_table has been invoked
    #[allow(dead_code)]
    pub fn load_call_count(&self) -> usize {
        self.load_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Catalog for TestCatalog {
    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Ok(())
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> Result<bool> {
        Ok(true)
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        Ok(vec![])
    }

    async fn table_exists(&self, _identifier: &TableIdent) -> Result<bool> {
        Ok(true)
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> Result<Table> {
        Err(Error::invalid_request(
            "TestCatalog::create_table not supported",
        ))
    }

    async fn load_table(&self, _identifier: &TableIdent) -> Result<Table> {
        self.load_calls.fetch_add(1, Ordering::SeqCst);
        let table = self.table.read().await;
        Ok(table.clone())
    }

    async fn drop_table(&self, _identifier: &TableIdent) -> Result<()> {
        Ok(())
    }

    async fn update_table_metadata(
        &self,
        _identifier: &TableIdent,
        old_metadata_location: &str,
        new_metadata_location: &str,
    ) -> Result<()> {
        self.updates.lock().await.push((
            old_metadata_location.to_string(),
            new_metadata_location.to_string(),
        ));

        if self.fail_next_update.swap(false, Ordering::SeqCst) {
            return Err(Error::concurrent_modification(
                "simulated concurrent modification",
            ));
        }

        let mut table = self.table.write().await;
        let file_io = table.file_io().clone();
        let identifier = table.identifier().clone();
        let metadata = table.metadata().clone();

        *table = Table::new(
            identifier,
            metadata,
            new_metadata_location.to_string(),
            file_io,
        );

        Ok(())
    }
}
