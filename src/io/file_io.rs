//! FileIO implementation using OpenDAL
//! Compatible with both WASM and native targets

use crate::error::{Error, Result};
use opendal::Operator;

/// File I/O abstraction for reading/writing Iceberg files
#[derive(Clone)]
pub struct FileIO {
    operator: Operator,
}

impl FileIO {
    /// Create a new FileIO with the given OpenDAL operator
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Read a file completely
    pub async fn read(&self, path: &str) -> Result<Vec<u8>> {
        self.operator
            .read(path)
            .await
            .map(|b| b.to_vec())
            .map_err(|e| Error::IoError(format!("Failed to read {}: {}", path, e)))
    }

    /// Write data to a file
    pub async fn write(&self, path: &str, data: Vec<u8>) -> Result<()> {
        self.operator
            .write(path, data)
            .await
            .map_err(|e| Error::IoError(format!("Failed to write {}: {}", path, e)))
    }

    /// Check if a file exists
    pub async fn exists(&self, path: &str) -> Result<bool> {
        match self.operator.exists(path).await {
            Ok(exists) => Ok(exists),
            Err(e) => Err(Error::IoError(format!(
                "Failed to check existence of {}: {}",
                path, e
            ))),
        }
    }

    /// Delete a file
    pub async fn delete(&self, path: &str) -> Result<()> {
        self.operator
            .delete(path)
            .await
            .map_err(|e| Error::IoError(format!("Failed to delete {}: {}", path, e)))
    }

    /// Get the underlying operator (for advanced use cases)
    pub fn operator(&self) -> &Operator {
        &self.operator
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_io_creation() {
        let op = Operator::via_iter(opendal::Scheme::Memory, []).unwrap();
        let _file_io = FileIO::new(op);
        // Just verify it compiles and constructs
    }
}
