//! Audit logging for security and compliance

use crate::Result;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp: i64,
    pub user: String,
    pub operation: String,
    pub details: String,
    pub success: bool,
}

impl AuditEntry {
    /// Create a new audit entry
    pub fn new(user: String, operation: String, details: String, success: bool) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        Self {
            timestamp,
            user,
            operation,
            details,
            success,
        }
    }
}

/// Audit log (stored as append-only log)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLog {
    entries: Vec<AuditEntry>,
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditLog {
    /// Create a new empty audit log
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Load audit log from disk
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::new());
        }

        let content = std::fs::read_to_string(path)?;
        let log: AuditLog = serde_json::from_str(&content)?;
        Ok(log)
    }

    /// Save audit log to disk
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let json = serde_json::to_string_pretty(self)?;

        // Atomic write: .tmp → rename
        let tmp_path = path.with_extension("tmp");
        std::fs::write(&tmp_path, json)?;
        std::fs::rename(tmp_path, path)?;

        Ok(())
    }

    /// Add an audit entry
    pub fn add_entry(&mut self, entry: AuditEntry) {
        self.entries.push(entry);
    }

    /// Get all entries
    pub fn entries(&self) -> &[AuditEntry] {
        &self.entries
    }

    /// Get entries for a specific user
    pub fn entries_for_user(&self, username: &str) -> Vec<AuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.user == username)
            .cloned()
            .collect()
    }

    /// Get entries for a specific operation type
    pub fn entries_for_operation(&self, operation: &str) -> Vec<AuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.operation == operation)
            .cloned()
            .collect()
    }
}

/// Audit logger with persistence
pub struct AuditLogger {
    log_path: PathBuf,
    log: tokio::sync::Mutex<AuditLog>,
}

impl AuditLogger {
    /// Create a new audit logger
    pub fn new<P: AsRef<Path>>(log_path: P) -> Result<Self> {
        let log_path = log_path.as_ref().to_path_buf();
        let log = AuditLog::load(&log_path)?;

        Ok(Self {
            log_path,
            log: tokio::sync::Mutex::new(log),
        })
    }

    /// Log an operation
    pub async fn log(
        &self,
        user: String,
        operation: String,
        details: String,
        success: bool,
    ) -> Result<()> {
        let entry = AuditEntry::new(user, operation, details, success);

        let mut log = self.log.lock().await;
        log.add_entry(entry);
        log.save(&self.log_path)?;

        Ok(())
    }

    /// Get all audit entries
    pub async fn get_entries(&self) -> Vec<AuditEntry> {
        let log = self.log.lock().await;
        log.entries().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_log() {
        let mut log = AuditLog::new();

        let entry1 = AuditEntry::new(
            "alice".to_string(),
            "INSERT".to_string(),
            "Inserted 5 rows".to_string(),
            true,
        );

        let entry2 = AuditEntry::new(
            "bob".to_string(),
            "SELECT".to_string(),
            "SELECT * FROM data".to_string(),
            true,
        );

        log.add_entry(entry1);
        log.add_entry(entry2);

        assert_eq!(log.entries().len(), 2);

        let alice_entries = log.entries_for_user("alice");
        assert_eq!(alice_entries.len(), 1);
        assert_eq!(alice_entries[0].operation, "INSERT");

        let select_entries = log.entries_for_operation("SELECT");
        assert_eq!(select_entries.len(), 1);
        assert_eq!(select_entries[0].user, "bob");
    }
}
