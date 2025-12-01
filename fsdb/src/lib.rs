// FSDB - FileStoreDatabase
// A Delta Lake native database with SQL support

// Core modules
pub mod batch_buffer;
pub mod delta_lake;
pub mod error;
pub mod metadata;
pub mod query;
pub mod security;
pub mod storage;
pub mod transaction;

// Database operations
pub mod database_ops;

// POSIX interface (NFS server)
pub mod nfs;

// Python bindings (UniFFI)
pub mod python;

// Public API
pub use database_ops::DatabaseOps;
pub use error::{Error, Result};
pub use transaction::Transaction;

// Generate UniFFI scaffolding (0.29+ uses proc-macros)
uniffi::setup_scaffolding!();
