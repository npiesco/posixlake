//! Metadata management for Delta Lake native databases

pub mod backup;
pub mod schema;

pub use backup::{BackupMetadata, BackupVerificationReport};
pub use schema::{DataTypeRepr, Schema, SchemaField, SchemaManager, SchemaVersion};
