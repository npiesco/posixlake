use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("ObjectStore error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Database not found at path: {0}")]
    DatabaseNotFound(String),

    #[error("Record not found: {0}")]
    RecordNotFound(String),

    #[error("Database already exists at path: {0}")]
    DatabaseAlreadyExists(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Transaction conflict: {0}")]
    TransactionConflict(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Bincode error: {0}")]
    Bincode(String),

    #[error("Delta Lake error: {0}")]
    DeltaTable(#[from] deltalake::DeltaTableError),

    #[error("{0}")]
    Other(String),
}

impl From<bincode::error::EncodeError> for Error {
    fn from(err: bincode::error::EncodeError) -> Self {
        Error::Bincode(err.to_string())
    }
}

impl From<bincode::error::DecodeError> for Error {
    fn from(err: bincode::error::DecodeError) -> Self {
        Error::Bincode(err.to_string())
    }
}

impl From<datafusion::error::DataFusionError> for Error {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        Error::InvalidOperation(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
