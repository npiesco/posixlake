//! Query engine integration with DataFusion

pub mod datafusion_provider;
pub mod executor;
pub mod pruning;

pub use datafusion_provider::PosixLakeTableProvider;
pub use executor::QueryExecutor;
