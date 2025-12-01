//! Query engine integration with DataFusion

pub mod datafusion_provider;
pub mod executor;
pub mod pruning;

pub use datafusion_provider::FsdbTableProvider;
pub use executor::QueryExecutor;
