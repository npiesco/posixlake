//! Delta Lake Integration
//!
//! Native Delta Lake format support with column statistics and operations.

pub mod data_skipping;
pub mod merge;
pub mod operations;
pub mod stats;

pub use data_skipping::{can_skip_file, extract_predicates, get_file_statistics, FileStats};
pub use merge::{
    MatchedDeleteClause, MatchedUpdateClause, MergeBuilder, MergeMetrics, NotMatchedInsertClause,
};
pub use operations::{optimize_table, vacuum_dry_run, vacuum_table, zorder_table, OptimizeMetrics};
pub use stats::{compute_column_statistics, get_column_statistics_from_delta, ColumnStats};
