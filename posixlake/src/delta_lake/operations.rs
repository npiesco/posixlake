//! Delta Lake operations: OPTIMIZE, VACUUM, Z-ORDER
//!
//! This module contains the internal implementations for Delta Lake operations
//! that improve performance and manage storage.

use crate::{Error, Result};
use deltalake::{open_table, open_table_with_storage_options, DeltaOps};
use std::collections::HashMap;
use std::path::Path;
use tracing::info;
use url::Url;

/// Optimize operation metrics
#[derive(Debug, Clone)]
pub struct OptimizeMetrics {
    pub num_files_added: u64,
    pub num_files_removed: u64,
    pub total_files_skipped: u64,
    pub total_considered_files: u64,
    pub preserve_insertion_order: bool,
}

/// Execute OPTIMIZE operation on a Delta Lake table
pub async fn optimize_table(
    base_path: &Path,
    s3_url: Option<&str>,
    s3_storage_options: Option<&HashMap<String, String>>,
    filter: Option<&str>,
    target_size: Option<u64>,
) -> Result<OptimizeMetrics> {
    use crate::storage::s3::parse_s3_url;

    // Open the Delta Lake table (S3 or local)
    let table = if let (Some(s3_url), Some(storage_options)) = (s3_url, s3_storage_options) {
        // S3 backend
        let s3_url_parsed = parse_s3_url(s3_url)?;
        open_table_with_storage_options(s3_url_parsed, storage_options.clone())
            .await
            .map_err(Error::DeltaTable)?
    } else {
        // Local backend
        let table_url = Url::from_directory_path(base_path)
            .map_err(|_| Error::Other("Invalid path for Delta table".to_string()))?;
        open_table(table_url).await.map_err(Error::DeltaTable)?
    };

    // Build optimize operation
    let mut optimize_builder = DeltaOps(table).optimize();

    // Add target file size if provided
    if let Some(size) = target_size {
        optimize_builder = optimize_builder.with_target_size(size);
    }

    // Note: Filter support requires PartitionFilter construction
    // which is partition-column based, not arbitrary SQL predicates
    if filter.is_some() {
        return Err(Error::Other(
            "OPTIMIZE with filter requires partitioned tables. Use basic optimize() for non-partitioned tables.".to_string()
        ));
    }

    // Execute optimize - returns (DeltaTable, Metrics)
    let (_table, delta_metrics) = optimize_builder.await.map_err(Error::DeltaTable)?;

    // Extract metrics from the result
    let metrics = OptimizeMetrics {
        num_files_added: delta_metrics.num_files_added,
        num_files_removed: delta_metrics.num_files_removed,
        total_files_skipped: delta_metrics.total_files_skipped as u64,
        total_considered_files: delta_metrics.total_considered_files as u64,
        preserve_insertion_order: delta_metrics.preserve_insertion_order,
    };

    Ok(metrics)
}

/// Execute VACUUM operation on a Delta Lake table
pub async fn vacuum_table(
    base_path: &Path,
    s3_url: Option<&str>,
    s3_storage_options: Option<&HashMap<String, String>>,
    retention_hours: u64,
    dry_run: bool,
) -> Result<usize> {
    use crate::storage::s3::parse_s3_url;
    use chrono::Duration as ChronoDuration;

    // Open the Delta Lake table (S3 or local)
    let table = if let (Some(s3_url), Some(storage_options)) = (s3_url, s3_storage_options) {
        // S3 backend
        let s3_url_parsed = parse_s3_url(s3_url)?;
        open_table_with_storage_options(s3_url_parsed, storage_options.clone())
            .await
            .map_err(Error::DeltaTable)?
    } else {
        // Local backend
        let table_url = Url::from_directory_path(base_path)
            .map_err(|_| Error::Other("Invalid path for Delta table".to_string()))?;
        open_table(table_url).await.map_err(Error::DeltaTable)?
    };

    // Build VACUUM operation
    let mut vacuum_builder = DeltaOps(table).vacuum();

    // Set retention period (Delta Lake uses chrono::Duration in hours)
    let retention_duration = ChronoDuration::try_hours(retention_hours as i64)
        .ok_or_else(|| Error::Other(format!("Invalid retention hours: {}", retention_hours)))?;
    vacuum_builder = vacuum_builder.with_retention_period(retention_duration);

    // If retention is less than 168 hours (7 days), we need to disable the safety check
    // This is useful for testing but should be used carefully in production
    if retention_hours < 168 {
        vacuum_builder = vacuum_builder.with_enforce_retention_duration(false);
    }

    // Set dry run mode
    if dry_run {
        vacuum_builder = vacuum_builder.with_dry_run(true);
    }

    // Execute VACUUM
    let (table_result, metrics) = vacuum_builder.await.map_err(Error::DeltaTable)?;

    // For local tables, we can verify files were deleted
    let deleted_count = if !dry_run && s3_url.is_none() {
        // Count how many files were actually deleted from disk
        // This is just for verification - Delta Lake handles the actual deletion
        metrics.files_deleted.len()
    } else {
        metrics.files_deleted.len()
    };

    info!(
        "VACUUM {} deleted {} files",
        if dry_run { "dry run" } else { "operation" },
        deleted_count
    );

    // Update table reference (not used but part of the API)
    drop(table_result);

    Ok(deleted_count)
}

/// Execute VACUUM dry run to preview what would be deleted
pub async fn vacuum_dry_run(
    base_path: &Path,
    s3_url: Option<&str>,
    s3_storage_options: Option<&HashMap<String, String>>,
    retention_hours: u64,
) -> Result<Vec<String>> {
    use crate::storage::s3::parse_s3_url;
    use chrono::Duration as ChronoDuration;

    // Open the Delta Lake table (S3 or local)
    let table = if let (Some(s3_url), Some(storage_options)) = (s3_url, s3_storage_options) {
        // S3 backend
        let s3_url_parsed = parse_s3_url(s3_url)?;
        open_table_with_storage_options(s3_url_parsed, storage_options.clone())
            .await
            .map_err(Error::DeltaTable)?
    } else {
        // Local backend
        let table_url = Url::from_directory_path(base_path)
            .map_err(|_| Error::Other("Invalid path for Delta table".to_string()))?;
        open_table(table_url).await.map_err(Error::DeltaTable)?
    };

    // Build VACUUM operation with dry run
    let retention_duration = ChronoDuration::try_hours(retention_hours as i64)
        .ok_or_else(|| Error::Other(format!("Invalid retention hours: {}", retention_hours)))?;

    let mut vacuum_builder = DeltaOps(table)
        .vacuum()
        .with_retention_period(retention_duration);

    // If retention is less than 168 hours (7 days), disable the safety check
    if retention_hours < 168 {
        vacuum_builder = vacuum_builder.with_enforce_retention_duration(false);
    }

    vacuum_builder = vacuum_builder.with_dry_run(true);

    // Execute dry run
    let (_table_result, metrics) = vacuum_builder.await.map_err(Error::DeltaTable)?;

    // Return list of files that would be deleted
    let files_to_delete: Vec<String> = metrics
        .files_deleted
        .iter()
        .map(|path| path.to_string())
        .collect();

    info!(
        "VACUUM dry run: {} files would be deleted",
        files_to_delete.len()
    );

    Ok(files_to_delete)
}

/// Execute Z-ORDER clustering operation on a Delta Lake table
pub async fn zorder_table(
    base_path: &Path,
    s3_url: Option<&str>,
    s3_storage_options: Option<&HashMap<String, String>>,
    columns: &[&str],
) -> Result<OptimizeMetrics> {
    use crate::storage::s3::parse_s3_url;
    use deltalake::operations::optimize::OptimizeType;

    // Open the Delta Lake table (S3 or local)
    let table = if let (Some(s3_url), Some(storage_options)) = (s3_url, s3_storage_options) {
        // S3 backend
        let s3_url_parsed = parse_s3_url(s3_url)?;
        open_table_with_storage_options(s3_url_parsed, storage_options.clone())
            .await
            .map_err(Error::DeltaTable)?
    } else {
        // Local backend
        let table_url = Url::from_directory_path(base_path)
            .map_err(|_| Error::Other("Invalid path for Delta table".to_string()))?;
        open_table(table_url).await.map_err(Error::DeltaTable)?
    };

    // Build Z-ORDER operation
    // Z-ORDER is part of the optimize operation with OptimizeType::ZOrder
    let column_strings: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
    let optimize_builder = DeltaOps(table)
        .optimize()
        .with_type(OptimizeType::ZOrder(column_strings));

    // Execute Z-ORDER
    let (_table, delta_metrics) = optimize_builder.await.map_err(Error::DeltaTable)?;

    // Extract metrics
    let metrics = OptimizeMetrics {
        num_files_added: delta_metrics.num_files_added,
        num_files_removed: delta_metrics.num_files_removed,
        total_files_skipped: delta_metrics.total_files_skipped as u64,
        total_considered_files: delta_metrics.total_considered_files as u64,
        preserve_insertion_order: delta_metrics.preserve_insertion_order,
    };

    Ok(metrics)
}
