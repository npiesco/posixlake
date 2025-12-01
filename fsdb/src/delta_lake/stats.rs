//! Column statistics computation and Delta Lake stats integration

use crate::Result;
use arrow::array::*;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use tracing::info;

/// Column statistics for a single column
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub min_value: serde_json::Value,
    pub max_value: serde_json::Value,
    pub null_count: u64,
}

/// Type alias for column statistics maps (min, max, null_count)
pub type ColumnStatsMaps = (
    HashMap<String, serde_json::Value>,
    HashMap<String, serde_json::Value>,
    HashMap<String, u64>,
);

/// Compute column statistics from a RecordBatch
pub fn compute_column_statistics(batch: &RecordBatch) -> Result<ColumnStatsMaps> {
    let mut min_values = HashMap::new();
    let mut max_values = HashMap::new();
    let mut null_counts = HashMap::new();

    let schema = batch.schema();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let column = batch.column(col_idx);
        let col_name = field.name().clone();

        // Count nulls
        let null_count = column.null_count() as u64;
        null_counts.insert(col_name.clone(), null_count);

        // Compute min/max based on data type
        match field.data_type() {
            DataType::Int32 => {
                if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
                    if let Some(min) = arrow::compute::min(array) {
                        min_values.insert(col_name.clone(), serde_json::json!(min));
                    }
                    if let Some(max) = arrow::compute::max(array) {
                        max_values.insert(col_name.clone(), serde_json::json!(max));
                    }
                }
            }
            DataType::Int64 => {
                if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
                    if let Some(min) = arrow::compute::min(array) {
                        min_values.insert(col_name.clone(), serde_json::json!(min));
                    }
                    if let Some(max) = arrow::compute::max(array) {
                        max_values.insert(col_name.clone(), serde_json::json!(max));
                    }
                }
            }
            DataType::Float32 => {
                if let Some(array) = column.as_any().downcast_ref::<Float32Array>() {
                    if let Some(min) = arrow::compute::min(array) {
                        min_values.insert(col_name.clone(), serde_json::json!(min));
                    }
                    if let Some(max) = arrow::compute::max(array) {
                        max_values.insert(col_name.clone(), serde_json::json!(max));
                    }
                }
            }
            DataType::Float64 => {
                if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
                    if let Some(min) = arrow::compute::min(array) {
                        min_values.insert(col_name.clone(), serde_json::json!(min));
                    }
                    if let Some(max) = arrow::compute::max(array) {
                        max_values.insert(col_name.clone(), serde_json::json!(max));
                    }
                }
            }
            DataType::Utf8 => {
                if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                    if let Some(min) = arrow::compute::min_string(array) {
                        min_values.insert(col_name.clone(), serde_json::json!(min));
                    }
                    if let Some(max) = arrow::compute::max_string(array) {
                        max_values.insert(col_name.clone(), serde_json::json!(max));
                    }
                }
            }
            _ => {}
        }
    }

    Ok((min_values, max_values, null_counts))
}

/// Get column statistics from Delta Lake transaction log
pub fn get_column_statistics_from_delta(
    base_path: &std::path::Path,
) -> Result<HashMap<String, ColumnStats>> {
    use crate::query::pruning::{is_value_greater_than, is_value_less_than};

    let delta_log_path = base_path.join("_delta_log");
    let mut column_stats: HashMap<String, ColumnStats> = HashMap::new();

    // Read all Delta Lake commit files
    let mut commit_files = Vec::new();
    for entry in std::fs::read_dir(&delta_log_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
            let filename = path.file_name().unwrap().to_str().unwrap();
            if let Some(version_str) = filename.strip_suffix(".json") {
                if let Ok(version) = version_str.parse::<u64>() {
                    commit_files.push((version, path.clone()));
                }
            }
        }
    }

    // Sort by version
    commit_files.sort_by_key(|(v, _)| *v);

    // Read stats from each commit file (JSONL format)
    for (_version, commit_path) in commit_files {
        let content = std::fs::read_to_string(&commit_path)?;

        // Parse each line as JSON
        for line in content.lines() {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                // Look for "add" actions which contain file stats
                if let Some(add) = json.get("add") {
                    if let Some(stats_str) = add.get("stats").and_then(|s| s.as_str()) {
                        // Parse the stats JSON string
                        if let Ok(stats_json) = serde_json::from_str::<serde_json::Value>(stats_str)
                        {
                            // Extract minValues, maxValues, nullCount
                            if let (Some(min_vals), Some(max_vals), Some(null_counts)) = (
                                stats_json.get("minValues"),
                                stats_json.get("maxValues"),
                                stats_json.get("nullCount"),
                            ) {
                                // Process each column
                                if let Some(min_obj) = min_vals.as_object() {
                                    for (col_name, min_val) in min_obj {
                                        if let Some(max_val) = max_vals.get(col_name) {
                                            let null_count = null_counts
                                                .get(col_name)
                                                .and_then(|v| v.as_u64())
                                                .unwrap_or(0);

                                            // Update global min/max for this column
                                            column_stats
                                                .entry(col_name.clone())
                                                .and_modify(|stats| {
                                                    if is_value_less_than(min_val, &stats.min_value)
                                                    {
                                                        stats.min_value = min_val.clone();
                                                    }
                                                    if is_value_greater_than(
                                                        max_val,
                                                        &stats.max_value,
                                                    ) {
                                                        stats.max_value = max_val.clone();
                                                    }
                                                    stats.null_count += null_count;
                                                })
                                                .or_insert(ColumnStats {
                                                    min_value: min_val.clone(),
                                                    max_value: max_val.clone(),
                                                    null_count,
                                                });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(column_stats)
}

/// Find Delta version from timestamp
pub fn find_delta_version_by_timestamp(
    restore_path: &std::path::Path,
    target_timestamp: u64,
) -> Result<u64> {
    let delta_log_path = restore_path.join("_delta_log");

    // Find highest version
    let mut max_version = 0u64;
    for entry in std::fs::read_dir(&delta_log_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
            let filename = path.file_name().unwrap().to_str().unwrap();
            if let Some(version_str) = filename.strip_suffix(".json") {
                if let Ok(version) = version_str.parse::<u64>() {
                    if version > max_version {
                        max_version = version;
                    }
                }
            }
        }
    }

    // Find the version whose timestamp is closest to but not after target_timestamp
    let mut target_version = 0u64;

    for version in 0..=max_version {
        let commit_file = format!("{:020}.json", version);
        let commit_path = delta_log_path.join(&commit_file);

        if commit_path.exists() {
            let content = std::fs::read_to_string(&commit_path)?;

            // Parse each line as JSON to find commitInfo
            for line in content.lines() {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                    if let Some(commit_info) = json.get("commitInfo") {
                        if let Some(timestamp) = commit_info.get("timestamp") {
                            if let Some(ts) = timestamp.as_i64() {
                                let commit_ts = ts as u64;
                                if commit_ts <= target_timestamp {
                                    target_version = version;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    info!(
        "Mapped timestamp {} to Delta version {}",
        target_timestamp, target_version
    );
    Ok(target_version)
}
