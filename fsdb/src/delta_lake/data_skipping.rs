//! Data Skipping - File-level pruning based on min/max statistics
//!
//! This module implements data skipping by reading per-file statistics from Delta Lake
//! transaction logs and evaluating query predicates against them to skip files that
//! cannot contain matching data.

use crate::Result;
use std::collections::HashMap;
use std::path::Path;
use tracing::{debug, info};

/// Per-file statistics from Delta Lake transaction log
#[derive(Debug, Clone)]
pub struct FileStats {
    pub path: String,
    pub size_bytes: u64,
    pub min_values: HashMap<String, serde_json::Value>,
    pub max_values: HashMap<String, serde_json::Value>,
    pub null_counts: HashMap<String, u64>,
    pub num_records: u64,
}

/// Extract per-file statistics from Delta Lake transaction log
///
/// Reads all add actions from the transaction log and extracts min/max statistics
/// for each Parquet file.
pub fn get_file_statistics(base_path: &Path) -> Result<Vec<FileStats>> {
    let delta_log_path = base_path.join("_delta_log");

    if !delta_log_path.exists() {
        return Ok(Vec::new());
    }

    // Read all Delta Lake commit files
    let mut commit_files = Vec::new();
    for entry in std::fs::read_dir(&delta_log_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
            let filename = path.file_name().unwrap().to_str().unwrap();
            // Skip checkpoint files for now
            if filename.contains(".checkpoint.") {
                continue;
            }
            if let Some(version_str) = filename.strip_suffix(".json") {
                if let Ok(version) = version_str.parse::<u64>() {
                    commit_files.push((version, path.clone()));
                }
            }
        }
    }

    // Sort by version
    commit_files.sort_by_key(|(v, _)| *v);

    // Track currently active files (path -> FileStats)
    // Need to handle remove actions too
    let mut active_files: HashMap<String, FileStats> = HashMap::new();

    // Read stats from each commit file (JSONL format)
    for (_version, commit_path) in commit_files {
        let content = std::fs::read_to_string(&commit_path)?;

        // Parse each line as JSON
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }

            if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                // Look for "add" actions which contain file stats
                if let Some(add) = json.get("add") {
                    let path = add
                        .get("path")
                        .and_then(|p| p.as_str())
                        .unwrap_or("")
                        .to_string();

                    let size_bytes = add.get("size").and_then(|s| s.as_u64()).unwrap_or(0);

                    let num_records = add.get("numRecords").and_then(|n| n.as_u64()).unwrap_or(0);

                    let mut min_values = HashMap::new();
                    let mut max_values = HashMap::new();
                    let mut null_counts = HashMap::new();

                    // Parse stats JSON (might be a string or object)
                    if let Some(stats) = add.get("stats") {
                        let stats_obj = if let Some(stats_str) = stats.as_str() {
                            // Stats is a JSON string, parse it
                            serde_json::from_str::<serde_json::Value>(stats_str).ok()
                        } else {
                            // Stats is already an object
                            Some(stats.clone())
                        };

                        if let Some(stats_json) = stats_obj {
                            // Extract minValues
                            if let Some(min_vals) = stats_json.get("minValues") {
                                if let Some(min_obj) = min_vals.as_object() {
                                    for (col_name, val) in min_obj {
                                        min_values.insert(col_name.clone(), val.clone());
                                    }
                                }
                            }

                            // Extract maxValues
                            if let Some(max_vals) = stats_json.get("maxValues") {
                                if let Some(max_obj) = max_vals.as_object() {
                                    for (col_name, val) in max_obj {
                                        max_values.insert(col_name.clone(), val.clone());
                                    }
                                }
                            }

                            // Extract nullCount
                            if let Some(null_cnts) = stats_json.get("nullCount") {
                                if let Some(null_obj) = null_cnts.as_object() {
                                    for (col_name, val) in null_obj {
                                        if let Some(count) = val.as_u64() {
                                            null_counts.insert(col_name.clone(), count);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    let file_stats = FileStats {
                        path: path.clone(),
                        size_bytes,
                        min_values,
                        max_values,
                        null_counts,
                        num_records,
                    };

                    active_files.insert(path, file_stats);
                }

                // Look for "remove" actions to handle deleted files
                if let Some(remove) = json.get("remove") {
                    if let Some(path) = remove.get("path").and_then(|p| p.as_str()) {
                        active_files.remove(path);
                    }
                }
            }
        }
    }

    debug!(
        "Extracted statistics for {} active files",
        active_files.len()
    );
    Ok(active_files.into_values().collect())
}

/// Evaluate if a file can be skipped based on a predicate
///
/// Returns true if the file can definitely be skipped (doesn't contain matching data).
/// Returns false if the file might contain matching data (needs to be read).
pub fn can_skip_file(
    file_stats: &FileStats,
    column: &str,
    operator: &str,
    value: &serde_json::Value,
) -> bool {
    // Get min/max for the column
    let min_val = file_stats.min_values.get(column);
    let max_val = file_stats.max_values.get(column);

    if min_val.is_none() || max_val.is_none() {
        // No statistics available, cannot skip
        return false;
    }

    let min_val = min_val.unwrap();
    let max_val = max_val.unwrap();

    // Evaluate predicate against file statistics
    match operator {
        ">" => {
            // col > value: Skip if max(col) <= value
            !is_value_greater_than(max_val, value)
        }
        ">=" => {
            // col >= value: Skip if max(col) < value
            is_value_less_than(max_val, value)
        }
        "<" => {
            // col < value: Skip if min(col) >= value
            !is_value_less_than(min_val, value)
        }
        "<=" => {
            // col <= value: Skip if min(col) > value
            is_value_greater_than(min_val, value)
        }
        "=" | "==" => {
            // col = value: Skip if value < min(col) OR value > max(col)
            is_value_less_than(value, min_val) || is_value_greater_than(value, max_val)
        }
        _ => {
            // Unknown operator, cannot skip
            false
        }
    }
}

/// Compare JSON values (less than)
fn is_value_less_than(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Number(a_num), serde_json::Value::Number(b_num)) => {
            if let (Some(a_i64), Some(b_i64)) = (a_num.as_i64(), b_num.as_i64()) {
                a_i64 < b_i64
            } else if let (Some(a_f64), Some(b_f64)) = (a_num.as_f64(), b_num.as_f64()) {
                a_f64 < b_f64
            } else {
                false
            }
        }
        (serde_json::Value::String(a_str), serde_json::Value::String(b_str)) => a_str < b_str,
        _ => false,
    }
}

/// Compare JSON values (greater than)
fn is_value_greater_than(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Number(a_num), serde_json::Value::Number(b_num)) => {
            if let (Some(a_i64), Some(b_i64)) = (a_num.as_i64(), b_num.as_i64()) {
                a_i64 > b_i64
            } else if let (Some(a_f64), Some(b_f64)) = (a_num.as_f64(), b_num.as_f64()) {
                a_f64 > b_f64
            } else {
                false
            }
        }
        (serde_json::Value::String(a_str), serde_json::Value::String(b_str)) => a_str > b_str,
        _ => false,
    }
}

/// Parse SQL query to extract simple predicates
///
/// This is a simple parser that extracts basic WHERE conditions like:
/// - age > 55
/// - value >= 210 AND value <= 290
/// - category >= 'W'
pub fn extract_predicates(sql: &str) -> Vec<(String, String, serde_json::Value)> {
    let mut predicates = Vec::new();

    // Find WHERE clause
    let sql_upper = sql.to_uppercase();
    if let Some(where_idx) = sql_upper.find("WHERE") {
        let where_clause = &sql[where_idx + 5..];

        // Split by AND/OR (simple approach)
        let parts: Vec<&str> = where_clause.split("AND").collect();

        for part in parts {
            let part = part.trim();

            // Try to match: column operator value
            // Support: >, >=, <, <=, =
            for op in &[">=", "<=", ">", "<", "="] {
                if let Some(op_idx) = part.find(op) {
                    let column = part[..op_idx].trim().to_string();
                    let value_str = part[op_idx + op.len()..].trim();

                    // Remove ORDER BY, LIMIT, etc.
                    let value_str = value_str.split_whitespace().next().unwrap_or(value_str);

                    // Parse value (int, float, or string)
                    let value = if value_str.starts_with('\'') || value_str.starts_with('"') {
                        // String value
                        let cleaned = value_str.trim_matches('\'').trim_matches('"');
                        serde_json::Value::String(cleaned.to_string())
                    } else if let Ok(i) = value_str.parse::<i64>() {
                        // Integer value
                        serde_json::json!(i)
                    } else if let Ok(f) = value_str.parse::<f64>() {
                        // Float value
                        serde_json::json!(f)
                    } else {
                        continue;
                    };

                    predicates.push((column, op.to_string(), value));
                    break; // Found operator, don't try others
                }
            }
        }
    }

    info!("Extracted {} predicates from query", predicates.len());
    predicates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_predicate_extraction() {
        let sql = "SELECT * FROM data WHERE age > 55";
        let predicates = extract_predicates(sql);
        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].0, "age");
        assert_eq!(predicates[0].1, ">");
        assert_eq!(predicates[0].2, serde_json::json!(55));
    }

    #[test]
    fn test_range_predicate_extraction() {
        let sql = "SELECT * FROM data WHERE value >= 210 AND value <= 290";
        let predicates = extract_predicates(sql);
        assert_eq!(predicates.len(), 2);
    }

    #[test]
    fn test_string_predicate_extraction() {
        let sql = "SELECT * FROM data WHERE category >= 'W'";
        let predicates = extract_predicates(sql);
        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].0, "category");
        assert_eq!(predicates[0].1, ">=");
        assert_eq!(predicates[0].2, serde_json::Value::String("W".to_string()));
    }
}
