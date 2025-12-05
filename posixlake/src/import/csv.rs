//! CSV import with automatic schema inference

use crate::database_ops::DatabaseOps;
use crate::{Error, Result};
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use tracing::info;

/// Create a new database by importing data from a CSV file
///
/// Schema is automatically inferred from the first 10 data rows:
/// - Try parse as Int64 → if all 10 succeed, use Int64
/// - Try parse as Float64 → if all 10 succeed, use Float64
/// - Try parse as Boolean (true/false/1/0) → if all 10 succeed, use Boolean
/// - Otherwise → default to String
///
/// All columns are nullable by default.
///
/// # Arguments
/// * `path` - Path where the Delta Lake database will be created
/// * `csv_path` - Path to the CSV file to import
///
/// # Example
/// ```no_run
/// # use posixlake::import::create_from_csv;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let db = create_from_csv("/path/to/db", "/path/to/data.csv").await?;
/// # Ok(())
/// # }
/// ```
pub async fn create_from_csv<P: AsRef<Path>, C: AsRef<Path>>(
    path: P,
    csv_path: C,
) -> Result<DatabaseOps> {
    let csv_path = csv_path.as_ref();
    info!("Creating database from CSV: {}", csv_path.display());

    // Read CSV file to infer schema
    let file = File::open(csv_path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    // Get header row
    let header_line = lines
        .next()
        .ok_or_else(|| Error::Other("CSV file is empty".to_string()))??;
    let headers: Vec<&str> = header_line.split(',').map(|s| s.trim()).collect();

    // Read first 10 data rows for type inference
    let mut sample_rows: Vec<Vec<String>> = Vec::new();
    for line_result in lines.take(10) {
        let line = line_result?;
        let values: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        sample_rows.push(values);
    }

    // Infer types for each column
    let mut fields = Vec::new();
    for (col_idx, col_name) in headers.iter().enumerate() {
        let col_values: Vec<&str> = sample_rows
            .iter()
            .filter_map(|row| row.get(col_idx).map(|s| s.as_str()))
            .filter(|s| !s.is_empty()) // Skip empty values for inference
            .collect();

        let data_type = infer_column_type(&col_values);
        fields.push(Field::new(*col_name, data_type, true)); // All columns nullable
    }

    let schema = Arc::new(Schema::new(fields));
    info!("Inferred schema: {:?}", schema);

    // Create the database with inferred schema
    let db = DatabaseOps::create(&path, schema.clone()).await?;

    // Now read and insert all CSV data using Arrow CSV reader
    let file = File::open(csv_path)?;
    let csv_reader = ReaderBuilder::new(schema.clone())
        .with_header(true)
        .build(file)?;

    for batch_result in csv_reader {
        let batch = batch_result?;
        if batch.num_rows() > 0 {
            db.insert(batch).await?;
        }
    }

    info!(
        "Successfully imported CSV to Delta Lake at: {}",
        path.as_ref().display()
    );
    Ok(db)
}

/// Infer the data type for a column based on sample values
fn infer_column_type(values: &[&str]) -> DataType {
    if values.is_empty() {
        return DataType::Utf8;
    }

    // Try Int64
    let all_int = values.iter().all(|v| v.parse::<i64>().is_ok());
    if all_int {
        return DataType::Int64;
    }

    // Try Float64
    let all_float = values.iter().all(|v| v.parse::<f64>().is_ok());
    if all_float {
        return DataType::Float64;
    }

    // Try Boolean
    let all_bool = values.iter().all(|v| {
        let lower = v.to_lowercase();
        matches!(lower.as_str(), "true" | "false" | "1" | "0")
    });
    if all_bool {
        return DataType::Boolean;
    }

    // Default to String
    DataType::Utf8
}
