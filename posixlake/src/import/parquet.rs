//! Parquet import - convert raw Parquet files to Delta Lake

use crate::database_ops::DatabaseOps;
use crate::{Error, Result};
use glob::glob;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

/// Create a new database by importing data from Parquet file(s)
///
/// Schema is read directly from Parquet file metadata (no inference needed).
/// Supports single file path or glob pattern (e.g., "*.parquet").
///
/// # Arguments
/// * `path` - Path where the Delta Lake database will be created
/// * `parquet_path` - Path to a single Parquet file or glob pattern
///
/// # Example
/// ```no_run
/// # use posixlake::import::create_from_parquet;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Single file
/// let db = create_from_parquet("/path/to/db", "/path/to/data.parquet").await?;
///
/// // Glob pattern
/// let db = create_from_parquet("/path/to/db", "/path/to/*.parquet").await?;
/// # Ok(())
/// # }
/// ```
pub async fn create_from_parquet<P: AsRef<Path>, Q: AsRef<Path>>(
    path: P,
    parquet_path: Q,
) -> Result<DatabaseOps> {
    let parquet_path = parquet_path.as_ref();
    info!("Creating database from Parquet: {}", parquet_path.display());

    // Resolve glob pattern or single file
    let parquet_files: Vec<PathBuf> = if parquet_path.to_string_lossy().contains('*') {
        // Glob pattern
        let pattern = parquet_path.to_string_lossy();
        glob(&pattern)
            .map_err(|e| Error::Other(format!("Invalid glob pattern: {}", e)))?
            .filter_map(|r| r.ok())
            .collect()
    } else {
        // Single file
        vec![parquet_path.to_path_buf()]
    };

    if parquet_files.is_empty() {
        return Err(Error::Other(
            "No Parquet files found matching pattern".to_string(),
        ));
    }

    info!("Found {} Parquet file(s) to import", parquet_files.len());

    // Read schema from first Parquet file
    let first_file = File::open(&parquet_files[0])?;
    let parquet_reader = SerializedFileReader::new(first_file)
        .map_err(|e| Error::Other(format!("Failed to read Parquet file: {}", e)))?;

    let schema_descr = parquet_reader.metadata().file_metadata().schema_descr();
    let arrow_schema = parquet_to_arrow_schema(
        schema_descr,
        parquet_reader
            .metadata()
            .file_metadata()
            .key_value_metadata(),
    )
    .map_err(|e| Error::Other(format!("Failed to convert Parquet schema to Arrow: {}", e)))?;

    let schema = Arc::new(arrow_schema);
    info!("Read schema from Parquet: {:?}", schema);

    // Create the database with the schema
    let db = DatabaseOps::create(&path, schema.clone()).await?;

    // Read and insert data from all Parquet files
    for parquet_file in &parquet_files {
        let file = File::open(parquet_file)?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::Other(format!("Failed to create Parquet reader: {}", e)))?;

        let reader = builder
            .build()
            .map_err(|e| Error::Other(format!("Failed to build Parquet reader: {}", e)))?;

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| Error::Other(format!("Failed to read Parquet batch: {}", e)))?;
            if batch.num_rows() > 0 {
                db.insert(batch).await?;
            }
        }
    }

    info!(
        "Successfully imported {} Parquet file(s) to Delta Lake at: {}",
        parquet_files.len(),
        path.as_ref().display()
    );
    Ok(db)
}
