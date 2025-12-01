//! Parquet file I/O for columnar data storage
//!
//! Features:
//! - Write Arrow RecordBatch to Parquet files
//! - Read Parquet files back to RecordBatch
//! - Compression support (Snappy default)
//! - Column statistics and metadata
//! - Filename convention: `data_<timestamp>_<txn_id>.parquet`

use crate::Result;
use arrow::array::{RecordBatch, RecordBatchReader};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use tracing::{debug, info};

/// Generate Parquet filename with timestamp and transaction ID
pub fn parquet_filename(timestamp_ms: i64, txn_id: u64) -> String {
    format!("data_{}_{}.parquet", timestamp_ms, txn_id)
}

/// Parquet file writer with Snappy compression
pub struct ParquetWriter {
    /// Writer properties (compression, encoding, etc.)
    properties: WriterProperties,
}

impl ParquetWriter {
    /// Create a new Parquet writer with Snappy compression (default)
    pub fn new() -> Self {
        Self::with_compression(Compression::SNAPPY)
    }

    /// Create a Parquet writer with custom compression
    pub fn with_compression(compression: Compression) -> Self {
        let properties = WriterProperties::builder()
            .set_compression(compression)
            .set_encoding(Encoding::PLAIN)
            .set_dictionary_enabled(true)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            .set_max_row_group_size(1024 * 1024) // 1M rows per row group
            .build();

        Self { properties }
    }

    /// Write a RecordBatch to a Parquet file
    pub fn write_batch<P: AsRef<Path>>(&self, path: P, batch: &RecordBatch) -> Result<()> {
        let path = path.as_ref();
        debug!(
            "Writing RecordBatch to Parquet: {} ({} rows, {} columns)",
            path.display(),
            batch.num_rows(),
            batch.num_columns()
        );

        // Create the file
        let file = File::create(path)?;

        // Create Arrow writer with our properties
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(self.properties.clone()))?;

        // Write the batch
        writer.write(batch)?;

        // Close and finalize (writes metadata)
        writer.close()?;

        info!(
            "Successfully wrote Parquet file: {} ({} bytes)",
            path.display(),
            std::fs::metadata(path)?.len()
        );

        Ok(())
    }
}

impl Default for ParquetWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Parquet file reader
pub struct ParquetReader;

impl ParquetReader {
    /// Create a new Parquet reader
    pub fn new() -> Self {
        Self
    }

    /// Read a Parquet file into a RecordBatch
    ///
    /// Note: This reads the entire file into memory. For large files,
    /// consider using the streaming API (future enhancement).
    pub fn read_batch<P: AsRef<Path>>(&self, path: P) -> Result<RecordBatch> {
        let path = path.as_ref();
        debug!("Reading Parquet file: {}", path.display());

        // Open the file
        let file = File::open(path)?;

        // Create Parquet reader builder
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // Get metadata for logging
        let metadata = builder.metadata();
        let num_row_groups = metadata.num_row_groups();
        let total_rows: i64 = (0..num_row_groups)
            .map(|i| metadata.row_group(i).num_rows())
            .sum();

        debug!(
            "Parquet file metadata: {} row groups, {} total rows",
            num_row_groups, total_rows
        );

        // Build the reader
        let mut reader = builder.build()?;

        // Read all batches and concatenate them
        let mut batches = Vec::new();
        for batch in reader.by_ref() {
            batches.push(batch?);
        }

        if batches.is_empty() {
            // Handle empty file - create an empty batch with the schema
            let schema = reader.schema();
            let empty_batch = RecordBatch::new_empty(schema);
            info!("Read empty Parquet file: {}", path.display());
            return Ok(empty_batch);
        }

        // Concatenate all batches into a single batch
        let schema = batches[0].schema();
        let combined_batch = arrow::compute::concat_batches(&schema, &batches)?;

        info!(
            "Successfully read Parquet file: {} ({} rows, {} columns)",
            path.display(),
            combined_batch.num_rows(),
            combined_batch.num_columns()
        );

        Ok(combined_batch)
    }
}

impl Default for ParquetReader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BooleanArray, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use std::fs;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Helper to create a test RecordBatch with multiple data types
    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, false),
        ]));

        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let name_array = Arc::new(StringArray::from(vec![
            "Alice", "Bob", "Charlie", "Diana", "Eve",
        ]));
        let score_array = Arc::new(Float64Array::from(vec![
            Some(95.5),
            Some(87.3),
            None, // NULL value
            Some(92.1),
            Some(88.8),
        ]));
        let active_array = Arc::new(BooleanArray::from(vec![true, false, true, true, false]));

        RecordBatch::try_new(
            schema,
            vec![id_array, name_array, score_array, active_array],
        )
        .expect("Failed to create test batch")
    }

    #[test]
    fn test_parquet_filename_generation() {
        let filename = parquet_filename(1704153600000, 42);
        assert_eq!(filename, "data_1704153600000_42.parquet");

        let filename2 = parquet_filename(0, 0);
        assert_eq!(filename2, "data_0_0.parquet");
    }

    #[test]
    fn test_parquet_write_and_read_round_trip() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_data.parquet");

        // Create test data
        let original_batch = create_test_batch();

        // Write to Parquet
        let writer = ParquetWriter::new();
        writer
            .write_batch(&file_path, &original_batch)
            .expect("Failed to write Parquet file");

        // Verify file exists
        assert!(file_path.exists(), "Parquet file should exist");
        let metadata = fs::metadata(&file_path).unwrap();
        assert!(metadata.len() > 0, "Parquet file should not be empty");

        // Read back from Parquet
        let reader = ParquetReader::new();
        let read_batch = reader
            .read_batch(&file_path)
            .expect("Failed to read Parquet file");

        // Verify schema matches
        assert_eq!(
            original_batch.schema(),
            read_batch.schema(),
            "Schema should match"
        );

        // Verify row count
        assert_eq!(
            original_batch.num_rows(),
            read_batch.num_rows(),
            "Row count should match"
        );

        // Verify column count
        assert_eq!(
            original_batch.num_columns(),
            read_batch.num_columns(),
            "Column count should match"
        );

        // Verify data integrity (column by column)
        for col_idx in 0..original_batch.num_columns() {
            let original_col = original_batch.column(col_idx);
            let read_col = read_batch.column(col_idx);
            assert_eq!(
                original_col, read_col,
                "Column {} data should match",
                col_idx
            );
        }
    }

    #[test]
    fn test_parquet_write_with_null_values() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_nulls.parquet");

        let batch = create_test_batch(); // Contains NULL in score column

        let writer = ParquetWriter::new();
        writer
            .write_batch(&file_path, &batch)
            .expect("Should handle NULL values");

        let reader = ParquetReader::new();
        let read_batch = reader.read_batch(&file_path).unwrap();

        // Verify NULL is preserved
        let score_col = read_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(score_col.is_null(2), "NULL value should be preserved");
        assert!(score_col.is_valid(0), "Non-NULL value should be valid");
    }

    #[test]
    fn test_parquet_write_empty_batch() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_empty.parquet");

        // Create empty batch with schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Arc::new(Int32Array::from(Vec::<i32>::new()));
        let name_array = Arc::new(StringArray::from(Vec::<&str>::new()));

        let empty_batch = RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap();

        let writer = ParquetWriter::new();
        writer
            .write_batch(&file_path, &empty_batch)
            .expect("Should handle empty batch");

        let reader = ParquetReader::new();
        let read_batch = reader.read_batch(&file_path).unwrap();

        assert_eq!(read_batch.num_rows(), 0, "Should have 0 rows");
        assert_eq!(read_batch.num_columns(), 2, "Should have 2 columns");
    }

    #[test]
    fn test_parquet_read_nonexistent_file() {
        let reader = ParquetReader::new();
        let result = reader.read_batch("/nonexistent/file.parquet");

        assert!(result.is_err(), "Should fail on nonexistent file");
    }

    #[test]
    fn test_parquet_write_multiple_batches_same_schema() {
        let temp_dir = TempDir::new().unwrap();

        // Write batch 1
        let batch1 = create_test_batch();
        let file1 = temp_dir.path().join(parquet_filename(1000, 1));
        let writer = ParquetWriter::new();
        writer.write_batch(&file1, &batch1).unwrap();

        // Write batch 2 (different data, same schema)
        let schema = batch1.schema();
        let id_array = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let name_array = Arc::new(StringArray::from(vec!["X", "Y", "Z"]));
        let score_array = Arc::new(Float64Array::from(vec![
            Some(100.0),
            Some(200.0),
            Some(300.0),
        ]));
        let active_array = Arc::new(BooleanArray::from(vec![false, false, true]));

        let batch2 = RecordBatch::try_new(
            schema,
            vec![id_array, name_array, score_array, active_array],
        )
        .unwrap();

        let file2 = temp_dir.path().join(parquet_filename(2000, 2));
        writer.write_batch(&file2, &batch2).unwrap();

        // Read both files
        let reader = ParquetReader::new();
        let read1 = reader.read_batch(&file1).unwrap();
        let read2 = reader.read_batch(&file2).unwrap();

        assert_eq!(read1.num_rows(), 5);
        assert_eq!(read2.num_rows(), 3);
        assert_eq!(read1.schema(), read2.schema());
    }
}
