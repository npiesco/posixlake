// Write Buffer for batching small writes to reduce database transaction overhead
//
// Architecture:
// - Buffer small writes until threshold is reached
// - Flush automatically on size limit or timeout
// - Batch multiple CSV rows into single database transaction
// - Reduce overhead from many small writes

use crate::database_ops::DatabaseOps;
use crate::error::Result;
use arrow::csv::ReaderBuilder as CsvReaderBuilder;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, info};

/// Configuration for write buffering
#[derive(Debug, Clone)]
pub struct WriteBufferConfig {
    /// Maximum number of rows to buffer before auto-flush
    pub max_rows: usize,

    /// Maximum buffer size in bytes before auto-flush
    pub max_bytes: usize,

    /// Maximum time to buffer before auto-flush
    pub max_duration: Duration,
}

impl Default for WriteBufferConfig {
    fn default() -> Self {
        Self {
            max_rows: 1000,                       // Flush after 1000 rows
            max_bytes: 1_048_576,                 // Flush after 1 MB
            max_duration: Duration::from_secs(5), // Flush after 5 seconds
        }
    }
}

/// Write buffer for batching small writes
pub struct WriteBuffer {
    db: Arc<DatabaseOps>,
    config: WriteBufferConfig,
    buffer: Arc<Mutex<BufferState>>,
}

struct BufferState {
    /// Buffered CSV data
    data: Vec<u8>,

    /// Number of rows in buffer (approximate, based on newlines)
    row_count: usize,

    /// Time when first write was buffered
    first_write: Option<Instant>,
}

impl WriteBuffer {
    /// Create a new write buffer
    pub fn new(db: Arc<DatabaseOps>) -> Self {
        Self::with_config(db, WriteBufferConfig::default())
    }

    /// Create a new write buffer with custom configuration
    pub fn with_config(db: Arc<DatabaseOps>, config: WriteBufferConfig) -> Self {
        debug!("Creating write buffer with config: {:?}", config);
        Self {
            db,
            config,
            buffer: Arc::new(Mutex::new(BufferState {
                data: Vec::new(),
                row_count: 0,
                first_write: None,
            })),
        }
    }

    /// Append data to buffer, auto-flush if thresholds exceeded
    pub async fn write(&self, data: &[u8]) -> Result<()> {
        let mut state = self.buffer.lock().await;

        // Set first write time if this is the first write
        if state.first_write.is_none() {
            state.first_write = Some(Instant::now());
        }

        // Append data to buffer
        state.data.extend_from_slice(data);

        // Count rows (approximate - count newlines)
        state.row_count += data.iter().filter(|&&b| b == b'\n').count();

        debug!(
            "Write buffered: {} bytes, {} rows total",
            data.len(),
            state.row_count
        );

        // Check if we need to flush
        let should_flush = self.should_flush(&state);

        if should_flush {
            info!(
                "Auto-flushing buffer: {} rows, {} bytes",
                state.row_count,
                state.data.len()
            );
            drop(state); // Release lock before flush
            self.flush().await?;
        }

        Ok(())
    }

    /// Flush buffered writes to database
    pub async fn flush(&self) -> Result<()> {
        // Get data from buffer
        let csv_data = {
            let state = self.buffer.lock().await;

            if state.data.is_empty() {
                debug!("Buffer is empty, nothing to flush");
                return Ok(());
            }

            info!(
                "Flushing write buffer: {} rows, {} bytes",
                state.row_count,
                state.data.len()
            );
            state.data.clone()
        }; // Lock released here

        // Parse CSV and insert into database (without holding lock)
        self.parse_and_insert(&csv_data).await?;

        // Clear buffer after successful flush
        {
            let mut state = self.buffer.lock().await;
            state.data.clear();
            state.row_count = 0;
            state.first_write = None;
        }

        info!("Write buffer flushed successfully");
        Ok(())
    }

    /// Check if buffer should be flushed
    fn should_flush(&self, state: &BufferState) -> bool {
        // Flush if row count exceeds threshold
        if state.row_count >= self.config.max_rows {
            debug!(
                "Row count threshold reached: {} >= {}",
                state.row_count, self.config.max_rows
            );
            return true;
        }

        // Flush if size exceeds threshold
        if state.data.len() >= self.config.max_bytes {
            debug!(
                "Size threshold reached: {} >= {}",
                state.data.len(),
                self.config.max_bytes
            );
            return true;
        }

        // Flush if duration threshold exceeded
        if let Some(first_write) = state.first_write {
            let elapsed = first_write.elapsed();
            if elapsed >= self.config.max_duration {
                debug!(
                    "Duration threshold reached: {:?} >= {:?}",
                    elapsed, self.config.max_duration
                );
                return true;
            }
        }

        false
    }

    /// Parse CSV data and insert into database
    async fn parse_and_insert(&self, csv_data: &[u8]) -> Result<()> {
        let csv_str = String::from_utf8_lossy(csv_data);
        debug!("Parsing buffered CSV data: {} bytes", csv_data.len());

        // Add schema header for CSV parsing
        let schema = self.db.schema();
        let column_names: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        let header = column_names.join(",");
        let csv_with_header = format!("{}\n{}", header, csv_str);

        // Parse CSV into RecordBatch
        let mut reader = CsvReaderBuilder::new(schema.clone())
            .with_header(true)
            .build(std::io::Cursor::new(csv_with_header.as_bytes()))?;

        let mut batches = Vec::new();
        for batch in reader.by_ref() {
            batches.push(batch?);
        }

        if batches.is_empty() {
            debug!("No valid rows to insert");
            return Ok(());
        }

        // Concatenate all batches
        let unified_batch = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            arrow::compute::concat_batches(&schema, &batches)?
        };

        info!(
            "Inserting {} buffered rows into database",
            unified_batch.num_rows()
        );
        self.db.insert(unified_batch).await?;

        Ok(())
    }

    /// Get current buffer stats
    pub async fn stats(&self) -> (usize, usize) {
        let state = self.buffer.lock().await;
        (state.row_count, state.data.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database_ops::DatabaseOps;
    use arrow::datatypes::{DataType, Field, Schema};
    use tempfile::TempDir;

    async fn create_test_db() -> (DatabaseOps, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let db = DatabaseOps::create(temp_dir.path().join("test_db"), schema)
            .await
            .unwrap();
        (db, temp_dir)
    }

    #[tokio::test]
    async fn test_write_buffer_basic() {
        let (db, _temp) = create_test_db().await;
        let buffer = WriteBuffer::new(Arc::new(db));

        // Write some data
        let data = b"1,Alice,100\n2,Bob,200\n";
        buffer.write(data).await.unwrap();

        // Check stats
        let (rows, bytes) = buffer.stats().await;
        assert_eq!(rows, 2);
        assert_eq!(bytes, data.len());

        // Flush
        buffer.flush().await.unwrap();

        // Buffer should be empty after flush
        let (rows, bytes) = buffer.stats().await;
        assert_eq!(rows, 0);
        assert_eq!(bytes, 0);
    }

    #[tokio::test]
    async fn test_write_buffer_auto_flush_on_size() {
        let (db, _temp) = create_test_db().await;

        let config = WriteBufferConfig {
            max_rows: 5, // Auto-flush after 5 rows
            max_bytes: 1_000_000,
            max_duration: Duration::from_secs(60),
        };

        let buffer = WriteBuffer::with_config(Arc::new(db), config);

        // Write 3 rows
        buffer.write(b"1,Alice,100\n").await.unwrap();
        buffer.write(b"2,Bob,200\n").await.unwrap();
        buffer.write(b"3,Charlie,300\n").await.unwrap();

        let (rows, _) = buffer.stats().await;
        assert_eq!(rows, 3);

        // Write 2 more rows - should trigger auto-flush
        buffer.write(b"4,Dave,400\n").await.unwrap();
        buffer.write(b"5,Eve,500\n").await.unwrap();

        // Buffer should be empty after auto-flush
        let (rows, bytes) = buffer.stats().await;
        assert_eq!(rows, 0);
        assert_eq!(bytes, 0);
    }

    #[tokio::test]
    async fn test_write_buffer_manual_flush() {
        let (db, _temp) = create_test_db().await;
        let db = Arc::new(db);
        let buffer = WriteBuffer::new(db.clone());

        // Write data
        buffer.write(b"1,Alice,100\n").await.unwrap();
        buffer.write(b"2,Bob,200\n").await.unwrap();

        // Manual flush
        buffer.flush().await.unwrap();

        // Verify data was inserted
        let results = db
            .query("SELECT COUNT(*) as count FROM data")
            .await
            .unwrap();
        assert!(!results.is_empty());
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 1);
    }
}
