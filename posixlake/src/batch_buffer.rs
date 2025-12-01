//! RecordBatch buffering for reducing transaction overhead
//!
//! Accumulates RecordBatches and flushes them in larger transactions to Delta Lake.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

/// Configuration for batch buffering
#[derive(Debug, Clone)]
pub struct BatchBufferConfig {
    /// Maximum number of rows before auto-flush
    pub max_rows: usize,
}

impl Default for BatchBufferConfig {
    fn default() -> Self {
        Self { max_rows: 1000 }
    }
}

/// Batch buffer state
struct BufferState {
    batches: Vec<RecordBatch>,
}

/// RecordBatch buffer for batching small writes
pub struct BatchBuffer {
    config: BatchBufferConfig,
    state: Arc<Mutex<BufferState>>,
}

impl BatchBuffer {
    /// Create a new batch buffer with default config
    pub fn new(_schema: SchemaRef) -> Self {
        Self::with_config(BatchBufferConfig::default())
    }

    /// Create a new batch buffer with custom config
    pub fn with_config(config: BatchBufferConfig) -> Self {
        debug!("Creating batch buffer with max_rows: {}", config.max_rows);
        Self {
            config,
            state: Arc::new(Mutex::new(BufferState {
                batches: Vec::new(),
            })),
        }
    }

    /// Add a batch to the buffer, returns true if auto-flush should occur
    pub async fn push(&self, batch: RecordBatch) -> bool {
        let mut state = self.state.lock().await;
        state.batches.push(batch);

        let total_rows: usize = state.batches.iter().map(|b| b.num_rows()).sum();
        debug!(
            "Buffer now has {} batches with {} total rows",
            state.batches.len(),
            total_rows
        );

        total_rows >= self.config.max_rows
    }

    /// Take all batches from buffer and return them
    pub async fn take_all(&self) -> Vec<RecordBatch> {
        let mut state = self.state.lock().await;
        std::mem::take(&mut state.batches)
    }

    /// Check if buffer is empty
    pub async fn is_empty(&self) -> bool {
        let state = self.state.lock().await;
        state.batches.is_empty()
    }

    /// Get current buffer stats (batch count, total rows)
    pub async fn stats(&self) -> (usize, usize) {
        let state = self.state.lock().await;
        let batch_count = state.batches.len();
        let total_rows = state.batches.iter().map(|b| b.num_rows()).sum();
        (batch_count, total_rows)
    }

    /// Concatenate all batches into a single batch
    pub fn concatenate_batches(
        schema: &SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> crate::Result<RecordBatch> {
        if batches.is_empty() {
            return Err(crate::Error::InvalidOperation(
                "Cannot concatenate empty batch list".into(),
            ));
        }

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        let concatenated =
            arrow::compute::concat_batches(schema, &batches).map_err(crate::Error::Arrow)?;

        info!(
            "Concatenated {} batches into single batch with {} rows",
            batches.len(),
            concatenated.num_rows()
        );

        Ok(concatenated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_test_batch(schema: SchemaRef, id: i32) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![id])) as ArrayRef,
                Arc::new(StringArray::from(vec![format!("row_{}", id)])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_batch_buffer_push() {
        let schema = create_test_schema();
        let buffer = BatchBuffer::new(schema.clone());

        let batch = create_test_batch(schema.clone(), 1);
        let should_flush = buffer.push(batch).await;

        assert!(!should_flush);
        assert!(!buffer.is_empty().await);

        let (count, rows) = buffer.stats().await;
        assert_eq!(count, 1);
        assert_eq!(rows, 1);
    }

    #[tokio::test]
    async fn test_batch_buffer_auto_flush_threshold() {
        let schema = create_test_schema();
        let config = BatchBufferConfig { max_rows: 3 };
        let buffer = BatchBuffer::with_config(config);

        // Add 2 batches (2 rows)
        buffer.push(create_test_batch(schema.clone(), 1)).await;
        let should_flush = buffer.push(create_test_batch(schema.clone(), 2)).await;
        assert!(!should_flush);

        // Add 1 more batch (3 rows total) - should trigger flush
        let should_flush = buffer.push(create_test_batch(schema.clone(), 3)).await;
        assert!(should_flush);
    }

    #[tokio::test]
    async fn test_batch_buffer_take_all() {
        let schema = create_test_schema();
        let buffer = BatchBuffer::new(schema.clone());

        buffer.push(create_test_batch(schema.clone(), 1)).await;
        buffer.push(create_test_batch(schema.clone(), 2)).await;

        let batches = buffer.take_all().await;
        assert_eq!(batches.len(), 2);
        assert!(buffer.is_empty().await);
    }

    #[tokio::test]
    async fn test_concatenate_batches() {
        let schema = create_test_schema();

        let batches = vec![
            create_test_batch(schema.clone(), 1),
            create_test_batch(schema.clone(), 2),
            create_test_batch(schema.clone(), 3),
        ];

        let result = BatchBuffer::concatenate_batches(&schema, batches).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 2);
    }
}
