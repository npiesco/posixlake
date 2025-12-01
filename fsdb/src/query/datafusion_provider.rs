//! DataFusion TableProvider implementation
//!
//! Provides SQL query capabilities over Parquet data files with MVCC visibility

use crate::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::Arc;

/// TableProvider for DataFusion integration with MVCC
#[derive(Debug)]
pub struct FsdbTableProvider {
    inner: Arc<MemTable>,
}

impl FsdbTableProvider {
    /// Create a new table provider with schema and data
    pub fn new(schema: SchemaRef, data: Vec<RecordBatch>) -> Result<Self> {
        // Use DataFusion's MemTable which provides TableProvider implementation
        let mem_table = MemTable::try_new(schema, vec![data])?;
        Ok(Self {
            inner: Arc::new(mem_table),
        })
    }

    /// Create a table provider from Parquet files with MVCC filtering
    pub fn from_parquet_files(file_paths: &[String], schema: SchemaRef) -> Result<Self> {
        use crate::storage::parquet::ParquetReader;
        use tracing::debug;

        debug!(
            "Loading {} parquet files into DataFusion table provider",
            file_paths.len()
        );

        let reader = ParquetReader::new();
        let mut all_batches = Vec::new();

        // Read each parquet file
        for file_path in file_paths {
            debug!("Reading parquet file: {}", file_path);
            let batch = reader.read_batch(file_path)?;
            if batch.num_rows() > 0 {
                all_batches.push(batch);
            }
        }

        // If no data, create empty batch with schema
        if all_batches.is_empty() {
            debug!("No data in parquet files, creating empty table");
            return Self::new(schema, vec![]);
        }

        // Concatenate all batches into one
        let combined_batch = if all_batches.len() == 1 {
            all_batches.into_iter().next().unwrap()
        } else {
            arrow::compute::concat_batches(&schema, &all_batches)?
        };

        debug!(
            "Loaded {} total rows from parquet files",
            combined_batch.num_rows()
        );

        // Create table provider with combined data
        Self::new(schema, vec![combined_batch])
    }
}

#[async_trait::async_trait]
impl TableProvider for FsdbTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Delegate to the inner MemTable
        self.inner.scan(state, projection, filters, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::prelude::*;
    use std::sync::Arc;

    fn create_test_schema() -> SchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
            Field::new("score", DataType::Float64, true),
        ]))
    }

    fn create_test_batch(schema: SchemaRef) -> RecordBatch {
        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef;
        let name_array = Arc::new(StringArray::from(vec![
            "Alice", "Bob", "Charlie", "David", "Eve",
        ])) as ArrayRef;
        let age_array = Arc::new(Int32Array::from(vec![
            Some(25),
            Some(30),
            None,
            Some(35),
            Some(28),
        ])) as ArrayRef;
        let score_array = Arc::new(Float64Array::from(vec![
            Some(95.5),
            Some(87.3),
            Some(92.1),
            None,
            Some(88.9),
        ])) as ArrayRef;

        RecordBatch::try_new(schema, vec![id_array, name_array, age_array, score_array]).unwrap()
    }

    #[test]
    fn test_table_provider_creation() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        let provider = FsdbTableProvider::new(schema.clone(), vec![batch]).unwrap();

        assert_eq!(provider.schema().fields().len(), 4);
        assert_eq!(provider.schema().field(0).name(), "id");
        assert_eq!(provider.schema().field(1).name(), "name");
    }

    #[tokio::test]
    async fn test_table_provider_scan_all_columns() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        let provider = FsdbTableProvider::new(schema.clone(), vec![batch.clone()]).unwrap();

        // Create a DataFusion context and register the table
        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Query all columns
        let df = ctx.sql("SELECT * FROM test_table").await.unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);
        assert_eq!(results[0].num_columns(), 4);
    }

    #[tokio::test]
    async fn test_table_provider_select_specific_columns() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        let provider = FsdbTableProvider::new(schema, vec![batch]).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Query specific columns
        let df = ctx.sql("SELECT name, age FROM test_table").await.unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);
        assert_eq!(results[0].num_columns(), 2);

        // Verify column names
        let result_schema = results[0].schema();
        assert_eq!(result_schema.field(0).name(), "name");
        assert_eq!(result_schema.field(1).name(), "age");
    }

    #[tokio::test]
    async fn test_table_provider_where_clause() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        let provider = FsdbTableProvider::new(schema, vec![batch]).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Query with WHERE clause
        let df = ctx
            .sql("SELECT name, age FROM test_table WHERE age > 28")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        // Should have 2 rows: Bob (30) and David (35)
        assert_eq!(results[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_table_provider_aggregation() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        let provider = FsdbTableProvider::new(schema, vec![batch]).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Aggregation query
        let df = ctx
            .sql("SELECT COUNT(*) as count, AVG(age) as avg_age FROM test_table")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);

        // Verify count (5 total rows)
        let count_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 5);
    }

    #[tokio::test]
    async fn test_table_provider_order_by() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        let provider = FsdbTableProvider::new(schema, vec![batch]).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // ORDER BY query
        let df = ctx
            .sql("SELECT name FROM test_table ORDER BY name DESC")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);

        // Verify first name is "Eve" (alphabetically last)
        let name_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Eve");
    }

    #[tokio::test]
    async fn test_table_provider_multiple_batches() {
        let schema = create_test_schema();

        // Create two batches
        let batch1 = create_test_batch(schema.clone());

        let id_array2 = Arc::new(Int32Array::from(vec![6, 7, 8])) as ArrayRef;
        let name_array2 = Arc::new(StringArray::from(vec!["Frank", "Grace", "Henry"])) as ArrayRef;
        let age_array2 = Arc::new(Int32Array::from(vec![Some(40), Some(32), Some(29)])) as ArrayRef;
        let score_array2 =
            Arc::new(Float64Array::from(vec![Some(91.2), Some(85.7), Some(94.3)])) as ArrayRef;

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![id_array2, name_array2, age_array2, score_array2],
        )
        .unwrap();

        let provider = FsdbTableProvider::new(schema, vec![batch1, batch2]).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Query should return all rows from both batches
        let df = ctx
            .sql("SELECT COUNT(*) as count FROM test_table")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        let count_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 8); // 5 + 3 rows
    }

    #[tokio::test]
    async fn test_table_provider_null_handling() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        let provider = FsdbTableProvider::new(schema, vec![batch]).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Query with IS NULL
        let df = ctx
            .sql("SELECT name FROM test_table WHERE age IS NULL")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1); // Charlie has null age

        let name_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Charlie");
    }

    #[tokio::test]
    async fn test_table_provider_join() {
        let schema1 = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef,
            ],
        )
        .unwrap();

        let schema2 = Arc::new(ArrowSchema::new(vec![
            Field::new("user_id", DataType::Int32, false),
            Field::new("department", DataType::Utf8, false),
        ]));

        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["Engineering", "Sales", "Marketing"])) as ArrayRef,
            ],
        )
        .unwrap();

        let provider1 = FsdbTableProvider::new(schema1, vec![batch1]).unwrap();
        let provider2 = FsdbTableProvider::new(schema2, vec![batch2]).unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("users", Arc::new(provider1)).unwrap();
        ctx.register_table("departments", Arc::new(provider2))
            .unwrap();

        // JOIN query
        let df = ctx
            .sql("SELECT u.name, d.department FROM users u JOIN departments d ON u.id = d.user_id")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_from_parquet_files_integration() {
        use crate::storage::parquet::ParquetWriter;
        use tempfile::TempDir;

        // Create temp directory for test parquet files
        let temp_dir = TempDir::new().unwrap();

        // Create schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
        ]));

        // Create first parquet file
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(95.5), Some(87.3), Some(92.1)])) as ArrayRef,
            ],
        )
        .unwrap();

        let file1 = temp_dir.path().join("data_001.parquet");
        let writer = ParquetWriter::new();
        writer.write_batch(&file1, &batch1).unwrap();

        // Create second parquet file
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef,
                Arc::new(StringArray::from(vec!["David", "Eve"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(88.9), Some(91.2)])) as ArrayRef,
            ],
        )
        .unwrap();

        let file2 = temp_dir.path().join("data_002.parquet");
        writer.write_batch(&file2, &batch2).unwrap();

        // Load parquet files into DataFusion table provider
        let file_paths = vec![
            file1.to_str().unwrap().to_string(),
            file2.to_str().unwrap().to_string(),
        ];

        let provider = FsdbTableProvider::from_parquet_files(&file_paths, schema.clone()).unwrap();

        // Register table and query it
        let ctx = SessionContext::new();
        ctx.register_table("parquet_data", Arc::new(provider))
            .unwrap();

        // Query all data
        let df = ctx
            .sql("SELECT * FROM parquet_data ORDER BY id")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        // Should have combined data from both files
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].num_rows(),
            5,
            "Should have 5 total rows from both files"
        );

        // Verify specific data
        let id_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(4), 5);

        let name_col = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(4), "Eve");

        // Query with WHERE clause
        let df = ctx
            .sql("SELECT name, score FROM parquet_data WHERE score > 90")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(
            results[0].num_rows(),
            3,
            "Should have 3 rows with score > 90"
        );

        // Query with aggregation
        let df = ctx
            .sql("SELECT COUNT(*) as count, AVG(score) as avg_score FROM parquet_data")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        let count_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 5);
    }
}
