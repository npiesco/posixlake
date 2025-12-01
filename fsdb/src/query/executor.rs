//! Query executor with DataFusion integration
//!
//! Provides SQL query execution over Parquet data files with MVCC transaction support

use crate::query::FsdbTableProvider;
use crate::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::prelude::*;
use std::sync::Arc;
use tracing::{debug, info};

/// Query executor for SQL operations with DataFusion
pub struct QueryExecutor {
    ctx: SessionContext,
    registration_lock: tokio::sync::Mutex<()>,
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryExecutor {
    /// Create a new query executor with DataFusion SessionContext
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
            registration_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Register a table with the executor (thread-safe)
    pub async fn register_table(
        &self,
        table_name: &str,
        schema: SchemaRef,
        data: Vec<RecordBatch>,
    ) -> Result<()> {
        // Lock to prevent concurrent registration of the same table
        let _lock = self.registration_lock.lock().await;

        debug!(
            "Registering table '{}' with {} batches",
            table_name,
            data.len()
        );

        // Deregister table if it already exists
        let _ = self.ctx.deregister_table(table_name);

        let provider = FsdbTableProvider::new(schema, data)?;
        self.ctx.register_table(table_name, Arc::new(provider))?;

        info!("Successfully registered table '{}'", table_name);
        Ok(())
    }

    /// Execute SQL query and return results as RecordBatches
    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        info!("Executing SQL: {}", sql);

        let df = self.ctx.sql(sql).await?;
        let results = df.collect().await?;

        debug!("Query returned {} batches", results.len());
        Ok(results)
    }

    /// Execute SQL query and return result count
    pub async fn execute_sql_count(&self, sql: &str) -> Result<usize> {
        let results = self.execute_sql(sql).await?;
        let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
        Ok(total_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("department", DataType::Utf8, false),
            Field::new("salary", DataType::Float64, true),
        ]))
    }

    fn create_test_batch(schema: SchemaRef) -> RecordBatch {
        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef;
        let name_array = Arc::new(StringArray::from(vec![
            "Alice", "Bob", "Charlie", "David", "Eve",
        ])) as ArrayRef;
        let dept_array = Arc::new(StringArray::from(vec![
            "Engineering",
            "Sales",
            "Engineering",
            "Marketing",
            "Sales",
        ])) as ArrayRef;
        let salary_array = Arc::new(Float64Array::from(vec![
            Some(120000.0),
            Some(85000.0),
            Some(95000.0),
            Some(78000.0),
            Some(92000.0),
        ])) as ArrayRef;

        RecordBatch::try_new(schema, vec![id_array, name_array, dept_array, salary_array]).unwrap()
    }

    #[test]
    fn test_executor_creation() {
        let _executor = QueryExecutor::new();
    }

    #[tokio::test]
    async fn test_execute_simple_select() {
        let executor = QueryExecutor::new();
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        executor
            .register_table("employees", schema, vec![batch])
            .await
            .unwrap();

        // Simple SELECT *
        let results = executor
            .execute_sql("SELECT * FROM employees")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);
        assert_eq!(results[0].num_columns(), 4);
    }

    #[tokio::test]
    async fn test_execute_select_with_projection() {
        let executor = QueryExecutor::new();
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        executor
            .register_table("employees", schema, vec![batch])
            .await
            .unwrap();

        // SELECT specific columns
        let results = executor
            .execute_sql("SELECT name, department FROM employees")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);
        assert_eq!(results[0].num_columns(), 2);

        // Verify column names
        let result_schema = results[0].schema();
        assert_eq!(result_schema.field(0).name(), "name");
        assert_eq!(result_schema.field(1).name(), "department");
    }

    #[tokio::test]
    async fn test_execute_where_clause() {
        let executor = QueryExecutor::new();
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        executor
            .register_table("employees", schema, vec![batch])
            .await
            .unwrap();

        // WHERE clause filtering
        let results = executor
            .execute_sql("SELECT name, salary FROM employees WHERE salary > 90000")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3); // Alice (120k), Charlie (95k), Eve (92k)
    }

    #[tokio::test]
    async fn test_execute_aggregation() {
        let executor = QueryExecutor::new();
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        executor
            .register_table("employees", schema, vec![batch])
            .await
            .unwrap();

        // COUNT and AVG aggregation
        let results = executor
            .execute_sql("SELECT COUNT(*) as count, AVG(salary) as avg_salary FROM employees")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);

        // Verify count
        let count_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 5);
    }

    #[tokio::test]
    async fn test_execute_group_by() {
        let executor = QueryExecutor::new();
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        executor
            .register_table("employees", schema, vec![batch])
            .await
            .unwrap();

        // GROUP BY query
        let results = executor
            .execute_sql(
                "SELECT department, COUNT(*) as employee_count FROM employees GROUP BY department",
            )
            .await
            .unwrap();

        // Count total rows across all batches
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // Engineering, Sales, Marketing
    }

    #[tokio::test]
    async fn test_execute_order_by() {
        let executor = QueryExecutor::new();
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        executor
            .register_table("employees", schema, vec![batch])
            .await
            .unwrap();

        // ORDER BY query
        let results = executor
            .execute_sql("SELECT name, salary FROM employees ORDER BY salary DESC")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);

        // Verify first row is highest salary (Alice - 120k)
        let name_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");
    }

    #[tokio::test]
    async fn test_execute_join() {
        let executor = QueryExecutor::new();

        // First table: employees
        let emp_schema = Arc::new(Schema::new(vec![
            Field::new("emp_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("dept_id", DataType::Int32, false),
        ]));

        let emp_batch = RecordBatch::try_new(
            emp_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef,
                Arc::new(Int32Array::from(vec![10, 20, 10])) as ArrayRef,
            ],
        )
        .unwrap();

        // Second table: departments
        let dept_schema = Arc::new(Schema::new(vec![
            Field::new("dept_id", DataType::Int32, false),
            Field::new("dept_name", DataType::Utf8, false),
        ]));

        let dept_batch = RecordBatch::try_new(
            dept_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
                Arc::new(StringArray::from(vec!["Engineering", "Sales", "Marketing"])) as ArrayRef,
            ],
        )
        .unwrap();

        executor
            .register_table("employees", emp_schema, vec![emp_batch])
            .await
            .unwrap();
        executor
            .register_table("departments", dept_schema, vec![dept_batch])
            .await
            .unwrap();

        // JOIN query
        let results = executor.execute_sql(
            "SELECT e.name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.dept_id"
        ).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3); // All 3 employees have matching departments
    }

    #[tokio::test]
    async fn test_execute_multiple_batches() {
        let executor = QueryExecutor::new();
        let schema = create_test_schema();

        // Create two batches
        let batch1 = create_test_batch(schema.clone());

        let id_array2 = Arc::new(Int32Array::from(vec![6, 7, 8])) as ArrayRef;
        let name_array2 = Arc::new(StringArray::from(vec!["Frank", "Grace", "Henry"])) as ArrayRef;
        let dept_array2 =
            Arc::new(StringArray::from(vec!["Engineering", "Sales", "Marketing"])) as ArrayRef;
        let salary_array2 = Arc::new(Float64Array::from(vec![
            Some(88000.0),
            Some(91000.0),
            Some(79000.0),
        ])) as ArrayRef;

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![id_array2, name_array2, dept_array2, salary_array2],
        )
        .unwrap();

        executor
            .register_table("employees", schema, vec![batch1, batch2])
            .await
            .unwrap();

        // Query should return all rows from both batches
        let count = executor
            .execute_sql_count("SELECT * FROM employees")
            .await
            .unwrap();
        assert_eq!(count, 8); // 5 + 3 rows
    }

    #[tokio::test]
    async fn test_execute_complex_query() {
        let executor = QueryExecutor::new();
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        executor
            .register_table("employees", schema, vec![batch])
            .await
            .unwrap();

        // Complex query with WHERE, GROUP BY, HAVING, ORDER BY
        let results = executor
            .execute_sql(
                "SELECT department, AVG(salary) as avg_salary 
             FROM employees 
             WHERE salary > 80000 
             GROUP BY department 
             HAVING AVG(salary) > 90000 
             ORDER BY avg_salary DESC",
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        // Should return Engineering (avg of Alice and Charlie's salaries > 90k)
        assert!(results[0].num_rows() > 0);
    }
}
