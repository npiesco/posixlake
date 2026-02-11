//! Delta Lake MERGE Operation
//!
//! Implements MERGE (UPSERT) operations for Delta Lake.
//! MERGE allows INSERT, UPDATE, and DELETE in a single atomic transaction.
//!
//! Note: delta-rs 0.29.4 doesn't have native MERGE support, so we implement it
//! using a combination of DataFusion queries and Delta Lake write/delete operations.

use crate::{Error, Result};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use deltalake::operations::write::SchemaMode;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaOps, DeltaTable};
use std::sync::Arc;
use tracing::{debug, info};

/// MERGE operation builder
///
/// Provides a fluent API for building MERGE operations.
///
/// # Example
/// ```no_run
/// use posixlake::delta_lake::merge::MergeBuilder;
/// use deltalake::DeltaTable;
/// use arrow::array::RecordBatch;
///
/// # async fn example(target_table: DeltaTable, source_data: RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
/// // MERGE operation with INSERT/UPDATE/DELETE
/// let result = MergeBuilder::new(target_table)
///     .with_source(source_data, "source")
///     .on("target.id = source.id")
///     .when_matched_update()
///         .condition("source._op = 'UPDATE'")
///         .set("name", "source.name")
///         .set("age", "source.age")
///         .then()
///     .when_matched_delete()
///         .condition("source._op = 'DELETE'")
///         .then()
///     .when_not_matched_insert()
///         .condition("source._op = 'INSERT'")
///         .values_all()
///     .execute()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct MergeBuilder {
    target: DeltaTable,
    source: Option<(RecordBatch, String)>, // (data, alias)
    join_condition: Option<String>,
    matched_updates: Vec<MatchedUpdateClause>,
    matched_deletes: Vec<MatchedDeleteClause>,
    not_matched_inserts: Vec<NotMatchedInsertClause>,
    primary_key: Option<String>,
}

/// Clause for WHEN MATCHED UPDATE
#[derive(Debug, Clone)]
pub struct MatchedUpdateClause {
    pub condition: Option<String>,
    pub updates: Vec<(String, String)>, // (column, expression)
}

/// Clause for WHEN MATCHED DELETE
#[derive(Debug, Clone)]
pub struct MatchedDeleteClause {
    pub condition: Option<String>,
}

/// Clause for WHEN NOT MATCHED INSERT
#[derive(Debug, Clone)]
pub struct NotMatchedInsertClause {
    pub condition: Option<String>,
    pub values: Vec<(String, String)>, // (column, expression)
}

impl MergeBuilder {
    /// Create a new MERGE builder for the target table
    pub fn new(target: DeltaTable) -> Self {
        Self {
            target,
            source: None,
            join_condition: None,
            matched_updates: Vec::new(),
            matched_deletes: Vec::new(),
            not_matched_inserts: Vec::new(),
            primary_key: None,
        }
    }

    /// Set the primary key column name for ID extraction
    pub fn with_primary_key(mut self, pk: Option<String>) -> Self {
        self.primary_key = pk;
        self
    }

    /// Set the source data for the MERGE
    pub fn with_source(mut self, source: RecordBatch, alias: impl Into<String>) -> Self {
        self.source = Some((source, alias.into()));
        self
    }

    /// Set the join condition (e.g., "target.id = source.id")
    pub fn on(mut self, condition: impl Into<String>) -> Self {
        self.join_condition = Some(condition.into());
        self
    }

    /// Add a WHEN MATCHED UPDATE clause
    pub fn when_matched_update(self) -> MatchedUpdateBuilder {
        MatchedUpdateBuilder {
            merge_builder: self,
            condition: None,
            updates: Vec::new(),
        }
    }

    /// Add a WHEN MATCHED DELETE clause
    pub fn when_matched_delete(self) -> MatchedDeleteBuilder {
        MatchedDeleteBuilder {
            merge_builder: self,
            condition: None,
        }
    }

    /// Add a WHEN NOT MATCHED INSERT clause
    pub fn when_not_matched_insert(self) -> NotMatchedInsertBuilder {
        NotMatchedInsertBuilder {
            merge_builder: self,
            condition: None,
            values: Vec::new(),
        }
    }

    /// Execute the MERGE operation
    ///
    /// This performs the MERGE in multiple steps within a single Delta Lake transaction:
    /// 1. Join source and target to identify matches
    /// 2. Apply DELETE operations for matched rows (if any)
    /// 3. Apply UPDATE operations for matched rows (implemented as INSERT of new values)
    /// 4. Apply INSERT operations for unmatched rows
    pub async fn execute(self) -> Result<MergeMetrics> {
        info!("Executing MERGE operation");
        info!(
            "Clauses: {} updates, {} deletes, {} inserts",
            self.matched_updates.len(),
            self.matched_deletes.len(),
            self.not_matched_inserts.len()
        );

        // Validate inputs
        let (source_data, source_alias) = self
            .source
            .as_ref()
            .ok_or_else(|| Error::Other("Source data not provided for MERGE".to_string()))?;

        let join_condition = self
            .join_condition
            .as_ref()
            .ok_or_else(|| Error::Other("Join condition not provided for MERGE".to_string()))?;

        // Create DataFusion context for query execution
        let ctx = SessionContext::new();

        // Register target table directly
        ctx.register_table("target", Arc::new(self.target.clone()))
            .map_err(|e| Error::Other(format!("Failed to register target table: {}", e)))?;

        // Register source data
        let source_schema = source_data.schema();
        let source_table = datafusion::datasource::MemTable::try_new(
            source_schema.clone(),
            vec![vec![(*source_data).clone()]],
        )
        .map_err(|e| Error::Other(format!("Failed to create source table: {}", e)))?;

        ctx.register_table(source_alias, Arc::new(source_table))
            .map_err(|e| Error::Other(format!("Failed to register source table: {}", e)))?;

        let mut metrics = MergeMetrics::default();

        // PHASE 1: Collect all IDs to delete and all batches to insert
        let mut all_ids_to_delete: Vec<String> = Vec::new();
        let mut all_batches_to_insert: Vec<RecordBatch> = Vec::new();

        // Process WHEN MATCHED DELETE clauses
        for delete_clause in &self.matched_deletes {
            let (ids, count) = self
                .collect_matched_delete_ids(&ctx, source_alias, join_condition, delete_clause)
                .await?;
            all_ids_to_delete.extend(ids);
            metrics.rows_deleted += count;
            debug!("DELETE clause matched {} rows", count);
        }

        // Process WHEN MATCHED UPDATE clauses (collect IDs to delete + batches to insert)
        for update_clause in &self.matched_updates {
            let (ids, batches, count) = self
                .collect_matched_update_data(
                    &ctx,
                    source_alias,
                    join_condition,
                    update_clause,
                    source_data,
                )
                .await?;
            all_ids_to_delete.extend(ids);
            all_batches_to_insert.extend(batches);
            metrics.rows_updated += count;
            debug!("UPDATE clause matched {} rows", count);
        }

        // Process WHEN NOT MATCHED INSERT clauses
        for insert_clause in &self.not_matched_inserts {
            let (batches, count) = self
                .collect_not_matched_insert_data(
                    &ctx,
                    source_alias,
                    join_condition,
                    insert_clause,
                    source_data,
                )
                .await?;
            all_batches_to_insert.extend(batches);
            metrics.rows_inserted += count;
            debug!("INSERT clause matched {} rows", count);
        }

        // PHASE 2: Execute batched operations
        // First: DELETE all rows that need to be deleted (from DELETE and UPDATE clauses)
        let mut table_ref = self.target.clone();
        if !all_ids_to_delete.is_empty() {
            debug!(
                "Executing batched DELETE for {} total rows",
                all_ids_to_delete.len()
            );
            // Deduplicate IDs
            all_ids_to_delete.sort_unstable();
            all_ids_to_delete.dedup();

            // Get the actual ID column name and check if it's a string type
            let target_schema = {
                let snapshot = table_ref.snapshot().map_err(Error::DeltaTable)?;
                let delta_schema = snapshot.schema();
                let arrow_fields: Result<Vec<_>> = delta_schema
                    .fields()
                    .map(|f| {
                        let arrow_type =
                            crate::database_ops::DatabaseOps::delta_to_arrow_type(f.data_type())?;
                        Ok(arrow::datatypes::Field::new(
                            f.name(),
                            arrow_type,
                            f.is_nullable(),
                        ))
                    })
                    .collect();
                arrow::datatypes::Schema::new(arrow_fields?)
            };
            let id_col_name = self.id_column_name(&target_schema)?;
            let is_string = self.id_column_is_string(&target_schema);

            let id_list = if is_string {
                all_ids_to_delete
                    .iter()
                    .map(|id| format!("'{}'", id.replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(", ")
            } else {
                all_ids_to_delete.join(", ")
            };
            let delete_predicate = format!("{} IN ({})", id_col_name, id_list);

            // DELETE returns the updated table - use it for subsequent operations
            let (updated_table, _metrics) = DeltaOps(table_ref)
                .delete()
                .with_predicate(delete_predicate)
                .await
                .map_err(Error::DeltaTable)?;
            table_ref = updated_table;

            debug!("Batched DELETE completed successfully");
        }

        // Second: INSERT all rows that need to be inserted (from INSERT and UPDATE clauses)
        // Do this as a SINGLE write operation to avoid transaction conflicts
        // Use the updated table reference from DELETE to avoid conflicts
        if !all_batches_to_insert.is_empty() {
            debug!(
                "Executing batched INSERT for {} batches in single operation",
                all_batches_to_insert.len()
            );

            DeltaOps(table_ref)
                .write(all_batches_to_insert)
                .with_save_mode(SaveMode::Append)
                .with_schema_mode(SchemaMode::Merge)
                .await
                .map_err(Error::DeltaTable)?;

            debug!("Batched INSERT completed successfully");
        }

        info!(
            "MERGE completed: inserted={}, updated={}, deleted={}",
            metrics.rows_inserted, metrics.rows_updated, metrics.rows_deleted
        );

        Ok(metrics)
    }

    /// Collect IDs for WHEN MATCHED DELETE clause (doesn't execute DELETE)
    async fn collect_matched_delete_ids(
        &self,
        ctx: &SessionContext,
        source_alias: &str,
        join_condition: &str,
        clause: &MatchedDeleteClause,
    ) -> Result<(Vec<String>, usize)> {
        debug!("Collecting MATCHED DELETE IDs");

        // Build SQL to find matching rows to delete
        let mut sql = format!(
            "SELECT target.* FROM target INNER JOIN {} ON {}",
            source_alias, join_condition
        );

        if let Some(condition) = &clause.condition {
            sql.push_str(&format!(" WHERE {}", condition));
        }

        debug!("DELETE SQL: {}", sql);

        // Execute query to get matching rows
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| Error::Other(format!("Failed to execute MATCHED DELETE query: {}", e)))?;

        let batches = df.collect().await.map_err(|e| {
            Error::Other(format!("Failed to collect MATCHED DELETE results: {}", e))
        })?;

        if batches.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Extract IDs from matching rows
        let ids = self.extract_ids_from_batches(&batches)?;

        debug!("Collected {} IDs for deletion", ids.len());
        Ok((ids, row_count))
    }

    /// Collect data for WHEN MATCHED UPDATE clause (returns IDs to delete + batches to insert)
    async fn collect_matched_update_data(
        &self,
        ctx: &SessionContext,
        source_alias: &str,
        join_condition: &str,
        clause: &MatchedUpdateClause,
        source_data: &RecordBatch,
    ) -> Result<(Vec<String>, Vec<RecordBatch>, usize)> {
        debug!("Executing MATCHED UPDATE clause");

        // Build SQL to find matching rows with updated values
        let update_columns = if clause.updates.is_empty() {
            // Update all columns (exclude _op column if present)
            source_data
                .schema()
                .fields()
                .iter()
                .filter(|f| f.name() != "_op")
                .map(|f| (f.name().clone(), format!("{}.{}", source_alias, f.name())))
                .collect()
        } else {
            clause.updates.clone()
        };

        let select_list = update_columns
            .iter()
            .map(|(col, expr)| format!("{} as {}", expr, col))
            .collect::<Vec<_>>()
            .join(", ");

        let mut sql = format!(
            "SELECT {} FROM target INNER JOIN {} ON {}",
            select_list, source_alias, join_condition
        );

        if let Some(condition) = &clause.condition {
            sql.push_str(&format!(" WHERE {}", condition));
        }

        // Execute query
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| Error::Other(format!("Failed to execute MATCHED UPDATE query: {}", e)))?;

        let batches = df.collect().await.map_err(|e| {
            Error::Other(format!("Failed to collect MATCHED UPDATE results: {}", e))
        })?;

        if batches.is_empty() {
            return Ok((Vec::new(), Vec::new(), 0));
        }

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Extract IDs to delete
        let ids = self.extract_ids_from_batches(&batches)?;

        debug!(
            "Collected {} rows for update (IDs to delete + batches to insert)",
            row_count
        );
        Ok((ids, batches, row_count))
    }

    /// Collect data for WHEN NOT MATCHED INSERT clause (returns batches to insert)
    async fn collect_not_matched_insert_data(
        &self,
        ctx: &SessionContext,
        source_alias: &str,
        join_condition: &str,
        clause: &NotMatchedInsertClause,
        source_data: &RecordBatch,
    ) -> Result<(Vec<RecordBatch>, usize)> {
        debug!("Executing NOT MATCHED INSERT clause");

        // Build SQL to find non-matching rows (LEFT ANTI JOIN)
        let insert_columns = if clause.values.is_empty() {
            // Insert all columns (exclude _op column if present)
            source_data
                .schema()
                .fields()
                .iter()
                .filter(|f| f.name() != "_op")
                .map(|f| (f.name().clone(), format!("{}.{}", source_alias, f.name())))
                .collect()
        } else {
            clause.values.clone()
        };

        let select_list = insert_columns
            .iter()
            .map(|(col, expr)| format!("{} as {}", expr, col))
            .collect::<Vec<_>>()
            .join(", ");

        let mut sql = format!(
            "SELECT {} FROM {} LEFT ANTI JOIN target ON {}",
            select_list, source_alias, join_condition
        );

        if let Some(condition) = &clause.condition {
            sql.push_str(&format!(" WHERE {}", condition));
        }

        // Execute query
        let df = ctx.sql(&sql).await.map_err(|e| {
            Error::Other(format!("Failed to execute NOT MATCHED INSERT query: {}", e))
        })?;

        let batches = df.collect().await.map_err(|e| {
            Error::Other(format!(
                "Failed to collect NOT MATCHED INSERT results: {}",
                e
            ))
        })?;

        if batches.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        debug!("Collected {} rows for insert", row_count);
        Ok((batches, row_count))
    }

    /// Extract IDs from batches using primary_key or first Int32/Int64 column heuristic
    fn extract_ids_from_batches(&self, batches: &[RecordBatch]) -> Result<Vec<String>> {
        use arrow::array::*;
        use arrow::datatypes::DataType;

        let mut ids = Vec::new();

        for batch in batches {
            let schema = batch.schema();

            // Use primary key if set, otherwise fall back to first integer column
            let id_field = if let Some(pk) = &self.primary_key {
                schema
                    .fields()
                    .iter()
                    .find(|f| f.name() == pk)
                    .ok_or_else(|| {
                        Error::Other(format!("Primary key column '{}' not found in batch", pk))
                    })?
            } else {
                schema
                    .fields()
                    .iter()
                    .find(|f| matches!(f.data_type(), DataType::Int32 | DataType::Int64))
                    .ok_or_else(|| Error::Other("No integer column found for ID".to_string()))?
            };

            let id_array = batch
                .column_by_name(id_field.name())
                .ok_or_else(|| Error::Other("ID column not found in batch".to_string()))?;

            match id_array.data_type() {
                DataType::Int32 => {
                    let int_array =
                        id_array
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .ok_or_else(|| {
                                Error::Other("Failed to downcast to Int32Array".to_string())
                            })?;
                    for i in 0..int_array.len() {
                        if !int_array.is_null(i) {
                            ids.push(int_array.value(i).to_string());
                        }
                    }
                }
                DataType::Int64 => {
                    let int_array =
                        id_array
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| {
                                Error::Other("Failed to downcast to Int64Array".to_string())
                            })?;
                    for i in 0..int_array.len() {
                        if !int_array.is_null(i) {
                            ids.push(int_array.value(i).to_string());
                        }
                    }
                }
                DataType::Utf8 => {
                    let str_array =
                        id_array
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                Error::Other("Failed to downcast to StringArray".to_string())
                            })?;
                    for i in 0..str_array.len() {
                        if !str_array.is_null(i) {
                            ids.push(str_array.value(i).to_string());
                        }
                    }
                }
                DataType::LargeUtf8 => {
                    let str_array = id_array
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(|| {
                            Error::Other("Failed to downcast to LargeStringArray".to_string())
                        })?;
                    for i in 0..str_array.len() {
                        if !str_array.is_null(i) {
                            ids.push(str_array.value(i).to_string());
                        }
                    }
                }
                _ => return Err(Error::Other("Unsupported ID column type".to_string())),
            }
        }

        Ok(ids)
    }

    /// Get the name of the ID column (primary_key or first integer column heuristic)
    fn id_column_name(&self, schema: &arrow::datatypes::Schema) -> Result<String> {
        use arrow::datatypes::DataType;

        if let Some(pk) = &self.primary_key {
            return Ok(pk.clone());
        }

        schema
            .fields()
            .iter()
            .find(|f| matches!(f.data_type(), DataType::Int32 | DataType::Int64))
            .map(|f| f.name().clone())
            .ok_or_else(|| Error::Other("No integer column found for ID".to_string()))
    }

    /// Check if the ID column is a string type (needs quoting in predicates)
    fn id_column_is_string(&self, schema: &arrow::datatypes::Schema) -> bool {
        use arrow::datatypes::DataType;

        let col_name = if let Some(pk) = &self.primary_key {
            pk.as_str()
        } else {
            return false; // heuristic always picks integers
        };

        schema
            .fields()
            .iter()
            .find(|f| f.name() == col_name)
            .map(|f| matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8))
            .unwrap_or(false)
    }
}

/// Builder for WHEN MATCHED UPDATE clause
pub struct MatchedUpdateBuilder {
    merge_builder: MergeBuilder,
    condition: Option<String>,
    updates: Vec<(String, String)>,
}

impl MatchedUpdateBuilder {
    /// Add a condition for this UPDATE clause
    pub fn condition(mut self, condition: impl Into<String>) -> Self {
        self.condition = Some(condition.into());
        self
    }

    /// Set a column to a specific expression
    pub fn set(mut self, column: impl Into<String>, expression: impl Into<String>) -> Self {
        self.updates.push((column.into(), expression.into()));
        self
    }

    /// Update all columns from source (shorthand)
    pub fn set_all(self) -> MergeBuilder {
        // Empty updates vector signals "update all"
        let clause = MatchedUpdateClause {
            condition: self.condition,
            updates: self.updates, // Will be populated during execution if empty
        };

        let mut builder = self.merge_builder;
        builder.matched_updates.push(clause);
        builder
    }

    /// Finish this clause and return to main builder
    pub fn then(self) -> MergeBuilder {
        let clause = MatchedUpdateClause {
            condition: self.condition,
            updates: self.updates,
        };

        let mut builder = self.merge_builder;
        builder.matched_updates.push(clause);
        builder
    }
}

/// Builder for WHEN MATCHED DELETE clause
pub struct MatchedDeleteBuilder {
    merge_builder: MergeBuilder,
    condition: Option<String>,
}

impl MatchedDeleteBuilder {
    /// Add a condition for this DELETE clause
    pub fn condition(mut self, condition: impl Into<String>) -> Self {
        self.condition = Some(condition.into());
        self
    }

    /// Finish this clause and return to main builder
    pub fn then(self) -> MergeBuilder {
        let clause = MatchedDeleteClause {
            condition: self.condition,
        };

        let mut builder = self.merge_builder;
        builder.matched_deletes.push(clause);
        builder
    }
}

/// Builder for WHEN NOT MATCHED INSERT clause
pub struct NotMatchedInsertBuilder {
    merge_builder: MergeBuilder,
    condition: Option<String>,
    values: Vec<(String, String)>,
}

impl NotMatchedInsertBuilder {
    /// Add a condition for this INSERT clause
    pub fn condition(mut self, condition: impl Into<String>) -> Self {
        self.condition = Some(condition.into());
        self
    }

    /// Set a column value for insert
    pub fn value(mut self, column: impl Into<String>, expression: impl Into<String>) -> Self {
        self.values.push((column.into(), expression.into()));
        self
    }

    /// Insert all columns from source (shorthand)
    pub fn values_all(self) -> MergeBuilder {
        let clause = NotMatchedInsertClause {
            condition: self.condition,
            values: self.values, // Will be populated during execution if empty
        };

        let mut builder = self.merge_builder;
        builder.not_matched_inserts.push(clause);
        builder
    }

    /// Finish this clause and return to main builder
    pub fn then(self) -> MergeBuilder {
        let clause = NotMatchedInsertClause {
            condition: self.condition,
            values: self.values,
        };

        let mut builder = self.merge_builder;
        builder.not_matched_inserts.push(clause);
        builder
    }
}

/// Metrics from MERGE operation
#[derive(Debug, Default, Clone)]
pub struct MergeMetrics {
    pub rows_inserted: usize,
    pub rows_updated: usize,
    pub rows_deleted: usize,
}

impl MergeMetrics {
    pub fn total_rows_affected(&self) -> usize {
        self.rows_inserted + self.rows_updated + self.rows_deleted
    }
}
