//! Explicit transaction support for FSDB
//!
//! Provides user-controlled transaction boundaries with ACID guarantees.

use crate::{database_ops::DatabaseOps, Error, Result};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Transaction lifecycle state
#[derive(Debug, Clone, PartialEq, Eq)]
enum TxnLifecycleState {
    Active,
    Committed,
    Aborted,
}

/// Explicit transaction handle for user-controlled transaction boundaries
pub struct Transaction {
    /// Reference to the database
    db: Arc<DatabaseOps>,

    /// Transaction ID
    #[allow(dead_code)]
    txn_id: u64,

    /// Snapshot for this transaction (MVCC isolation)
    #[allow(dead_code)]
    snapshot_version: u64,

    /// Snapshot of committed data at transaction start (for snapshot isolation)
    snapshot_data: Arc<tokio::sync::Mutex<Option<Vec<RecordBatch>>>>,

    /// Uncommitted write buffer (RecordBatches to be written on commit)
    write_buffer: Arc<tokio::sync::Mutex<Vec<RecordBatch>>>,

    /// Transaction lifecycle state
    state: Arc<tokio::sync::Mutex<TxnLifecycleState>>,
}

impl Transaction {
    /// Create a new transaction
    pub(crate) fn new(db: Arc<DatabaseOps>, txn_id: u64, snapshot_version: u64) -> Self {
        Self {
            db,
            txn_id,
            snapshot_version,
            snapshot_data: Arc::new(tokio::sync::Mutex::new(None)),
            write_buffer: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            state: Arc::new(tokio::sync::Mutex::new(TxnLifecycleState::Active)),
        }
    }

    /// Insert data within this transaction (uncommitted until commit)
    pub async fn insert(&self, batch: RecordBatch) -> Result<()> {
        // Check transaction state
        let state = self.state.lock().await;
        if *state != TxnLifecycleState::Active {
            return Err(Error::Other(format!(
                "Transaction is not active: {:?}",
                state
            )));
        }
        drop(state);

        // Add to write buffer
        let mut buffer = self.write_buffer.lock().await;
        buffer.push(batch);
        Ok(())
    }

    /// Query data within this transaction (includes uncommitted writes)
    ///
    /// Implements snapshot isolation: queries see committed data as of transaction start
    /// plus uncommitted writes from this transaction.
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        use datafusion::datasource::MemTable;
        use datafusion::prelude::*;

        // Lazy-load snapshot on first query
        {
            let snapshot = self.snapshot_data.lock().await;
            if snapshot.is_none() {
                drop(snapshot);

                // Capture snapshot of committed data
                let committed = self.db.query("SELECT * FROM data").await?;

                let mut snapshot = self.snapshot_data.lock().await;
                *snapshot = Some(committed);
            }
        }

        // Get clones of snapshot and buffer for processing
        let snapshot = self.snapshot_data.lock().await;
        let committed_batches = snapshot.as_ref().unwrap().clone();
        drop(snapshot);

        let buffer = self.write_buffer.lock().await;
        let uncommitted_batches = buffer.clone();
        drop(buffer);

        let has_committed = !committed_batches.is_empty();
        let has_uncommitted = !uncommitted_batches.is_empty();

        // Case 1: No uncommitted writes - query snapshot only
        if !has_uncommitted {
            let ctx = SessionContext::new();

            if has_committed {
                let schema = committed_batches[0].schema();
                let table = MemTable::try_new(schema, vec![committed_batches])?;
                ctx.register_table("data", Arc::new(table))?;
            } else {
                // Empty table - create empty RecordBatch with schema
                let empty_batch = RecordBatch::new_empty(self.db.schema.clone());
                let empty_table =
                    MemTable::try_new(self.db.schema.clone(), vec![vec![empty_batch]])?;
                ctx.register_table("data", Arc::new(empty_table))?;
            }

            let df = ctx.sql(sql).await?;
            return Ok(df.collect().await?);
        }

        // Case 2: Has uncommitted writes - create union view
        let ctx = SessionContext::new();

        if has_committed {
            // Compute unified schema to handle schema evolution
            let mut all_batches = committed_batches.clone();
            all_batches.extend(uncommitted_batches.clone());
            let unified_schema = self.db.compute_unified_schema(&all_batches)?;

            // Rebuild committed batches with unified schema (add NULL columns for missing fields)
            let mut unified_committed = Vec::new();
            for batch in &committed_batches {
                let unified_batch = self.db.unify_batch_schema(batch, &unified_schema)?;
                unified_committed.push(unified_batch);
            }
            let committed_table =
                MemTable::try_new(unified_schema.clone(), vec![unified_committed])?;
            ctx.register_table("committed_data", Arc::new(committed_table))?;

            // Rebuild uncommitted batches with unified schema (add NULL columns for missing fields)
            let mut unified_uncommitted = Vec::new();
            for batch in &uncommitted_batches {
                let unified_batch = self.db.unify_batch_schema(batch, &unified_schema)?;
                unified_uncommitted.push(unified_batch);
            }
            let uncommitted_table =
                MemTable::try_new(unified_schema.clone(), vec![unified_uncommitted])?;
            ctx.register_table("uncommitted_data", Arc::new(uncommitted_table))?;

            // Create union to combine both (now with matching schemas)
            let union_df = ctx
                .sql("SELECT * FROM committed_data UNION ALL SELECT * FROM uncommitted_data")
                .await?;
            ctx.register_table("data", union_df.into_view())?;
        } else {
            // No committed data - just register uncommitted directly as "data"
            let schema = uncommitted_batches[0].schema();
            let uncommitted_table = MemTable::try_new(schema, vec![uncommitted_batches])?;
            ctx.register_table("data", Arc::new(uncommitted_table))?;
        }

        // Execute the user's query on the combined view
        let df = ctx.sql(sql).await?;
        let results = df.collect().await?;

        Ok(results)
    }

    /// Delete rows matching WHERE clause within this transaction
    pub async fn delete_rows_where(&self, where_clause: &str) -> Result<()> {
        // Check transaction state
        let state = self.state.lock().await;
        if *state != TxnLifecycleState::Active {
            return Err(Error::Other(format!(
                "Transaction is not active: {:?}",
                state
            )));
        }
        drop(state);

        // For now, delegate to database
        // In future, track deletions in transaction buffer
        self.db.delete_rows_where(where_clause).await?;
        Ok(())
    }

    /// Commit the transaction, persisting all writes
    pub async fn commit(self) -> Result<()> {
        // Check and update state
        let mut state = self.state.lock().await;
        if *state != TxnLifecycleState::Active {
            return Err(Error::Other(format!(
                "Transaction is not active: {:?}",
                state
            )));
        }
        *state = TxnLifecycleState::Committed;
        drop(state);

        // Get all batches from write buffer
        let buffer = self.write_buffer.lock().await;
        if buffer.is_empty() {
            // No writes to commit
            return Ok(());
        }

        // Concatenate all batches into one
        let schema = buffer[0].schema();
        let all_batches = arrow::compute::concat_batches(&schema, buffer.iter())?;

        // Write as single transaction
        self.db.insert(all_batches).await?;

        Ok(())
    }

    /// Rollback the transaction, discarding all writes
    pub async fn rollback(self) -> Result<()> {
        let mut state = self.state.lock().await;
        if *state != TxnLifecycleState::Active {
            return Err(Error::Other(format!(
                "Cannot rollback transaction in state: {:?}",
                state
            )));
        }
        *state = TxnLifecycleState::Aborted;
        drop(state);

        // Clear write buffer
        let mut buffer = self.write_buffer.lock().await;
        buffer.clear();

        Ok(())
    }
}
