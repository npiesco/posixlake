// Python bindings for FSDB using UniFFI
//
// This module provides Python bindings for all FSDB functionality including:
// - Database creation/opening (local and S3)
// - Data insertion and querying (including JSON support)
// - Time travel (version and timestamp-based)
// - Delta Lake operations (OPTIMIZE, VACUUM, Z-ORDER)
// - Authentication and RBAC
// - Backup and restore
// - Monitoring and health checks
// - Data skipping statistics

use crate::database_ops::DatabaseOps as CoreDatabaseOps;
use crate::error::Error as CoreError;
use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Error types for FSDB Python bindings
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum FsdbError {
    #[error("IO error: {message}")]
    IoError { message: String },

    #[error("Serialization error: {message}")]
    SerializationError { message: String },

    #[error("ObjectStore error: {message}")]
    ObjectStoreError { message: String },

    #[error("Arrow error: {message}")]
    ArrowError { message: String },

    #[error("Parquet error: {message}")]
    ParquetError { message: String },

    #[error("Database not found: {message}")]
    DatabaseNotFound { message: String },

    #[error("Record not found: {message}")]
    RecordNotFound { message: String },

    #[error("Database already exists: {message}")]
    DatabaseAlreadyExists { message: String },

    #[error("Invalid operation: {message}")]
    InvalidOperation { message: String },

    #[error("Transaction conflict: {message}")]
    TransactionConflict { message: String },

    #[error("WAL error: {message}")]
    WalError { message: String },

    #[error("Bincode error: {message}")]
    BincodeError { message: String },

    #[error("Delta Lake error: {message}")]
    DeltaLakeError { message: String },

    #[error("{message}")]
    Other { message: String },
}

impl From<CoreError> for FsdbError {
    fn from(err: CoreError) -> Self {
        match err {
            CoreError::Io(e) => FsdbError::IoError {
                message: e.to_string(),
            },
            CoreError::Serialization(e) => FsdbError::SerializationError {
                message: e.to_string(),
            },
            CoreError::ObjectStore(e) => FsdbError::ObjectStoreError {
                message: e.to_string(),
            },
            CoreError::Arrow(e) => FsdbError::ArrowError {
                message: e.to_string(),
            },
            CoreError::Parquet(e) => FsdbError::ParquetError {
                message: e.to_string(),
            },
            CoreError::DatabaseNotFound(msg) => FsdbError::DatabaseNotFound { message: msg },
            CoreError::RecordNotFound(msg) => FsdbError::RecordNotFound { message: msg },
            CoreError::DatabaseAlreadyExists(msg) => {
                FsdbError::DatabaseAlreadyExists { message: msg }
            }
            CoreError::InvalidOperation(msg) => FsdbError::InvalidOperation { message: msg },
            CoreError::TransactionConflict(msg) => FsdbError::TransactionConflict { message: msg },
            CoreError::Wal(msg) => FsdbError::WalError { message: msg },
            CoreError::Bincode(msg) => FsdbError::BincodeError { message: msg },
            CoreError::DeltaTable(e) => FsdbError::DeltaLakeError {
                message: e.to_string(),
            },
            CoreError::Other(msg) => FsdbError::Other { message: msg },
        }
    }
}

/// Schema field definition
#[derive(Debug, Clone, uniffi::Record)]
pub struct Field {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Database schema
#[derive(Debug, Clone, uniffi::Record)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    fn to_arrow_schema(&self) -> Result<Arc<ArrowSchema>, FsdbError> {
        let fields: Result<Vec<ArrowField>, _> = self
            .fields
            .iter()
            .map(|f| {
                let data_type = match f.data_type.as_str() {
                    "Int8" => DataType::Int8,
                    "Int16" => DataType::Int16,
                    "Int32" => DataType::Int32,
                    "Int64" => DataType::Int64,
                    "UInt8" => DataType::UInt8,
                    "UInt16" => DataType::UInt16,
                    "UInt32" => DataType::UInt32,
                    "UInt64" => DataType::UInt64,
                    "Float32" => DataType::Float32,
                    "Float64" => DataType::Float64,
                    "String" | "Utf8" => DataType::Utf8,
                    "LargeUtf8" => DataType::LargeUtf8,
                    "Boolean" | "Bool" => DataType::Boolean,
                    "Binary" => DataType::Binary,
                    "LargeBinary" => DataType::LargeBinary,
                    "Date32" => DataType::Date32,
                    "Date64" => DataType::Date64,
                    "Timestamp" => {
                        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
                    }
                    _ => {
                        return Err(FsdbError::InvalidOperation {
                            message: format!("Unsupported data type: {}", f.data_type),
                        })
                    }
                };
                Ok(ArrowField::new(&f.name, data_type, f.nullable))
            })
            .collect();

        Ok(Arc::new(ArrowSchema::new(fields?)))
    }

    fn from_arrow_schema(schema: &ArrowSchema) -> Self {
        let fields = schema
            .fields()
            .iter()
            .map(|f| {
                let data_type = match f.data_type() {
                    DataType::Int8 => "Int8",
                    DataType::Int16 => "Int16",
                    DataType::Int32 => "Int32",
                    DataType::Int64 => "Int64",
                    DataType::UInt8 => "UInt8",
                    DataType::UInt16 => "UInt16",
                    DataType::UInt32 => "UInt32",
                    DataType::UInt64 => "UInt64",
                    DataType::Float32 => "Float32",
                    DataType::Float64 => "Float64",
                    DataType::Utf8 | DataType::LargeUtf8 => "String",
                    DataType::Boolean => "Boolean",
                    DataType::Binary => "Binary",
                    DataType::LargeBinary => "LargeBinary",
                    DataType::Date32 => "Date32",
                    DataType::Date64 => "Date64",
                    DataType::Timestamp(_, _) => "Timestamp",
                    _ => "String", // Fallback for complex types
                };
                Field {
                    name: f.name().clone(),
                    data_type: data_type.to_string(),
                    nullable: f.is_nullable(),
                }
            })
            .collect();

        Schema { fields }
    }
}

/// Query result row
#[derive(Debug, Clone, uniffi::Record)]
pub struct Row {
    pub values: HashMap<String, String>,
}

/// Database metrics for monitoring
#[derive(Debug, Clone, uniffi::Record)]
pub struct DatabaseMetrics {
    pub total_queries: u64,
    pub total_inserts: u64,
    pub total_deletes: u64,
    pub total_transactions: u64,
    pub total_errors: u64,
    pub avg_query_latency_ms: f64,
    pub max_query_latency_ms: f64,
    pub uptime_seconds: f64,
}

/// Health status
#[derive(Debug, Clone, uniffi::Record)]
pub struct HealthStatus {
    pub status: String,
    pub uptime_seconds: f64,
    pub total_files: u64,
    pub total_rows: u64,
    pub total_size_bytes: u64,
}

/// Data skipping statistics
#[derive(Debug, Clone, uniffi::Record)]
pub struct DataSkippingStats {
    pub total_files: u64,
    pub files_read: u64,
    pub files_skipped: u64,
    pub bytes_scanned: u64,
    pub bytes_skipped: u64,
}

/// S3 configuration
#[derive(Debug, Clone, uniffi::Record)]
pub struct S3Config {
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
}

/// Main DatabaseOps wrapper for Python
#[derive(uniffi::Object)]
pub struct DatabaseOps {
    inner: Arc<CoreDatabaseOps>,
    runtime: Arc<tokio::runtime::Runtime>,
}

#[uniffi::export]
impl DatabaseOps {
    /// Create a new database with the given schema
    #[uniffi::constructor]
    pub fn create(path: String, schema: Schema) -> Result<Arc<Self>, FsdbError> {
        let arrow_schema = schema.to_arrow_schema()?;
        let runtime = Arc::new(
            tokio::runtime::Runtime::new().map_err(|e| FsdbError::Other {
                message: e.to_string(),
            })?,
        );
        let db = runtime.block_on(CoreDatabaseOps::create(&path, arrow_schema))?;
        Ok(Arc::new(Self {
            inner: Arc::new(db),
            runtime,
        }))
    }

    /// Create a new database with authentication enabled
    #[uniffi::constructor]
    pub fn create_with_auth(
        path: String,
        schema: Schema,
        auth_enabled: bool,
    ) -> Result<Arc<Self>, FsdbError> {
        let arrow_schema = schema.to_arrow_schema()?;
        let runtime = Arc::new(
            tokio::runtime::Runtime::new().map_err(|e| FsdbError::Other {
                message: e.to_string(),
            })?,
        );
        let db = runtime.block_on(CoreDatabaseOps::create_with_auth(
            &path,
            arrow_schema,
            auth_enabled,
        ))?;
        Ok(Arc::new(Self {
            inner: Arc::new(db),
            runtime,
        }))
    }

    /// Create a new database on S3
    #[uniffi::constructor]
    pub fn create_with_s3(
        s3_path: String,
        schema: Schema,
        s3_config: S3Config,
    ) -> Result<Arc<Self>, FsdbError> {
        let arrow_schema = schema.to_arrow_schema()?;
        let runtime = Arc::new(
            tokio::runtime::Runtime::new().map_err(|e| FsdbError::Other {
                message: e.to_string(),
            })?,
        );
        let db = runtime.block_on(CoreDatabaseOps::create_with_s3(
            &s3_path,
            arrow_schema,
            &s3_config.endpoint,
            &s3_config.access_key_id,
            &s3_config.secret_access_key,
        ))?;
        Ok(Arc::new(Self {
            inner: Arc::new(db),
            runtime,
        }))
    }

    /// Open an existing database
    #[uniffi::constructor]
    pub fn open(path: String) -> Result<Arc<Self>, FsdbError> {
        let runtime = Arc::new(
            tokio::runtime::Runtime::new().map_err(|e| FsdbError::Other {
                message: e.to_string(),
            })?,
        );
        let db = runtime.block_on(CoreDatabaseOps::open(&path))?;
        Ok(Arc::new(Self {
            inner: Arc::new(db),
            runtime,
        }))
    }

    /// Open an existing database with credentials
    #[uniffi::constructor]
    pub fn open_with_credentials(
        path: String,
        username: String,
        password: String,
    ) -> Result<Arc<Self>, FsdbError> {
        let runtime = Arc::new(
            tokio::runtime::Runtime::new().map_err(|e| FsdbError::Other {
                message: e.to_string(),
            })?,
        );

        // For credentials, we need to leak the strings to get 'static lifetime
        // This is acceptable since credentials are typically used for the lifetime of the app
        let username_static: &'static str = Box::leak(username.into_boxed_str());
        let password_static: &'static str = Box::leak(password.into_boxed_str());
        let credentials = Some((username_static, password_static));

        let db = runtime.block_on(CoreDatabaseOps::open_with_credentials(&path, credentials))?;
        Ok(Arc::new(Self {
            inner: Arc::new(db),
            runtime,
        }))
    }

    /// Open an existing database on S3
    #[uniffi::constructor]
    pub fn open_with_s3(s3_path: String, s3_config: S3Config) -> Result<Arc<Self>, FsdbError> {
        let runtime = Arc::new(
            tokio::runtime::Runtime::new().map_err(|e| FsdbError::Other {
                message: e.to_string(),
            })?,
        );
        let db = runtime.block_on(CoreDatabaseOps::open_with_s3(
            &s3_path,
            &s3_config.endpoint,
            &s3_config.access_key_id,
            &s3_config.secret_access_key,
        ))?;
        Ok(Arc::new(Self {
            inner: Arc::new(db),
            runtime,
        }))
    }

    // Data operations

    /// Insert data from JSON string
    pub fn insert_json(&self, json_data: String) -> Result<u64, FsdbError> {
        let value: Value =
            serde_json::from_str(&json_data).map_err(|e| FsdbError::SerializationError {
                message: e.to_string(),
            })?;

        // Convert JSON array to RecordBatch
        let array = value
            .as_array()
            .ok_or_else(|| FsdbError::InvalidOperation {
                message: "JSON must be an array of objects".to_string(),
            })?;

        // Build RecordBatch from JSON
        let batch = self.json_array_to_record_batch(array)?;
        let rows_inserted = batch.num_rows() as u64;

        self.runtime.block_on(self.inner.insert(batch))?;
        Ok(rows_inserted)
    }

    /// Insert data from JSON string with buffering for better performance
    ///
    /// This method buffers small writes and automatically flushes when the buffer
    /// reaches 1000 rows (default). Use this for many small inserts to reduce
    /// transaction overhead.
    ///
    /// Returns the number of rows buffered (not necessarily committed yet).
    /// Call flush_write_buffer() to force a commit.
    pub fn insert_buffered_json(&self, json_data: String) -> Result<u64, FsdbError> {
        let value: Value =
            serde_json::from_str(&json_data).map_err(|e| FsdbError::SerializationError {
                message: e.to_string(),
            })?;

        // Convert JSON array to RecordBatch
        let array = value
            .as_array()
            .ok_or_else(|| FsdbError::InvalidOperation {
                message: "JSON must be an array of objects".to_string(),
            })?;

        // Build RecordBatch from JSON
        let batch = self.json_array_to_record_batch(array)?;
        let rows_buffered = batch.num_rows() as u64;

        self.runtime.block_on(self.inner.insert_buffered(batch))?;
        Ok(rows_buffered)
    }

    /// Flush any buffered writes to Delta Lake immediately
    ///
    /// Forces all buffered data from insert_buffered_json() to be committed.
    /// Call this before reading data to ensure consistency, or at shutdown.
    pub fn flush_write_buffer(&self) -> Result<(), FsdbError> {
        self.runtime.block_on(self.inner.flush_write_buffer())?;
        Ok(())
    }

    /// Query data with SQL
    pub fn query(&self, sql: String) -> Result<Vec<Row>, FsdbError> {
        let result = self.runtime.block_on(self.inner.query(&sql))?;
        Ok(self.record_batches_to_rows(result))
    }

    /// Query data and return as JSON string
    pub fn query_json(&self, sql: String) -> Result<String, FsdbError> {
        let result = self.runtime.block_on(self.inner.query(&sql))?;
        let json_array = self.record_batches_to_json(result)?;
        serde_json::to_string_pretty(&json_array).map_err(|e| FsdbError::SerializationError {
            message: e.to_string(),
        })
    }

    /// Delete rows matching a WHERE clause
    pub fn delete_rows_where(&self, predicate: String) -> Result<u64, FsdbError> {
        let count = self
            .runtime
            .block_on(self.inner.delete_rows_where(&predicate))?;
        Ok(count as u64)
    }

    /// Execute MERGE (UPSERT) operation from JSON data
    ///
    /// JSON must be an array of objects with an "_op" field specifying the operation:
    /// - "INSERT": Insert new rows
    /// - "UPDATE": Update existing rows  
    /// - "DELETE": Delete existing rows
    ///
    /// The join is performed on the specified join_column (typically "id").
    ///
    /// Returns a JSON string with merge metrics: rows_inserted, rows_updated, rows_deleted
    pub fn merge_json(&self, json_data: String, join_column: String) -> Result<String, FsdbError> {
        // Parse JSON
        let value: Value =
            serde_json::from_str(&json_data).map_err(|e| FsdbError::SerializationError {
                message: e.to_string(),
            })?;

        let array = value
            .as_array()
            .ok_or_else(|| FsdbError::InvalidOperation {
                message: "JSON must be an array of objects".to_string(),
            })?;

        if array.is_empty() {
            return Ok(r#"{"rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0}"#.to_string());
        }

        // Build RecordBatch from JSON with _op column included
        let batch = self.json_array_to_record_batch_with_op(array)?;

        // Execute MERGE
        let metrics = self.runtime.block_on(async {
            let merge_builder = self.inner.merge().await?;

            merge_builder
                .with_source(batch, "source")
                .on(format!("target.{} = source.{}", join_column, join_column))
                .when_matched_update()
                .condition("source._op = 'UPDATE'")
                .set_all()
                .when_matched_delete()
                .condition("source._op = 'DELETE'")
                .then()
                .when_not_matched_insert()
                .condition("source._op = 'INSERT'")
                .values_all()
                .execute()
                .await
        })?;

        // Return metrics as JSON
        let result = serde_json::json!({
            "rows_inserted": metrics.rows_inserted,
            "rows_updated": metrics.rows_updated,
            "rows_deleted": metrics.rows_deleted,
            "total_affected": metrics.total_rows_affected()
        });

        serde_json::to_string_pretty(&result).map_err(|e| FsdbError::SerializationError {
            message: e.to_string(),
        })
    }

    // Time travel operations

    /// Query data at a specific version
    pub fn query_version(&self, sql: String, version: i64) -> Result<Vec<Row>, FsdbError> {
        let result = self
            .runtime
            .block_on(self.inner.query_version(&sql, version))?;
        Ok(self.record_batches_to_rows(result))
    }

    /// Query data at a specific version and return as JSON
    pub fn query_version_json(&self, sql: String, version: i64) -> Result<String, FsdbError> {
        let result = self
            .runtime
            .block_on(self.inner.query_version(&sql, version))?;
        let json_array = self.record_batches_to_json(result)?;
        serde_json::to_string_pretty(&json_array).map_err(|e| FsdbError::SerializationError {
            message: e.to_string(),
        })
    }

    /// Query data at a specific timestamp (milliseconds since epoch)
    pub fn query_timestamp(&self, sql: String, timestamp_ms: i64) -> Result<Vec<Row>, FsdbError> {
        let result = self
            .runtime
            .block_on(self.inner.query_timestamp(&sql, timestamp_ms))?;
        Ok(self.record_batches_to_rows(result))
    }

    /// Query data at a specific timestamp and return as JSON
    pub fn query_timestamp_json(
        &self,
        sql: String,
        timestamp_ms: i64,
    ) -> Result<String, FsdbError> {
        let result = self
            .runtime
            .block_on(self.inner.query_timestamp(&sql, timestamp_ms))?;
        let json_array = self.record_batches_to_json(result)?;
        serde_json::to_string_pretty(&json_array).map_err(|e| FsdbError::SerializationError {
            message: e.to_string(),
        })
    }

    // Delta Lake operations

    /// Run OPTIMIZE to compact files
    pub fn optimize(&self) -> Result<(), FsdbError> {
        self.runtime.block_on(self.inner.optimize())?;
        Ok(())
    }

    /// Run OPTIMIZE with a target file size
    pub fn optimize_with_target_size(&self, target_size_bytes: u64) -> Result<(), FsdbError> {
        self.runtime
            .block_on(self.inner.optimize_with_target_size(target_size_bytes))?;
        Ok(())
    }

    /// Run OPTIMIZE with a filter
    pub fn optimize_with_filter(&self, filter: String) -> Result<(), FsdbError> {
        self.runtime
            .block_on(self.inner.optimize_with_filter(&filter))?;
        Ok(())
    }

    /// Run VACUUM to remove old files
    pub fn vacuum(&self, retention_hours: u64) -> Result<(), FsdbError> {
        self.runtime.block_on(self.inner.vacuum(retention_hours))?;
        Ok(())
    }

    /// Run VACUUM dry run to see what would be deleted
    pub fn vacuum_dry_run(&self, retention_hours: u64) -> Result<Vec<String>, FsdbError> {
        let files = self
            .runtime
            .block_on(self.inner.vacuum_dry_run(retention_hours))?;
        Ok(files)
    }

    /// Run Z-ORDER on specified columns
    pub fn zorder(&self, columns: Vec<String>) -> Result<(), FsdbError> {
        let column_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        self.runtime.block_on(self.inner.zorder(&column_refs))?;
        Ok(())
    }

    // User management

    /// Create a new user with roles
    pub fn create_user(
        &self,
        username: String,
        password: String,
        roles: Vec<String>,
    ) -> Result<(), FsdbError> {
        let role_refs: Vec<&str> = roles.iter().map(|s| s.as_str()).collect();
        self.runtime
            .block_on(self.inner.create_user(&username, &password, &role_refs))?;
        Ok(())
    }

    // Backup and restore

    /// Create a full backup
    pub fn backup(&self, backup_path: String) -> Result<(), FsdbError> {
        self.runtime.block_on(self.inner.backup(&backup_path))?;
        Ok(())
    }

    /// Create an incremental backup
    pub fn backup_incremental(
        &self,
        base_backup_path: String,
        incremental_path: String,
    ) -> Result<(), FsdbError> {
        self.runtime.block_on(
            self.inner
                .backup_incremental(&base_backup_path, &incremental_path),
        )?;
        Ok(())
    }

    // Monitoring and health

    /// Get database metrics
    pub fn get_metrics(&self) -> DatabaseMetrics {
        let core_metrics = self.runtime.block_on(self.inner.get_metrics());
        DatabaseMetrics {
            total_queries: core_metrics.total_queries,
            total_inserts: core_metrics.total_inserts,
            total_deletes: core_metrics.total_deletes,
            total_transactions: core_metrics.total_transactions,
            total_errors: core_metrics.total_errors,
            avg_query_latency_ms: core_metrics.avg_query_latency_ms,
            max_query_latency_ms: core_metrics.max_query_latency_ms,
            uptime_seconds: core_metrics.uptime_seconds,
        }
    }

    /// Get health status
    pub fn health_check(&self) -> HealthStatus {
        let core_health = self.runtime.block_on(self.inner.health_check());
        HealthStatus {
            status: core_health.status,
            uptime_seconds: core_health.uptime_seconds,
            total_files: core_health.total_files as u64,
            total_rows: core_health.total_rows,
            total_size_bytes: core_health.total_size_bytes,
        }
    }

    /// Get data skipping statistics
    pub fn get_data_skipping_stats(&self) -> Result<DataSkippingStats, FsdbError> {
        let stats = self
            .runtime
            .block_on(self.inner.get_data_skipping_stats())?;
        Ok(DataSkippingStats {
            total_files: stats.total_files as u64,
            files_read: stats.files_read as u64,
            files_skipped: stats.files_skipped as u64,
            bytes_scanned: stats.bytes_scanned,
            bytes_skipped: stats.bytes_skipped,
        })
    }

    /// Get database schema
    pub fn get_schema(&self) -> Schema {
        Schema::from_arrow_schema(&self.inner.schema)
    }

    /// Get database base path
    pub fn get_base_path(&self) -> String {
        format!("{}", self.inner.base_path().display())
    }
}

// Helper methods for data conversion
impl DatabaseOps {
    /// Convert JSON array to RecordBatch
    /// Arrow JSON reader expects newline-delimited JSON (NDJSON), not JSON array
    fn json_array_to_record_batch(
        &self,
        json_array: &[Value],
    ) -> Result<arrow::array::RecordBatch, FsdbError> {
        use arrow::json::ReaderBuilder;
        use std::io::Cursor;

        // Convert JSON array to newline-delimited JSON (NDJSON)
        let ndjson: String = json_array
            .iter()
            .map(|v| serde_json::to_string(v).unwrap_or_default())
            .collect::<Vec<String>>()
            .join("\n");

        let cursor = Cursor::new(ndjson.as_bytes());
        let mut reader = ReaderBuilder::new(self.inner.schema.clone())
            .build(cursor)
            .map_err(|e| FsdbError::ArrowError {
                message: e.to_string(),
            })?;

        reader
            .next()
            .ok_or_else(|| FsdbError::InvalidOperation {
                message: "No data to insert".to_string(),
            })?
            .map_err(|e| FsdbError::ArrowError {
                message: e.to_string(),
            })
    }

    /// Convert JSON array to RecordBatch with _op column included
    /// Used for MERGE operations where we need the _op column
    fn json_array_to_record_batch_with_op(
        &self,
        json_array: &[Value],
    ) -> Result<arrow::array::RecordBatch, FsdbError> {
        use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
        use arrow::json::ReaderBuilder;
        use std::io::Cursor;

        // Create schema with _op column added
        let mut fields = self.inner.schema.fields().to_vec();
        fields.push(Arc::new(ArrowField::new(
            "_op",
            arrow::datatypes::DataType::Utf8,
            false,
        )));
        let schema_with_op = Arc::new(ArrowSchema::new(fields));

        // Convert JSON array to newline-delimited JSON (NDJSON)
        let ndjson: String = json_array
            .iter()
            .map(|v| serde_json::to_string(v).unwrap_or_default())
            .collect::<Vec<String>>()
            .join("\n");

        let cursor = Cursor::new(ndjson.as_bytes());
        let mut reader = ReaderBuilder::new(schema_with_op)
            .build(cursor)
            .map_err(|e| FsdbError::ArrowError {
                message: e.to_string(),
            })?;

        reader
            .next()
            .ok_or_else(|| FsdbError::InvalidOperation {
                message: "No data to insert".to_string(),
            })?
            .map_err(|e| FsdbError::ArrowError {
                message: e.to_string(),
            })
    }

    /// Convert RecordBatches to Rows
    fn record_batches_to_rows(&self, batches: Vec<arrow::array::RecordBatch>) -> Vec<Row> {
        let mut rows = Vec::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let mut values = HashMap::new();
                for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                    let col = batch.column(col_idx);
                    let value = format!("{:?}", col.slice(row_idx, 1));
                    // Clean up the format (remove array wrapper)
                    let clean_value = value
                        .trim_start_matches('[')
                        .trim_end_matches(']')
                        .to_string();
                    values.insert(field.name().clone(), clean_value);
                }
                rows.push(Row { values });
            }
        }
        rows
    }

    /// Convert RecordBatches to JSON
    fn record_batches_to_json(
        &self,
        batches: Vec<arrow::array::RecordBatch>,
    ) -> Result<Vec<Value>, FsdbError> {
        use arrow::json::ArrayWriter;
        use std::io::Cursor;

        let mut json_rows = Vec::new();
        for batch in batches {
            let mut cursor = Cursor::new(Vec::new());
            let mut writer = ArrayWriter::new(&mut cursor);
            writer.write(&batch).map_err(|e| FsdbError::ArrowError {
                message: e.to_string(),
            })?;
            writer.finish().map_err(|e| FsdbError::ArrowError {
                message: e.to_string(),
            })?;

            let json_bytes = cursor.into_inner();
            let json_value: Value =
                serde_json::from_slice(&json_bytes).map_err(|e| FsdbError::SerializationError {
                    message: e.to_string(),
                })?;

            if let Value::Array(arr) = json_value {
                json_rows.extend(arr);
            }
        }
        Ok(json_rows)
    }
}

/// NFS Server for exposing database as POSIX filesystem
#[derive(uniffi::Object)]
pub struct NfsServer {
    inner: Arc<tokio::sync::Mutex<Option<crate::nfs::NfsServer>>>,
    runtime: Arc<tokio::runtime::Runtime>,
    port: u16,
}

#[uniffi::export]
impl NfsServer {
    /// Create and start a new NFS server
    #[uniffi::constructor]
    pub fn new(db: Arc<DatabaseOps>, port: u16) -> Result<Arc<Self>, FsdbError> {
        let runtime = Arc::new(
            tokio::runtime::Runtime::new().map_err(|e| FsdbError::Other {
                message: e.to_string(),
            })?,
        );

        let db_inner = db.inner.clone();
        let server = runtime.block_on(crate::nfs::NfsServer::new(db_inner, port))?;

        Ok(Arc::new(Self {
            inner: Arc::new(tokio::sync::Mutex::new(Some(server))),
            runtime,
            port,
        }))
    }

    /// Get the port the server is listening on
    pub fn get_port(&self) -> u16 {
        self.port
    }

    /// Check if server is ready
    pub fn is_ready(&self) -> bool {
        let inner = self.runtime.block_on(self.inner.lock());
        inner
            .as_ref()
            .map(|s| self.runtime.block_on(s.is_ready()))
            .unwrap_or(false)
    }

    /// Shutdown the NFS server
    pub fn shutdown(&self) -> Result<(), FsdbError> {
        let server = self.runtime.block_on(async {
            let mut inner = self.inner.lock().await;
            inner.take()
        });

        if let Some(server) = server {
            self.runtime.block_on(server.shutdown())?;
        }
        Ok(())
    }

    /// Get mount command for this platform
    pub fn get_mount_command(&self, #[allow(unused_variables)] mount_point: String) -> Vec<String> {
        #[cfg(target_os = "macos")]
        {
            vec![
                "sudo".to_string(),
                "mount_nfs".to_string(),
                "-o".to_string(),
                format!(
                    "nolocks,vers=3,tcp,port={},mountport={}",
                    self.port, self.port
                ),
                "localhost:/".to_string(),
                mount_point,
            ]
        }

        #[cfg(target_os = "linux")]
        {
            vec![
                "sudo".to_string(),
                "mount".to_string(),
                "-t".to_string(),
                "nfs".to_string(),
                "-o".to_string(),
                format!(
                    "nolocks,vers=3,tcp,port={},mountport={}",
                    self.port, self.port
                ),
                "localhost:/".to_string(),
                mount_point,
            ]
        }

        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            vec!["echo".to_string(), "Unsupported platform".to_string()]
        }
    }

    /// Get unmount command for this platform
    pub fn get_unmount_command(
        &self,
        #[allow(unused_variables)] mount_point: String,
    ) -> Vec<String> {
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            vec!["sudo".to_string(), "umount".to_string(), mount_point]
        }

        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            vec!["echo".to_string(), "Unsupported platform".to_string()]
        }
    }
}

// Standalone restore functions
#[uniffi::export]
pub fn restore(backup_path: String, restore_path: String) -> Result<(), FsdbError> {
    let runtime = tokio::runtime::Runtime::new().map_err(|e| FsdbError::Other {
        message: e.to_string(),
    })?;
    runtime.block_on(CoreDatabaseOps::restore(&backup_path, &restore_path))?;
    Ok(())
}

#[uniffi::export]
pub fn restore_to_transaction(
    backup_path: String,
    restore_path: String,
    transaction_id: u64,
) -> Result<(), FsdbError> {
    let runtime = tokio::runtime::Runtime::new().map_err(|e| FsdbError::Other {
        message: e.to_string(),
    })?;
    runtime.block_on(CoreDatabaseOps::restore_to_transaction(
        &backup_path,
        &restore_path,
        transaction_id,
    ))?;
    Ok(())
}
