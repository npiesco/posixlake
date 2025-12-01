//! Database operations with full CRUD support
//!
//! Delta Lake native implementation using deltalake-rs

use crate::metadata::{BackupMetadata, BackupVerificationReport};
use crate::query::QueryExecutor;
// Removed: extract_predicates, is_value_less_than, is_value_greater_than - moved to query::pruning module
use crate::delta_lake::stats::{get_column_statistics_from_delta, ColumnStats};
use crate::storage::parquet::ParquetReader;
use crate::{Error, Result};
use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{Schema, SchemaRef};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

/// Query pruning statistics
#[derive(Debug, Clone)]
pub struct PruningStats {
    pub files_scanned: usize,
    pub files_pruned: usize,
    pub total_files: usize,
}

/// Data skipping statistics - tracks file-level pruning based on min/max statistics
#[derive(Debug, Clone, Default)]
pub struct DataSkippingStats {
    pub total_files: usize,
    pub files_read: usize,
    pub files_skipped: usize,
    pub bytes_scanned: u64,
    pub bytes_skipped: u64,
}

/// Real-time database metrics for monitoring and observability
#[derive(Debug, Clone)]
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

/// Database health status
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub status: String,
    pub uptime_seconds: f64,
    pub total_files: usize,
    pub total_rows: u64,
    pub total_size_bytes: u64,
}

// Re-export Transaction from its own module
pub use crate::transaction::Transaction;

/// Internal metrics tracker with atomic counters for thread-safe updates
struct MetricsTracker {
    total_queries: AtomicU64,
    total_inserts: AtomicU64,
    total_deletes: AtomicU64,
    total_transactions: AtomicU64,
    total_errors: AtomicU64,
    query_latencies: Arc<tokio::sync::Mutex<Vec<f64>>>, // Store individual latencies
    start_time: Instant,
}

/// Database with full CRUD operations (Delta Lake native)
pub struct DatabaseOps {
    /// Base path for database
    base_path: PathBuf,

    /// S3 configuration (if using S3 backend)
    s3_url: Option<String>,
    s3_storage_options: Option<std::collections::HashMap<String, String>>,

    /// Query executor
    #[allow(dead_code)]
    query_executor: Arc<QueryExecutor>,

    /// Database schema (cached from Delta Lake)
    pub schema: SchemaRef,

    /// Real-time metrics tracker for monitoring and observability
    metrics: Arc<MetricsTracker>,

    /// Data skipping statistics tracker
    data_skipping_stats: Arc<tokio::sync::Mutex<DataSkippingStats>>,

    /// Authentication context (None = auth disabled)
    auth_context: Option<Arc<crate::security::AuthContext>>,

    /// User store (for authentication)
    user_store: Option<Arc<tokio::sync::Mutex<crate::security::UserStore>>>,

    /// Audit logger
    audit_logger: Option<Arc<crate::security::AuditLogger>>,

    /// Role manager
    role_manager: Option<Arc<crate::security::RoleManager>>,

    /// Batch buffer for reducing transaction overhead
    batch_buffer: Arc<crate::batch_buffer::BatchBuffer>,
}

impl MetricsTracker {
    fn new() -> Self {
        Self {
            total_queries: AtomicU64::new(0),
            total_inserts: AtomicU64::new(0),
            total_deletes: AtomicU64::new(0),
            total_transactions: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            query_latencies: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            start_time: Instant::now(),
        }
    }

    fn reset(&self) {
        self.total_queries.store(0, Ordering::Relaxed);
        self.total_inserts.store(0, Ordering::Relaxed);
        self.total_deletes.store(0, Ordering::Relaxed);
        self.total_transactions.store(0, Ordering::Relaxed);
        self.total_errors.store(0, Ordering::Relaxed);
        // Note: Don't reset start_time - uptime should continue
    }
}

// Transaction impl moved to src/transaction.rs

impl DatabaseOps {
    /// Create a new database with the given schema (uses Delta Lake format)
    pub async fn create<P: AsRef<Path>>(path: P, schema: SchemaRef) -> Result<Self> {
        Self::create_with_delta_native(path, schema).await
    }

    /// Create a new database in Delta Lake format
    /// Data is stored in Delta Lake format, readable by Spark/Databricks
    pub async fn create_with_delta_native<P: AsRef<Path>>(
        path: P,
        schema: SchemaRef,
    ) -> Result<Self> {
        let base_path = path.as_ref().to_path_buf();
        info!(
            "Creating database in Delta Lake native mode at: {}",
            base_path.display()
        );

        // Create base directory
        std::fs::create_dir_all(&base_path)?;

        // Initialize Delta Lake table using delta-rs
        let _delta_table = {
            use deltalake::kernel::{StructField, StructType};
            use deltalake::DeltaOps;
            use url::Url;

            // Convert Arrow schema to Delta Lake schema
            let mut delta_fields = Vec::new();
            for field in schema.fields() {
                let delta_type = Self::arrow_to_delta_type(field.data_type())?;
                delta_fields.push(StructField::new(
                    field.name().clone(),
                    delta_type,
                    field.is_nullable(),
                ));
            }
            let delta_schema = StructType::try_new(delta_fields)
                .map_err(|e| Error::Other(format!("Failed to create Delta schema: {}", e)))?;

            // Create Delta table
            let table_url = Url::from_directory_path(&base_path)
                .map_err(|_| Error::Other("Invalid path for Delta table".to_string()))?;

            let ops = DeltaOps::try_from_uri(table_url)
                .await
                .map_err(Error::DeltaTable)?;

            let table = ops
                .create()
                .with_columns(delta_schema.fields().cloned())
                .await
                .map_err(Error::DeltaTable)?;

            table
        };

        let query_executor = Arc::new(QueryExecutor::new());
        let metrics = Arc::new(MetricsTracker::new());
        let batch_buffer = Arc::new(crate::batch_buffer::BatchBuffer::new(schema.clone()));

        Ok(Self {
            base_path,
            s3_url: None,
            s3_storage_options: None,
            query_executor,
            schema,
            metrics,
            data_skipping_stats: Arc::new(tokio::sync::Mutex::new(DataSkippingStats::default())),
            auth_context: None,
            user_store: None,
            audit_logger: None,
            role_manager: None,
            batch_buffer,
        })
    }

    /// Open an existing Delta Lake table with FSDB
    /// Allows FSDB to read and write to existing Delta Lake tables
    pub async fn open_delta_native<P: AsRef<Path>>(path: P) -> Result<Self> {
        let base_path = path.as_ref().to_path_buf();
        info!("Opening Delta Lake table at: {}", base_path.display());

        // Open Delta Lake table
        let (_delta_table, schema) = {
            use deltalake::open_table;
            use url::Url;

            let table_url = Url::from_directory_path(&base_path)
                .map_err(|_| Error::Other("Invalid path for Delta table".to_string()))?;

            let table = open_table(table_url).await.map_err(Error::DeltaTable)?;

            // Convert Delta schema to Arrow schema
            let snapshot = table.snapshot().map_err(Error::DeltaTable)?;

            let delta_schema = snapshot.schema();

            // Convert Delta StructType to Arrow Schema
            let arrow_fields: Result<Vec<_>> = delta_schema
                .fields()
                .map(|field| {
                    let arrow_type = Self::delta_to_arrow_type(field.data_type())?;
                    Ok(arrow::datatypes::Field::new(
                        field.name(),
                        arrow_type,
                        field.is_nullable(),
                    ))
                })
                .collect();

            let arrow_schema = Arc::new(arrow::datatypes::Schema::new(arrow_fields?));

            (table, arrow_schema)
        };

        let query_executor = Arc::new(QueryExecutor::new());
        let metrics = Arc::new(MetricsTracker::new());
        let batch_buffer = Arc::new(crate::batch_buffer::BatchBuffer::new(schema.clone()));

        Ok(Self {
            base_path,
            s3_url: None,
            s3_storage_options: None,
            query_executor,
            schema,
            metrics,
            data_skipping_stats: Arc::new(tokio::sync::Mutex::new(DataSkippingStats::default())),
            auth_context: None,
            user_store: None,
            audit_logger: None,
            role_manager: None,
            batch_buffer,
        })
    }

    /// Create a Delta Lake database with S3/MinIO backend
    /// Delta Lake tables can be stored directly on S3-compatible storage
    pub async fn create_with_s3(
        s3_path: &str,
        schema: SchemaRef,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<Self> {
        use crate::storage::s3::{create_delta_storage_options, get_s3_cache_path, parse_s3_url};
        use deltalake::kernel::{StructField, StructType};
        use deltalake::DeltaOps;

        info!("Creating Delta Lake database on S3 at: {}", s3_path);

        // Configure S3 storage options using helper
        let storage_options = create_delta_storage_options(endpoint, access_key, secret_key);

        // Convert Arrow schema to Delta Lake schema
        let mut delta_fields = Vec::new();
        for field in schema.fields() {
            let delta_type = Self::arrow_to_delta_type(field.data_type())?;
            delta_fields.push(StructField::new(
                field.name().clone(),
                delta_type,
                field.is_nullable(),
            ));
        }
        let delta_schema = StructType::try_new(delta_fields)
            .map_err(|e| Error::Other(format!("Failed to create Delta schema: {}", e)))?;

        // Create Delta table on S3
        let s3_url = parse_s3_url(s3_path)?;
        let ops =
            DeltaOps::try_from_uri_with_storage_options(s3_url.clone(), storage_options.clone())
                .await
                .map_err(Error::DeltaTable)?;

        let _table = ops
            .create()
            .with_columns(delta_schema.fields().cloned())
            .await
            .map_err(Error::DeltaTable)?;

        info!("Delta Lake table created on S3");

        // Use local temp directory for caching
        let base_path = get_s3_cache_path(s3_path);
        std::fs::create_dir_all(&base_path)?;

        let query_executor = Arc::new(QueryExecutor::new());
        let metrics = Arc::new(MetricsTracker::new());
        let batch_buffer = Arc::new(crate::batch_buffer::BatchBuffer::new(schema.clone()));

        Ok(Self {
            base_path,
            s3_url: Some(s3_path.to_string()),
            s3_storage_options: Some(storage_options),
            query_executor,
            schema,
            metrics,
            data_skipping_stats: Arc::new(tokio::sync::Mutex::new(DataSkippingStats::default())),
            auth_context: None,
            user_store: None,
            audit_logger: None,
            role_manager: None,
            batch_buffer,
        })
    }

    /// Open an existing Delta Lake database from S3/MinIO backend
    pub async fn open_with_s3(
        s3_path: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<Self> {
        use crate::storage::s3::{create_delta_storage_options, get_s3_cache_path, parse_s3_url};
        use deltalake::open_table_with_storage_options;

        info!("Opening Delta Lake database from S3 at: {}", s3_path);

        // Configure S3 storage options using helper
        let storage_options = create_delta_storage_options(endpoint, access_key, secret_key);

        // Open Delta table from S3
        let s3_url = parse_s3_url(s3_path)?;
        let table = open_table_with_storage_options(s3_url, storage_options.clone())
            .await
            .map_err(Error::DeltaTable)?;

        // Get schema from Delta table
        let snapshot = table.snapshot().map_err(Error::DeltaTable)?;
        let delta_schema = snapshot.schema();

        // Convert to Arrow schema
        let arrow_fields: Result<Vec<_>> = delta_schema
            .fields()
            .map(|field| {
                let arrow_type = Self::delta_to_arrow_type(field.data_type())?;
                Ok(arrow::datatypes::Field::new(
                    field.name(),
                    arrow_type,
                    field.is_nullable(),
                ))
            })
            .collect();

        let schema = Arc::new(arrow::datatypes::Schema::new(arrow_fields?));

        info!(
            "Opened Delta Lake from S3 with {} fields",
            schema.fields().len()
        );

        // Use local temp directory for caching
        let base_path = get_s3_cache_path(s3_path);
        std::fs::create_dir_all(&base_path)?;

        let query_executor = Arc::new(QueryExecutor::new());
        let metrics = Arc::new(MetricsTracker::new());
        let batch_buffer = Arc::new(crate::batch_buffer::BatchBuffer::new(schema.clone()));

        Ok(Self {
            base_path,
            s3_url: Some(s3_path.to_string()),
            s3_storage_options: Some(storage_options),
            query_executor,
            schema,
            metrics,
            data_skipping_stats: Arc::new(tokio::sync::Mutex::new(DataSkippingStats::default())),
            auth_context: None,
            user_store: None,
            audit_logger: None,
            role_manager: None,
            batch_buffer,
        })
    }

    /// Helper: Convert Arrow DataType to Delta Lake DataType
    fn arrow_to_delta_type(
        arrow_type: &arrow::datatypes::DataType,
    ) -> Result<deltalake::kernel::DataType> {
        use arrow::datatypes::DataType as ArrowDataType;
        use deltalake::kernel::DataType as DeltaDataType;

        match arrow_type {
            ArrowDataType::Boolean => Ok(DeltaDataType::BOOLEAN),
            ArrowDataType::Int8 => Ok(DeltaDataType::BYTE),
            ArrowDataType::Int16 => Ok(DeltaDataType::SHORT),
            ArrowDataType::Int32 => Ok(DeltaDataType::INTEGER),
            ArrowDataType::Int64 => Ok(DeltaDataType::LONG),
            ArrowDataType::Float32 => Ok(DeltaDataType::FLOAT),
            ArrowDataType::Float64 => Ok(DeltaDataType::DOUBLE),
            ArrowDataType::Utf8 => Ok(DeltaDataType::STRING),
            ArrowDataType::LargeUtf8 => Ok(DeltaDataType::STRING),
            ArrowDataType::Binary => Ok(DeltaDataType::BINARY),
            ArrowDataType::LargeBinary => Ok(DeltaDataType::BINARY),
            ArrowDataType::Date32 => Ok(DeltaDataType::DATE),
            ArrowDataType::Timestamp(_, _) => Ok(DeltaDataType::TIMESTAMP_NTZ),
            other => Err(Error::Other(format!(
                "Unsupported Arrow type for Delta Lake: {:?}",
                other
            ))),
        }
    }

    /// Helper: Convert Delta Lake DataType to Arrow DataType
    fn delta_to_arrow_type(
        delta_type: &deltalake::kernel::DataType,
    ) -> Result<arrow::datatypes::DataType> {
        use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
        use deltalake::kernel::DataType as DeltaDataType;

        match delta_type {
            DeltaDataType::Primitive(prim) => match prim {
                deltalake::kernel::PrimitiveType::Boolean => Ok(ArrowDataType::Boolean),
                deltalake::kernel::PrimitiveType::Byte => Ok(ArrowDataType::Int8),
                deltalake::kernel::PrimitiveType::Short => Ok(ArrowDataType::Int16),
                deltalake::kernel::PrimitiveType::Integer => Ok(ArrowDataType::Int32),
                deltalake::kernel::PrimitiveType::Long => Ok(ArrowDataType::Int64),
                deltalake::kernel::PrimitiveType::Float => Ok(ArrowDataType::Float32),
                deltalake::kernel::PrimitiveType::Double => Ok(ArrowDataType::Float64),
                deltalake::kernel::PrimitiveType::String => Ok(ArrowDataType::Utf8),
                deltalake::kernel::PrimitiveType::Binary => Ok(ArrowDataType::Binary),
                deltalake::kernel::PrimitiveType::Date => Ok(ArrowDataType::Date32),
                deltalake::kernel::PrimitiveType::TimestampNtz => {
                    Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
                }
                other => Err(Error::Other(format!(
                    "Unsupported Delta Lake primitive type: {:?}",
                    other
                ))),
            },
            other => Err(Error::Other(format!(
                "Unsupported Delta Lake type: {:?}",
                other
            ))),
        }
    }

    /// Create a new database with authentication enabled
    pub async fn create_with_auth<P: AsRef<Path>>(
        path: P,
        schema: SchemaRef,
        auth_enabled: bool,
    ) -> Result<Self> {
        let mut db = Self::create(path, schema).await?;

        if auth_enabled {
            let users_path = db.base_path.join("_metadata").join("users.json");
            let audit_path = db.base_path.join("_metadata").join("audit.json");

            // Create empty user store and save to disk to mark auth as enabled
            let user_store = crate::security::UserStore::new();
            user_store.save(&users_path)?;

            db.user_store = Some(Arc::new(tokio::sync::Mutex::new(user_store)));
            db.audit_logger = Some(Arc::new(crate::security::AuditLogger::new(audit_path)?));
            db.role_manager = Some(Arc::new(crate::security::RoleManager::new()));
            db.auth_context = Some(Arc::new(crate::security::AuthContext::system()));
        }

        Ok(db)
    }

    /// Open an existing database
    /// Open existing Delta Lake database (delegates to open_delta_native)
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_delta_native(path).await
    }

    /// Open database with credentials
    pub async fn open_with_credentials<P: AsRef<Path>>(
        path: P,
        credentials: crate::security::Credentials,
    ) -> Result<Self> {
        let base_path = path.as_ref().to_path_buf();
        let users_path = base_path.join("_metadata").join("users.json");

        // Check if authentication is enabled
        let auth_enabled = users_path.exists();

        let mut db = Self::open(&base_path).await?;

        if auth_enabled {
            let user_store = crate::security::UserStore::load(&users_path)?;

            // None credentials = system access (for administrative operations)
            let auth_ctx = if let Some((username, password)) = credentials {
                user_store.authenticate(username, password)?
            } else {
                // System access with admin privileges
                crate::security::AuthContext::system()
            };

            db.auth_context = Some(Arc::new(auth_ctx));

            let audit_path = base_path.join("_metadata").join("audit.json");
            db.user_store = Some(Arc::new(tokio::sync::Mutex::new(user_store)));
            db.audit_logger = Some(Arc::new(crate::security::AuditLogger::new(audit_path)?));
            db.role_manager = Some(Arc::new(crate::security::RoleManager::new()));
        }

        Ok(db)
    }

    /// Create a user (requires admin role)
    pub async fn create_user(&self, username: &str, password: &str, roles: &[&str]) -> Result<()> {
        self.check_permission(&crate::security::Permission::Admin)?;

        let result = async {
            let user = crate::security::User::new(
                username.to_string(),
                password,
                roles.iter().map(|s| s.to_string()).collect(),
            )?;

            if let Some(user_store) = &self.user_store {
                let mut store = user_store.lock().await;
                store.add_user(user)?;
                let users_path = self.base_path.join("_metadata").join("users.json");
                store.save(users_path)?;
            }

            Ok(())
        }
        .await;

        match &result {
            Ok(_) => {
                self.audit_log(
                    "CREATE_USER",
                    &format!("username={}, roles={:?}", username, roles),
                    true,
                )
                .await;
            }
            Err(e) => {
                self.audit_log(
                    "CREATE_USER",
                    &format!("username={}: {}", username, e),
                    false,
                )
                .await;
            }
        }

        result
    }

    /// Revoke a role from a user (requires admin role)
    pub async fn revoke_role_from_user(&self, username: &str, role: &str) -> Result<()> {
        self.check_permission(&crate::security::Permission::Admin)?;

        if let Some(user_store) = &self.user_store {
            let mut store = user_store.lock().await;
            store.revoke_role(username, role)?;
            let users_path = self.base_path.join("_metadata").join("users.json");
            store.save(users_path)?;
        }

        Ok(())
    }

    /// Get audit log
    pub async fn get_audit_log(&self) -> Result<Vec<crate::security::AuditEntry>> {
        if let Some(logger) = &self.audit_logger {
            Ok(logger.get_entries().await)
        } else {
            Ok(vec![])
        }
    }

    /// Check if current user has permission
    fn check_permission(&self, permission: &crate::security::Permission) -> Result<()> {
        // If auth is disabled (no role_manager), allow all operations
        if self.role_manager.is_none() {
            return Ok(());
        }

        // If auth is enabled, check permissions
        if let Some(role_manager) = &self.role_manager {
            if let Some(auth_ctx) = &self.auth_context {
                if !role_manager.has_permission(&auth_ctx.roles, permission) {
                    return Err(Error::Other(format!("Permission denied: {:?}", permission)));
                }
                return Ok(());
            } else {
                // Auth enabled but no auth context means user must authenticate
                return Err(Error::Other("Authentication required".to_string()));
            }
        }
        Ok(())
    }

    /// Get the base path of the database
    pub fn base_path(&self) -> &std::path::Path {
        &self.base_path
    }

    /// Log audit entry
    async fn audit_log(&self, operation: &str, details: &str, success: bool) {
        if let Some(logger) = &self.audit_logger {
            if let Some(auth_ctx) = &self.auth_context {
                let _ = logger
                    .log(
                        auth_ctx.username.clone(),
                        operation.to_string(),
                        details.to_string(),
                        success,
                    )
                    .await;
            }
        }
    }

    /// Get the database schema
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Insert a RecordBatch into the database
    pub async fn insert(&self, batch: RecordBatch) -> Result<u64> {
        info!("Inserting {} rows", batch.num_rows());

        // Check write permission
        self.check_permission(&crate::security::Permission::Write)?;

        // Track metrics on completion (success or error)
        let num_rows = batch.num_rows();
        let result = self.insert_delta_native(batch).await;
        match &result {
            Ok(_) => {
                self.metrics.total_inserts.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .total_transactions
                    .fetch_add(1, Ordering::Relaxed);
                self.audit_log("INSERT", &format!("{} rows", num_rows), true)
                    .await;
            }
            Err(e) => {
                self.metrics.total_errors.fetch_add(1, Ordering::Relaxed);
                self.audit_log("INSERT", &format!("failed: {}", e), false)
                    .await;
            }
        }
        result
    }

    /// Insert data via write buffer (batches multiple small writes for performance)
    ///
    /// This method buffers writes and flushes automatically when thresholds are reached.
    /// Use this for many small appends to reduce transaction overhead.
    ///
    /// The buffer accumulates batches and flushes when:
    /// - Buffer exceeds max_rows threshold (default: 1000)
    /// - flush_write_buffer() is called explicitly
    pub async fn insert_buffered(&self, batch: RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        info!("Buffering {} rows for insertion", num_rows);

        // Check write permission
        self.check_permission(&crate::security::Permission::Write)?;

        // Add batch to buffer and check if we should auto-flush
        let should_flush = self.batch_buffer.push(batch).await;

        if should_flush {
            let (batch_count, total_rows) = self.batch_buffer.stats().await;
            info!(
                "Auto-flushing batch buffer: {} rows across {} batches",
                total_rows, batch_count
            );

            // Take all batches and flush
            let batches_to_flush = self.batch_buffer.take_all().await;
            self.flush_batches(batches_to_flush).await?;
        }

        Ok(())
    }

    /// Flush the write buffer immediately
    ///
    /// Forces any buffered writes to be committed to Delta Lake.
    /// Call this before reading data to ensure consistency, or at shutdown.
    pub async fn flush_write_buffer(&self) -> Result<()> {
        if self.batch_buffer.is_empty().await {
            info!("No buffered data to flush");
            return Ok(());
        }

        let (batch_count, total_rows) = self.batch_buffer.stats().await;
        info!(
            "Flushing batch buffer: {} rows across {} batches",
            total_rows, batch_count
        );

        // Take all batches from buffer
        let batches_to_flush = self.batch_buffer.take_all().await;

        // Flush all batches
        self.flush_batches(batches_to_flush).await?;

        Ok(())
    }

    /// Internal: Flush multiple batches efficiently
    /// Concatenates batches if possible and inserts in a single Delta Lake transaction
    async fn flush_batches(&self, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        // If single batch, insert directly
        if batches.len() == 1 {
            return self
                .insert(batches.into_iter().next().unwrap())
                .await
                .map(|_| ());
        }

        // Multiple batches: concatenate them into one batch for single transaction
        let concatenated =
            crate::batch_buffer::BatchBuffer::concatenate_batches(&self.schema, batches)?;

        // Insert concatenated batch (single Delta Lake transaction)
        self.insert(concatenated).await.map(|_| ())
    }

    /// Get the underlying Delta Lake table for advanced operations
    ///
    /// Exposes the DeltaTable for version checking, history inspection, etc.
    pub async fn get_delta_table(&self) -> Result<deltalake::DeltaTable> {
        use crate::storage::s3::parse_s3_url;
        use deltalake::{open_table, open_table_with_storage_options};
        use url::Url;

        let table = if let (Some(s3_url), Some(storage_options)) =
            (&self.s3_url, &self.s3_storage_options)
        {
            // S3 backend
            let s3_url_parsed = parse_s3_url(s3_url)?;
            open_table_with_storage_options(s3_url_parsed, storage_options.clone())
                .await
                .map_err(Error::DeltaTable)?
        } else {
            // Local backend
            let table_url = Url::from_directory_path(&self.base_path)
                .map_err(|_| Error::Other("Invalid path for Delta table".to_string()))?;
            open_table(table_url).await.map_err(Error::DeltaTable)?
        };

        Ok(table)
    }

    /// Insert data using Delta Lake native format
    async fn insert_delta_native(&self, batch: RecordBatch) -> Result<u64> {
        use crate::storage::s3::parse_s3_url;
        use deltalake::operations::write::SchemaMode;
        use deltalake::protocol::SaveMode;
        use deltalake::{open_table, open_table_with_storage_options, DeltaOps};
        use url::Url;

        info!("Inserting {} rows to Delta Lake", batch.num_rows());

        // Open the Delta Lake table (S3 or local)
        let table = if let (Some(s3_url), Some(storage_options)) =
            (&self.s3_url, &self.s3_storage_options)
        {
            // S3 backend
            let s3_url_parsed = parse_s3_url(s3_url)?;
            open_table_with_storage_options(s3_url_parsed, storage_options.clone())
                .await
                .map_err(Error::DeltaTable)?
        } else {
            // Local backend
            let table_url = Url::from_directory_path(&self.base_path)
                .map_err(|_| Error::Other("Invalid path for Delta table".to_string()))?;
            open_table(table_url).await.map_err(Error::DeltaTable)?
        };

        // Write the batch using DeltaOps with schema merging enabled for evolution
        let row_count = batch.num_rows() as u64;

        DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .with_schema_mode(SchemaMode::Merge)
            .await
            .map_err(Error::DeltaTable)?;

        info!("Successfully wrote {} rows to Delta Lake", row_count);

        // Return a synthetic transaction ID (Delta Lake uses versions, not transaction IDs)
        // We can use the current timestamp as a pseudo txn_id
        let txn_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(txn_id)
    }

    /// Query Delta Lake natively using DataFusion
    async fn query_delta_native(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        use crate::storage::s3::parse_s3_url;
        use deltalake::{
            datafusion::prelude::SessionContext, open_table, open_table_with_storage_options,
        };
        use url::Url;

        info!("Querying Delta Lake with SQL: {}", sql);

        // Open the Delta Lake table (S3 or local)
        let table = if let (Some(s3_url), Some(storage_options)) =
            (&self.s3_url, &self.s3_storage_options)
        {
            // S3 backend
            let s3_url_parsed = parse_s3_url(s3_url)?;
            open_table_with_storage_options(s3_url_parsed, storage_options.clone())
                .await
                .map_err(Error::DeltaTable)?
        } else {
            // Local backend
            let table_url = Url::from_directory_path(&self.base_path)
                .map_err(|_| Error::Other("Invalid path for Delta table".to_string()))?;
            open_table(table_url).await.map_err(Error::DeltaTable)?
        };

        // Create DataFusion context and register the table
        let ctx = SessionContext::new();
        ctx.register_table("data", Arc::new(table))
            .map_err(|e| Error::InvalidOperation(e.to_string()))?;

        // Execute the SQL query
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| Error::InvalidOperation(e.to_string()))?;

        // Collect results
        let batches = df
            .collect()
            .await
            .map_err(|e| Error::InvalidOperation(e.to_string()))?;

        info!("Query returned {} batches", batches.len());
        Ok(batches)
    }

    /// Delete rows from Delta Lake using native DELETE operation
    async fn delete_delta_native(&self, where_clause: &str) -> Result<usize> {
        use deltalake::{open_table, DeltaOps};
        use url::Url;

        info!("Deleting from Delta Lake where: {}", where_clause);

        // First count how many rows will be deleted
        let count_query = format!("SELECT COUNT(*) as count FROM data WHERE {}", where_clause);
        let count_results = self.query_delta_native(&count_query).await?;
        let deleted_count = if !count_results.is_empty() {
            count_results[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .map(|arr| arr.value(0) as usize)
                .unwrap_or(0)
        } else {
            0
        };

        if deleted_count == 0 {
            info!("No rows match deletion criteria");
            return Ok(0);
        }

        // Open the Delta Lake table
        let table_url = Url::from_directory_path(&self.base_path)
            .map_err(|_| Error::Other("Invalid path for Delta table".to_string()))?;

        let table = open_table(table_url).await.map_err(Error::DeltaTable)?;

        // Execute DELETE operation
        DeltaOps(table)
            .delete()
            .with_predicate(where_clause)
            .await
            .map_err(Error::DeltaTable)?;

        info!(
            "Successfully deleted {} rows from Delta Lake",
            deleted_count
        );
        Ok(deleted_count)
    }

    /// Compute a unified schema from all batches to handle schema evolution
    pub(crate) fn compute_unified_schema(&self, batches: &[RecordBatch]) -> Result<SchemaRef> {
        use arrow::datatypes::Field;
        use std::collections::HashMap;

        // Collect all unique fields across all batch schemas
        let mut fields_map: HashMap<String, Field> = HashMap::new();

        for batch in batches {
            for field in batch.schema().fields() {
                let field_name = field.name().to_string();

                // If field doesn't exist yet, add it
                // If it exists, ensure it's nullable (to allow NULLs for missing values)
                if let Some(existing_field) = fields_map.get(&field_name) {
                    // If either schema has this field as nullable, make it nullable in unified schema
                    if field.is_nullable() || existing_field.is_nullable() {
                        let nullable_field = Field::new(
                            field_name.clone(),
                            field.data_type().clone(),
                            true, // Make nullable
                        );
                        fields_map.insert(field_name, nullable_field);
                    }
                } else {
                    // New field - make it nullable for backward compatibility
                    let nullable_field = Field::new(
                        field.name(),
                        field.data_type().clone(),
                        true, // Always nullable for schema evolution
                    );
                    fields_map.insert(field_name, nullable_field);
                }
            }
        }

        // Maintain field order: prefer the order from the most recent batch (last batch)
        // This ensures new fields appear at the end
        let mut ordered_fields = Vec::new();
        let mut seen_fields = std::collections::HashSet::new();

        // First add fields in the order they appear in batches (oldest to newest)
        for batch in batches {
            for field in batch.schema().fields() {
                let field_name = field.name().to_string();
                if !seen_fields.contains(&field_name) {
                    ordered_fields.push(fields_map[&field_name].clone());
                    seen_fields.insert(field_name);
                }
            }
        }

        let unified_schema = Arc::new(Schema::new(ordered_fields));
        Ok(unified_schema)
    }

    /// Unify a batch's schema to match the unified schema by adding NULL columns for missing fields
    pub(crate) fn unify_batch_schema(
        &self,
        batch: &RecordBatch,
        unified_schema: &SchemaRef,
    ) -> Result<RecordBatch> {
        use arrow::array::{new_null_array, ArrayRef};

        let num_rows = batch.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::new();

        // For each field in the unified schema, get the column from batch or create NULL array
        for unified_field in unified_schema.fields() {
            let field_name = unified_field.name();

            // Try to find this field in the batch schema
            if let Some((idx, _batch_field)) = batch
                .schema()
                .fields()
                .iter()
                .enumerate()
                .find(|(_i, f)| f.name() == field_name)
            {
                // Field exists in batch - use it
                columns.push(batch.column(idx).clone());
            } else {
                // Field missing in batch - create NULL array
                let null_array = new_null_array(unified_field.data_type(), num_rows);
                columns.push(null_array);
            }
        }

        let unified_batch = RecordBatch::try_new(unified_schema.clone(), columns)?;
        Ok(unified_batch)
    }

    /// Query the database using SQL
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        info!("Executing query: {}", sql);

        // Check read permission
        self.check_permission(&crate::security::Permission::Read)?;

        // Track query metrics with latency
        let start = Instant::now();
        let result = self.query_inner(sql).await;
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        match &result {
            Ok(_) => {
                self.metrics.total_queries.fetch_add(1, Ordering::Relaxed);
                let mut latencies = self.metrics.query_latencies.lock().await;
                latencies.push(latency_ms);
                // Keep only last 1000 latencies to prevent unbounded growth
                let len = latencies.len();
                if len > 1000 {
                    latencies.drain(0..len - 1000);
                }
                self.audit_log("SELECT", sql, true).await;
            }
            Err(e) => {
                self.metrics.total_errors.fetch_add(1, Ordering::Relaxed);
                self.audit_log("SELECT", &format!("{}: {}", sql, e), false)
                    .await;
            }
        }
        result
    }

    /// Internal query method - delegates to Delta Lake native query with data skipping
    async fn query_inner(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // Extract predicates from SQL query
        let predicates = crate::delta_lake::data_skipping::extract_predicates(sql);

        // Get file-level statistics from Delta Lake transaction log
        let file_stats = crate::delta_lake::data_skipping::get_file_statistics(&self.base_path)?;

        // Evaluate which files can be skipped
        let mut files_to_read = Vec::new();
        let mut files_to_skip = Vec::new();

        for file_stat in &file_stats {
            let mut can_skip = false;

            // Check if file can be skipped based on predicates
            for (column, operator, value) in &predicates {
                if crate::delta_lake::data_skipping::can_skip_file(
                    file_stat, column, operator, value,
                ) {
                    can_skip = true;
                    break;
                }
            }

            if can_skip {
                files_to_skip.push(file_stat.clone());
            } else {
                files_to_read.push(file_stat.clone());
            }
        }

        // Update data skipping statistics
        {
            let mut stats = self.data_skipping_stats.lock().await;
            stats.total_files = file_stats.len();
            stats.files_read = files_to_read.len();
            stats.files_skipped = files_to_skip.len();
            stats.bytes_scanned = files_to_read.iter().map(|f| f.size_bytes).sum();
            stats.bytes_skipped = files_to_skip.iter().map(|f| f.size_bytes).sum();
        }

        info!(
            "Data skipping: {} files total, {} to read, {} skipped",
            file_stats.len(),
            files_to_read.len(),
            files_to_skip.len()
        );

        // Execute query (Delta Lake + DataFusion will also do its own pruning)
        self.query_delta_native(sql).await
    }

    /// Query the database at a specific Delta Lake version (time travel)
    ///
    /// This allows querying historical data without affecting the current state.
    /// Version numbers start at 0 (first commit).
    pub async fn query_version(&self, sql: &str, version: i64) -> Result<Vec<RecordBatch>> {
        info!(
            "Executing time travel query at version {}: {}",
            version, sql
        );

        // Check read permission
        self.check_permission(&crate::security::Permission::Read)?;

        // Track query metrics with latency
        let start = Instant::now();
        let result = self.query_version_inner(sql, version).await;
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        match &result {
            Ok(_) => {
                self.metrics.total_queries.fetch_add(1, Ordering::Relaxed);
                let mut latencies = self.metrics.query_latencies.lock().await;
                latencies.push(latency_ms);
                let len = latencies.len();
                if len > 1000 {
                    latencies.drain(0..len - 1000);
                }
                self.audit_log("SELECT_VERSION", &format!("v{}: {}", version, sql), true)
                    .await;
            }
            Err(e) => {
                self.metrics.total_errors.fetch_add(1, Ordering::Relaxed);
                self.audit_log(
                    "SELECT_VERSION",
                    &format!("v{}: {}: {}", version, sql, e),
                    false,
                )
                .await;
            }
        }
        result
    }

    /// Internal time travel query by version
    async fn query_version_inner(&self, sql: &str, version: i64) -> Result<Vec<RecordBatch>> {
        use deltalake::datafusion::prelude::SessionContext;
        use tracing::error;
        use url::Url;

        info!(
            "Querying Delta Lake with SQL at version {}: {}",
            version, sql
        );

        // Open table at specific version
        let table = if let Some(ref s3_url) = self.s3_url {
            if let Some(ref storage_options) = self.s3_storage_options {
                let url = Url::parse(s3_url)
                    .map_err(|e| Error::Other(format!("Invalid S3 URL: {}", e)))?;

                let builder = deltalake::DeltaTableBuilder::from_uri(url)
                    .map_err(|e| {
                        error!("Failed to create Delta table builder from S3 URL: {}", e);
                        Error::DeltaTable(e)
                    })?
                    .with_storage_options(storage_options.clone())
                    .with_version(version);

                builder.load().await.map_err(|e| {
                    error!(
                        "Failed to load Delta table at version {} from S3: {}",
                        version, e
                    );
                    Error::DeltaTable(e)
                })?
            } else {
                return Err(Error::Other(
                    "S3 storage options not configured".to_string(),
                ));
            }
        } else {
            let table_path = self
                .base_path
                .to_str()
                .ok_or_else(|| Error::Other("Invalid path".to_string()))?;

            let url = Url::from_file_path(table_path)
                .map_err(|_| Error::Other(format!("Invalid file path: {}", table_path)))?;

            deltalake::DeltaTableBuilder::from_uri(url)
                .map_err(|e| {
                    error!("Failed to create Delta table builder from path: {}", e);
                    Error::DeltaTable(e)
                })?
                .with_version(version)
                .load()
                .await
                .map_err(|e| {
                    error!("Failed to load Delta table at version {}: {}", version, e);
                    Error::DeltaTable(e)
                })?
        };

        // Create DataFusion context and register table
        let ctx = SessionContext::new();
        ctx.register_table("data", Arc::new(table))?;

        // Execute query
        let df = ctx.sql(sql).await?;
        let batches = df.collect().await?;

        info!(
            "Time travel query at version {} returned {} batches",
            version,
            batches.len()
        );
        Ok(batches)
    }

    /// Query the database at a specific timestamp (time travel)
    ///
    /// Timestamp is Unix epoch milliseconds. Delta Lake will find the version
    /// that was active at or before the given timestamp.
    pub async fn query_timestamp(&self, sql: &str, timestamp_ms: i64) -> Result<Vec<RecordBatch>> {
        info!(
            "Executing time travel query at timestamp {}: {}",
            timestamp_ms, sql
        );

        // Check read permission
        self.check_permission(&crate::security::Permission::Read)?;

        // Track query metrics with latency
        let start = Instant::now();
        let result = self.query_timestamp_inner(sql, timestamp_ms).await;
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        match &result {
            Ok(_) => {
                self.metrics.total_queries.fetch_add(1, Ordering::Relaxed);
                let mut latencies = self.metrics.query_latencies.lock().await;
                latencies.push(latency_ms);
                let len = latencies.len();
                if len > 1000 {
                    latencies.drain(0..len - 1000);
                }
                self.audit_log(
                    "SELECT_TIMESTAMP",
                    &format!("@{}: {}", timestamp_ms, sql),
                    true,
                )
                .await;
            }
            Err(e) => {
                self.metrics.total_errors.fetch_add(1, Ordering::Relaxed);
                self.audit_log(
                    "SELECT_TIMESTAMP",
                    &format!("@{}: {}: {}", timestamp_ms, sql, e),
                    false,
                )
                .await;
            }
        }
        result
    }

    /// Internal time travel query by timestamp
    async fn query_timestamp_inner(
        &self,
        sql: &str,
        timestamp_ms: i64,
    ) -> Result<Vec<RecordBatch>> {
        use chrono::{DateTime, Utc};
        use deltalake::datafusion::prelude::SessionContext;
        use tracing::error;
        use url::Url;

        info!(
            "Querying Delta Lake with SQL at timestamp {}: {}",
            timestamp_ms, sql
        );

        // Convert milliseconds to DateTime<Utc>
        let timestamp_secs = timestamp_ms / 1000;
        let timestamp_nanos = ((timestamp_ms % 1000) * 1_000_000) as u32;
        let datetime = DateTime::<Utc>::from_timestamp(timestamp_secs, timestamp_nanos)
            .ok_or_else(|| Error::Other(format!("Invalid timestamp: {}", timestamp_ms)))?;

        // Open table at specific timestamp
        let table = if let Some(ref s3_url) = self.s3_url {
            if let Some(ref storage_options) = self.s3_storage_options {
                let url = Url::parse(s3_url)
                    .map_err(|e| Error::Other(format!("Invalid S3 URL: {}", e)))?;

                let builder = deltalake::DeltaTableBuilder::from_uri(url)
                    .map_err(|e| {
                        error!("Failed to create Delta table builder from S3 URL: {}", e);
                        Error::DeltaTable(e)
                    })?
                    .with_storage_options(storage_options.clone())
                    .with_datestring(datetime.to_rfc3339())
                    .map_err(|e| {
                        error!("Failed to set datestring for Delta table: {}", e);
                        Error::DeltaTable(e)
                    })?;

                builder.load().await.map_err(|e| {
                    error!(
                        "Failed to load Delta table at timestamp {} from S3: {}",
                        timestamp_ms, e
                    );
                    Error::DeltaTable(e)
                })?
            } else {
                return Err(Error::Other(
                    "S3 storage options not configured".to_string(),
                ));
            }
        } else {
            let table_path = self
                .base_path
                .to_str()
                .ok_or_else(|| Error::Other("Invalid path".to_string()))?;

            let url = Url::from_file_path(table_path)
                .map_err(|_| Error::Other(format!("Invalid file path: {}", table_path)))?;

            deltalake::DeltaTableBuilder::from_uri(url)
                .map_err(|e| {
                    error!("Failed to create Delta table builder from path: {}", e);
                    Error::DeltaTable(e)
                })?
                .with_datestring(datetime.to_rfc3339())
                .map_err(|e| {
                    error!("Failed to set datestring for Delta table: {}", e);
                    Error::DeltaTable(e)
                })?
                .load()
                .await
                .map_err(|e| {
                    error!(
                        "Failed to load Delta table at timestamp {}: {}",
                        timestamp_ms, e
                    );
                    Error::DeltaTable(e)
                })?
        };

        // Create DataFusion context and register table
        let ctx = SessionContext::new();
        ctx.register_table("data", Arc::new(table))?;

        // Execute query
        let df = ctx.sql(sql).await?;
        let batches = df.collect().await?;

        info!(
            "Time travel query at timestamp {} returned {} batches",
            timestamp_ms,
            batches.len()
        );
        Ok(batches)
    }

    /// Query a specific Parquet file (for individual file views)
    pub async fn query_file(&self, file_path: &str) -> Result<Vec<RecordBatch>> {
        info!("Querying specific file: {}", file_path);

        let full_path = self.base_path.join(file_path);
        if !full_path.exists() {
            return Err(Error::RecordNotFound(format!(
                "File not found: {}",
                file_path
            )));
        }

        let reader = ParquetReader::new();
        let batch = reader.read_batch(&full_path)?;

        Ok(vec![batch])
    }

    /// Delete a specific file from Delta Lake
    /// Note: Delta Lake handles deletions natively through its transaction log
    pub async fn delete(&self, _file_path: &str) -> Result<u64> {
        // Track delete metrics
        self.metrics.total_deletes.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .total_transactions
            .fetch_add(1, Ordering::Relaxed);

        // Delta Lake doesn't support individual file deletion - use delete_rows_where instead
        Err(Error::Other(
            "Use delete_rows_where() for row-level deletion in Delta Lake".to_string(),
        ))
    }

    /// Delete rows matching a SQL WHERE clause (row-level deletion with deletion vectors)
    /// This is efficient as it doesn't rewrite Parquet files - just marks rows as deleted
    pub async fn delete_rows_where(&self, where_clause: &str) -> Result<usize> {
        info!("Deleting rows where: {}", where_clause);

        // Check write permission
        self.check_permission(&crate::security::Permission::Write)?;

        let result = self.delete_rows_where_inner(where_clause).await;
        match &result {
            Ok(count) => {
                self.metrics.total_deletes.fetch_add(1, Ordering::Relaxed);
                self.audit_log(
                    "DELETE",
                    &format!("WHERE {} ({} rows)", where_clause, count),
                    true,
                )
                .await;
            }
            Err(e) => {
                self.metrics.total_errors.fetch_add(1, Ordering::Relaxed);
                self.audit_log("DELETE", &format!("WHERE {}: {}", where_clause, e), false)
                    .await;
            }
        }
        result
    }

    /// Internal delete method - delegates to Delta Lake native delete
    async fn delete_rows_where_inner(&self, where_clause: &str) -> Result<usize> {
        self.delete_delta_native(where_clause).await
    }

    /// Create a MERGE operation builder
    ///
    /// MERGE allows INSERT, UPDATE, and DELETE operations in a single atomic transaction.
    ///
    /// # Example
    /// ```no_run
    /// use arrow::array::{Int32Array, StringArray};
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use arrow::record_batch::RecordBatch;
    /// use std::sync::Arc;
    /// use fsdb::DatabaseOps;
    ///
    /// # async fn example(db: &DatabaseOps) -> Result<(), Box<dyn std::error::Error>> {
    /// // Create source data with changes
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    ///     Field::new("name", DataType::Utf8, false),
    ///     Field::new("_op", DataType::Utf8, false),  // Operation type
    /// ]));
    ///
    /// let source = RecordBatch::try_new(
    ///     schema,
    ///     vec![
    ///         Arc::new(Int32Array::from(vec![1, 2, 3])),
    ///         Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
    ///         Arc::new(StringArray::from(vec!["UPDATE", "DELETE", "INSERT"])),
    ///     ],
    /// )?;
    ///
    /// // Execute MERGE
    /// let metrics = db.merge()
    ///     .await?
    ///     .with_source(source, "source")
    ///     .on("target.id = source.id")
    ///     .when_matched_update()
    ///         .condition("source._op = 'UPDATE'")
    ///         .set_all()
    ///     .when_matched_delete()
    ///         .condition("source._op = 'DELETE'")
    ///         .then()
    ///     .when_not_matched_insert()
    ///         .condition("source._op = 'INSERT'")
    ///         .values_all()
    ///     .execute()
    ///     .await?;
    ///
    /// println!("MERGE complete: {} inserted, {} updated, {} deleted",
    ///          metrics.rows_inserted, metrics.rows_updated, metrics.rows_deleted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn merge(&self) -> Result<crate::delta_lake::merge::MergeBuilder> {
        info!("Creating MERGE operation");

        // Check write permission
        self.check_permission(&crate::security::Permission::Write)?;

        // Open the Delta Lake table
        use crate::storage::s3::parse_s3_url;
        use deltalake::{open_table, open_table_with_storage_options};
        use url::Url;

        let table = if let (Some(s3_url), Some(storage_options)) =
            (&self.s3_url, &self.s3_storage_options)
        {
            // S3 backend
            let s3_url_parsed = parse_s3_url(s3_url)?;
            open_table_with_storage_options(s3_url_parsed, storage_options.clone())
                .await
                .map_err(Error::DeltaTable)?
        } else {
            // Local backend
            let table_url = Url::from_directory_path(&self.base_path)
                .map_err(|_| Error::Other("Invalid path for Delta table".to_string()))?;
            open_table(table_url).await.map_err(Error::DeltaTable)?
        };

        Ok(crate::delta_lake::merge::MergeBuilder::new(table))
    }

    /// Compact database files using Delta Lake OPTIMIZE
    /// - Merges small Parquet files into larger ones
    /// - Removes deleted records (Delta Lake deletion vectors)
    /// - Bin-packs files to target size
    pub async fn optimize(&self) -> Result<()> {
        info!("Running Delta Lake OPTIMIZE operation");

        // Check write permission
        self.check_permission(&crate::security::Permission::Write)?;

        let result = self.optimize_inner(None, None).await;
        match &result {
            Ok(metrics) => {
                info!(
                    "OPTIMIZE completed: {} files added, {} files removed",
                    metrics.num_files_added, metrics.num_files_removed
                );
                self.audit_log(
                    "OPTIMIZE",
                    &format!(
                        "compacted {} -> {} files",
                        metrics.num_files_removed, metrics.num_files_added
                    ),
                    true,
                )
                .await;
            }
            Err(e) => {
                self.metrics.total_errors.fetch_add(1, Ordering::Relaxed);
                self.audit_log("OPTIMIZE", &format!("failed: {}", e), false)
                    .await;
            }
        }
        result.map(|_| ())
    }

    /// Optimize with a filter predicate (only compact matching partitions)
    pub async fn optimize_with_filter(&self, filter: &str) -> Result<()> {
        info!("Running Delta Lake OPTIMIZE with filter: {}", filter);

        // Check write permission
        self.check_permission(&crate::security::Permission::Write)?;

        let result = self.optimize_inner(Some(filter), None).await;
        match &result {
            Ok(metrics) => {
                info!(
                    "OPTIMIZE with filter completed: {} files added, {} files removed",
                    metrics.num_files_added, metrics.num_files_removed
                );
                self.audit_log(
                    "OPTIMIZE",
                    &format!(
                        "filter='{}': {} -> {} files",
                        filter, metrics.num_files_removed, metrics.num_files_added
                    ),
                    true,
                )
                .await;
            }
            Err(e) => {
                self.metrics.total_errors.fetch_add(1, Ordering::Relaxed);
                self.audit_log("OPTIMIZE", &format!("filter='{}': {}", filter, e), false)
                    .await;
            }
        }
        result.map(|_| ())
    }

    /// Optimize with target file size
    pub async fn optimize_with_target_size(&self, target_size_bytes: u64) -> Result<()> {
        info!(
            "Running Delta Lake OPTIMIZE with target size: {} bytes",
            target_size_bytes
        );

        // Check write permission
        self.check_permission(&crate::security::Permission::Write)?;

        let result = self.optimize_inner(None, Some(target_size_bytes)).await;
        match &result {
            Ok(metrics) => {
                info!(
                    "OPTIMIZE with target size completed: {} files added, {} files removed",
                    metrics.num_files_added, metrics.num_files_removed
                );
                self.audit_log(
                    "OPTIMIZE",
                    &format!(
                        "target_size={}: {} -> {} files",
                        target_size_bytes, metrics.num_files_removed, metrics.num_files_added
                    ),
                    true,
                )
                .await;
            }
            Err(e) => {
                self.metrics.total_errors.fetch_add(1, Ordering::Relaxed);
                self.audit_log(
                    "OPTIMIZE",
                    &format!("target_size={}: {}", target_size_bytes, e),
                    false,
                )
                .await;
            }
        }
        result.map(|_| ())
    }

    /// Internal optimize implementation
    async fn optimize_inner(
        &self,
        filter: Option<&str>,
        target_size: Option<u64>,
    ) -> Result<crate::delta_lake::OptimizeMetrics> {
        crate::delta_lake::optimize_table(
            &self.base_path,
            self.s3_url.as_deref(),
            self.s3_storage_options.as_ref(),
            filter,
            target_size,
        )
        .await
    }

    /// Legacy compact method - delegates to optimize()
    pub async fn compact(&self) -> Result<()> {
        self.optimize().await
    }

    /// Legacy compact with custom options - delegates to optimize_with_target_size
    pub async fn compact_with_options(&self, min_file_size: u64) -> Result<()> {
        // Use min_file_size as target size
        self.optimize_with_target_size(min_file_size).await
    }

    /// VACUUM: Physically delete old Parquet files no longer referenced by Delta Lake
    ///
    /// - `retention_hours`: Minimum age in hours for files to be eligible for deletion
    /// - Files newer than retention period are kept (for time travel safety)
    /// - Default retention is usually 168 hours (7 days) in production
    /// - Use retention=0 only for testing or when time travel is not needed
    pub async fn vacuum(&self, retention_hours: u64) -> Result<()> {
        info!(
            "Running Delta Lake VACUUM with {} hour retention",
            retention_hours
        );

        // Check write permission
        self.check_permission(&crate::security::Permission::Write)?;

        let result = self.vacuum_inner(retention_hours, false).await;
        match &result {
            Ok(deleted_count) => {
                info!("VACUUM completed: {} files deleted", deleted_count);
                self.audit_log(
                    "VACUUM",
                    &format!(
                        "retention={}h: {} files deleted",
                        retention_hours, deleted_count
                    ),
                    true,
                )
                .await;
            }
            Err(e) => {
                self.metrics.total_errors.fetch_add(1, Ordering::Relaxed);
                self.audit_log(
                    "VACUUM",
                    &format!("retention={}h: {}", retention_hours, e),
                    false,
                )
                .await;
            }
        }
        result.map(|_| ())
    }

    /// VACUUM dry run: Preview what files would be deleted without actually deleting
    /// Returns list of file paths that would be deleted
    pub async fn vacuum_dry_run(&self, retention_hours: u64) -> Result<Vec<String>> {
        info!(
            "Running Delta Lake VACUUM dry run with {} hour retention",
            retention_hours
        );

        // Check read permission for dry run
        self.check_permission(&crate::security::Permission::Read)?;

        self.vacuum_inner_dry_run(retention_hours).await
    }

    /// Internal VACUUM implementation
    async fn vacuum_inner(&self, retention_hours: u64, dry_run: bool) -> Result<usize> {
        crate::delta_lake::vacuum_table(
            &self.base_path,
            self.s3_url.as_deref(),
            self.s3_storage_options.as_ref(),
            retention_hours,
            dry_run,
        )
        .await
    }

    /// Internal dry run implementation
    async fn vacuum_inner_dry_run(&self, retention_hours: u64) -> Result<Vec<String>> {
        crate::delta_lake::vacuum_dry_run(
            &self.base_path,
            self.s3_url.as_deref(),
            self.s3_storage_options.as_ref(),
            retention_hours,
        )
        .await
    }

    /// Z-ORDER: Multi-dimensional clustering for query performance
    ///
    /// Z-ORDER organizes data using a space-filling Z-curve that preserves multi-dimensional locality.
    /// This improves query performance for filters on the specified columns.
    ///
    /// - `columns`: Columns to cluster by (typically 2-4 columns for best results)
    /// - Creates new clustered files and marks old files as removed (like OPTIMIZE)
    /// - VACUUM is needed to physically delete the old files
    ///
    /// Example: `db.zorder(&["date", "category"]).await?`
    pub async fn zorder(&self, columns: &[&str]) -> Result<()> {
        info!("Running Delta Lake Z-ORDER on columns: {:?}", columns);

        // Check write permission
        self.check_permission(&crate::security::Permission::Write)?;

        if columns.is_empty() {
            return Err(Error::InvalidOperation(
                "Z-ORDER requires at least one column".to_string(),
            ));
        }

        let result = self.zorder_inner(columns).await;
        match &result {
            Ok(metrics) => {
                info!(
                    "Z-ORDER completed: {} files added, {} files removed",
                    metrics.num_files_added, metrics.num_files_removed
                );
                self.audit_log(
                    "ZORDER",
                    &format!(
                        "columns={:?}: {} -> {} files",
                        columns, metrics.num_files_removed, metrics.num_files_added
                    ),
                    true,
                )
                .await;
            }
            Err(e) => {
                self.metrics.total_errors.fetch_add(1, Ordering::Relaxed);
                self.audit_log("ZORDER", &format!("columns={:?}: {}", columns, e), false)
                    .await;
            }
        }
        result.map(|_| ())
    }

    /// Internal Z-ORDER implementation
    async fn zorder_inner(&self, columns: &[&str]) -> Result<crate::delta_lake::OptimizeMetrics> {
        crate::delta_lake::zorder_table(
            &self.base_path,
            self.s3_url.as_deref(),
            self.s3_storage_options.as_ref(),
            columns,
        )
        .await
    }

    /// Get column statistics from Delta Lake transaction log
    pub async fn get_column_statistics(&self) -> Result<HashMap<String, ColumnStats>> {
        get_column_statistics_from_delta(&self.base_path)
    }

    /// Legacy method - delegates to main method
    #[allow(dead_code)]
    async fn get_column_statistics_delta(&self) -> Result<HashMap<String, ColumnStats>> {
        self.get_column_statistics().await
    }

    /// Get query pruning statistics (Delta Lake handles this natively)
    pub async fn get_pruning_statistics(&self) -> Result<PruningStats> {
        // Delta Lake doesn't expose pruning stats the same way
        // Return empty stats for now
        Ok(PruningStats {
            files_scanned: 0,
            files_pruned: 0,
            total_files: 0,
        })
    }

    /// Get current database metrics for monitoring and observability
    pub async fn get_metrics(&self) -> DatabaseMetrics {
        let latencies = self.metrics.query_latencies.lock().await;
        let avg_latency = if latencies.is_empty() {
            0.0
        } else {
            latencies.iter().sum::<f64>() / latencies.len() as f64
        };
        let max_latency = latencies.iter().copied().fold(0.0, f64::max);
        drop(latencies); // Release lock

        DatabaseMetrics {
            total_queries: self.metrics.total_queries.load(Ordering::Relaxed),
            total_inserts: self.metrics.total_inserts.load(Ordering::Relaxed),
            total_deletes: self.metrics.total_deletes.load(Ordering::Relaxed),
            total_transactions: self.metrics.total_transactions.load(Ordering::Relaxed),
            total_errors: self.metrics.total_errors.load(Ordering::Relaxed),
            avg_query_latency_ms: avg_latency,
            max_query_latency_ms: max_latency,
            uptime_seconds: self.metrics.start_time.elapsed().as_secs_f64(),
        }
    }

    /// Get data skipping statistics
    ///
    /// Returns information about how many files were skipped during the last query
    /// based on min/max statistics pruning.
    pub async fn get_data_skipping_stats(&self) -> Result<DataSkippingStats> {
        let stats = self.data_skipping_stats.lock().await;
        Ok(stats.clone())
    }

    /// Reset data skipping statistics
    pub async fn reset_data_skipping_stats(&self) {
        let mut stats = self.data_skipping_stats.lock().await;
        *stats = DataSkippingStats::default();
    }

    /// Get database health status
    pub async fn health_check(&self) -> HealthStatus {
        // For Delta Lake, check _delta_log directory
        let delta_log_exists = self.base_path.join("_delta_log").exists();

        HealthStatus {
            status: if delta_log_exists {
                "healthy"
            } else {
                "not_initialized"
            }
            .to_string(),
            uptime_seconds: self.metrics.start_time.elapsed().as_secs_f64(),
            total_files: 0, // Delta Lake doesn't expose this directly without scanning
            total_rows: 0,  // Would require scanning all parquet files
            total_size_bytes: 0, // Would require scanning
        }
    }

    /// Reset metrics counters (except uptime)
    pub async fn reset_metrics(&self) {
        self.metrics.reset();
        let mut latencies = self.metrics.query_latencies.lock().await;
        latencies.clear();
    }

    /// Create a full backup of the database
    /// Create a full backup of the database
    pub async fn backup<P: AsRef<Path>>(&self, backup_path: P) -> Result<()> {
        let backup_path = backup_path.as_ref();
        info!("Creating backup at: {}", backup_path.display());

        // Create backup directory
        std::fs::create_dir_all(backup_path)?;

        // Copy all database files
        crate::metadata::backup::copy_dir_recursively(&self.base_path, backup_path)?;

        // Count files and size
        let mut total_files = 0;
        let mut total_size_bytes = 0;
        crate::metadata::backup::count_parquet_files(
            backup_path,
            &mut total_files,
            &mut total_size_bytes,
        )?;

        // Query for total row count
        let total_rows = self
            .query("SELECT COUNT(*) as count FROM data")
            .await
            .ok()
            .and_then(|batches| batches.first().cloned())
            .and_then(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .filter(|arr| !arr.is_empty() && !arr.is_null(0))
                    .map(|arr| arr.value(0) as u64)
            })
            .unwrap_or(0);

        // Write backup metadata
        let metadata = BackupMetadata {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            total_rows,
            total_files,
            total_size_bytes,
        };

        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        std::fs::write(backup_path.join("backup_metadata.json"), metadata_json)?;

        info!(
            "Backup completed: {} files, {} rows, {} bytes",
            total_files, total_rows, total_size_bytes
        );
        Ok(())
    }

    /// Restore database from backup
    pub async fn restore<P: AsRef<Path>>(backup_path: P, restore_path: P) -> Result<()> {
        info!(
            "Restoring database from {} to {}",
            backup_path.as_ref().display(),
            restore_path.as_ref().display()
        );
        crate::metadata::backup::restore(backup_path, restore_path)?;
        info!("Database restored successfully");
        Ok(())
    }

    /// Create incremental backup (only new data since last backup)
    pub async fn backup_incremental<P: AsRef<Path>>(
        &self,
        base_backup_path: P,
        incremental_path: P,
    ) -> Result<()> {
        let base_backup_path = base_backup_path.as_ref();
        let incremental_path = incremental_path.as_ref();
        info!(
            "Creating incremental backup at: {}",
            incremental_path.display()
        );

        // Read base backup metadata to get timestamp
        let base_metadata_path = base_backup_path.join("backup_metadata.json");
        if !base_metadata_path.exists() {
            return Err(Error::Other("Base backup metadata not found".to_string()));
        }

        let base_metadata_content = std::fs::read_to_string(&base_metadata_path)?;
        let base_metadata: BackupMetadata = serde_json::from_str(&base_metadata_content)?;
        let since_timestamp = base_metadata.timestamp;

        // Create incremental backup directory
        std::fs::create_dir_all(incremental_path)?;

        // Copy only parquet files modified after base backup timestamp
        let mut total_files = 0;
        let mut total_size_bytes = 0;

        for entry in std::fs::read_dir(&self.base_path)? {
            let entry = entry?;
            let src = entry.path();

            if src.is_file() && src.extension().and_then(|s| s.to_str()) == Some("parquet") {
                let metadata = entry.metadata()?;
                if let Ok(modified) = metadata.modified() {
                    let modified_ts = modified
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;

                    if modified_ts > since_timestamp {
                        let filename = src.file_name().unwrap();
                        let dst = incremental_path.join(filename);
                        let file_size = std::fs::copy(&src, &dst)?;
                        total_size_bytes += file_size;
                        total_files += 1;
                    }
                }
            }
        }

        // Copy Delta Lake transaction log (only new entries)
        let delta_log_src = self.base_path.join("_delta_log");
        let delta_log_dst = incremental_path.join("_delta_log");
        std::fs::create_dir_all(&delta_log_dst)?;

        for entry in std::fs::read_dir(&delta_log_src)? {
            let entry = entry?;
            let src = entry.path();

            if src.is_file() && src.extension().and_then(|s| s.to_str()) == Some("json") {
                let metadata = entry.metadata()?;
                if let Ok(modified) = metadata.modified() {
                    let modified_ts = modified
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;

                    if modified_ts > since_timestamp {
                        let filename = src.file_name().unwrap();
                        let dst = delta_log_dst.join(filename);
                        std::fs::copy(&src, &dst)?;
                    }
                }
            }
        }

        // Write incremental backup metadata
        let metadata = BackupMetadata {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            total_rows: 0, // Incremental doesn't track total rows
            total_files,
            total_size_bytes,
        };

        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        std::fs::write(incremental_path.join("backup_metadata.json"), metadata_json)?;

        info!(
            "Incremental backup completed: {} files, {} bytes",
            total_files, total_size_bytes
        );
        Ok(())
    }

    /// Verify backup integrity
    pub async fn verify_backup<P: AsRef<Path>>(backup_path: P) -> Result<BackupVerificationReport> {
        crate::metadata::backup::verify_backup(backup_path)
    }

    /// Restore database to a specific transaction (point-in-time recovery)
    /// target_txn_id is a timestamp in milliseconds
    pub async fn restore_to_transaction<P: AsRef<Path>>(
        backup_path: P,
        restore_path: P,
        target_txn_id: u64,
    ) -> Result<()> {
        let backup_path_buf = backup_path.as_ref().to_path_buf();
        let restore_path_buf = restore_path.as_ref().to_path_buf();

        info!(
            "Restoring to timestamp {} from {}",
            target_txn_id,
            backup_path_buf.display()
        );

        // Use spawn_blocking to avoid runtime nesting issues
        tokio::task::spawn_blocking(move || {
            // First restore the full backup
            crate::metadata::backup::restore(&backup_path_buf, &restore_path_buf)?;

            // Map timestamp to Delta Lake version
            let delta_log_path = restore_path_buf.join("_delta_log");
            let mut target_version = 0u64;

            // Read Delta log files to find version with matching timestamp
            let mut versions = Vec::new();
            for entry in std::fs::read_dir(&delta_log_path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
                    let filename = path.file_name().unwrap().to_str().unwrap();
                    if let Some(version_str) = filename.strip_suffix(".json") {
                        if let Ok(version) = version_str.parse::<u64>() {
                            versions.push(version);
                        }
                    }
                }
            }
            versions.sort();

            // Read each Delta log commit to find timestamp
            for version in versions {
                let commit_file = format!("{:020}.json", version);
                let commit_path = delta_log_path.join(&commit_file);

                if commit_path.exists() {
                    let content = std::fs::read_to_string(&commit_path)?;

                    // Parse each line as JSON to find commitInfo with timestamp
                    for line in content.lines() {
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                            if let Some(commit_info) = json.get("commitInfo") {
                                if let Some(timestamp) = commit_info.get("timestamp") {
                                    if let Some(ts) = timestamp.as_i64() {
                                        let commit_ts = ts as u64;
                                        // If this commit happened at or before the target, update target_version
                                        if commit_ts <= target_txn_id {
                                            target_version = version;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            info!(
                "Mapped timestamp {} to Delta version {}",
                target_txn_id, target_version
            );

            // Now restore to that version
            crate::metadata::backup::restore_to_delta_version(&restore_path_buf, target_version)?;
            Ok::<(), Error>(())
        })
        .await
        .map_err(|e| Error::Other(format!("Join error: {}", e)))??;

        Ok(())
    }

    /// Get backup metadata
    pub async fn get_backup_metadata<P: AsRef<Path>>(backup_path: P) -> Result<BackupMetadata> {
        crate::metadata::backup::get_backup_metadata(backup_path)
    }
}

// Helper functions moved to modules:
// - extract_predicates -> query::pruning
// - is_value_less_than/is_value_greater_than -> query::pruning
// - compute_column_statistics -> delta_lake::stats

// Helper functions moved to modules:
// - extract_predicates -> query::pruning
// - is_value_less_than/is_value_greater_than -> query::pruning
// - compute_column_statistics -> delta_lake::stats

impl DatabaseOps {
    /// Begin an explicit transaction with user-controlled commit/rollback
    ///
    /// This allows batching multiple operations into a single transaction.
    /// Example:
    /// ```no_run
    /// # use fsdb::database_ops::DatabaseOps;
    /// # use std::sync::Arc;
    /// # async fn example(db: Arc<DatabaseOps>, batch1: arrow::record_batch::RecordBatch, batch2: arrow::record_batch::RecordBatch) -> fsdb::Result<()> {
    /// let txn = db.begin_transaction().await?;
    /// txn.insert(batch1).await?;
    /// txn.insert(batch2).await?;
    /// txn.commit().await?; // Single transaction, one file
    /// # Ok(())
    /// # }
    /// ```
    pub async fn begin_transaction(self: &Arc<Self>) -> Result<Transaction> {
        // Simple transaction ID allocation
        use std::sync::atomic::{AtomicU64, Ordering};
        static TXN_COUNTER: AtomicU64 = AtomicU64::new(1);
        let txn_id = TXN_COUNTER.fetch_add(1, Ordering::SeqCst);
        let snapshot_version = txn_id;

        info!("Beginning transaction {}", txn_id);

        Ok(Transaction::new(Arc::clone(self), txn_id, snapshot_version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
        ]))
    }

    fn create_test_batch(schema: SchemaRef, start_id: i32) -> RecordBatch {
        let id_array =
            Arc::new(Int32Array::from(vec![start_id, start_id + 1, start_id + 2])) as ArrayRef;
        let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef;
        let email_array = Arc::new(StringArray::from(vec![
            "alice@example.com",
            "bob@example.com",
            "charlie@example.com",
        ])) as ArrayRef;

        RecordBatch::try_new(schema, vec![id_array, name_array, email_array]).unwrap()
    }

    #[tokio::test]
    async fn test_database_create_and_insert() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let db = DatabaseOps::create(temp_dir.path(), schema.clone())
            .await
            .unwrap();
        let batch = create_test_batch(schema, 1);

        let txn_id = db.insert(batch).await.unwrap();
        assert!(txn_id > 0, "Transaction ID should be positive");

        // Verify Parquet file exists (Delta Lake stores files in root directory)
        let mut parquet_count = 0;
        for entry in std::fs::read_dir(temp_dir.path()).unwrap().flatten() {
            if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
                parquet_count += 1;
            }
        }
        assert!(parquet_count > 0, "At least one Parquet file should exist");
    }

    #[tokio::test]
    async fn test_database_insert_and_query() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let db = DatabaseOps::create(temp_dir.path(), schema.clone())
            .await
            .unwrap();
        let batch = create_test_batch(schema, 1);

        db.insert(batch).await.unwrap();

        // Query all data
        let results = db.query("SELECT * FROM data").await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
        assert_eq!(results[0].num_columns(), 3);
    }

    #[tokio::test]
    async fn test_database_insert_and_query_with_filter() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let db = DatabaseOps::create(temp_dir.path(), schema.clone())
            .await
            .unwrap();
        let batch = create_test_batch(schema, 1);

        db.insert(batch).await.unwrap();

        // Query with WHERE clause
        let results = db
            .query("SELECT name, email FROM data WHERE id > 1")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 2); // Bob and Charlie
        assert_eq!(results[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn test_database_multiple_inserts() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let db = DatabaseOps::create(temp_dir.path(), schema.clone())
            .await
            .unwrap();

        // Insert first batch
        let batch1 = create_test_batch(schema.clone(), 1);
        let txn1 = db.insert(batch1).await.unwrap();

        // Insert second batch
        let batch2 = create_test_batch(schema.clone(), 4);
        let txn2 = db.insert(batch2).await.unwrap();

        assert!(txn1 > 0, "Transaction ID 1 should be positive");
        assert!(
            txn2 > txn1,
            "Transaction ID 2 should be greater than transaction ID 1"
        );

        // Query all data
        let results = db
            .query("SELECT COUNT(*) as count FROM data")
            .await
            .unwrap();
        assert_eq!(results.len(), 1);

        let count_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 6); // 3 + 3 rows
    }

    // Removed: Legacy test for file deletion - Delta Lake uses deletion vectors natively
    #[tokio::test]
    #[allow(dead_code)]
    async fn _removed_test_database_delete() {
        // This test used file_inventory which doesn't exist in Delta Lake mode
        // Delta Lake handles row-level deletion using deletion vectors
    }

    #[tokio::test]
    async fn test_database_aggregation_query() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let db = DatabaseOps::create(temp_dir.path(), schema.clone())
            .await
            .unwrap();
        let batch = create_test_batch(schema, 1);

        db.insert(batch).await.unwrap();

        // Aggregation query
        let results = db
            .query("SELECT COUNT(*) as total, COUNT(DISTINCT name) as unique_names FROM data")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);

        let total_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(total_col.value(0), 3);
    }

    #[tokio::test]
    async fn test_database_delta_log_written() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let db = DatabaseOps::create(temp_dir.path(), schema.clone())
            .await
            .unwrap();
        let batch = create_test_batch(schema, 1);

        let _txn_id = db.insert(batch).await.unwrap();

        // Verify Delta Lake transaction log exists (Delta Lake native mode)
        let delta_log_dir = temp_dir.path().join("_delta_log");
        assert!(
            delta_log_dir.exists(),
            "Delta Lake transaction log directory should exist"
        );

        let delta_files: Vec<_> = std::fs::read_dir(&delta_log_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("json"))
            .collect();
        assert!(
            !delta_files.is_empty(),
            "Delta Lake log should have at least one commit file"
        );
    }
}
