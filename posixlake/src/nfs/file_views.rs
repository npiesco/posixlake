use crate::database_ops::DatabaseOps;
use crate::error::Result;
use arrow::array::RecordBatch;
use arrow::csv::{ReaderBuilder as CsvReaderBuilder, Writer as CsvWriter};
use arrow::json::{ArrayWriter as JsonArrayWriter, LineDelimitedWriter as JsonLinesWriter};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// CSV file view that generates CSV content from database query results
/// With lazy loading: content is generated on-demand, not cached internally.
/// Caching is handled by the NFS cache layer for better performance.
pub struct CsvFileView {
    db: Arc<DatabaseOps>,
    /// Optional: specific file to query (if None, query all data)
    file_path: Option<String>,
}

impl CsvFileView {
    /// Create a new CSV file view for a database (queries all data)
    pub fn new(db: Arc<DatabaseOps>) -> Self {
        debug!("Creating CSV file view for all data (lazy loading)");
        CsvFileView {
            db,
            file_path: None,
        }
    }

    /// Create a new CSV file view for a specific Parquet file
    pub fn new_for_file(db: Arc<DatabaseOps>, file_path: String) -> Self {
        debug!(
            "Creating CSV file view for specific file: {} (lazy loading)",
            file_path
        );
        CsvFileView {
            db,
            file_path: Some(file_path),
        }
    }

    /// Generate CSV content from database query results (lazy loading - no internal cache)
    /// Content is generated fresh on each call. Caching is handled by NFS cache layer.
    pub async fn generate_csv(&self) -> Result<Vec<u8>> {
        debug!("Generating CSV content from database (lazy loading)");

        // Query data - either all data or specific file
        let batches = if let Some(ref file_path) = self.file_path {
            // Query specific Parquet file
            debug!("Querying specific file: {}", file_path);
            self.db.query_file(file_path).await?
        } else {
            // Query all data from the database
            self.db.query("SELECT * FROM data").await?
        };

        if batches.is_empty() {
            debug!("No data in database, returning empty CSV with headers only");
            // If no data, create empty CSV with schema headers
            let schema = self.db.schema();
            let mut buffer = Vec::new();
            {
                let mut writer = CsvWriter::new(&mut buffer);

                // Write just the headers by creating an empty batch
                let empty_batch = RecordBatch::new_empty(schema);
                writer.write(&empty_batch)?;
            }

            return Ok(buffer);
        }

        // Rebuild each batch with canonical schema to handle metadata differences
        let schema = self.db.schema();
        let mut casted_batches = Vec::new();
        for batch in &batches {
            // Extract columns in schema order
            let mut columns = Vec::new();
            for field in schema.fields() {
                let col = batch.column_by_name(field.name()).ok_or_else(|| {
                    crate::error::Error::InvalidOperation(format!(
                        "Missing column: {}",
                        field.name()
                    ))
                })?;
                columns.push(col.clone());
            }
            // Create new batch with canonical schema
            let casted = RecordBatch::try_new(schema.clone(), columns)?;
            casted_batches.push(casted);
        }

        // Concatenate all casted batches
        let unified_batch = arrow::compute::concat_batches(&schema, &casted_batches)?;

        // Generate CSV from unified batch
        let mut buffer = Vec::new();
        {
            let mut writer = CsvWriter::new(&mut buffer);
            writer.write(&unified_batch)?;
        }

        debug!("Generated CSV content: {} bytes", buffer.len());
        Ok(buffer)
    }

    /// Get the size of the CSV content (generates on-demand)
    pub async fn size(&self) -> Result<u64> {
        let content = self.generate_csv().await?;
        Ok(content.len() as u64)
    }

    /// Get the full CSV content (for NFS cache layer)
    pub async fn get_full_content(&self) -> Result<Vec<u8>> {
        self.generate_csv().await
    }

    /// Read a portion of the CSV content at the given offset (lazy loading)
    /// Content is generated fresh on each read. NFS cache layer handles caching.
    pub async fn read(&self, offset: u64, size: u32) -> Result<Vec<u8>> {
        let content = self.generate_csv().await?;
        let offset = offset as usize;
        let size = size as usize;

        if offset >= content.len() {
            return Ok(Vec::new());
        }

        let end = std::cmp::min(offset + size, content.len());
        Ok(content[offset..end].to_vec())
    }

    /// Apply write operation: handles both appends and overwrites with row deletion
    /// Detects if rows were deleted by comparing old vs new CSV content
    ///
    /// cached_current_csv: Optional pre-fetched CSV content from cache (performance optimization)
    pub async fn apply_write(
        &self,
        data: &[u8],
        cached_current_csv: Option<Vec<u8>>,
    ) -> Result<()> {
        let _start_time = std::time::Instant::now();
        info!("Applying write: {} bytes", data.len());

        let new_csv_str = String::from_utf8_lossy(data);
        debug!(
            "Write data preview: {}",
            &new_csv_str.chars().take(200).collect::<String>()
        );

        // Get current CSV content to detect if this is an overwrite with deletions
        // Use cached content if available (fast path), otherwise generate (slow path)
        let csv_gen_start = std::time::Instant::now();
        let (current_csv_bytes, was_cached) = if let Some(cached) = cached_current_csv {
            debug!("Using cached CSV content ({} bytes)", cached.len());
            (cached, true)
        } else {
            debug!("No cached content, generating CSV from database");
            (self.generate_csv().await?, false)
        };
        let csv_gen_duration = csv_gen_start.elapsed();
        debug!(
            "CSV retrieval took: {:?} (cached: {})",
            csv_gen_duration, was_cached
        );

        let current_csv_str = String::from_utf8_lossy(&current_csv_bytes);

        info!(
            "Current CSV size: {} bytes, New CSV size: {} bytes",
            current_csv_bytes.len(),
            data.len()
        );

        // Check if this looks like an overwrite (has header and potentially fewer rows)
        let new_has_header = new_csv_str
            .lines()
            .next()
            .map(|l| l.contains(',') && !l.chars().next().unwrap_or('0').is_numeric())
            .unwrap_or(false);

        if new_has_header && data.len() < current_csv_bytes.len() {
            // Potential deletion detected - compare CSVs
            info!("Detected potential row deletion (overwrite with smaller size and header)");
            return self
                .handle_csv_overwrite(&current_csv_str, &new_csv_str)
                .await;
        } else if new_has_header {
            // Full overwrite (replace all data)
            warn!("Full CSV overwrite detected - replacing all data");
            return self
                .handle_csv_overwrite(&current_csv_str, &new_csv_str)
                .await;
        } else {
            // Append mode - no header, just new rows
            info!("Append mode detected");
            return self.handle_csv_append(data).await;
        }
    }

    /// Handle CSV append (original logic)
    async fn handle_csv_append(&self, data: &[u8]) -> Result<()> {
        let csv_str = String::from_utf8_lossy(data);

        // Parse CSV - we need to add headers to make it valid CSV
        let schema = self.db.schema();
        let header_line = schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
            .join(",");

        // Combine header + new data
        let full_csv = format!("{}\n{}", header_line, csv_str.trim());

        // Parse CSV using Arrow CSV reader
        let cursor = std::io::Cursor::new(full_csv.as_bytes());
        let csv_reader = CsvReaderBuilder::new(schema.clone())
            .with_header(true)
            .build(cursor)?;

        // Read all batches
        let mut all_rows = Vec::new();
        for batch_result in csv_reader {
            let batch = batch_result?;
            info!("Parsed batch with {} rows", batch.num_rows());
            all_rows.push(batch);
        }

        if all_rows.is_empty() {
            error!("No rows parsed from CSV write");
            return Ok(()); // Empty write is OK
        }

        // Insert all rows into database
        for batch in all_rows {
            if batch.num_rows() > 0 {
                info!("Inserting {} rows into database", batch.num_rows());
                self.db.insert(batch).await?;
            }
        }

        info!("Append committed");
        Ok(())
    }

    /// Handle CSV overwrite with deletion detection
    async fn handle_csv_overwrite(&self, old_csv: &str, new_csv: &str) -> Result<()> {
        let overwrite_start = std::time::Instant::now();
        info!("Processing CSV overwrite with UPDATE/DELETE/INSERT detection (MERGE)");

        let schema = self.db.schema();

        // Find ID column (first Int32/Int64 field, or first field if none)
        let id_column_name = schema
            .fields()
            .iter()
            .find(|f| {
                matches!(
                    f.data_type(),
                    arrow::datatypes::DataType::Int32 | arrow::datatypes::DataType::Int64
                )
            })
            .map(|f| f.name().as_str())
            .unwrap_or_else(|| schema.field(0).name().as_str());

        info!(
            "Using '{}' as ID column for row identification",
            id_column_name
        );

        // Parse both CSVs into RecordBatches
        let parse_start = std::time::Instant::now();
        let old_batch = self.parse_csv_to_batch(old_csv)?;
        let new_batch = self.parse_csv_to_batch(new_csv)?;
        let parse_duration = parse_start.elapsed();
        debug!("CSV parsing took: {:?}", parse_duration);

        info!(
            "Old CSV has {} rows, New CSV has {} rows",
            old_batch.num_rows(),
            new_batch.num_rows()
        );

        // Build HashMaps of ID -> Row for comparison
        let old_map = self.build_id_to_row_map(&old_batch, id_column_name)?;
        let new_map = self.build_id_to_row_map(&new_batch, id_column_name)?;

        debug!(
            "Built row maps: old={} rows, new={} rows",
            old_map.len(),
            new_map.len()
        );

        // Detect changes
        let mut insert_ids = Vec::new();
        let mut update_ids = Vec::new();
        let mut delete_ids = Vec::new();

        // Find INSERTs and UPDATEs (new IDs or changed values)
        for (id, new_row) in &new_map {
            if let Some(old_row) = old_map.get(id) {
                // ID exists in both - check if values changed
                if old_row != new_row {
                    update_ids.push(id.clone());
                    debug!("UPDATE detected for ID {}", id);
                }
            } else {
                // ID only in new - INSERT
                insert_ids.push(id.clone());
                debug!("INSERT detected for ID {}", id);
            }
        }

        // Find DELETEs (IDs only in old)
        for id in old_map.keys() {
            if !new_map.contains_key(id) {
                delete_ids.push(id.clone());
                debug!("DELETE detected for ID {}", id);
            }
        }

        info!(
            "Changes detected: {} inserts, {} updates, {} deletes",
            insert_ids.len(),
            update_ids.len(),
            delete_ids.len()
        );

        // If no changes, nothing to do
        if insert_ids.is_empty() && update_ids.is_empty() && delete_ids.is_empty() {
            info!("No changes detected, skipping MERGE");
            return Ok(());
        }

        // Use MERGE operation for efficiency
        let merge_start = std::time::Instant::now();
        info!("Executing MERGE operation");

        // Execute MERGE for INSERT/UPDATE if needed
        let mut metrics = crate::delta_lake::merge::MergeMetrics::default();

        if !insert_ids.is_empty() || !update_ids.is_empty() {
            // Build source data with _op column for MERGE
            let source_batch =
                self.build_merge_source_batch(&new_batch, &insert_ids, &update_ids)?;

            if source_batch.num_rows() > 0 {
                info!("Executing MERGE for INSERT/UPDATE operations");
                metrics = self
                    .db
                    .merge()
                    .await?
                    .with_source(source_batch, "source")
                    .on(format!(
                        "target.{} = source.{}",
                        id_column_name, id_column_name
                    ))
                    .when_matched_update()
                    .condition("source._op = 'UPDATE'")
                    .set_all()
                    .when_not_matched_insert()
                    .condition("source._op = 'INSERT'")
                    .values_all()
                    .execute()
                    .await?;
            }
        }

        // Handle deletes separately if needed (MERGE doesn't support DELETE from source in our impl)
        if !delete_ids.is_empty() {
            let delete_where = format!("{} IN ({})", id_column_name, delete_ids.join(", "));
            debug!("Executing DELETE: {}", delete_where);
            let deleted_count = self.db.delete_rows_where(&delete_where).await?;
            info!("Deleted {} rows", deleted_count);
        }

        let merge_duration = merge_start.elapsed();
        let total_duration = overwrite_start.elapsed();

        info!(
            "MERGE completed: {} inserts, {} updates, {} deletes in {:?} (total: {:?})",
            metrics.rows_inserted,
            metrics.rows_updated,
            delete_ids.len(),
            merge_duration,
            total_duration
        );

        Ok(())
    }

    /// Extract ID values from CSV string (legacy method, kept for reference)
    #[allow(dead_code)]
    fn extract_ids_from_csv(&self, csv_content: &str, id_column: &str) -> Result<HashSet<String>> {
        let mut ids = HashSet::new();
        let lines: Vec<&str> = csv_content.lines().collect();

        if lines.is_empty() {
            return Ok(ids);
        }

        // Parse header to find ID column index
        let header = lines[0];
        let headers: Vec<&str> = header.split(',').collect();
        let id_index = headers
            .iter()
            .position(|&h| h.trim() == id_column)
            .ok_or_else(|| {
                crate::error::Error::InvalidOperation(format!(
                    "ID column '{}' not found in CSV header",
                    id_column
                ))
            })?;

        debug!("ID column '{}' is at index {}", id_column, id_index);

        // Extract IDs from data rows
        for line in lines.iter().skip(1) {
            if line.trim().is_empty() {
                continue;
            }

            let values: Vec<&str> = line.split(',').collect();
            if values.len() > id_index {
                let id_value = values[id_index].trim();
                if !id_value.is_empty() {
                    ids.insert(id_value.to_string());
                }
            }
        }

        debug!("Extracted {} IDs from CSV", ids.len());
        Ok(ids)
    }

    /// Parse CSV string into a RecordBatch
    fn parse_csv_to_batch(&self, csv_content: &str) -> Result<RecordBatch> {
        use std::io::Cursor;

        let cursor = Cursor::new(csv_content.as_bytes());
        let csv_reader = CsvReaderBuilder::new(self.db.schema())
            .with_header(true)
            .build(cursor)?;

        // Read all batches and concatenate
        let mut batches = Vec::new();
        for batch_result in csv_reader {
            let batch = batch_result?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }

        if batches.is_empty() {
            // Return empty batch with schema
            return Ok(RecordBatch::new_empty(self.db.schema()));
        }

        // Concatenate batches if multiple
        if batches.len() == 1 {
            Ok(batches.into_iter().next().unwrap())
        } else {
            arrow::compute::concat_batches(&self.db.schema(), &batches)
                .map_err(crate::error::Error::Arrow)
        }
    }

    /// Build a HashMap of ID -> Row (as string representation) for comparison
    fn build_id_to_row_map(
        &self,
        batch: &RecordBatch,
        id_column: &str,
    ) -> Result<HashMap<String, String>> {
        use arrow::array::*;
        use arrow::datatypes::DataType;

        let mut map = HashMap::new();

        if batch.num_rows() == 0 {
            return Ok(map);
        }

        // Find ID column
        let id_array = batch.column_by_name(id_column).ok_or_else(|| {
            crate::error::Error::InvalidOperation(format!("ID column '{}' not found", id_column))
        })?;

        // Extract IDs and build row representations
        for row_idx in 0..batch.num_rows() {
            // Get ID value
            let id_str = match id_array.data_type() {
                DataType::Int32 => {
                    let array = id_array.as_any().downcast_ref::<Int32Array>().unwrap();
                    array.value(row_idx).to_string()
                }
                DataType::Int64 => {
                    let array = id_array.as_any().downcast_ref::<Int64Array>().unwrap();
                    array.value(row_idx).to_string()
                }
                DataType::Utf8 => {
                    let array = id_array.as_any().downcast_ref::<StringArray>().unwrap();
                    array.value(row_idx).to_string()
                }
                _ => {
                    return Err(crate::error::Error::InvalidOperation(
                        "Unsupported ID column type".to_string(),
                    ));
                }
            };

            // Build row representation (all column values concatenated)
            let mut row_repr = String::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let val_str = match col.data_type() {
                    DataType::Int32 => {
                        let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
                        if array.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            array.value(row_idx).to_string()
                        }
                    }
                    DataType::Int64 => {
                        let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
                        if array.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            array.value(row_idx).to_string()
                        }
                    }
                    DataType::Utf8 => {
                        let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                        if array.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            array.value(row_idx).to_string()
                        }
                    }
                    DataType::Float64 => {
                        let array = col.as_any().downcast_ref::<Float64Array>().unwrap();
                        if array.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            array.value(row_idx).to_string()
                        }
                    }
                    _ => "UNSUPPORTED".to_string(),
                };

                if !row_repr.is_empty() {
                    row_repr.push('|');
                }
                row_repr.push_str(&val_str);
            }

            map.insert(id_str, row_repr);
        }

        Ok(map)
    }

    /// Build source batch with _op column for MERGE operation
    fn build_merge_source_batch(
        &self,
        batch: &RecordBatch,
        insert_ids: &[String],
        update_ids: &[String],
    ) -> Result<RecordBatch> {
        use arrow::array::*;
        use arrow::compute;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(self.db.schema()));
        }

        // Get ID column to filter rows
        let schema = batch.schema();
        let id_column = schema
            .fields()
            .iter()
            .find(|f| matches!(f.data_type(), DataType::Int32 | DataType::Int64))
            .ok_or_else(|| {
                crate::error::Error::InvalidOperation("No ID column found".to_string())
            })?;

        let id_array = batch.column_by_name(id_column.name()).ok_or_else(|| {
            crate::error::Error::InvalidOperation("ID column not found in batch".to_string())
        })?;

        // Build vectors for filtered rows
        let mut indices_to_include: Vec<u64> = Vec::new();
        let mut ops = Vec::new();

        for row_idx in 0..batch.num_rows() {
            let id_str = match id_array.data_type() {
                DataType::Int32 => {
                    let array = id_array.as_any().downcast_ref::<Int32Array>().unwrap();
                    array.value(row_idx).to_string()
                }
                DataType::Int64 => {
                    let array = id_array.as_any().downcast_ref::<Int64Array>().unwrap();
                    array.value(row_idx).to_string()
                }
                _ => continue,
            };

            if insert_ids.contains(&id_str) {
                indices_to_include.push(row_idx as u64);
                ops.push("INSERT");
            } else if update_ids.contains(&id_str) {
                indices_to_include.push(row_idx as u64);
                ops.push("UPDATE");
            }
        }

        if indices_to_include.is_empty() {
            return Ok(RecordBatch::new_empty(self.db.schema()));
        }

        debug!(
            "Filtering batch to {} rows for MERGE",
            indices_to_include.len()
        );

        // Filter each column to include only the selected rows
        let mut filtered_columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx);
            let indices_array = UInt64Array::from(indices_to_include.clone());
            let filtered_col =
                compute::take(col, &indices_array, None).map_err(crate::error::Error::Arrow)?;
            filtered_columns.push(filtered_col);
        }

        // Create filtered batch
        let filtered_batch = RecordBatch::try_new(schema.clone(), filtered_columns)
            .map_err(crate::error::Error::Arrow)?;

        // Add _op column
        let mut new_fields: Vec<Field> =
            schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        new_fields.push(Field::new("_op", DataType::Utf8, false));
        let new_schema = Arc::new(ArrowSchema::new(new_fields));

        let mut new_columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
        for col_idx in 0..filtered_batch.num_columns() {
            new_columns.push(filtered_batch.column(col_idx).clone());
        }

        // Add _op column data
        let op_array = StringArray::from(ops);
        new_columns.push(Arc::new(op_array));

        debug!(
            "Built MERGE source batch with {} rows",
            indices_to_include.len()
        );

        RecordBatch::try_new(new_schema, new_columns).map_err(crate::error::Error::Arrow)
    }
}

/// JSON file view that generates JSON or JSON Lines content from database query results
pub struct JsonFileView {
    db: Arc<DatabaseOps>,
    cached_content: Option<Vec<u8>>,
    is_json_lines: bool, // true for .jsonl, false for .json
}

impl JsonFileView {
    /// Create a new JSON array file view (data.json)
    pub fn new(db: Arc<DatabaseOps>) -> Self {
        info!("Creating JSON array file view for all data");
        JsonFileView {
            db,
            cached_content: None,
            is_json_lines: false,
        }
    }

    /// Create a new JSON Lines file view (data.jsonl)
    pub fn new_json_lines(db: Arc<DatabaseOps>) -> Self {
        info!("Creating JSON Lines file view for all data");
        JsonFileView {
            db,
            cached_content: None,
            is_json_lines: true,
        }
    }

    /// Generate JSON content from all data in the database
    pub async fn generate_content(&mut self) -> Result<&[u8]> {
        if let Some(ref content) = self.cached_content {
            debug!("Returning cached JSON content ({} bytes)", content.len());
            return Ok(content);
        }

        info!("Generating JSON content from database");

        // Query all data
        let batches = self.db.query("SELECT * FROM data").await?;

        if batches.is_empty() {
            let empty = if self.is_json_lines {
                Vec::new()
            } else {
                b"[]".to_vec()
            };
            self.cached_content = Some(empty);
            return Ok(self.cached_content.as_ref().unwrap());
        }

        // Convert to JSON
        let mut buffer = Vec::new();

        if self.is_json_lines {
            // JSON Lines format (one JSON object per line)
            let mut writer = JsonLinesWriter::new(&mut buffer);
            for batch in batches {
                writer.write(&batch)?;
            }
            writer.finish()?;
        } else {
            // JSON array format
            let mut writer = JsonArrayWriter::new(&mut buffer);
            for batch in batches {
                writer.write(&batch)?;
            }
            writer.finish()?;
        }

        info!("Generated JSON content: {} bytes", buffer.len());
        self.cached_content = Some(buffer);
        Ok(self.cached_content.as_ref().unwrap())
    }

    /// Get the size of the generated content
    pub async fn size(&mut self) -> Result<u64> {
        let content = self.generate_content().await?;
        Ok(content.len() as u64)
    }

    /// Read a portion of the file
    pub async fn read(&mut self, offset: u64, count: u64) -> Result<Vec<u8>> {
        let content = self.generate_content().await?;
        let start = offset as usize;
        let end = std::cmp::min(start + count as usize, content.len());

        if start >= content.len() {
            return Ok(Vec::new());
        }

        Ok(content[start..end].to_vec())
    }
}

/// Special query file that executes SQL and returns results as CSV
pub struct QueryFile {
    db: Arc<DatabaseOps>,
    query: Option<String>,
    result_content: Option<Vec<u8>>,
}

impl QueryFile {
    pub fn new(db: Arc<DatabaseOps>) -> Self {
        info!("Creating special .query file");
        QueryFile {
            db,
            query: None,
            result_content: None,
        }
    }

    /// Write SQL query to the file
    pub async fn write_query(&mut self, query_bytes: &[u8]) -> Result<()> {
        let query = String::from_utf8_lossy(query_bytes).to_string();
        info!("Received SQL query: {}", query);

        // Execute the query immediately
        let batches = self.db.query(&query).await?;

        // Convert results to CSV
        let mut buffer = Vec::new();
        {
            let mut writer = CsvWriter::new(&mut buffer);
            for batch in batches {
                writer.write(&batch)?;
            }
            // Writer is dropped here, releasing borrow on buffer
        }

        info!(
            "Query executed, generated {} bytes of CSV results",
            buffer.len()
        );
        self.query = Some(query);
        self.result_content = Some(buffer);
        Ok(())
    }

    /// Read query results
    pub async fn read(&mut self, offset: u64, count: u64) -> Result<Vec<u8>> {
        let content = self.result_content.as_ref().ok_or_else(|| {
            crate::error::Error::InvalidOperation("No query has been executed".to_string())
        })?;

        let start = offset as usize;
        let end = std::cmp::min(start + count as usize, content.len());

        if start >= content.len() {
            return Ok(Vec::new());
        }

        Ok(content[start..end].to_vec())
    }

    pub async fn size(&self) -> Result<u64> {
        Ok(self.result_content.as_ref().map_or(0, |c| c.len() as u64))
    }
}

/// Special schema.sql file that shows schema as SQL CREATE TABLE statement
pub struct SchemaFile {
    db: Arc<DatabaseOps>,
}

impl SchemaFile {
    pub fn new(db: Arc<DatabaseOps>) -> Self {
        info!("Creating special schema.sql file");
        SchemaFile { db }
    }

    /// Generate SQL CREATE TABLE statement from schema
    pub async fn generate_content(&self) -> Result<Vec<u8>> {
        let schema = self.db.schema();

        let mut sql = String::from("CREATE TABLE data (\n");

        for (i, field) in schema.fields().iter().enumerate() {
            sql.push_str("  ");
            sql.push_str(field.name());
            sql.push(' ');

            // Map Arrow data type to SQL type
            let sql_type = match field.data_type() {
                arrow::datatypes::DataType::Int8 => "TINYINT",
                arrow::datatypes::DataType::Int16 => "SMALLINT",
                arrow::datatypes::DataType::Int32 => "INT",
                arrow::datatypes::DataType::Int64 => "BIGINT",
                arrow::datatypes::DataType::UInt8 => "UNSIGNED TINYINT",
                arrow::datatypes::DataType::UInt16 => "UNSIGNED SMALLINT",
                arrow::datatypes::DataType::UInt32 => "UNSIGNED INT",
                arrow::datatypes::DataType::UInt64 => "UNSIGNED BIGINT",
                arrow::datatypes::DataType::Float32 => "FLOAT",
                arrow::datatypes::DataType::Float64 => "DOUBLE",
                arrow::datatypes::DataType::Utf8 => "VARCHAR",
                arrow::datatypes::DataType::LargeUtf8 => "TEXT",
                arrow::datatypes::DataType::Boolean => "BOOLEAN",
                arrow::datatypes::DataType::Date32 => "DATE",
                arrow::datatypes::DataType::Date64 => "DATE",
                arrow::datatypes::DataType::Timestamp(_, _) => "TIMESTAMP",
                _ => "TEXT",
            };
            sql.push_str(sql_type);

            if !field.is_nullable() {
                sql.push_str(" NOT NULL");
            }

            if i < schema.fields().len() - 1 {
                sql.push(',');
            }
            sql.push('\n');
        }

        sql.push_str(");\n");

        info!("Generated schema SQL: {} bytes", sql.len());
        Ok(sql.into_bytes())
    }

    pub async fn read(&self, offset: u64, count: u64) -> Result<Vec<u8>> {
        let content = self.generate_content().await?;
        let start = offset as usize;
        let end = std::cmp::min(start + count as usize, content.len());

        if start >= content.len() {
            return Ok(Vec::new());
        }

        Ok(content[start..end].to_vec())
    }

    pub async fn size(&self) -> Result<u64> {
        let content = self.generate_content().await?;
        Ok(content.len() as u64)
    }
}

/// Special .stats file that shows database statistics as JSON
pub struct StatsFile {
    #[allow(dead_code)]
    db: Arc<DatabaseOps>,
}

impl StatsFile {
    pub fn new(db: Arc<DatabaseOps>) -> Self {
        info!("Creating special .stats file");
        StatsFile { db }
    }

    /// Generate statistics JSON (Delta Lake mode)
    pub async fn generate_content(&self) -> Result<Vec<u8>> {
        // For Delta Lake, scan the base directory and query for row count
        let base_path = self.db.base_path();

        // Count parquet files in the table directory
        let mut total_files = 0;
        let mut total_size_bytes = 0;
        let mut file_stats = Vec::new();

        if let Ok(entries) = std::fs::read_dir(base_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                    if let Ok(metadata) = std::fs::metadata(&path) {
                        total_files += 1;
                        let size = metadata.len();
                        total_size_bytes += size;

                        file_stats.push(serde_json::json!({
                            "path": path.file_name().unwrap().to_string_lossy(),
                            "size_bytes": size,
                        }));
                    }
                }
            }
        }

        // Query for total row count
        let total_rows = match self.db.query("SELECT COUNT(*) as count FROM data").await {
            Ok(batches) => {
                if !batches.is_empty() {
                    if let Some(array) = batches[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                    {
                        array.value(0) as u64
                    } else {
                        0
                    }
                } else {
                    0
                }
            }
            Err(_) => 0,
        };

        let stats = serde_json::json!({
            "total_rows": total_rows,
            "total_files": total_files,
            "total_size_bytes": total_size_bytes,
            "note": "Delta Lake mode - stats computed from parquet files",
            "files": file_stats
        });

        // Legacy code kept for reference:
        // let active_files = file_inventory.list_active_files();
        // "files": active_files.iter().map(|f| {
        //     serde_json::json!({
        //         "path": f.path,
        //         "txn_id": f.txn_id,
        //         "timestamp": f.timestamp,
        //         "row_count": f.row_count,
        //         "size_bytes": f.size_bytes,
        //     })
        // }).collect::<Vec<_>>()

        let json = serde_json::to_string_pretty(&stats)?;
        info!("Generated statistics JSON: {} bytes", json.len());
        Ok(json.into_bytes())
    }

    pub async fn read(&self, offset: u64, count: u64) -> Result<Vec<u8>> {
        let content = self.generate_content().await?;
        let start = offset as usize;
        let end = std::cmp::min(start + count as usize, content.len());

        if start >= content.len() {
            return Ok(Vec::new());
        }

        Ok(content[start..end].to_vec())
    }

    pub async fn size(&self) -> Result<u64> {
        let content = self.generate_content().await?;
        Ok(content.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_csv_view_with_data() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let db = DatabaseOps::create(&db_path, schema.clone()).await.unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        )
        .unwrap();

        db.insert(batch).await.unwrap();

        let view = CsvFileView::new(Arc::new(db));
        let content = view.generate_csv().await.unwrap();
        let csv_str = String::from_utf8_lossy(&content);

        assert!(csv_str.contains("id,name"));
        assert!(csv_str.contains("1,Alice"));
        assert!(csv_str.contains("2,Bob"));
    }
}
