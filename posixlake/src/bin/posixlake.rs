//! posixlake CLI - Mount database as POSIX filesystem via NFS server
//!
//! Usage:
//!   posixlake create <DB_PATH> --schema "id:Int64,name:String"
//!   posixlake create <DB_PATH> --from-csv <CSV_FILE>
//!   posixlake create <DB_PATH> --from-parquet <PARQUET_FILE>
//!   posixlake mount <DB_PATH> <MOUNT_POINT> [--port PORT]
//!   posixlake unmount <MOUNT_POINT>
//!   posixlake status <MOUNT_POINT>

use arrow::datatypes::{DataType, Field, Schema};
use clap::{Parser, Subcommand};
use posixlake::nfs::NfsServer;
use posixlake::{error::Result, DatabaseOps};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;

/// posixlake - POSIX Filesystem Database (NFS Server)
#[derive(Parser)]
#[command(name = "posixlake")]
#[command(version = "0.1.0")]
#[command(about = "Mount posixlake as a POSIX filesystem via NFS server", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new Delta Lake database
    Create {
        /// Path for the new database
        #[arg(value_name = "DB_PATH")]
        db_path: PathBuf,

        /// Schema definition: "col1:Type1,col2:Type2,..."
        /// Supported types: Int64, Int32, Float64, Float32, String, Boolean
        #[arg(long, conflicts_with_all = ["from_csv", "from_parquet"])]
        schema: Option<String>,

        /// Create database by importing from CSV file (auto-infers schema)
        #[arg(long, conflicts_with_all = ["schema", "from_parquet"])]
        from_csv: Option<PathBuf>,

        /// Create database by importing from Parquet file(s) (supports glob patterns)
        #[arg(long, conflicts_with_all = ["schema", "from_csv"])]
        from_parquet: Option<PathBuf>,
    },

    /// Mount a database as a filesystem via NFS
    Mount {
        /// Path to the database directory
        #[arg(value_name = "DB_PATH")]
        db_path: PathBuf,

        /// Mount point directory
        #[arg(value_name = "MOUNT_POINT")]
        mount_point: PathBuf,

        /// NFS server port (default: 12049)
        #[arg(long, short = 'p', default_value = "12049")]
        port: u16,
    },

    /// Unmount a mounted database
    Unmount {
        /// Mount point to unmount
        #[arg(value_name = "MOUNT_POINT")]
        mount_point: PathBuf,
    },

    /// Check mount status
    Status {
        /// Mount point to check
        #[arg(value_name = "MOUNT_POINT")]
        mount_point: PathBuf,
    },

    /// Test S3/MinIO backend
    S3Test {
        /// S3 URI (e.g., s3://bucket/path)
        #[arg(value_name = "S3_PATH")]
        s3_path: String,

        /// MinIO endpoint (default: http://localhost:9000)
        #[arg(long, default_value = "http://localhost:9000")]
        endpoint: String,

        /// Access key (default: minioadmin)
        #[arg(long, default_value = "minioadmin")]
        access_key: String,

        /// Secret key (default: minioadmin)
        #[arg(long, default_value = "minioadmin")]
        secret_key: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Create {
            db_path,
            schema,
            from_csv,
            from_parquet,
        } => {
            match (schema, from_csv, from_parquet) {
                (Some(schema_str), None, None) => {
                    // Create with explicit schema
                    eprintln!("Creating database at: {}", db_path.display());
                    let parsed_schema = parse_schema(&schema_str)?;
                    let _db = DatabaseOps::create(&db_path, Arc::new(parsed_schema)).await?;
                    eprintln!("Database created successfully with schema:");
                    eprintln!("  {}", schema_str);
                    Ok(())
                }
                (None, Some(csv_path), None) => {
                    // Create from CSV
                    eprintln!("Creating database from CSV: {}", csv_path.display());
                    let _db = DatabaseOps::create_from_csv(&db_path, &csv_path).await?;
                    eprintln!("Database created successfully from CSV");
                    eprintln!("  Source: {}", csv_path.display());
                    eprintln!("  Database: {}", db_path.display());
                    Ok(())
                }
                (None, None, Some(parquet_path)) => {
                    // Create from Parquet
                    eprintln!("Creating database from Parquet: {}", parquet_path.display());
                    let _db = DatabaseOps::create_from_parquet(&db_path, &parquet_path).await?;
                    eprintln!("Database created successfully from Parquet");
                    eprintln!("  Source: {}", parquet_path.display());
                    eprintln!("  Database: {}", db_path.display());
                    Ok(())
                }
                (None, None, None) => {
                    eprintln!("Error: Must specify one of --schema, --from-csv, or --from-parquet");
                    std::process::exit(1);
                }
                _ => {
                    // This shouldn't happen due to clap's conflicts_with_all
                    eprintln!("Error: Cannot specify multiple schema sources");
                    std::process::exit(1);
                }
            }
        }

        Commands::Mount {
            db_path,
            mount_point,
            port,
        } => {
            // Open database
            eprintln!("Opening database: {}", db_path.display());
            let db = DatabaseOps::open(&db_path).await?;
            let db = Arc::new(db);

            // Start NFS server
            eprintln!("Starting NFS server on port {}", port);
            let server = NfsServer::new(db.clone(), port).await?;

            // Wait for server to be fully ready
            eprintln!("Waiting for NFS server to be ready...");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            // Create mount point if it doesn't exist
            if !mount_point.exists() {
                std::fs::create_dir_all(&mount_point)?;
            }

            // Mount using OS NFS client
            eprintln!("Mounting NFS at {} (requires sudo)", mount_point.display());

            #[cfg(target_os = "macos")]
            let mount_status = std::process::Command::new("sudo")
                .arg("mount_nfs")
                .arg("-o")
                .arg(format!(
                    "nolocks,vers=3,tcp,port={},mountport={}",
                    port, port
                ))
                .arg("localhost:/")
                .arg(mount_point.as_os_str())
                .status();

            #[cfg(target_os = "linux")]
            let mount_status = std::process::Command::new("sudo")
                .arg("mount")
                .arg("-t")
                .arg("nfs")
                .arg("-o")
                .arg(format!(
                    "nolock,noac,soft,timeo=10,retrans=2,vers=3,tcp,port={},mountport={}",
                    port, port
                ))
                .arg("localhost:/")
                .arg(mount_point.as_os_str())
                .status();

            #[cfg(target_os = "windows")]
            let mount_status = std::process::Command::new("mount")
                .arg("-o")
                .arg("anon")
                .arg(format!("\\\\localhost\\"))
                .arg(format!(
                    "{}:",
                    mount_point.to_string_lossy().chars().next().unwrap()
                ))
                .status();

            match mount_status {
                Ok(s) if s.success() => {
                    eprintln!("Successfully mounted at {}", mount_point.display());
                    eprintln!("Database is now accessible via POSIX commands:");
                    eprintln!("  ls {}", mount_point.display());
                    eprintln!("  cat {}/data/data.csv", mount_point.display());
                    eprintln!();
                    eprintln!("Press Ctrl+C to unmount and shutdown");

                    // Wait for Ctrl+C
                    signal::ctrl_c().await?;
                    eprintln!("\nShutting down...");

                    // Unmount
                    #[cfg(any(target_os = "macos", target_os = "linux"))]
                    std::process::Command::new("sudo")
                        .arg("umount")
                        .arg(mount_point.as_os_str())
                        .status()
                        .ok();

                    server.shutdown().await?;
                    eprintln!("Unmounted and shutdown complete");
                    Ok(())
                }
                Ok(s) => {
                    eprintln!("Mount failed with exit code: {:?}", s.code());
                    eprintln!();
                    eprintln!(
                        "NFS server is running on port {}. You can mount manually:",
                        port
                    );
                    #[cfg(target_os = "macos")]
                    eprintln!("  sudo mount_nfs -o nolocks,vers=3,tcp,port={},mountport={} localhost:/posixlake {}", port, port, mount_point.display());
                    #[cfg(target_os = "linux")]
                    eprintln!("  sudo mount -t nfs -o nolock,noac,soft,timeo=10,retrans=2,vers=3,tcp,port={},mountport={} localhost:/posixlake {}", port, port, mount_point.display());
                    #[cfg(target_os = "windows")]
                    eprintln!(
                        "  mount -o anon,nolock,vers=3,port={},mountport={} \\\\localhost\\ {}",
                        port,
                        port,
                        mount_point.display()
                    );
                    eprintln!();
                    eprintln!("Press Ctrl+C to shutdown server");

                    // Keep server running
                    signal::ctrl_c().await?;
                    eprintln!("\nShutting down...");
                    server.shutdown().await?;
                    Ok(())
                }
                Err(e) => {
                    eprintln!("Failed to execute mount command: {}", e);
                    eprintln!();
                    eprintln!(
                        "NFS server is running on port {}. You can mount manually:",
                        port
                    );
                    #[cfg(target_os = "macos")]
                    eprintln!("  sudo mount_nfs -o nolocks,vers=3,tcp,port={},mountport={} localhost:/posixlake {}", port, port, mount_point.display());
                    #[cfg(target_os = "linux")]
                    eprintln!("  sudo mount -t nfs -o nolock,noac,soft,timeo=10,retrans=2,vers=3,tcp,port={},mountport={} localhost:/posixlake {}", port, port, mount_point.display());
                    #[cfg(target_os = "windows")]
                    eprintln!(
                        "  mount -o anon,nolock,vers=3,port={},mountport={} \\\\localhost\\ {}",
                        port,
                        port,
                        mount_point.display()
                    );
                    eprintln!();
                    eprintln!("Press Ctrl+C to shutdown server");

                    // Keep server running
                    signal::ctrl_c().await?;
                    eprintln!("\nShutting down...");
                    server.shutdown().await?;
                    Ok(())
                }
            }
        }

        Commands::Unmount { mount_point } => {
            #[cfg(any(target_os = "macos", target_os = "linux"))]
            let status = std::process::Command::new("sudo")
                .arg("umount")
                .arg(mount_point.as_os_str())
                .status();

            #[cfg(target_os = "windows")]
            let status = std::process::Command::new("net")
                .arg("use")
                .arg(mount_point.as_os_str())
                .arg("/delete")
                .status();

            match status {
                Ok(s) if s.success() => {
                    println!("Successfully unmounted {}", mount_point.display());
                    Ok(())
                }
                Ok(s) => {
                    eprintln!("Unmount failed with exit code: {:?}", s.code());
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("Failed to execute unmount command: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::Status { mount_point } => {
            eprintln!("Status command not yet implemented.");
            eprintln!("Use standard OS commands:");
            eprintln!("  macOS/Linux: df | grep {}", mount_point.display());
            eprintln!("  Windows:     net use | findstr {}", mount_point.display());
            std::process::exit(1);
        }

        Commands::S3Test {
            s3_path,
            endpoint,
            access_key,
            secret_key,
        } => {
            use arrow::array::{Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::record_batch::RecordBatch;

            eprintln!("=== Testing S3/MinIO backend ===");
            eprintln!("  S3 Path: {}", s3_path);
            eprintln!("  Endpoint: {}", endpoint);
            eprintln!();

            // Define schema
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("email", DataType::Utf8, false),
            ]));

            // Create database in S3
            eprintln!("[1/4] Creating database in S3...");
            let db = DatabaseOps::create_with_s3(
                &s3_path,
                schema.clone(),
                &endpoint,
                &access_key,
                &secret_key,
            )
            .await?;
            eprintln!("      ✓ Database created");
            eprintln!();

            // Insert test data
            eprintln!("[2/4] Inserting test data...");
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
                    Arc::new(StringArray::from(vec![
                        "alice@example.com",
                        "bob@example.com",
                        "charlie@example.com",
                    ])),
                ],
            )?;

            let txn_id = db.insert(batch).await?;
            eprintln!("      ✓ Inserted 3 rows (transaction {})", txn_id);
            eprintln!();

            // Query data
            eprintln!("[3/4] Querying data...");
            let results = db.query("SELECT * FROM data ORDER BY id").await?;
            eprintln!("      ✓ Query returned {} batch(es)", results.len());

            if !results.is_empty() {
                let batch = &results[0];
                eprintln!("      ✓ Rows: {}", batch.num_rows());
                eprintln!("      ✓ Columns: {}", batch.num_columns());

                // Pretty print first few rows
                use arrow::util::pretty::pretty_format_batches;
                let formatted = pretty_format_batches(&results)?;
                eprintln!();
                eprintln!("{}", formatted);
            }
            eprintln!();

            // Test reopen
            eprintln!("[4/4] Reopening database from S3...");
            let db2 =
                DatabaseOps::open_with_s3(&s3_path, &endpoint, &access_key, &secret_key).await?;
            eprintln!("      ✓ Database reopened");
            eprintln!();

            // Query again to verify persistence
            eprintln!("      Verifying data persistence...");
            let results2 = db2.query("SELECT COUNT(*) as count FROM data").await?;
            if !results2.is_empty() {
                use arrow::util::pretty::pretty_format_batches;
                let formatted = pretty_format_batches(&results2)?;
                eprintln!("{}", formatted);
            }
            eprintln!();

            eprintln!("=== S3/MinIO backend test PASSED ===");
            Ok(())
        }
    }
}

/// Parse schema string in format "col1:Type1,col2:Type2,..."
/// Supported types: Int64, Int32, Float64, Float32, String, Boolean
fn parse_schema(schema_str: &str) -> Result<Schema> {
    let mut fields = Vec::new();

    for part in schema_str.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let parts: Vec<&str> = part.split(':').collect();
        if parts.len() != 2 {
            return Err(posixlake::error::Error::InvalidOperation(format!(
                "Invalid schema field '{}'. Expected format: 'name:Type'",
                part
            )));
        }

        let name = parts[0].trim();
        let type_str = parts[1].trim();

        let data_type = match type_str.to_lowercase().as_str() {
            "int64" | "bigint" | "long" => DataType::Int64,
            "int32" | "int" | "integer" => DataType::Int32,
            "float64" | "double" => DataType::Float64,
            "float32" | "float" => DataType::Float32,
            "string" | "utf8" | "text" | "varchar" => DataType::Utf8,
            "boolean" | "bool" => DataType::Boolean,
            _ => {
                return Err(posixlake::error::Error::InvalidOperation(format!(
                    "Unknown type '{}'. Supported: Int64, Int32, Float64, Float32, String, Boolean",
                    type_str
                )));
            }
        };

        fields.push(Field::new(name, data_type, true)); // All columns nullable
    }

    if fields.is_empty() {
        return Err(posixlake::error::Error::InvalidOperation(
            "Schema must have at least one field".to_string(),
        ));
    }

    Ok(Schema::new(fields))
}
