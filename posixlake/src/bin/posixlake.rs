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
use clap::{Parser, Subcommand, ValueEnum};
use posixlake::error::{Error, Result};
#[cfg(target_os = "windows")]
use posixlake::nfs::windows::MOUNT_OPTIONS;
use posixlake::nfs::NfsServer;
use posixlake::storage::s3::parse_s3_uri;
use posixlake::DatabaseOps;
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
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

    /// Manage S3/MinIO local test environment
    S3 {
        #[command(subcommand)]
        command: S3Commands,
    },
}

#[derive(Subcommand)]
enum S3Commands {
    /// Start local S3/MinIO for testing
    Start {
        /// Container engine to use
        #[arg(long, value_enum, default_value = "docker")]
        engine: S3Engine,

        /// Start mode: compose (docker-compose.yml)
        #[arg(long, value_enum, default_value = "compose")]
        mode: S3Mode,

        /// Compose file path
        #[arg(long, default_value = "docker-compose.yml")]
        compose_file: PathBuf,
    },

    /// Stop local S3/MinIO
    Stop {
        /// Container engine to use
        #[arg(long, value_enum, default_value = "docker")]
        engine: S3Engine,

        /// Stop mode: compose (docker-compose.yml)
        #[arg(long, value_enum, default_value = "compose")]
        mode: S3Mode,

        /// Compose file path
        #[arg(long, default_value = "docker-compose.yml")]
        compose_file: PathBuf,
    },
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum S3Engine {
    Docker,
    Podman,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum S3Mode {
    Compose,
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
            // Skip for Windows drive letters (e.g., "Z:") which can't be created as directories
            #[cfg(target_os = "windows")]
            let is_drive_letter = {
                let path_str = mount_point.to_string_lossy();
                path_str.len() <= 3
                    && path_str
                        .chars()
                        .next()
                        .map(|c| c.is_ascii_alphabetic())
                        .unwrap_or(false)
                    && path_str.chars().nth(1) == Some(':')
            };
            #[cfg(not(target_os = "windows"))]
            let is_drive_letter = false;

            if !is_drive_letter && !mount_point.exists() {
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
            let mount_status = std::process::Command::new("C:\\Windows\\System32\\mount.exe")
                .arg("-o")
                .arg("anon")
                .arg("\\\\localhost\\share")
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
                        "  mount -o {} \\\\localhost\\share {}",
                        MOUNT_OPTIONS,
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
                        "  mount -o {} \\\\localhost\\share {}",
                        MOUNT_OPTIONS,
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
            #[cfg(target_os = "windows")]
            {
                // Check Windows NFS mount status using net use
                let output = std::process::Command::new("net").arg("use").output();

                match output {
                    Ok(out) => {
                        let stdout = String::from_utf8_lossy(&out.stdout);
                        let mount_str = mount_point.to_string_lossy();
                        if stdout.contains(&*mount_str) {
                            println!("mounted: {} is an active NFS mount", mount_point.display());
                        } else {
                            println!(
                                "not mounted: {} is not currently mounted",
                                mount_point.display()
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("unknown: failed to check mount status: {}", e);
                        std::process::exit(1);
                    }
                }
                Ok(())
            }

            #[cfg(target_os = "linux")]
            {
                // Check Linux mount status using /proc/mounts
                let mounts = std::fs::read_to_string("/proc/mounts").unwrap_or_default();
                let mount_str = mount_point.to_string_lossy();
                if mounts.contains(&*mount_str) {
                    println!("mounted: {} is an active mount", mount_point.display());
                } else {
                    println!(
                        "not mounted: {} is not currently mounted",
                        mount_point.display()
                    );
                }
                Ok(())
            }

            #[cfg(target_os = "macos")]
            {
                // Check macOS mount status using mount command
                let output = std::process::Command::new("mount").output();

                match output {
                    Ok(out) => {
                        let stdout = String::from_utf8_lossy(&out.stdout);
                        let mount_str = mount_point.to_string_lossy();
                        if stdout.contains(&*mount_str) {
                            println!("mounted: {} is an active mount", mount_point.display());
                        } else {
                            println!(
                                "not mounted: {} is not currently mounted",
                                mount_point.display()
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("unknown: failed to check mount status: {}", e);
                        std::process::exit(1);
                    }
                }
                Ok(())
            }
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
            if is_local_endpoint(&endpoint)
                && !wait_for_endpoint(&endpoint, std::time::Duration::from_secs(2))
            {
                eprintln!("      MinIO not reachable; starting local MinIO...");
                start_local_minio_for_test()?;
                if !wait_for_endpoint(&endpoint, std::time::Duration::from_secs(30)) {
                    return Err(Error::Other(format!(
                        "MinIO did not become ready at {}",
                        endpoint
                    )));
                }
            }
            let db = match DatabaseOps::create_with_s3(
                &s3_path,
                schema.clone(),
                &endpoint,
                &access_key,
                &secret_key,
            )
            .await
            {
                Ok(db) => db,
                Err(err) if should_open_existing_db(&err) => {
                    eprintln!("      Database already exists; opening...");
                    DatabaseOps::open_with_s3(&s3_path, &endpoint, &access_key, &secret_key).await?
                }
                Err(err) if should_auto_start_minio(&err) => {
                    eprintln!("      MinIO not ready or bucket missing; starting local MinIO...");
                    start_local_minio_for_test()?;
                    if !wait_for_endpoint(&endpoint, std::time::Duration::from_secs(30)) {
                        return Err(Error::Other(format!(
                            "MinIO did not become ready at {}",
                            endpoint
                        )));
                    }
                    if is_local_endpoint(&endpoint) {
                        ensure_bucket_for_test(&s3_path, &endpoint, &access_key, &secret_key)?;
                    }
                    match DatabaseOps::create_with_s3(
                        &s3_path,
                        schema.clone(),
                        &endpoint,
                        &access_key,
                        &secret_key,
                    )
                    .await
                    {
                        Ok(db) => db,
                        Err(err) if should_open_existing_db(&err) => {
                            eprintln!("      Database already exists after startup; opening...");
                            DatabaseOps::open_with_s3(&s3_path, &endpoint, &access_key, &secret_key)
                                .await?
                        }
                        Err(err) => return Err(err),
                    }
                }
                Err(err) => return Err(err),
            };
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

        Commands::S3 { command } => match command {
            S3Commands::Start {
                engine,
                mode: S3Mode::Compose,
                compose_file,
            } => {
                eprintln!("Starting S3/MinIO via compose...");
                let (program, args) = build_compose_up_command(engine, &compose_file);
                #[cfg(target_os = "windows")]
                {
                    if engine == S3Engine::Podman {
                        run_wsl_command(engine, &args)?;
                        eprintln!("S3/MinIO started.");
                        return Ok(());
                    }
                }
                run_command_with_fallback(engine, &program, &args)?;
                eprintln!("S3/MinIO started.");
                Ok(())
            }
            S3Commands::Stop {
                engine,
                mode: S3Mode::Compose,
                compose_file,
            } => {
                eprintln!("Stopping S3/MinIO via compose...");
                let (program, args) = build_compose_down_command(engine, &compose_file);
                #[cfg(target_os = "windows")]
                {
                    if engine == S3Engine::Podman {
                        run_wsl_command(engine, &args)?;
                        eprintln!("S3/MinIO stopped.");
                        return Ok(());
                    }
                }
                run_command_with_fallback(engine, &program, &args)?;
                eprintln!("S3/MinIO stopped.");
                Ok(())
            }
        },
    }
}

fn run_command(program: &str, args: &[String]) -> Result<()> {
    let output = match std::process::Command::new(program).args(args).output() {
        Ok(output) => output,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(Error::Other(format!("Program not found: {}", program)))
        }
        Err(err) => return Err(Error::Io(err)),
    };

    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let details = if stderr.trim().is_empty() {
            stdout.trim()
        } else {
            stderr.trim()
        };
        Err(Error::Other(format!(
            "Command failed: {} {:?}: {}",
            program,
            output.status.code(),
            details
        )))
    }
}

fn run_command_with_fallback(engine: S3Engine, program: &str, args: &[String]) -> Result<()> {
    match run_command(program, args) {
        Ok(()) => Ok(()),
        Err(err) => {
            if should_try_wsl_fallback(engine, args) {
                return run_wsl_command(engine, args);
            }

            if is_program_not_found(&err) {
                let fallback_engine = match engine {
                    S3Engine::Docker => S3Engine::Podman,
                    S3Engine::Podman => S3Engine::Docker,
                };
                let fallback_program = engine_program(fallback_engine);
                match run_command(&fallback_program, args) {
                    Ok(()) => Ok(()),
                    Err(err) if is_program_not_found(&err) => run_wsl_command(engine, args),
                    Err(err) => Err(err),
                }
            } else {
                Err(err)
            }
        }
    }
}

#[cfg(target_os = "windows")]
fn should_try_wsl_fallback(engine: S3Engine, args: &[String]) -> bool {
    engine == S3Engine::Podman && args.first().map(|a| a == "compose").unwrap_or(false)
}

#[cfg(not(target_os = "windows"))]
fn should_try_wsl_fallback(_engine: S3Engine, _args: &[String]) -> bool {
    false
}

fn should_auto_start_minio(err: &Error) -> bool {
    let message = err.to_string().to_lowercase();
    message.contains("nosuchbucket")
        || message.contains("connection refused")
        || message.contains("connectionrefused")
        || message.contains("refused")
        || message.contains("failed to connect")
        || message.contains("connection reset")
        || message.contains("connect error")
}

fn should_open_existing_db(err: &Error) -> bool {
    let message = err.to_string().to_lowercase();
    message.contains("tablealreadyexists")
        || (message.contains("already") && message.contains("exist"))
}

fn start_local_minio_for_test() -> Result<()> {
    let compose_file = PathBuf::from("docker-compose.yml");
    let (program, args) = build_compose_up_command(S3Engine::Podman, &compose_file);
    run_command_with_fallback(S3Engine::Podman, &program, &args)
}

fn ensure_bucket_for_test(
    s3_path: &str,
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<()> {
    let (bucket, _) = parse_s3_uri(s3_path)?;
    let engine = S3Engine::Podman;
    let program = engine_program(engine);

    let run_engine = |args: &[String]| -> Result<()> {
        #[cfg(target_os = "windows")]
        {
            if engine == S3Engine::Podman {
                return run_wsl_command(engine, args);
            }
        }
        run_command_with_fallback(engine, &program, args)
    };

    let setup_args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "--network=host".to_string(),
        "--entrypoint".to_string(),
        "/bin/sh".to_string(),
        "docker.io/minio/mc:latest".to_string(),
        "-c".to_string(),
        format!(
            "mc alias set myminio {} {} {} && mc mb myminio/{} || true && mc anonymous set download myminio/{} || true",
            endpoint, access_key, secret_key, bucket, bucket
        ),
    ];

    let mut setup_ok = false;
    for _ in 0..15 {
        if run_engine(&setup_args).is_ok() {
            setup_ok = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    if !setup_ok {
        return Err(Error::Other("Unable to configure MinIO bucket".to_string()));
    }

    Ok(())
}

fn wait_for_endpoint(endpoint: &str, max_wait: std::time::Duration) -> bool {
    let url = match url::Url::parse(endpoint) {
        Ok(url) => url,
        Err(_) => return false,
    };
    let host = match url.host_str() {
        Some(host) => host,
        None => return false,
    };
    let port = url.port_or_known_default().unwrap_or(80);
    let deadline = std::time::Instant::now() + max_wait;

    while std::time::Instant::now() < deadline {
        if let Ok(mut addrs) = (host, port).to_socket_addrs() {
            for addr in addrs.by_ref() {
                if std::net::TcpStream::connect_timeout(
                    &addr,
                    std::time::Duration::from_millis(500),
                )
                .is_ok()
                {
                    return true;
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    false
}

fn is_local_endpoint(endpoint: &str) -> bool {
    let url = match url::Url::parse(endpoint) {
        Ok(url) => url,
        Err(_) => return false,
    };
    matches!(
        url.host_str(),
        Some("localhost") | Some("127.0.0.1") | Some("::1")
    )
}

fn is_program_not_found(err: &Error) -> bool {
    match err {
        Error::Io(err) => err.kind() == std::io::ErrorKind::NotFound,
        Error::Other(message) => message.starts_with("Program not found:"),
        _ => false,
    }
}

#[cfg(target_os = "windows")]
fn run_wsl_command(engine: S3Engine, args: &[String]) -> Result<()> {
    let distro = std::env::var("WSL_DISTRO").unwrap_or_else(|_| "Ubuntu".to_string());
    let wsl_full_path = preferred_wsl_path();
    let cmd_candidates = [
        "C:\\Windows\\System32\\cmd.exe",
        "C:\\Windows\\Sysnative\\cmd.exe",
        "cmd.exe",
    ];

    let is_compose = args.first().map(|a| a == "compose").unwrap_or(false);
    let is_up = args.iter().any(|arg| arg == "up");
    let is_down = args.iter().any(|arg| arg == "down");

    if engine == S3Engine::Podman && is_compose {
        if is_up {
            return wsl_podman_direct_start(&distro, engine);
        }
        if is_down {
            return wsl_podman_direct_stop(&distro, engine);
        }
    }

    let mut wsl_args = vec![
        "-d".to_string(),
        distro.clone(),
        "sh".to_string(),
        "-lc".to_string(),
    ];
    let command = build_wsl_command(engine, args)?;
    wsl_args.push(command.clone());

    match run_command(&wsl_full_path, &wsl_args) {
        Ok(()) => return Ok(()),
        Err(Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) if engine == S3Engine::Podman && is_compose && (is_up || is_down) => {
            return if is_up {
                wsl_podman_direct_start(&distro, engine)
            } else {
                wsl_podman_direct_stop(&distro, engine)
            };
        }
        Err(err) => return Err(err),
    }

    for candidate in ["wsl.exe"] {
        match run_command(candidate, &wsl_args) {
            Ok(()) => return Ok(()),
            Err(Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) if engine == S3Engine::Podman && is_compose && (is_up || is_down) => {
                return if is_up {
                    wsl_podman_direct_start(&distro, engine)
                } else {
                    wsl_podman_direct_stop(&distro, engine)
                };
            }
            Err(err) => return Err(err),
        }
    }

    let wsl_cmd = format!(
        "{} -d {} sh -lc {}",
        shell_escape(&wsl_full_path),
        shell_escape(&distro),
        shell_escape(&command)
    );

    for candidate in cmd_candidates {
        let cmd_args = ["/c".to_string(), wsl_cmd.clone()];
        match run_command(candidate, &cmd_args) {
            Ok(()) => return Ok(()),
            Err(Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) if engine == S3Engine::Podman && is_compose && (is_up || is_down) => {
                return if is_up {
                    wsl_podman_direct_start(&distro, engine)
                } else {
                    wsl_podman_direct_stop(&distro, engine)
                };
            }
            Err(err) => return Err(err),
        }
    }

    Err(Error::Other(
        "WSL not available on PATH or System32".to_string(),
    ))
}

#[cfg(not(target_os = "windows"))]
fn run_wsl_command(_engine: S3Engine, _args: &[String]) -> Result<()> {
    Err(Error::Other(
        "WSL fallback is only supported on Windows".to_string(),
    ))
}

#[cfg(target_os = "windows")]
fn preferred_wsl_path() -> String {
    let is_32_bit =
        cfg!(target_pointer_width = "32") || std::env::var_os("PROCESSOR_ARCHITEW6432").is_some();

    if is_32_bit {
        "C:\\Windows\\Sysnative\\wsl.exe".to_string()
    } else {
        "C:\\Windows\\System32\\wsl.exe".to_string()
    }
}

#[cfg(target_os = "windows")]
fn wsl_podman_direct_start(distro: &str, engine: S3Engine) -> Result<()> {
    let engine = engine_program(engine);
    let _ = wsl_shell_success(distro, &format!("{} rm -f posixlake-minio", engine));

    if let Err(err) = wsl_shell(
        distro,
        &format!(
            "{} run -d --name posixlake-minio -p 9000:9000 -p 9001:9001 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin docker.io/minio/minio:latest server /data --console-address ':9001'",
            engine
        ),
    ) {
        let message = err.to_string().to_lowercase();
        if message.contains("address already in use") || message.contains("port is already allocated") {
            eprintln!("MinIO already running on port 9000; continuing...");
        } else {
            return Err(err);
        }
    }

    let setup_cmd = format!(
        "{} run --rm --network=host --entrypoint /bin/sh docker.io/minio/mc:latest -c \"mc alias set myminio http://localhost:9000 minioadmin minioadmin && mc mb myminio/posixlake-test || true && mc anonymous set download myminio/posixlake-test || true\"",
        engine
    );
    let mut setup_ok = false;
    for _ in 0..15 {
        if wsl_shell_success(distro, &setup_cmd) {
            setup_ok = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    if !setup_ok {
        eprintln!("MinIO not ready for bucket setup; continuing...");
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn wsl_podman_direct_stop(distro: &str, engine: S3Engine) -> Result<()> {
    let engine = engine_program(engine);
    wsl_shell(distro, &format!("{} rm -f posixlake-minio", engine))
}

#[cfg(target_os = "windows")]
fn wsl_shell_success(distro: &str, command: &str) -> bool {
    wsl_shell(distro, command).is_ok()
}

#[cfg(target_os = "windows")]
fn wsl_shell(distro: &str, command: &str) -> Result<()> {
    let wsl_full_path = preferred_wsl_path();
    let args = vec![
        "-d".to_string(),
        distro.to_string(),
        "sh".to_string(),
        "-lc".to_string(),
        command.to_string(),
    ];
    run_command(&wsl_full_path, &args)
}

#[cfg(target_os = "windows")]
fn build_wsl_command(engine: S3Engine, args: &[String]) -> Result<String> {
    let mut args = args.to_vec();

    if let Some(index) = args.iter().position(|arg| arg == "-f") {
        if let Some(path_arg) = args.get(index + 1).cloned() {
            let path = PathBuf::from(path_arg);
            let abs = if path.is_absolute() {
                path
            } else {
                std::env::current_dir()?.join(path)
            };
            args[index + 1] = windows_path_to_wsl(abs)?;
        }
    }

    let cwd = windows_path_to_wsl(std::env::current_dir()?)?;
    let mut cmd_parts = vec![engine_program(engine)];
    cmd_parts.extend(args);

    let cmd = cmd_parts
        .iter()
        .map(|part| shell_escape(part))
        .collect::<Vec<_>>()
        .join(" ");

    Ok(format!("cd {} && {}", shell_escape(&cwd), cmd))
}

#[cfg(target_os = "windows")]
fn windows_path_to_wsl(path: PathBuf) -> Result<String> {
    let path_str = path.to_string_lossy();
    let mut chars = path_str.chars();
    let drive = chars
        .next()
        .ok_or_else(|| Error::Other("Invalid path".to_string()))?;
    if chars.next() != Some(':') {
        return Err(Error::Other(format!("Unsupported path: {}", path_str)));
    }
    let rest: String = chars.collect();
    let rest = rest.replace('\\', "/");
    Ok(format!(
        "/mnt/{}/{}",
        drive.to_ascii_lowercase(),
        rest.trim_start_matches('/')
    ))
}

#[cfg(target_os = "windows")]
fn shell_escape(value: &str) -> String {
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "-._/:".contains(c))
    {
        return value.to_string();
    }

    let escaped = value.replace('"', "\\\"");
    format!("\"{}\"", escaped)
}

fn build_compose_up_command(engine: S3Engine, compose_file: &Path) -> (String, Vec<String>) {
    (
        engine_program(engine),
        vec![
            "compose".to_string(),
            "-f".to_string(),
            compose_file.to_string_lossy().to_string(),
            "up".to_string(),
            "-d".to_string(),
            "minio".to_string(),
            "minio-init".to_string(),
        ],
    )
}

fn build_compose_down_command(engine: S3Engine, compose_file: &Path) -> (String, Vec<String>) {
    (
        engine_program(engine),
        vec![
            "compose".to_string(),
            "-f".to_string(),
            compose_file.to_string_lossy().to_string(),
            "down".to_string(),
        ],
    )
}

fn engine_program(engine: S3Engine) -> String {
    match engine {
        S3Engine::Docker => "docker".to_string(),
        S3Engine::Podman => "podman".to_string(),
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
