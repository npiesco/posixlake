//! Backup and restore functionality for FSDB databases

use crate::{Error, Result};
use std::collections::HashSet;
use std::path::Path;
use tracing::{debug, info};

/// Backup metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackupMetadata {
    pub timestamp: u64,
    pub total_rows: u64,
    pub total_files: usize,
    pub total_size_bytes: u64,
}

/// Backup verification report
#[derive(Debug, Clone)]
pub struct BackupVerificationReport {
    pub files_verified: usize,
    pub total_size_bytes: u64,
    pub schema_valid: bool,
}

/// Helper function to recursively copy a directory
pub fn copy_dir_recursively(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if src_path.is_dir() {
            copy_dir_recursively(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

/// Count parquet files recursively
pub fn count_parquet_files(dir: &Path, files: &mut usize, size: &mut u64) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let filename = path.file_name().unwrap().to_str().unwrap_or("");

        // Skip hidden directories like _delta_log, _metadata, etc.
        if path.is_dir() && !filename.starts_with('_') {
            count_parquet_files(&path, files, size)?;
        } else if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            let metadata = entry.metadata()?;
            *size += metadata.len();
            *files += 1;
        }
    }
    Ok(())
}

/// Verify backup integrity
pub fn verify_backup<P: AsRef<Path>>(backup_path: P) -> Result<BackupVerificationReport> {
    let backup_path = backup_path.as_ref();
    info!("Verifying backup at: {}", backup_path.display());

    // Check backup structure exists
    if !backup_path.exists() {
        return Err(Error::Other("Backup directory not found".to_string()));
    }

    // Verify Delta Lake transaction log exists
    let delta_log_path = backup_path.join("_delta_log");
    if !delta_log_path.exists() {
        return Err(Error::Other(
            "Delta Lake transaction log missing from backup".to_string(),
        ));
    }

    // Count and validate parquet files
    let mut files_verified = 0;
    let mut total_size = 0u64;
    let mut schema_valid = false;

    count_parquet_files(backup_path, &mut files_verified, &mut total_size)?;

    // Check if Delta Lake log has schema information
    if delta_log_path.exists() {
        for entry in std::fs::read_dir(&delta_log_path)? {
            let entry = entry?;
            if entry.path().extension().and_then(|s| s.to_str()) == Some("json") {
                schema_valid = true;
                break;
            }
        }
    }

    info!(
        "Backup verification completed: {} files, {} bytes",
        files_verified, total_size
    );
    Ok(BackupVerificationReport {
        files_verified,
        total_size_bytes: total_size,
        schema_valid,
    })
}

/// Get backup metadata
pub fn get_backup_metadata<P: AsRef<Path>>(backup_path: P) -> Result<BackupMetadata> {
    let backup_path = backup_path.as_ref();
    let metadata_path = backup_path.join("backup_metadata.json");

    if !metadata_path.exists() {
        return Err(Error::Other("Backup metadata not found".to_string()));
    }

    let content = std::fs::read_to_string(&metadata_path)?;
    let metadata: BackupMetadata = serde_json::from_str(&content)?;
    Ok(metadata)
}

/// Restore database from backup
pub fn restore<P: AsRef<Path>>(backup_path: P, restore_path: P) -> Result<()> {
    let backup_path = backup_path.as_ref();
    let restore_path = restore_path.as_ref();
    info!(
        "Restoring database from {} to {}",
        backup_path.display(),
        restore_path.display()
    );

    // Validate backup exists
    if !backup_path.exists() {
        return Err(Error::Other(format!(
            "Backup not found: {}",
            backup_path.display()
        )));
    }

    // Create restore directory structure
    std::fs::create_dir_all(restore_path)?;

    // Restore all parquet files from backup root to restore root
    for entry in std::fs::read_dir(backup_path)? {
        let entry = entry?;
        let src = entry.path();
        if src.is_file() && src.extension().and_then(|s| s.to_str()) == Some("parquet") {
            let filename = src.file_name().unwrap();
            let dst = restore_path.join(filename);
            std::fs::copy(&src, &dst)?;
            debug!("Restored parquet file: {:?}", filename);
        }
    }

    // Restore Delta Lake transaction log
    let delta_log_src = backup_path.join("_delta_log");
    let delta_log_dst = restore_path.join("_delta_log");
    if delta_log_src.exists() {
        copy_dir_recursively(&delta_log_src, &delta_log_dst)?;
        debug!("Restored Delta Lake transaction log");
    }

    // Restore metadata
    let metadata_src = backup_path.join("_metadata");
    let metadata_dst = restore_path.join("_metadata");
    if metadata_src.exists() {
        copy_dir_recursively(&metadata_src, &metadata_dst)?;
        debug!("Restored FSDB metadata");
    }

    // Restore transaction log
    let txn_log_src = backup_path.join("_txn_log");
    let txn_log_dst = restore_path.join("_txn_log");
    if txn_log_src.exists() {
        copy_dir_recursively(&txn_log_src, &txn_log_dst)?;
        debug!("Restored FSDB transaction log");
    }

    // Restore WAL
    let wal_src = backup_path.join("_wal");
    let wal_dst = restore_path.join("_wal");
    if wal_src.exists() {
        copy_dir_recursively(&wal_src, &wal_dst)?;
        debug!("Restored WAL");
    }

    info!("Database restored successfully");
    Ok(())
}

/// Restore to Delta version (trim commits after target version)
pub fn restore_to_delta_version<P: AsRef<Path>>(
    restore_path: P,
    target_version: u64,
) -> Result<()> {
    let restore_path = restore_path.as_ref();
    info!("Trimming Delta Lake database to version {}", target_version);

    // Filter the Delta Lake log to only include versions up to target_version
    let delta_log_path = restore_path.join("_delta_log");

    // Read all commit files from Delta log and filter
    let mut commit_files = Vec::new();
    for entry in std::fs::read_dir(&delta_log_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
            let filename = path.file_name().unwrap().to_str().unwrap();
            // Parse version from filename (format: 00000000000000000001.json)
            if let Some(version_str) = filename.strip_suffix(".json") {
                if let Ok(version) = version_str.parse::<u64>() {
                    commit_files.push((version, path.clone()));
                }
            }
        }
    }

    // Remove commit files with version > target_version
    for (version, path) in commit_files {
        if version > target_version {
            std::fs::remove_file(&path)?;
            debug!(
                "Removed Delta commit file for version {} (after target version {})",
                version, target_version
            );
        }
    }

    // Now read the Delta table at the target version to figure out which parquet files should exist
    use url::Url;

    let table_url = Url::from_directory_path(restore_path).unwrap();
    let table = tokio::runtime::Runtime::new().unwrap().block_on(async {
        deltalake::open_table(table_url)
            .await
            .map_err(Error::DeltaTable)
    })?;

    // Get the list of active files at this version from the table
    let active_files: HashSet<String> = table
        .get_file_uris()
        .map_err(Error::DeltaTable)?
        .map(|uri| {
            // Extract just the filename from the URI
            uri.split('/').next_back().unwrap_or(&uri).to_string()
        })
        .collect();

    // Remove parquet files not in the active set
    for entry in std::fs::read_dir(restore_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            let filename = path.file_name().unwrap().to_str().unwrap();
            if !active_files.contains(filename) {
                std::fs::remove_file(&path)?;
                debug!(
                    "Removed parquet file {} (not active at version {})",
                    filename, target_version
                );
            }
        }
    }

    info!(
        "Point-in-time restore to Delta Lake version {} completed",
        target_version
    );
    Ok(())
}
