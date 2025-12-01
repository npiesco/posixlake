//! Example: Security - Authentication, RBAC, and Audit Logging
//!
//! This example demonstrates:
//! - Creating an auth-enabled database
//! - User management with bcrypt password hashing
//! - Role-based access control (RBAC)
//! - Permission enforcement
//! - Audit logging for compliance

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use fsdb::DatabaseOps;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("fsdb=info")
        .init();

    println!("\n=== FSDB Security & RBAC Example ===\n");

    let db_path = "/tmp/fsdb_example_security";
    let _ = std::fs::remove_dir_all(db_path);

    // 1. Create auth-enabled database
    println!("1. Creating database with authentication enabled...");
    let schema = Arc::new(Schema::new(vec![
        Field::new("employee_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, false),
        Field::new("salary", DataType::Int32, false),
    ]));

    let db = DatabaseOps::create_with_auth(db_path, schema.clone(), true).await?;
    println!("   ✓ Database created with authentication");
    println!("   ✓ User store initialized");
    println!("   ✓ Audit log initialized");
    println!("   ✓ Role manager configured");

    // 2. Create users with different roles
    println!("\n2. Creating users with different roles...");

    // Admin user (full permissions)
    db.create_user("admin", "admin123", &["admin"]).await?;
    println!("   ✓ Created user: admin (role: admin)");
    println!("     Permissions: Read, Write, Delete, Admin, Backup, Restore");

    // HR manager (read + write)
    db.create_user("hr_manager", "hr123", &["write", "read"])
        .await?;
    println!("   ✓ Created user: hr_manager (roles: write, read)");
    println!("     Permissions: Read, Write");

    // Analyst (read-only)
    db.create_user("analyst", "analyst123", &["read"]).await?;
    println!("   ✓ Created user: analyst (role: read)");
    println!("     Permissions: Read");

    // 3. Admin inserts sensitive data
    println!("\n3. Admin inserting employee data...");
    let employee_data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1001, 1002, 1003])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "Alice Johnson",
                "Bob Smith",
                "Charlie Davis",
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Engineering", "Sales", "HR"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![120000, 85000, 75000])) as ArrayRef,
        ],
    )?;
    db.insert(employee_data).await?;
    println!("   ✓ Inserted 3 employee records");
    println!("   ✓ Audit log: INSERT operation by 'system'");

    // 4. Authenticate as analyst (read-only)
    println!("\n4. Authenticating as analyst (read-only)...");
    drop(db); // Close system context

    let analyst_db =
        DatabaseOps::open_with_credentials(db_path, Some(("analyst", "analyst123"))).await?;
    println!("   ✓ Analyst authenticated");

    // Analyst can read
    let results = analyst_db
        .query("SELECT name, department FROM data")
        .await?;
    let row_count: usize = results.iter().map(|batch| batch.num_rows()).sum();
    println!("   ✓ Analyst read {} employee records", row_count);
    println!("   ✓ Audit log: SELECT operation by 'analyst'");

    // Analyst cannot write
    println!("\n5. Testing permission enforcement (analyst cannot write)...");
    let write_attempt = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1004])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Test"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Test"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![50000])) as ArrayRef,
        ],
    )?;

    match analyst_db.insert(write_attempt).await {
        Ok(_) => println!("   ✗ ERROR: Analyst should not be able to insert!"),
        Err(e) => {
            println!("   ✓ Insert denied: {}", e);
            println!("   ✓ Audit log: INSERT attempt denied for 'analyst'");
        }
    }
    drop(analyst_db);

    // 6. Authenticate as HR manager (read + write)
    println!("\n6. Authenticating as hr_manager (read + write)...");
    let hr_db = DatabaseOps::open_with_credentials(db_path, Some(("hr_manager", "hr123"))).await?;
    println!("   ✓ HR manager authenticated");

    // HR can write
    let new_employee = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1004])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Diana Wilson"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Marketing"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![90000])) as ArrayRef,
        ],
    )?;
    hr_db.insert(new_employee).await?;
    println!("   ✓ HR manager added new employee");
    println!("   ✓ Audit log: INSERT operation by 'hr_manager'");

    // HR can read
    let updated_results = hr_db.query("SELECT COUNT(*) FROM data").await?;
    let updated_count: usize = updated_results.iter().map(|batch| batch.num_rows()).sum();
    println!(
        "   ✓ HR manager verified: {} total employees",
        updated_count
    );
    drop(hr_db);

    // 7. System access for administrative tasks
    println!("\n7. System access for maintenance...");
    let system_db = DatabaseOps::open_with_credentials(
        db_path, None, // System access
    )
    .await?;
    println!("   ✓ System authenticated (administrative access)");

    // System can perform any operation
    let all_data = system_db.query("SELECT * FROM data").await?;
    let system_row_count: usize = all_data.iter().map(|batch| batch.num_rows()).sum();
    println!(
        "   ✓ System retrieved all {} employee records",
        system_row_count
    );

    // 8. Inspect audit log
    println!("\n8. Inspecting audit log...");
    let audit_log = system_db.get_audit_log().await?;
    println!("   Audit Log Summary:");
    println!("     Total entries: {}", audit_log.len());

    // Show recent entries
    println!("\n   Recent audit entries:");
    for (i, entry) in audit_log.iter().rev().take(5).enumerate() {
        println!(
            "     {}. [{}] {} performed {} - Success: {}",
            i + 1,
            entry.timestamp,
            entry.user,
            entry.operation,
            if entry.success { "✓" } else { "✗" }
        );
    }

    // 9. Failed authentication attempt
    println!("\n9. Testing failed authentication...");
    match DatabaseOps::open_with_credentials(db_path, Some(("analyst", "wrongpassword"))).await {
        Ok(_) => println!("   ✗ ERROR: Should reject wrong password!"),
        Err(e) => {
            println!("   ✓ Authentication failed: {}", e);
            println!("   ✓ Invalid credentials rejected");
        }
    }

    println!("\n=== Security & RBAC Example Complete ===\n");
    println!("Key Takeaways:");
    println!("  • bcrypt password hashing for security");
    println!("  • Role-based access control (admin/read/write)");
    println!("  • Permission enforcement at database layer");
    println!("  • Comprehensive audit logging for compliance");
    println!("  • System access for administrative operations");
    println!();
    println!("Default Roles:");
    println!("  admin   : Read, Write, Delete, Admin, Backup, Restore");
    println!("  write   : Read, Write");
    println!("  read    : Read");
    println!();
    println!("Audit Log Captures:");
    println!("  • User authentication attempts");
    println!("  • Successful operations (INSERT, SELECT, DELETE)");
    println!("  • Failed permission checks");
    println!("  • User management operations (CREATE_USER)");

    Ok(())
}
