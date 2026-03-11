fn main() {
    // UniFFI with proc-macros doesn't need explicit scaffolding generation
    // The #[uniffi::export] macros embed metadata directly in the library

    // Reproducible build metadata
    println!(
        "cargo:rustc-env=POSIXLAKE_BUILD_COMMIT={}",
        git_commit_hash()
    );
    println!("cargo:rustc-env=POSIXLAKE_BUILD_DATE={}", chrono_date_utc());
    println!(
        "cargo:rustc-env=POSIXLAKE_BUILD_TARGET={}",
        std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string())
    );
    println!(
        "cargo:rustc-env=POSIXLAKE_BUILD_PROFILE={}",
        std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string())
    );
}

fn git_commit_hash() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn chrono_date_utc() -> String {
    // Use a simple UTC date without pulling in chrono at build time
    std::process::Command::new("date")
        .args(["-u", "+%Y-%m-%d"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}
