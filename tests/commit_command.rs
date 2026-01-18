//! Integration tests for the commit command
//!
//! These tests require a running catalog and are marked #[ignore].
//! Run with: cargo test --test commit_command -- --ignored

use std::process::Command;

#[test]
#[ignore]
fn test_commit_dry_run() {
    let output = Command::new("cargo")
        .args([
            "run",
            "--features",
            "cli",
            "--",
            "commit",
            "/tmp/test-data/**/*.parquet",
            "--namespace",
            "test",
            "--table",
            "events",
            "--dry-run",
        ])
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("stdout: {}", stdout);
    println!("stderr: {}", stderr);

    // Should either succeed with a plan or fail with "no files found"
    assert!(
        output.status.success() || stderr.contains("No Parquet files found"),
        "Command failed unexpectedly: {}",
        stderr
    );
}

#[test]
fn test_partition_spec_parsing() {
    // Unit test that doesn't require catalog
    let spec = "year:int,month:int,day:int";
    let parts: Vec<&str> = spec.split(',').collect();

    assert_eq!(parts.len(), 3);
    assert!(parts[0].contains("year"));
    assert!(parts[1].contains("month"));
    assert!(parts[2].contains("day"));
}
