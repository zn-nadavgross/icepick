//! Tests for the commit command
//!
//! Integration tests require a running catalog and are marked #[ignore].
//! Run with: cargo test --test commit_command -- --ignored

use icepick::cli::commands::commit::{
    build_partition_spec, check_schema_compatibility, parse_partition_spec,
    parse_partition_values_arg, parse_type_str,
};
use icepick::spec::{NestedField, PrimitiveType, Schema, Type};
use std::process::Command;

fn schema(fields: &[(&str, PrimitiveType)]) -> Schema {
    Schema::builder()
        .with_fields(
            fields
                .iter()
                .enumerate()
                .map(|(i, (n, t))| {
                    NestedField::required_field(
                        (i + 1) as i32,
                        n.to_string(),
                        Type::Primitive(t.clone()),
                    )
                })
                .collect(),
        )
        .build()
        .unwrap()
}

#[test]
fn test_parse_partition_spec() {
    let result = parse_partition_spec("year:int,month:int").unwrap();
    assert_eq!(
        result,
        vec![
            ("year".into(), PrimitiveType::Int),
            ("month".into(), PrimitiveType::Int)
        ]
    );
    // Test type aliases
    assert_eq!(parse_type_str("bool").unwrap(), PrimitiveType::Boolean);
    assert_eq!(parse_type_str("integer").unwrap(), PrimitiveType::Int);
    assert_eq!(parse_type_str("bigint").unwrap(), PrimitiveType::Long);
    // Error cases
    assert!(parse_partition_spec("year:invalid")
        .unwrap_err()
        .contains("Unknown type"));
    assert!(parse_partition_spec("year")
        .unwrap_err()
        .contains("Expected format"));
}

#[test]
fn test_parse_partition_values() {
    let result = parse_partition_values_arg("year=2024,month=01").unwrap();
    assert_eq!(result.get("year"), Some(&"2024".to_string()));
    assert_eq!(result.get("month"), Some(&"01".to_string()));
}

#[test]
fn test_check_schema_compatibility() {
    let s1 = schema(&[("id", PrimitiveType::Long), ("name", PrimitiveType::String)]);
    let s2 = schema(&[("id", PrimitiveType::Long), ("name", PrimitiveType::String)]);
    assert!(check_schema_compatibility(&s1, &s2).is_ok());
    // Field count mismatch
    let s3 = schema(&[("id", PrimitiveType::Long)]);
    assert!(check_schema_compatibility(&s1, &s3)
        .unwrap_err()
        .contains("field count"));
    // Field name mismatch
    let s4 = schema(&[
        ("user_id", PrimitiveType::Long),
        ("name", PrimitiveType::String),
    ]);
    assert!(check_schema_compatibility(&s1, &s4)
        .unwrap_err()
        .contains("field name"));
    // Field type mismatch
    let s5 = schema(&[("id", PrimitiveType::Int), ("name", PrimitiveType::String)]);
    assert!(check_schema_compatibility(&s1, &s5)
        .unwrap_err()
        .contains("type mismatch"));
}

#[test]
fn test_build_partition_spec() {
    let s = schema(&[
        ("id", PrimitiveType::Long),
        ("year", PrimitiveType::Int),
        ("month", PrimitiveType::Int),
    ]);
    let result = build_partition_spec("year:int,month:int", &s).unwrap();
    assert_eq!(result.fields().len(), 2);
    assert_eq!(result.fields()[0].name(), "year");
    // Column not found
    let s2 = schema(&[("id", PrimitiveType::Long)]);
    assert!(build_partition_spec("year:int", &s2)
        .unwrap_err()
        .contains("not found"));
    // Type mismatch
    let s3 = schema(&[("year", PrimitiveType::String)]);
    assert!(build_partition_spec("year:int", &s3)
        .unwrap_err()
        .contains("type mismatch"));
}

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
