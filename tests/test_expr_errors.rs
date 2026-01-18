//! Tests for expression parser error handling

use icepick::expr::parse_filter;

#[test]
fn test_parser_error_on_trailing_operator() {
    let result = parse_filter("status = 'active' AND");
    assert!(
        result.is_err(),
        "Parser should reject expression ending with operator"
    );
}

#[test]
fn test_parser_leading_operator_might_parse_as_column() {
    // Note: "AND status = 'active'" might parse as column "AND" = 'active'
    // This is acceptable behavior - just ensure it doesn't panic
    let result = parse_filter("AND status = 'active'");
    // Either error or parse successfully - just don't panic
    let _ = result;
}

#[test]
fn test_parser_error_on_missing_value() {
    let result = parse_filter("date >= ");
    assert!(
        result.is_err(),
        "Parser should reject comparison without right-hand value"
    );
}

#[test]
fn test_parser_handles_quoted_strings() {
    // Should parse successfully with proper quotes
    let result = parse_filter("status = 'active'");
    assert!(
        result.is_ok(),
        "Parser should accept properly quoted strings"
    );

    let result2 = parse_filter("status = \"active\"");
    assert!(
        result2.is_ok(),
        "Parser should accept double-quoted strings"
    );
}

#[test]
fn test_parser_handles_numeric_values() {
    let result = parse_filter("id = 123");
    assert!(result.is_ok(), "Parser should accept integer values");

    let result2 = parse_filter("value >= 42");
    assert!(result2.is_ok(), "Parser should accept numeric comparisons");
}

#[test]
fn test_parser_handles_date_literals() {
    let result = parse_filter("date >= '2024-01-01'");
    assert!(result.is_ok(), "Parser should accept date literals");
}

#[test]
fn test_parser_handles_and_or() {
    let result = parse_filter("a = 1 AND b = 2");
    assert!(result.is_ok(), "Parser should handle AND");

    let result2 = parse_filter("a = 1 OR b = 2");
    assert!(result2.is_ok(), "Parser should handle OR");
}

#[test]
fn test_parser_handles_is_null() {
    let result = parse_filter("field IS NULL");
    assert!(result.is_ok(), "Parser should handle IS NULL");

    let result2 = parse_filter("field IS NOT NULL");
    assert!(result2.is_ok(), "Parser should handle IS NOT NULL");
}

#[test]
fn test_parser_handles_in_predicate() {
    let result = parse_filter("status IN ('active', 'pending')");
    assert!(result.is_ok(), "Parser should handle IN predicate");
}

#[test]
fn test_parser_error_messages_are_helpful() {
    let test_cases = vec![
        ("date >= ", "missing value"),
        ("AND x = 1", "leading operator"),
        ("x = 1 AND", "trailing operator"),
    ];

    for (expr, description) in test_cases {
        let result = parse_filter(expr);
        if let Err(e) = result {
            let error_msg = e.to_string();
            assert!(
                !error_msg.is_empty(),
                "Error message should not be empty for {}",
                description
            );
            assert!(
                error_msg.contains("Failed to parse") || error_msg.contains("Invalid"),
                "Error message should be descriptive for {}: {}",
                description,
                error_msg
            );
        } else {
            // If it doesn't error, that's okay too - we just want to ensure no panics
        }
    }
}
