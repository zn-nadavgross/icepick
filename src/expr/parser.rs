//! Simple expression parser for CLI filter strings
//!
//! Parses expressions like:
//! - `date >= '2024-01-01'`
//! - `status = 'active' AND age > 18`
//! - `region IN ('us-west', 'eu-central')`

use super::date::parse_date_to_days;
use crate::error::{Error, Result};
use crate::expr::{ComparisonOp, Datum, Predicate};

/// Parse a filter expression string into a Predicate
///
/// Supports:
/// - Comparisons: `column = value`, `column > value`, etc.
/// - AND/OR: `expr1 AND expr2`, `expr1 OR expr2`
/// - IS NULL / IS NOT NULL: `column IS NULL`, `column IS NOT NULL`
///
/// Values can be:
/// - Strings: 'value' or "value"
/// - Numbers: 123, -45, 3.14
/// - Dates: '2024-01-15' (automatically detected from format)
pub fn parse_filter(input: &str) -> Result<Predicate> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(Predicate::AlwaysTrue);
    }

    // Try to parse as OR expression first (lowest precedence)
    if let Some(pred) = try_parse_or(input)? {
        return Ok(pred);
    }

    Err(Error::invalid_input(format!(
        "Failed to parse filter expression: {}",
        input
    )))
}

fn try_parse_or(input: &str) -> Result<Option<Predicate>> {
    // Split by OR (case insensitive), respecting quotes
    let parts = split_by_keyword(input, " OR ");
    if parts.len() > 1 {
        let mut preds = Vec::new();
        for part in parts {
            let part_str: &str = part;
            if let Some(pred) = try_parse_and(part_str.trim())? {
                preds.push(pred);
            } else {
                return Ok(None);
            }
        }
        return Ok(Some(Predicate::or(preds)));
    }

    try_parse_and(input)
}

fn try_parse_and(input: &str) -> Result<Option<Predicate>> {
    // Split by AND (case insensitive), respecting quotes
    let parts = split_by_keyword(input, " AND ");
    if parts.len() > 1 {
        let mut preds = Vec::new();
        for part in parts {
            let part_str: &str = part;
            if let Some(pred) = try_parse_comparison(part_str.trim())? {
                preds.push(pred);
            } else {
                return Ok(None);
            }
        }
        return Ok(Some(Predicate::and(preds)));
    }

    try_parse_comparison(input)
}

fn try_parse_comparison(input: &str) -> Result<Option<Predicate>> {
    let input = input.trim();

    // Try IS NOT NULL
    if let Some(col) = input
        .strip_suffix(" IS NOT NULL")
        .or_else(|| input.strip_suffix(" is not null"))
    {
        return Ok(Some(Predicate::is_not_null(col.trim())));
    }

    // Try IS NULL
    if let Some(col) = input
        .strip_suffix(" IS NULL")
        .or_else(|| input.strip_suffix(" is null"))
    {
        return Ok(Some(Predicate::is_null(col.trim())));
    }

    // Try IN
    if let Some((col, values)) = try_parse_in(input)? {
        return Ok(Some(Predicate::is_in(col, values)));
    }

    // Try comparison operators (ordered by length to match longer first)
    for (op_str, op) in [
        ("!=", ComparisonOp::NotEq),
        ("<>", ComparisonOp::NotEq),
        (">=", ComparisonOp::GtEq),
        ("<=", ComparisonOp::LtEq),
        ("=", ComparisonOp::Eq),
        (">", ComparisonOp::Gt),
        ("<", ComparisonOp::Lt),
    ] {
        if let Some(idx) = input.find(op_str) {
            let col = input[..idx].trim();
            let val_str = input[idx + op_str.len()..].trim();

            if col.is_empty() || val_str.is_empty() {
                continue;
            }

            let datum = parse_value(val_str)?;
            return Ok(Some(Predicate::Comparison {
                column: col.into(),
                op,
                value: datum,
            }));
        }
    }

    Ok(None)
}

fn try_parse_in(input: &str) -> Result<Option<(String, Vec<Datum>)>> {
    // Look for pattern: column IN (val1, val2, ...)
    let upper = input.to_uppercase();
    let Some(in_pos) = upper.find(" IN (") else {
        return Ok(None);
    };

    let col = input[..in_pos].trim();
    let rest = input[in_pos + 4..].trim(); // Skip " IN "

    // Must start with ( and end with )
    if !rest.starts_with('(') || !rest.ends_with(')') {
        return Ok(None);
    }

    let values_str = &rest[1..rest.len() - 1];
    let values: Result<Vec<Datum>> = values_str
        .split(',')
        .map(|s| parse_value(s.trim()))
        .collect();

    Ok(Some((col.to_string(), values?)))
}

fn parse_value(s: &str) -> Result<Datum> {
    let s = s.trim();

    // Check for quoted string
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        let inner = &s[1..s.len() - 1];

        // Check if it looks like a date (YYYY-MM-DD)
        if inner.len() == 10
            && inner.chars().nth(4) == Some('-')
            && inner.chars().nth(7) == Some('-')
        {
            if let Some(days) = parse_date_to_days(inner) {
                return Ok(Datum::Date(days));
            }
        }

        return Ok(Datum::String(inner.to_string()));
    }

    // Try to parse as number
    if let Ok(n) = s.parse::<i64>() {
        if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
            return Ok(Datum::Int(n as i32));
        }
        return Ok(Datum::Long(n));
    }

    if let Ok(n) = s.parse::<f64>() {
        return Ok(Datum::Double(n));
    }

    // Treat as unquoted string identifier (shouldn't happen in valid expressions)
    Err(Error::invalid_input(format!(
        "Invalid value in filter expression: {}",
        s
    )))
}

/// Split string by keyword, respecting quoted strings
fn split_by_keyword<'a>(input: &'a str, keyword: &str) -> Vec<&'a str> {
    let upper = input.to_uppercase();
    let keyword_upper = keyword.to_uppercase();

    let mut result = Vec::new();
    let mut start = 0;
    let mut in_quote = false;
    let mut quote_char = ' ';
    let mut i = 0;

    let chars: Vec<char> = input.chars().collect();

    while i < chars.len() {
        let c = chars[i];

        if !in_quote && (c == '\'' || c == '"') {
            in_quote = true;
            quote_char = c;
        } else if in_quote && c == quote_char {
            in_quote = false;
        } else if !in_quote {
            // Check if keyword starts at this position
            let remaining = &upper[i..];
            if remaining.starts_with(&keyword_upper) {
                result.push(&input[start..i]);
                start = i + keyword.len();
                i += keyword.len();
                continue;
            }
        }

        i += 1;
    }

    result.push(&input[start..]);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_eq() {
        let pred = parse_filter("status = 'active'").unwrap();
        assert!(matches!(
            pred,
            Predicate::Comparison {
                op: ComparisonOp::Eq,
                ..
            }
        ));
    }

    #[test]
    fn test_parse_gt() {
        let pred = parse_filter("age > 18").unwrap();
        assert!(matches!(
            pred,
            Predicate::Comparison {
                op: ComparisonOp::Gt,
                ..
            }
        ));
    }

    #[test]
    fn test_parse_and() {
        let pred = parse_filter("status = 'active' AND age > 18").unwrap();
        assert!(matches!(pred, Predicate::And(_)));
    }

    #[test]
    fn test_parse_or() {
        let pred = parse_filter("region = 'us' OR region = 'eu'").unwrap();
        assert!(matches!(pred, Predicate::Or(_)));
    }

    #[test]
    fn test_parse_is_null() {
        let pred = parse_filter("email IS NULL").unwrap();
        assert!(matches!(pred, Predicate::IsNull(_)));
    }

    #[test]
    fn test_parse_is_not_null() {
        let pred = parse_filter("email IS NOT NULL").unwrap();
        assert!(matches!(pred, Predicate::IsNotNull(_)));
    }

    #[test]
    fn test_parse_date() {
        let pred = parse_filter("date >= '2024-01-01'").unwrap();
        if let Predicate::Comparison { value, .. } = pred {
            assert!(matches!(value, Datum::Date(_)));
        } else {
            panic!("Expected comparison predicate");
        }
    }

    #[test]
    fn test_parse_in() {
        let pred = parse_filter("region IN ('us', 'eu', 'asia')").unwrap();
        assert!(matches!(pred, Predicate::In { .. }));
    }

    #[test]
    fn test_parse_complex() {
        let pred =
            parse_filter("date >= '2024-01-01' AND status = 'active' AND region IN ('us', 'eu')")
                .unwrap();
        assert!(matches!(pred, Predicate::And(_)));
    }
}
