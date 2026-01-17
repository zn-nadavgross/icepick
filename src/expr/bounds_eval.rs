//! Column bounds evaluation for file filtering
//!
//! This module provides functions to evaluate predicates against column statistics
//! (min/max bounds) to determine if a file might contain matching rows.

use crate::expr::{ColumnRef, ComparisonOp, Datum, Predicate};
use crate::spec::{PrimitiveType, Schema, Type};
use std::collections::HashMap;

/// Resolve a column reference to a field ID using the schema
fn resolve_column_id(col: &ColumnRef, schema: &Schema) -> Option<i32> {
    match col {
        ColumnRef::Id(id) => Some(*id),
        ColumnRef::Named(name) => schema.as_struct().field_by_name(name).map(|f| f.id()),
    }
}

/// Get the primitive type for a field ID from the schema
fn get_field_type(field_id: i32, schema: &Schema) -> Option<&PrimitiveType> {
    schema.as_struct().field_by_id(field_id).and_then(|f| {
        if let Type::Primitive(p) = f.field_type() {
            Some(p)
        } else {
            None
        }
    })
}

/// Evaluate a predicate against file column bounds
///
/// Returns true if the file MIGHT contain matching rows.
/// Returns false only if we can definitively prove no matches exist based on bounds.
///
/// # Arguments
/// * `predicate` - The predicate to evaluate
/// * `schema` - The table schema for resolving column references
/// * `lower_bounds` - Map of field_id -> lower bound bytes
/// * `upper_bounds` - Map of field_id -> upper bound bytes
/// * `null_counts` - Map of field_id -> null value count
/// * `row_count` - Total number of rows in the file
pub fn evaluate_bounds(
    predicate: &Predicate,
    schema: &Schema,
    lower_bounds: &HashMap<i32, Vec<u8>>,
    upper_bounds: &HashMap<i32, Vec<u8>>,
    null_counts: &HashMap<i32, i64>,
    row_count: i64,
) -> bool {
    match predicate {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,

        Predicate::Comparison { column, op, value } => {
            let Some(field_id) = resolve_column_id(column, schema) else {
                return true;
            };

            let Some(prim_type) = get_field_type(field_id, schema) else {
                return true;
            };

            // Get bounds for this column
            let lower = lower_bounds
                .get(&field_id)
                .and_then(|b| decode_bound(b, prim_type));
            let upper = upper_bounds
                .get(&field_id)
                .and_then(|b| decode_bound(b, prim_type));

            evaluate_comparison(value, *op, lower.as_ref(), upper.as_ref())
        }

        Predicate::IsNull(column) => {
            let Some(field_id) = resolve_column_id(column, schema) else {
                return true;
            };

            // Check null count - if 0, no nulls in file
            match null_counts.get(&field_id) {
                Some(&0) => false,
                _ => true, // Unknown or has nulls
            }
        }

        Predicate::IsNotNull(column) => {
            let Some(field_id) = resolve_column_id(column, schema) else {
                return true;
            };

            // Check if all values are null
            match null_counts.get(&field_id) {
                Some(&count) if count == row_count => false,
                _ => true, // Unknown or has non-nulls
            }
        }

        Predicate::In { column, values } => {
            let Some(field_id) = resolve_column_id(column, schema) else {
                return true;
            };

            let Some(prim_type) = get_field_type(field_id, schema) else {
                return true;
            };

            let lower = lower_bounds
                .get(&field_id)
                .and_then(|b| decode_bound(b, prim_type));
            let upper = upper_bounds
                .get(&field_id)
                .and_then(|b| decode_bound(b, prim_type));

            // If we have bounds, check if any value in the set could be in range
            if let (Some(lower), Some(upper)) = (&lower, &upper) {
                for v in values {
                    // Value is in range if lower <= v <= upper
                    let ge_lower = v
                        .compare(lower)
                        .map(|o| o != std::cmp::Ordering::Less)
                        .unwrap_or(true);
                    let le_upper = v
                        .compare(upper)
                        .map(|o| o != std::cmp::Ordering::Greater)
                        .unwrap_or(true);
                    if ge_lower && le_upper {
                        return true;
                    }
                }
                return false;
            }

            true
        }

        Predicate::And(preds) => preds.iter().all(|p| {
            evaluate_bounds(
                p,
                schema,
                lower_bounds,
                upper_bounds,
                null_counts,
                row_count,
            )
        }),

        Predicate::Or(preds) => preds.iter().any(|p| {
            evaluate_bounds(
                p,
                schema,
                lower_bounds,
                upper_bounds,
                null_counts,
                row_count,
            )
        }),

        Predicate::Not(inner) => {
            // NOT is complex for bounds pruning - we can only prune in specific cases
            // For now, be conservative
            !evaluate_bounds(
                inner,
                schema,
                lower_bounds,
                upper_bounds,
                null_counts,
                row_count,
            )
        }
    }
}

/// Evaluate a comparison predicate against bounds
///
/// Returns true if the file might contain rows matching: column op value
fn evaluate_comparison(
    value: &Datum,
    op: ComparisonOp,
    lower: Option<&Datum>,
    upper: Option<&Datum>,
) -> bool {
    match op {
        // col = X: skip if X < lower OR X > upper
        ComparisonOp::Eq => {
            if let Some(lower) = lower {
                if let Some(ord) = value.compare(lower) {
                    if ord == std::cmp::Ordering::Less {
                        return false; // X < lower, no match possible
                    }
                }
            }
            if let Some(upper) = upper {
                if let Some(ord) = value.compare(upper) {
                    if ord == std::cmp::Ordering::Greater {
                        return false; // X > upper, no match possible
                    }
                }
            }
            true
        }

        // col != X: skip only if lower = upper = X (all values are X)
        ComparisonOp::NotEq => {
            if let (Some(lower), Some(upper)) = (lower, upper) {
                if lower == upper && value == lower {
                    return false;
                }
            }
            true
        }

        // col < X: skip if lower >= X
        ComparisonOp::Lt => {
            if let Some(lower) = lower {
                if let Some(ord) = lower.compare(value) {
                    if ord != std::cmp::Ordering::Less {
                        return false; // lower >= X, all values >= X
                    }
                }
            }
            true
        }

        // col <= X: skip if lower > X
        ComparisonOp::LtEq => {
            if let Some(lower) = lower {
                if let Some(ord) = lower.compare(value) {
                    if ord == std::cmp::Ordering::Greater {
                        return false; // lower > X, all values > X
                    }
                }
            }
            true
        }

        // col > X: skip if upper <= X
        ComparisonOp::Gt => {
            if let Some(upper) = upper {
                if let Some(ord) = upper.compare(value) {
                    if ord != std::cmp::Ordering::Greater {
                        return false; // upper <= X, all values <= X
                    }
                }
            }
            true
        }

        // col >= X: skip if upper < X
        ComparisonOp::GtEq => {
            if let Some(upper) = upper {
                if let Some(ord) = upper.compare(value) {
                    if ord == std::cmp::Ordering::Less {
                        return false; // upper < X, all values < X
                    }
                }
            }
            true
        }
    }
}

/// Decode bound bytes to a Datum
fn decode_bound(bytes: &[u8], prim_type: &PrimitiveType) -> Option<Datum> {
    match prim_type {
        PrimitiveType::Boolean => {
            if bytes.is_empty() {
                return None;
            }
            Some(Datum::Bool(bytes[0] != 0))
        }
        PrimitiveType::Int => {
            let arr: [u8; 4] = bytes.get(..4)?.try_into().ok()?;
            Some(Datum::Int(i32::from_le_bytes(arr)))
        }
        PrimitiveType::Long => {
            let arr: [u8; 8] = bytes.get(..8)?.try_into().ok()?;
            Some(Datum::Long(i64::from_le_bytes(arr)))
        }
        PrimitiveType::Float => {
            let arr: [u8; 4] = bytes.get(..4)?.try_into().ok()?;
            Some(Datum::Float(f32::from_le_bytes(arr)))
        }
        PrimitiveType::Double => {
            let arr: [u8; 8] = bytes.get(..8)?.try_into().ok()?;
            Some(Datum::Double(f64::from_le_bytes(arr)))
        }
        PrimitiveType::Date => {
            let arr: [u8; 4] = bytes.get(..4)?.try_into().ok()?;
            Some(Datum::Date(i32::from_le_bytes(arr)))
        }
        PrimitiveType::Time | PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
            let arr: [u8; 8] = bytes.get(..8)?.try_into().ok()?;
            Some(Datum::Timestamp(i64::from_le_bytes(arr)))
        }
        PrimitiveType::String | PrimitiveType::Uuid => {
            String::from_utf8(bytes.to_vec()).ok().map(Datum::String)
        }
        PrimitiveType::Binary | PrimitiveType::Fixed(_) => Some(Datum::Binary(bytes.to_vec())),
        PrimitiveType::Decimal { .. } => {
            // Decimal requires precision/scale handling, skip for now
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evaluate_eq_in_range() {
        let lower = Some(Datum::Int(10));
        let upper = Some(Datum::Int(100));
        let value = Datum::Int(50);

        assert!(evaluate_comparison(
            &value,
            ComparisonOp::Eq,
            lower.as_ref(),
            upper.as_ref()
        ));
    }

    #[test]
    fn test_evaluate_eq_below_range() {
        let lower = Some(Datum::Int(10));
        let upper = Some(Datum::Int(100));
        let value = Datum::Int(5);

        assert!(!evaluate_comparison(
            &value,
            ComparisonOp::Eq,
            lower.as_ref(),
            upper.as_ref()
        ));
    }

    #[test]
    fn test_evaluate_eq_above_range() {
        let lower = Some(Datum::Int(10));
        let upper = Some(Datum::Int(100));
        let value = Datum::Int(150);

        assert!(!evaluate_comparison(
            &value,
            ComparisonOp::Eq,
            lower.as_ref(),
            upper.as_ref()
        ));
    }

    #[test]
    fn test_evaluate_lt_skip() {
        // col < 5 when lower = 10 -> skip (all values >= 10)
        let lower = Some(Datum::Int(10));
        let upper = Some(Datum::Int(100));
        let value = Datum::Int(5);

        assert!(!evaluate_comparison(
            &value,
            ComparisonOp::Lt,
            lower.as_ref(),
            upper.as_ref()
        ));
    }

    #[test]
    fn test_evaluate_lt_no_skip() {
        // col < 50 when lower = 10 -> might match
        let lower = Some(Datum::Int(10));
        let upper = Some(Datum::Int(100));
        let value = Datum::Int(50);

        assert!(evaluate_comparison(
            &value,
            ComparisonOp::Lt,
            lower.as_ref(),
            upper.as_ref()
        ));
    }

    #[test]
    fn test_evaluate_gt_skip() {
        // col > 150 when upper = 100 -> skip (all values <= 100)
        let lower = Some(Datum::Int(10));
        let upper = Some(Datum::Int(100));
        let value = Datum::Int(150);

        assert!(!evaluate_comparison(
            &value,
            ComparisonOp::Gt,
            lower.as_ref(),
            upper.as_ref()
        ));
    }

    #[test]
    fn test_decode_bound_int() {
        let bytes = 42i32.to_le_bytes().to_vec();
        assert_eq!(
            decode_bound(&bytes, &PrimitiveType::Int),
            Some(Datum::Int(42))
        );
    }

    #[test]
    fn test_decode_bound_string() {
        let bytes = b"hello".to_vec();
        assert_eq!(
            decode_bound(&bytes, &PrimitiveType::String),
            Some(Datum::String("hello".to_string()))
        );
    }
}
