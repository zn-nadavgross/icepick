//! Partition predicate evaluation for file filtering
//!
//! This module provides functions to evaluate predicates against partition values
//! to determine if a file might contain matching rows.

use crate::expr::{ColumnRef, ComparisonOp, Datum, Predicate};
use crate::spec::{PartitionField, PartitionSpec, PrimitiveType, Schema, Type};
use std::collections::HashMap;

/// Iceberg partition transforms
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transform {
    /// Identity transform (value unchanged)
    Identity,
    /// Year transform for date/timestamp
    Year,
    /// Month transform for date/timestamp
    Month,
    /// Day transform for date/timestamp
    Day,
    /// Hour transform for timestamp
    Hour,
    /// Bucket hash transform
    Bucket(u32),
    /// Truncate transform
    Truncate(u32),
    /// Void transform (always null)
    Void,
}

impl Transform {
    /// Parse a transform string from Iceberg metadata
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.to_lowercase();
        if s == "identity" {
            return Some(Transform::Identity);
        }
        if s == "year" {
            return Some(Transform::Year);
        }
        if s == "month" {
            return Some(Transform::Month);
        }
        if s == "day" {
            return Some(Transform::Day);
        }
        if s == "hour" {
            return Some(Transform::Hour);
        }
        if s == "void" {
            return Some(Transform::Void);
        }
        if let Some(n) = s.strip_prefix("bucket[").and_then(|s| s.strip_suffix(']')) {
            if let Ok(num) = n.parse::<u32>() {
                return Some(Transform::Bucket(num));
            }
        }
        if let Some(n) = s.strip_prefix("truncate[").and_then(|s| s.strip_suffix(']')) {
            if let Ok(num) = n.parse::<u32>() {
                return Some(Transform::Truncate(num));
            }
        }
        None
    }
}

/// Information about how a source column maps to a partition field
#[derive(Debug, Clone)]
pub struct PartitionMapping {
    /// Source column field ID
    pub source_id: i32,
    /// Partition field ID (used as key in partition values map)
    pub partition_field_id: i32,
    /// Transform applied to source column
    pub transform: Transform,
}

/// Build a mapping from source column IDs to partition fields
pub fn build_partition_mapping(spec: &PartitionSpec) -> Vec<PartitionMapping> {
    spec.fields()
        .iter()
        .filter_map(|f| {
            let transform = Transform::parse(f.transform())?;
            Some(PartitionMapping {
                source_id: f.source_id(),
                partition_field_id: f.field_id(),
                transform,
            })
        })
        .collect()
}

/// Resolve a column reference to a field ID using the schema
pub fn resolve_column_id(col: &ColumnRef, schema: &Schema) -> Option<i32> {
    match col {
        ColumnRef::Id(id) => Some(*id),
        ColumnRef::Named(name) => schema.as_struct().field_by_name(name).map(|f| f.id()),
    }
}

/// Project a predicate to partition columns
///
/// Returns a new predicate that can be evaluated against partition values.
/// If a column in the predicate is not a partition column, it is replaced with AlwaysTrue.
pub fn project_to_partition(
    predicate: &Predicate,
    schema: &Schema,
    spec: &PartitionSpec,
) -> Predicate {
    let mapping = build_partition_mapping(spec);

    project_predicate_impl(predicate, schema, &mapping)
}

fn project_predicate_impl(
    predicate: &Predicate,
    schema: &Schema,
    mapping: &[PartitionMapping],
) -> Predicate {
    match predicate {
        Predicate::AlwaysTrue => Predicate::AlwaysTrue,
        Predicate::AlwaysFalse => Predicate::AlwaysFalse,

        Predicate::Comparison { column, op, value } => {
            if let Some(field_id) = resolve_column_id(column, schema) {
                // Find partition mapping for this source column
                if let Some(pm) = mapping.iter().find(|m| m.source_id == field_id) {
                    // Transform the value based on the partition transform
                    if let Some(transformed_value) =
                        transform_value_for_partition(value, pm.transform)
                    {
                        // For non-identity transforms, some operations can't be pushed down
                        let can_push = match pm.transform {
                            Transform::Identity => true,
                            Transform::Year | Transform::Month | Transform::Day => {
                                // Range predicates can be pushed for temporal transforms
                                // but need careful handling of boundaries
                                matches!(
                                    op,
                                    ComparisonOp::Eq | ComparisonOp::Lt | ComparisonOp::GtEq
                                )
                            }
                            Transform::Hour => matches!(op, ComparisonOp::Eq),
                            Transform::Bucket(_) => matches!(op, ComparisonOp::Eq),
                            Transform::Truncate(_) => matches!(op, ComparisonOp::Eq),
                            Transform::Void => false,
                        };

                        if can_push {
                            return Predicate::Comparison {
                                column: ColumnRef::Id(pm.partition_field_id),
                                op: *op,
                                value: transformed_value,
                            };
                        }
                    }
                }
            }
            // Cannot project to partition - return true (file might contain matches)
            Predicate::AlwaysTrue
        }

        Predicate::IsNull(column) => {
            if let Some(field_id) = resolve_column_id(column, schema) {
                if let Some(pm) = mapping.iter().find(|m| m.source_id == field_id) {
                    // IS NULL can always be pushed to partition
                    return Predicate::IsNull(ColumnRef::Id(pm.partition_field_id));
                }
            }
            Predicate::AlwaysTrue
        }

        Predicate::IsNotNull(column) => {
            if let Some(field_id) = resolve_column_id(column, schema) {
                if let Some(pm) = mapping.iter().find(|m| m.source_id == field_id) {
                    return Predicate::IsNotNull(ColumnRef::Id(pm.partition_field_id));
                }
            }
            Predicate::AlwaysTrue
        }

        Predicate::In { column, values } => {
            if let Some(field_id) = resolve_column_id(column, schema) {
                if let Some(pm) = mapping.iter().find(|m| m.source_id == field_id) {
                    // Only identity transform supports IN pushdown reliably
                    if pm.transform == Transform::Identity {
                        return Predicate::In {
                            column: ColumnRef::Id(pm.partition_field_id),
                            values: values.clone(),
                        };
                    }
                }
            }
            Predicate::AlwaysTrue
        }

        Predicate::And(preds) => {
            let projected: Vec<_> = preds
                .iter()
                .map(|p| project_predicate_impl(p, schema, mapping))
                .collect();
            Predicate::and(projected)
        }

        Predicate::Or(preds) => {
            let projected: Vec<_> = preds
                .iter()
                .map(|p| project_predicate_impl(p, schema, mapping))
                .collect();
            // If any branch is always true, the whole OR is always true
            if projected.iter().any(|p| p.is_always_true()) {
                Predicate::AlwaysTrue
            } else {
                Predicate::or(projected)
            }
        }

        Predicate::Not(_) => {
            // NOT is tricky for partition pruning - we can't simply negate
            // because partition values might not uniquely identify rows
            Predicate::AlwaysTrue
        }
    }
}

/// Transform a datum value based on the partition transform
fn transform_value_for_partition(value: &Datum, transform: Transform) -> Option<Datum> {
    match transform {
        Transform::Identity => Some(value.clone()),

        Transform::Year => match value {
            // Date: days since epoch -> year
            Datum::Date(days) => {
                let year = days_to_year(*days);
                Some(Datum::Int(year))
            }
            // Timestamp: microseconds since epoch -> year
            Datum::Timestamp(micros) => {
                let days = (*micros / 86_400_000_000) as i32;
                let year = days_to_year(days);
                Some(Datum::Int(year))
            }
            // String date like "2024-01-15"
            Datum::String(s) => parse_date_year(s).map(Datum::Int),
            _ => None,
        },

        Transform::Month => match value {
            Datum::Date(days) => {
                let (year, month) = days_to_year_month(*days);
                Some(Datum::Int(year * 12 + month - 1))
            }
            Datum::Timestamp(micros) => {
                let days = (*micros / 86_400_000_000) as i32;
                let (year, month) = days_to_year_month(days);
                Some(Datum::Int(year * 12 + month - 1))
            }
            Datum::String(s) => {
                parse_date_year_month(s).map(|(year, month)| Datum::Int(year * 12 + month - 1))
            }
            _ => None,
        },

        Transform::Day => match value {
            Datum::Date(days) => Some(Datum::Int(*days)),
            Datum::Timestamp(micros) => {
                let days = (*micros / 86_400_000_000) as i32;
                Some(Datum::Int(days))
            }
            Datum::String(s) => parse_date_to_days(s).map(Datum::Int),
            _ => None,
        },

        Transform::Hour => match value {
            Datum::Timestamp(micros) => {
                let hours = (*micros / 3_600_000_000) as i32;
                Some(Datum::Int(hours))
            }
            _ => None,
        },

        Transform::Bucket(_) => {
            // Bucket transform requires computing hash of the value
            // For simplicity, we don't transform - predicate will be AlwaysTrue
            None
        }

        Transform::Truncate(width) => {
            match value {
                Datum::Int(v) => Some(Datum::Int((v / width as i32) * width as i32)),
                Datum::Long(v) => Some(Datum::Long((v / width as i64) * width as i64)),
                Datum::String(s) => {
                    let truncated: String = s.chars().take(width as usize).collect();
                    Some(Datum::String(truncated))
                }
                _ => None,
            }
        }

        Transform::Void => None,
    }
}

/// Evaluate a projected predicate against partition values
///
/// Returns true if the partition MIGHT contain matching rows.
/// Returns false only if we can definitively prove no matches exist.
pub fn evaluate_partition(
    predicate: &Predicate,
    partition_values: &HashMap<i32, Vec<u8>>,
    partition_fields: &[PartitionField],
    schema: &Schema,
) -> bool {
    match predicate {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,

        Predicate::Comparison { column, op, value } => {
            let field_id = match column {
                ColumnRef::Id(id) => *id,
                ColumnRef::Named(_) => return true, // Can't evaluate named refs against partition
            };

            // Find the partition field to get its type
            let field_type = partition_fields
                .iter()
                .find(|f| f.field_id() == field_id)
                .and_then(|pf| {
                    // Get source field type from schema
                    schema.as_struct().field_by_id(pf.source_id())
                })
                .map(|f| f.field_type());

            // Get partition value bytes
            let Some(bytes) = partition_values.get(&field_id) else {
                // No value means null partition - only match IS NULL predicates
                return true;
            };

            // Decode and compare
            if let Some(partition_datum) = decode_partition_value(bytes, field_type) {
                if let Some(ordering) = partition_datum.compare(value) {
                    return op.evaluate(ordering);
                }
            }

            // Can't evaluate - assume might match
            true
        }

        Predicate::IsNull(column) => {
            let field_id = match column {
                ColumnRef::Id(id) => *id,
                ColumnRef::Named(_) => return true,
            };

            // Partition is null if not in the map
            !partition_values.contains_key(&field_id)
        }

        Predicate::IsNotNull(column) => {
            let field_id = match column {
                ColumnRef::Id(id) => *id,
                ColumnRef::Named(_) => return true,
            };

            partition_values.contains_key(&field_id)
        }

        Predicate::In { column, values } => {
            let field_id = match column {
                ColumnRef::Id(id) => *id,
                ColumnRef::Named(_) => return true,
            };

            let field_type = partition_fields
                .iter()
                .find(|f| f.field_id() == field_id)
                .and_then(|pf| schema.as_struct().field_by_id(pf.source_id()))
                .map(|f| f.field_type());

            let Some(bytes) = partition_values.get(&field_id) else {
                return true;
            };

            if let Some(partition_datum) = decode_partition_value(bytes, field_type) {
                // Check if partition value is in the set
                for v in values {
                    if partition_datum.compare(v) == Some(std::cmp::Ordering::Equal) {
                        return true;
                    }
                }
                return false;
            }

            true
        }

        Predicate::And(preds) => preds
            .iter()
            .all(|p| evaluate_partition(p, partition_values, partition_fields, schema)),

        Predicate::Or(preds) => preds
            .iter()
            .any(|p| evaluate_partition(p, partition_values, partition_fields, schema)),

        Predicate::Not(inner) => !evaluate_partition(inner, partition_values, partition_fields, schema),
    }
}

/// Decode raw bytes to a Datum based on the field type
fn decode_partition_value(bytes: &[u8], field_type: Option<&Type>) -> Option<Datum> {
    let typ = field_type?;

    match typ {
        Type::Primitive(prim) => decode_primitive(bytes, prim),
        _ => None,
    }
}

fn decode_primitive(bytes: &[u8], prim: &PrimitiveType) -> Option<Datum> {
    match prim {
        PrimitiveType::Boolean => {
            if bytes.is_empty() {
                return None;
            }
            Some(Datum::Bool(bytes[0] != 0))
        }
        PrimitiveType::Int => {
            if bytes.len() < 4 {
                return None;
            }
            let arr: [u8; 4] = bytes[..4].try_into().ok()?;
            Some(Datum::Int(i32::from_le_bytes(arr)))
        }
        PrimitiveType::Long => {
            if bytes.len() < 8 {
                return None;
            }
            let arr: [u8; 8] = bytes[..8].try_into().ok()?;
            Some(Datum::Long(i64::from_le_bytes(arr)))
        }
        PrimitiveType::Float => {
            if bytes.len() < 4 {
                return None;
            }
            let arr: [u8; 4] = bytes[..4].try_into().ok()?;
            Some(Datum::Float(f32::from_le_bytes(arr)))
        }
        PrimitiveType::Double => {
            if bytes.len() < 8 {
                return None;
            }
            let arr: [u8; 8] = bytes[..8].try_into().ok()?;
            Some(Datum::Double(f64::from_le_bytes(arr)))
        }
        PrimitiveType::Date => {
            if bytes.len() < 4 {
                return None;
            }
            let arr: [u8; 4] = bytes[..4].try_into().ok()?;
            Some(Datum::Date(i32::from_le_bytes(arr)))
        }
        PrimitiveType::Time | PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
            if bytes.len() < 8 {
                return None;
            }
            let arr: [u8; 8] = bytes[..8].try_into().ok()?;
            Some(Datum::Timestamp(i64::from_le_bytes(arr)))
        }
        PrimitiveType::String | PrimitiveType::Uuid => {
            String::from_utf8(bytes.to_vec())
                .ok()
                .map(Datum::String)
        }
        PrimitiveType::Binary | PrimitiveType::Fixed(_) => Some(Datum::Binary(bytes.to_vec())),
        PrimitiveType::Decimal { .. } => {
            // Decimal decoding is complex, skip for now
            None
        }
    }
}

// Date utility functions

/// Convert days since Unix epoch to year (Iceberg uses 1970-01-01 as epoch)
fn days_to_year(days: i32) -> i32 {
    // Approximate calculation
    let approx_years = days / 365;
    let year = 1970 + approx_years;

    // Adjust for leap years and edge cases
    let year_start = year_to_days(year);
    if days < year_start {
        year - 1
    } else if days >= year_to_days(year + 1) {
        year + 1
    } else {
        year
    }
}

/// Convert days since Unix epoch to (year, month) where month is 1-12
fn days_to_year_month(days: i32) -> (i32, i32) {
    let year = days_to_year(days);
    let year_start = year_to_days(year);
    let day_of_year = days - year_start;

    let is_leap = is_leap_year(year);
    let days_in_months: [i32; 12] = if is_leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut cumulative = 0;
    for (i, &days_in_month) in days_in_months.iter().enumerate() {
        if day_of_year < cumulative + days_in_month {
            return (year, (i + 1) as i32);
        }
        cumulative += days_in_month;
    }

    (year, 12)
}

/// Convert year to days since Unix epoch (Jan 1 of that year)
fn year_to_days(year: i32) -> i32 {
    let y = year - 1970;
    if y >= 0 {
        y * 365 + (y + 1) / 4 - (y + 69) / 100 + (y + 369) / 400
    } else {
        y * 365 + y / 4 - (y - 31) / 100 + (y - 31) / 400
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Parse a date string like "2024-01-15" to year
fn parse_date_year(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if !parts.is_empty() {
        parts[0].parse().ok()
    } else {
        None
    }
}

/// Parse a date string like "2024-01-15" to (year, month)
fn parse_date_year_month(s: &str) -> Option<(i32, i32)> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() >= 2 {
        let year = parts[0].parse().ok()?;
        let month = parts[1].parse().ok()?;
        Some((year, month))
    } else {
        None
    }
}

/// Parse a date string like "2024-01-15" to days since epoch
fn parse_date_to_days(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() >= 3 {
        let year: i32 = parts[0].parse().ok()?;
        let month: i32 = parts[1].parse().ok()?;
        let day: i32 = parts[2].parse().ok()?;

        let year_days = year_to_days(year);
        let is_leap = is_leap_year(year);
        let days_before_month: [i32; 12] = if is_leap {
            [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
        } else {
            [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
        };

        if (1..=12).contains(&month) {
            Some(year_days + days_before_month[(month - 1) as usize] + day - 1)
        } else {
            None
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transform_parse() {
        assert_eq!(Transform::parse("identity"), Some(Transform::Identity));
        assert_eq!(Transform::parse("Identity"), Some(Transform::Identity));
        assert_eq!(Transform::parse("year"), Some(Transform::Year));
        assert_eq!(Transform::parse("bucket[16]"), Some(Transform::Bucket(16)));
        assert_eq!(
            Transform::parse("truncate[100]"),
            Some(Transform::Truncate(100))
        );
        assert_eq!(Transform::parse("void"), Some(Transform::Void));
    }

    #[test]
    fn test_days_to_year() {
        // 1970-01-01 is day 0
        assert_eq!(days_to_year(0), 1970);
        // 2024-01-01 is approximately day 19724
        let days_2024 = year_to_days(2024);
        assert_eq!(days_to_year(days_2024), 2024);
    }

    #[test]
    fn test_parse_date_to_days() {
        let days = parse_date_to_days("2024-01-15").unwrap();
        let (year, month) = days_to_year_month(days);
        assert_eq!(year, 2024);
        assert_eq!(month, 1);
    }

    #[test]
    fn test_decode_primitive() {
        // Int
        let bytes = 42i32.to_le_bytes().to_vec();
        assert_eq!(
            decode_primitive(&bytes, &PrimitiveType::Int),
            Some(Datum::Int(42))
        );

        // String
        let bytes = b"hello".to_vec();
        assert_eq!(
            decode_primitive(&bytes, &PrimitiveType::String),
            Some(Datum::String("hello".to_string()))
        );
    }
}
