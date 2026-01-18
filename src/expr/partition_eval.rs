//! Partition predicate evaluation for file filtering
//!
//! This module provides functions to evaluate predicates against partition values
//! to determine if a file might contain matching rows.

use super::date::{
    days_to_year, days_to_year_month, parse_date_to_days, parse_date_year, parse_date_year_month,
};
use crate::expr::{ColumnRef, ComparisonOp, Datum, Predicate};
use crate::spec::{PartitionField, PartitionSpec, Schema, Type};
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
        if let Some(n) = s
            .strip_prefix("truncate[")
            .and_then(|s| s.strip_suffix(']'))
        {
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
                            // partition_field_id comes from Iceberg metadata and should be valid
                            // If it's invalid, this indicates corrupted metadata
                            let column = ColumnRef::id(pm.partition_field_id)
                                .expect("partition field ID from metadata should be positive");
                            return Predicate::Comparison {
                                column,
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
                    let col = ColumnRef::id(pm.partition_field_id)
                        .expect("partition field ID from metadata should be positive");
                    return Predicate::IsNull(col);
                }
            }
            Predicate::AlwaysTrue
        }

        Predicate::IsNotNull(column) => {
            if let Some(field_id) = resolve_column_id(column, schema) {
                if let Some(pm) = mapping.iter().find(|m| m.source_id == field_id) {
                    let col = ColumnRef::id(pm.partition_field_id)
                        .expect("partition field ID from metadata should be positive");
                    return Predicate::IsNotNull(col);
                }
            }
            Predicate::AlwaysTrue
        }

        Predicate::In { column, values } => {
            if let Some(field_id) = resolve_column_id(column, schema) {
                if let Some(pm) = mapping.iter().find(|m| m.source_id == field_id) {
                    // Only identity transform supports IN pushdown reliably
                    if pm.transform == Transform::Identity {
                        let col = ColumnRef::id(pm.partition_field_id)
                            .expect("partition field ID from metadata should be positive");
                        return Predicate::In {
                            column: col,
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
                // Use checked arithmetic to prevent overflow for extreme year values
                year.checked_mul(12)
                    .and_then(|v| v.checked_add(month - 1))
                    .map(Datum::Int)
            }
            Datum::Timestamp(micros) => {
                let days = (*micros / 86_400_000_000) as i32;
                let (year, month) = days_to_year_month(days);
                // Use checked arithmetic to prevent overflow for extreme year values
                year.checked_mul(12)
                    .and_then(|v| v.checked_add(month - 1))
                    .map(Datum::Int)
            }
            Datum::String(s) => parse_date_year_month(s).and_then(|(year, month)| {
                // Use checked arithmetic to prevent overflow for extreme year values
                year.checked_mul(12)
                    .and_then(|v| v.checked_add(month - 1))
                    .map(Datum::Int)
            }),
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

        Transform::Truncate(width) => match value {
            Datum::Int(v) => Some(Datum::Int((v / width as i32) * width as i32)),
            Datum::Long(v) => Some(Datum::Long((v / width as i64) * width as i64)),
            Datum::String(s) => {
                let truncated: String = s.chars().take(width as usize).collect();
                Some(Datum::String(truncated))
            }
            _ => None,
        },

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

        Predicate::Not(inner) => {
            !evaluate_partition(inner, partition_values, partition_fields, schema)
        }
    }
}

/// Decode raw bytes to a Datum based on the field type
fn decode_partition_value(bytes: &[u8], field_type: Option<&Type>) -> Option<Datum> {
    let typ = field_type?;

    match typ {
        Type::Primitive(prim) => Datum::from_bytes(bytes, prim),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::date::year_to_days;
    use super::*;
    use crate::PrimitiveType;

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
            Datum::from_bytes(&bytes, &PrimitiveType::Int),
            Some(Datum::Int(42))
        );

        // String
        let bytes = b"hello".to_vec();
        assert_eq!(
            Datum::from_bytes(&bytes, &PrimitiveType::String),
            Some(Datum::String("hello".to_string()))
        );
    }
}
