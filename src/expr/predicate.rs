//! Predicate expressions for filtering Iceberg tables

use crate::spec::PrimitiveType;
use std::fmt;

/// A scalar value for comparison
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    /// Boolean value
    Bool(bool),
    /// 32-bit integer
    Int(i32),
    /// 64-bit integer
    Long(i64),
    /// 32-bit float
    Float(f32),
    /// 64-bit float
    Double(f64),
    /// String value
    String(String),
    /// Date as days since Unix epoch
    Date(i32),
    /// Timestamp as microseconds since Unix epoch
    Timestamp(i64),
    /// Binary data
    Binary(Vec<u8>),
}

impl Datum {
    /// Decode a datum from Iceberg binary representation
    ///
    /// This decodes raw bytes into a Datum based on the primitive type.
    /// Used for reading partition values and column bounds from manifest files.
    pub fn from_bytes(bytes: &[u8], prim_type: &PrimitiveType) -> Option<Self> {
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

    /// Check if this datum can be compared with another
    pub fn is_comparable_to(&self, other: &Datum) -> bool {
        use Datum::*;
        matches!(
            (self, other),
            (Bool(_), Bool(_))
                | (Int(_), Int(_))
                | (Int(_), Long(_))
                | (Long(_), Int(_))
                | (Long(_), Long(_))
                | (Float(_), Float(_))
                | (Float(_), Double(_))
                | (Double(_), Float(_))
                | (Double(_), Double(_))
                | (String(_), String(_))
                | (Date(_), Date(_))
                | (Timestamp(_), Timestamp(_))
                | (Binary(_), Binary(_))
        )
    }

    /// Compare two datums, returning ordering if comparable
    pub fn compare(&self, other: &Datum) -> Option<std::cmp::Ordering> {
        use Datum::*;

        match (self, other) {
            (Bool(a), Bool(b)) => Some(a.cmp(b)),
            (Int(a), Int(b)) => Some(a.cmp(b)),
            (Int(a), Long(b)) => Some((*a as i64).cmp(b)),
            (Long(a), Int(b)) => Some(a.cmp(&(*b as i64))),
            (Long(a), Long(b)) => Some(a.cmp(b)),
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Float(a), Double(b)) => (*a as f64).partial_cmp(b),
            (Double(a), Float(b)) => a.partial_cmp(&(*b as f64)),
            (Double(a), Double(b)) => a.partial_cmp(b),
            (String(a), String(b)) => Some(a.cmp(b)),
            (Date(a), Date(b)) => Some(a.cmp(b)),
            (Timestamp(a), Timestamp(b)) => Some(a.cmp(b)),
            (Binary(a), Binary(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Datum::Bool(v) => write!(f, "{}", v),
            Datum::Int(v) => write!(f, "{}", v),
            Datum::Long(v) => write!(f, "{}", v),
            Datum::Float(v) => write!(f, "{}", v),
            Datum::Double(v) => write!(f, "{}", v),
            Datum::String(v) => write!(f, "'{}'", v),
            Datum::Date(v) => write!(f, "DATE({})", v),
            Datum::Timestamp(v) => write!(f, "TIMESTAMP({})", v),
            Datum::Binary(v) => write!(f, "BINARY({} bytes)", v.len()),
        }
    }
}

// Convenience From implementations
impl From<bool> for Datum {
    fn from(v: bool) -> Self {
        Datum::Bool(v)
    }
}

impl From<i32> for Datum {
    fn from(v: i32) -> Self {
        Datum::Int(v)
    }
}

impl From<i64> for Datum {
    fn from(v: i64) -> Self {
        Datum::Long(v)
    }
}

impl From<f32> for Datum {
    fn from(v: f32) -> Self {
        Datum::Float(v)
    }
}

impl From<f64> for Datum {
    fn from(v: f64) -> Self {
        Datum::Double(v)
    }
}

impl From<String> for Datum {
    fn from(v: String) -> Self {
        Datum::String(v)
    }
}

impl From<&str> for Datum {
    fn from(v: &str) -> Self {
        Datum::String(v.to_string())
    }
}

/// Binary comparison operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    /// Equal (=)
    Eq,
    /// Not equal (!=)
    NotEq,
    /// Less than (<)
    Lt,
    /// Less than or equal (<=)
    LtEq,
    /// Greater than (>)
    Gt,
    /// Greater than or equal (>=)
    GtEq,
}

impl ComparisonOp {
    /// Evaluate the operator on an ordering result
    pub fn evaluate(&self, ordering: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering;
        matches!(
            (self, ordering),
            (ComparisonOp::Eq, Ordering::Equal)
                | (ComparisonOp::NotEq, Ordering::Less | Ordering::Greater)
                | (ComparisonOp::Lt, Ordering::Less)
                | (ComparisonOp::LtEq, Ordering::Less | Ordering::Equal)
                | (ComparisonOp::Gt, Ordering::Greater)
                | (ComparisonOp::GtEq, Ordering::Greater | Ordering::Equal)
        )
    }

    /// Get the negation of this operator
    pub fn negate(&self) -> Self {
        match self {
            ComparisonOp::Eq => ComparisonOp::NotEq,
            ComparisonOp::NotEq => ComparisonOp::Eq,
            ComparisonOp::Lt => ComparisonOp::GtEq,
            ComparisonOp::LtEq => ComparisonOp::Gt,
            ComparisonOp::Gt => ComparisonOp::LtEq,
            ComparisonOp::GtEq => ComparisonOp::Lt,
        }
    }
}

impl fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComparisonOp::Eq => write!(f, "="),
            ComparisonOp::NotEq => write!(f, "!="),
            ComparisonOp::Lt => write!(f, "<"),
            ComparisonOp::LtEq => write!(f, "<="),
            ComparisonOp::Gt => write!(f, ">"),
            ComparisonOp::GtEq => write!(f, ">="),
        }
    }
}

/// A reference to a column
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnRef {
    /// Reference by column name
    Named(String),
    /// Reference by field ID
    Id(i32),
}

impl ColumnRef {
    /// Create a named column reference
    pub fn named(name: impl Into<String>) -> Self {
        ColumnRef::Named(name.into())
    }

    /// Create a column reference by ID
    pub fn id(id: i32) -> Self {
        ColumnRef::Id(id)
    }

    /// Get the column name if this is a named reference
    pub fn name(&self) -> Option<&str> {
        match self {
            ColumnRef::Named(n) => Some(n),
            ColumnRef::Id(_) => None,
        }
    }
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnRef::Named(n) => write!(f, "{}", n),
            ColumnRef::Id(id) => write!(f, "#{}", id),
        }
    }
}

impl From<String> for ColumnRef {
    fn from(v: String) -> Self {
        ColumnRef::Named(v)
    }
}

impl From<&str> for ColumnRef {
    fn from(v: &str) -> Self {
        ColumnRef::Named(v.to_string())
    }
}

impl From<i32> for ColumnRef {
    fn from(v: i32) -> Self {
        ColumnRef::Id(v)
    }
}

/// A predicate expression for filtering
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Always evaluates to true
    AlwaysTrue,
    /// Always evaluates to false
    AlwaysFalse,
    /// Column comparison: column op value
    Comparison {
        /// Column reference
        column: ColumnRef,
        /// Comparison operator
        op: ComparisonOp,
        /// Value to compare against
        value: Datum,
    },
    /// Column IS NULL
    IsNull(ColumnRef),
    /// Column IS NOT NULL
    IsNotNull(ColumnRef),
    /// Column IN (values...)
    In {
        /// Column reference
        column: ColumnRef,
        /// Set of values
        values: Vec<Datum>,
    },
    /// Logical AND of predicates
    And(Vec<Predicate>),
    /// Logical OR of predicates
    Or(Vec<Predicate>),
    /// Logical NOT of predicate
    Not(Box<Predicate>),
}

impl Predicate {
    /// Create an AND of multiple predicates
    pub fn and(predicates: impl IntoIterator<Item = Predicate>) -> Self {
        let preds: Vec<_> = predicates.into_iter().collect();
        if preds.is_empty() {
            Predicate::AlwaysTrue
        } else if preds.len() == 1 {
            preds.into_iter().next().unwrap()
        } else {
            Predicate::And(preds)
        }
    }

    /// Create an OR of multiple predicates
    pub fn or(predicates: impl IntoIterator<Item = Predicate>) -> Self {
        let preds: Vec<_> = predicates.into_iter().collect();
        if preds.is_empty() {
            Predicate::AlwaysFalse
        } else if preds.len() == 1 {
            preds.into_iter().next().unwrap()
        } else {
            Predicate::Or(preds)
        }
    }

    /// Create a NOT predicate (negation)
    pub fn negate(predicate: Predicate) -> Self {
        match predicate {
            Predicate::AlwaysTrue => Predicate::AlwaysFalse,
            Predicate::AlwaysFalse => Predicate::AlwaysTrue,
            Predicate::Not(inner) => *inner,
            other => Predicate::Not(Box::new(other)),
        }
    }

    /// Create an equality comparison
    pub fn eq(column: impl Into<ColumnRef>, value: impl Into<Datum>) -> Self {
        Predicate::Comparison {
            column: column.into(),
            op: ComparisonOp::Eq,
            value: value.into(),
        }
    }

    /// Create a not-equal comparison
    pub fn not_eq(column: impl Into<ColumnRef>, value: impl Into<Datum>) -> Self {
        Predicate::Comparison {
            column: column.into(),
            op: ComparisonOp::NotEq,
            value: value.into(),
        }
    }

    /// Create a less-than comparison
    pub fn lt(column: impl Into<ColumnRef>, value: impl Into<Datum>) -> Self {
        Predicate::Comparison {
            column: column.into(),
            op: ComparisonOp::Lt,
            value: value.into(),
        }
    }

    /// Create a less-than-or-equal comparison
    pub fn lt_eq(column: impl Into<ColumnRef>, value: impl Into<Datum>) -> Self {
        Predicate::Comparison {
            column: column.into(),
            op: ComparisonOp::LtEq,
            value: value.into(),
        }
    }

    /// Create a greater-than comparison
    pub fn gt(column: impl Into<ColumnRef>, value: impl Into<Datum>) -> Self {
        Predicate::Comparison {
            column: column.into(),
            op: ComparisonOp::Gt,
            value: value.into(),
        }
    }

    /// Create a greater-than-or-equal comparison
    pub fn gt_eq(column: impl Into<ColumnRef>, value: impl Into<Datum>) -> Self {
        Predicate::Comparison {
            column: column.into(),
            op: ComparisonOp::GtEq,
            value: value.into(),
        }
    }

    /// Create an IS NULL predicate
    pub fn is_null(column: impl Into<ColumnRef>) -> Self {
        Predicate::IsNull(column.into())
    }

    /// Create an IS NOT NULL predicate
    pub fn is_not_null(column: impl Into<ColumnRef>) -> Self {
        Predicate::IsNotNull(column.into())
    }

    /// Create an IN predicate
    pub fn is_in(column: impl Into<ColumnRef>, values: impl IntoIterator<Item = Datum>) -> Self {
        Predicate::In {
            column: column.into(),
            values: values.into_iter().collect(),
        }
    }

    /// Check if this predicate is always true
    pub fn is_always_true(&self) -> bool {
        matches!(self, Predicate::AlwaysTrue)
    }

    /// Check if this predicate is always false
    pub fn is_always_false(&self) -> bool {
        matches!(self, Predicate::AlwaysFalse)
    }

    /// Get all column references in this predicate
    pub fn columns(&self) -> Vec<&ColumnRef> {
        match self {
            Predicate::AlwaysTrue | Predicate::AlwaysFalse => vec![],
            Predicate::Comparison { column, .. } => vec![column],
            Predicate::IsNull(column) | Predicate::IsNotNull(column) => vec![column],
            Predicate::In { column, .. } => vec![column],
            Predicate::And(preds) | Predicate::Or(preds) => {
                preds.iter().flat_map(|p| p.columns()).collect()
            }
            Predicate::Not(pred) => pred.columns(),
        }
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Predicate::AlwaysTrue => write!(f, "TRUE"),
            Predicate::AlwaysFalse => write!(f, "FALSE"),
            Predicate::Comparison { column, op, value } => {
                write!(f, "{} {} {}", column, op, value)
            }
            Predicate::IsNull(column) => write!(f, "{} IS NULL", column),
            Predicate::IsNotNull(column) => write!(f, "{} IS NOT NULL", column),
            Predicate::In { column, values } => {
                write!(f, "{} IN (", column)?;
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, ")")
            }
            Predicate::And(preds) => {
                write!(f, "(")?;
                for (i, p) in preds.iter().enumerate() {
                    if i > 0 {
                        write!(f, " AND ")?;
                    }
                    write!(f, "{}", p)?;
                }
                write!(f, ")")
            }
            Predicate::Or(preds) => {
                write!(f, "(")?;
                for (i, p) in preds.iter().enumerate() {
                    if i > 0 {
                        write!(f, " OR ")?;
                    }
                    write!(f, "{}", p)?;
                }
                write!(f, ")")
            }
            Predicate::Not(pred) => write!(f, "NOT {}", pred),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datum_comparison() {
        assert_eq!(
            Datum::Int(5).compare(&Datum::Int(10)),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            Datum::Int(10).compare(&Datum::Long(5)),
            Some(std::cmp::Ordering::Greater)
        );
        assert_eq!(
            Datum::String("abc".into()).compare(&Datum::String("def".into())),
            Some(std::cmp::Ordering::Less)
        );
        // Incompatible types
        assert_eq!(Datum::Int(5).compare(&Datum::String("5".into())), None);
    }

    #[test]
    fn test_predicate_builders() {
        let p = Predicate::eq("name", "Alice");
        assert!(matches!(
            p,
            Predicate::Comparison {
                op: ComparisonOp::Eq,
                ..
            }
        ));

        let p = Predicate::and([Predicate::gt("age", 18), Predicate::lt("age", 65)]);
        assert!(matches!(p, Predicate::And(_)));
    }

    #[test]
    fn test_predicate_display() {
        let p = Predicate::and([
            Predicate::eq("status", "active"),
            Predicate::gt_eq("age", 21),
        ]);
        assert_eq!(p.to_string(), "(status = 'active' AND age >= 21)");
    }

    #[test]
    fn test_not_simplification() {
        assert!(matches!(
            Predicate::negate(Predicate::AlwaysTrue),
            Predicate::AlwaysFalse
        ));
        assert!(matches!(
            Predicate::negate(Predicate::AlwaysFalse),
            Predicate::AlwaysTrue
        ));

        // Double negation
        let p = Predicate::negate(Predicate::negate(Predicate::eq("x", 1)));
        assert!(matches!(p, Predicate::Comparison { .. }));
    }

    #[test]
    fn test_columns() {
        let p = Predicate::and([
            Predicate::eq("name", "test"),
            Predicate::gt("age", 18),
            Predicate::is_not_null("email"),
        ]);
        let cols = p.columns();
        assert_eq!(cols.len(), 3);
    }
}
