//! Predicate expressions for filtering Iceberg tables
use crate::spec::PrimitiveType;
use std::fmt;

/// A scalar value for comparison (Bool, Int, Long, Float, Double, String, Date, Timestamp, Binary)
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Bool(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    Date(i32),
    Timestamp(i64),
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
macro_rules! impl_from_for_datum {
    ($t:ty, $variant:ident) => {
        impl From<$t> for Datum {
            fn from(v: $t) -> Self {
                Datum::$variant(v)
            }
        }
    };
}
impl_from_for_datum!(bool, Bool);
impl_from_for_datum!(i32, Int);
impl_from_for_datum!(i64, Long);
impl_from_for_datum!(f32, Float);
impl_from_for_datum!(f64, Double);
impl_from_for_datum!(String, String);
impl From<&str> for Datum {
    fn from(v: &str) -> Self {
        Datum::String(v.to_string())
    }
}

/// Binary comparison operators (Eq, NotEq, Lt, LtEq, Gt, GtEq)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
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
    /// Create a named column reference with validation
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidInput` if the name is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use icepick::expr::ColumnRef;
    ///
    /// let col = ColumnRef::named("age").unwrap();
    /// assert_eq!(col.name(), Some("age"));
    ///
    /// let empty = ColumnRef::named("");
    /// assert!(empty.is_err());
    /// ```
    pub fn named(name: impl Into<String>) -> crate::error::Result<Self> {
        let name_str = name.into();
        if name_str.is_empty() {
            return Err(crate::error::Error::invalid_input(
                "Column name cannot be empty",
            ));
        }
        Ok(ColumnRef::Named(name_str))
    }

    /// Create a column reference by field ID with validation
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidInput` if the ID is not positive (must be > 0).
    /// Field IDs in the Iceberg spec must be positive integers.
    ///
    /// # Examples
    ///
    /// ```
    /// use icepick::expr::ColumnRef;
    ///
    /// let col = ColumnRef::id(42).unwrap();
    ///
    /// let negative = ColumnRef::id(-1);
    /// assert!(negative.is_err());
    ///
    /// let zero = ColumnRef::id(0);
    /// assert!(zero.is_err());
    /// ```
    pub fn id(id: i32) -> crate::error::Result<Self> {
        if id <= 0 {
            return Err(crate::error::Error::invalid_input(format!(
                "Field ID must be positive, got {}",
                id
            )));
        }
        Ok(ColumnRef::Id(id))
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
    /// Convert a String to a ColumnRef::Named variant
    ///
    /// # Panics
    ///
    /// This conversion does not validate the input. Empty strings will create
    /// invalid column references. Use `ColumnRef::named()` for validated construction.
    fn from(v: String) -> Self {
        ColumnRef::Named(v)
    }
}

impl From<&str> for ColumnRef {
    /// Convert a string slice to a ColumnRef::Named variant
    ///
    /// # Panics
    ///
    /// This conversion does not validate the input. Empty strings will create
    /// invalid column references. Use `ColumnRef::named()` for validated construction.
    fn from(v: &str) -> Self {
        ColumnRef::Named(v.to_string())
    }
}

impl From<i32> for ColumnRef {
    /// Convert an i32 to a ColumnRef::Id variant
    ///
    /// # Panics
    ///
    /// This conversion does not validate the input. Non-positive IDs will create
    /// invalid column references. Use `ColumnRef::id()` for validated construction.
    fn from(v: i32) -> Self {
        ColumnRef::Id(v)
    }
}

/// Predicate expression for filtering (AlwaysTrue, AlwaysFalse, Comparison, IsNull, IsNotNull, In, And, Or, Not)
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    AlwaysTrue,
    AlwaysFalse,
    Comparison {
        column: ColumnRef,
        op: ComparisonOp,
        value: Datum,
    },
    IsNull(ColumnRef),
    IsNotNull(ColumnRef),
    In {
        column: ColumnRef,
        values: Vec<Datum>,
    },
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
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

    #[test]
    fn test_column_ref_named_validation() {
        // Valid name should succeed
        let col = ColumnRef::named("age");
        assert!(col.is_ok());
        assert_eq!(col.unwrap().name(), Some("age"));

        // Empty string should fail
        let empty = ColumnRef::named("");
        assert!(empty.is_err());
        assert!(empty
            .unwrap_err()
            .to_string()
            .contains("Column name cannot be empty"));

        // Empty String should also fail
        let empty_string = ColumnRef::named(String::new());
        assert!(empty_string.is_err());
    }

    #[test]
    fn test_column_ref_id_validation() {
        // Valid positive ID should succeed
        let col = ColumnRef::id(1);
        assert!(col.is_ok());
        assert!(matches!(col.unwrap(), ColumnRef::Id(1)));

        let col42 = ColumnRef::id(42);
        assert!(col42.is_ok());
        assert!(matches!(col42.unwrap(), ColumnRef::Id(42)));

        // Zero should fail
        let zero = ColumnRef::id(0);
        assert!(zero.is_err());
        assert!(zero.unwrap_err().to_string().contains("must be positive"));

        // Negative IDs should fail
        let negative = ColumnRef::id(-1);
        assert!(negative.is_err());
        assert!(negative
            .unwrap_err()
            .to_string()
            .contains("must be positive"));

        let very_negative = ColumnRef::id(-999);
        assert!(very_negative.is_err());
        assert!(very_negative
            .unwrap_err()
            .to_string()
            .contains("must be positive"));
    }

    #[test]
    fn test_column_ref_from_impls_no_validation() {
        // From impls should still work but don't validate
        // These document the unsafe behavior

        // String conversion - allows empty (but creates invalid ref)
        let _col_from_str: ColumnRef = "valid_name".into();
        let _empty_from_str: ColumnRef = "".into(); // Invalid but allowed

        // i32 conversion - allows negative (but creates invalid ref)
        let _col_from_i32: ColumnRef = 42.into();
        let _negative_from_i32: ColumnRef = (-1).into(); // Invalid but allowed
        let _zero_from_i32: ColumnRef = 0.into(); // Invalid but allowed
    }
}
