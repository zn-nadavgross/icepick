use crate::error::{Error, Result};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, PrimitiveArray,
    StringArray,
};
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use std::cmp::Ordering;
use std::collections::{hash_map::Entry, HashMap};

#[derive(Debug, Clone)]
pub(super) enum BoundValue {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    Utf8(String),
    Binary(Vec<u8>),
    Decimal128(i128),
}

impl BoundValue {
    fn cmp(&self, other: &BoundValue) -> Ordering {
        match (self, other) {
            (BoundValue::Int32(a), BoundValue::Int32(b)) => a.cmp(b),
            (BoundValue::Int64(a), BoundValue::Int64(b)) => a.cmp(b),
            (BoundValue::Float32(a), BoundValue::Float32(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (BoundValue::Float64(a), BoundValue::Float64(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (BoundValue::Boolean(a), BoundValue::Boolean(b)) => a.cmp(b),
            (BoundValue::Utf8(a), BoundValue::Utf8(b)) => a.cmp(b),
            (BoundValue::Binary(a), BoundValue::Binary(b)) => a.cmp(b),
            (BoundValue::Decimal128(a), BoundValue::Decimal128(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }

    fn encode(&self) -> Vec<u8> {
        match self {
            BoundValue::Int32(v) => v.to_le_bytes().to_vec(),
            BoundValue::Int64(v) => v.to_le_bytes().to_vec(),
            BoundValue::Float32(v) => v.to_le_bytes().to_vec(),
            BoundValue::Float64(v) => v.to_le_bytes().to_vec(),
            BoundValue::Boolean(v) => vec![u8::from(*v)],
            BoundValue::Utf8(v) => v.as_bytes().to_vec(),
            BoundValue::Binary(v) => v.clone(),
            BoundValue::Decimal128(v) => v.to_be_bytes().to_vec(),
        }
    }
}

pub(super) struct BoundState {
    lower_bound_values: HashMap<i32, BoundValue>,
    upper_bound_values: HashMap<i32, BoundValue>,
}

impl BoundState {
    pub(super) fn new() -> Self {
        Self {
            lower_bound_values: HashMap::new(),
            upper_bound_values: HashMap::new(),
        }
    }

    pub(super) fn merge(&mut self, field_id: i32, lower: BoundValue, upper: BoundValue) {
        match self.lower_bound_values.entry(field_id) {
            Entry::Occupied(mut entry) => {
                if lower.cmp(entry.get()) == Ordering::Less {
                    entry.insert(lower);
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(lower);
            }
        }

        match self.upper_bound_values.entry(field_id) {
            Entry::Occupied(mut entry) => {
                if upper.cmp(entry.get()) == Ordering::Greater {
                    entry.insert(upper);
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(upper);
            }
        }
    }

    pub(super) fn into_encoded(self) -> (HashMap<i32, Vec<u8>>, HashMap<i32, Vec<u8>>) {
        let lower = self
            .lower_bound_values
            .into_iter()
            .map(|(field, bound)| (field, bound.encode()))
            .collect();
        let upper = self
            .upper_bound_values
            .into_iter()
            .map(|(field, bound)| (field, bound.encode()))
            .collect();
        (lower, upper)
    }
}

pub(super) fn compute_bounds(
    data_type: &DataType,
    column: &dyn Array,
) -> Result<Option<(BoundValue, BoundValue)>> {
    match data_type {
        DataType::Int32 | DataType::Date32 => {
            let array = downcast::<Int32Array>(column, "Int32Array")?;
            Ok(primitive_min_max(array)
                .map(|(min, max)| (BoundValue::Int32(min), BoundValue::Int32(max))))
        }
        DataType::Int64 | DataType::Date64 | DataType::Time64(_) | DataType::Timestamp(_, _) => {
            let array = downcast::<Int64Array>(column, "Int64Array")?;
            Ok(primitive_min_max(array)
                .map(|(min, max)| (BoundValue::Int64(min), BoundValue::Int64(max))))
        }
        DataType::Float32 => {
            let array = downcast::<Float32Array>(column, "Float32Array")?;
            Ok(primitive_min_max(array)
                .map(|(min, max)| (BoundValue::Float32(min), BoundValue::Float32(max))))
        }
        DataType::Float64 => {
            let array = downcast::<Float64Array>(column, "Float64Array")?;
            Ok(primitive_min_max(array)
                .map(|(min, max)| (BoundValue::Float64(min), BoundValue::Float64(max))))
        }
        DataType::Boolean => Ok(boolean_min_max(column)
            .map(|(min, max)| (BoundValue::Boolean(min), BoundValue::Boolean(max)))),
        DataType::Utf8 => {
            let array = downcast::<StringArray>(column, "StringArray")?;
            Ok(string_min_max(array)
                .map(|(min, max)| (BoundValue::Utf8(min), BoundValue::Utf8(max))))
        }
        DataType::LargeUtf8 => {
            let array = downcast::<LargeStringArray>(column, "LargeStringArray")?;
            Ok(large_string_min_max(array)
                .map(|(min, max)| (BoundValue::Utf8(min), BoundValue::Utf8(max))))
        }
        DataType::Binary => {
            let array = downcast::<BinaryArray>(column, "BinaryArray")?;
            Ok(binary_min_max(array)
                .map(|(min, max)| (BoundValue::Binary(min), BoundValue::Binary(max))))
        }
        DataType::LargeBinary => {
            let array = downcast::<LargeBinaryArray>(column, "LargeBinaryArray")?;
            Ok(large_binary_min_max(array)
                .map(|(min, max)| (BoundValue::Binary(min), BoundValue::Binary(max))))
        }
        DataType::FixedSizeBinary(_) => {
            let array = downcast::<FixedSizeBinaryArray>(column, "FixedSizeBinaryArray")?;
            Ok(fixed_size_binary_min_max(array)
                .map(|(min, max)| (BoundValue::Binary(min), BoundValue::Binary(max))))
        }
        DataType::Decimal128(_, _) => {
            let array = downcast::<Decimal128Array>(column, "Decimal128Array")?;
            Ok(decimal_min_max(array)
                .map(|(min, max)| (BoundValue::Decimal128(min), BoundValue::Decimal128(max))))
        }
        _ => Ok(None),
    }
}

fn downcast<'a, T>(column: &'a dyn Array, type_name: &'static str) -> Result<&'a T>
where
    T: Array + 'static,
{
    column
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| Error::invalid_input(format!("Expected {type_name}")))
}

fn primitive_min_max<T>(array: &PrimitiveArray<T>) -> Option<(T::Native, T::Native)>
where
    T: ArrowPrimitiveType,
    T::Native: Copy + PartialOrd,
{
    let mut iter = array.iter().flatten();

    let first = iter.next()?;
    let mut min = first;
    let mut max = first;

    for value in iter {
        if value < min {
            min = value;
        }
        if value > max {
            max = value;
        }
    }

    Some((min, max))
}

fn boolean_min_max(array: &dyn Array) -> Option<(bool, bool)> {
    let bool_array = array.as_any().downcast_ref::<BooleanArray>()?;
    let mut has_true = false;
    let mut has_false = false;

    for i in 0..bool_array.len() {
        if bool_array.is_null(i) {
            continue;
        }
        if bool_array.value(i) {
            has_true = true;
        } else {
            has_false = true;
        }
    }

    match (has_false, has_true) {
        (false, false) => None,
        (true, false) => Some((false, false)),
        (false, true) => Some((true, true)),
        (true, true) => Some((false, true)),
    }
}

fn string_min_max(array: &StringArray) -> Option<(String, String)> {
    let mut iter = array.iter().filter_map(|value| value.map(str::to_string));
    let first = iter.next()?;
    let mut min = first.clone();
    let mut max = first;

    for value in iter {
        if value < min {
            min = value.clone();
        }
        if value > max {
            max = value.clone();
        }
    }

    Some((min, max))
}

fn large_string_min_max(array: &LargeStringArray) -> Option<(String, String)> {
    let mut iter = array.iter().filter_map(|value| value.map(str::to_string));
    let first = iter.next()?;
    let mut min = first.clone();
    let mut max = first;

    for value in iter {
        if value < min {
            min = value.clone();
        }
        if value > max {
            max = value.clone();
        }
    }

    Some((min, max))
}

fn binary_min_max(array: &BinaryArray) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut iter = array
        .iter()
        .filter_map(|value| value.map(|bytes| bytes.to_vec()));
    let first = iter.next()?;
    let mut min = first.clone();
    let mut max = first;

    for value in iter {
        if value < min {
            min = value.clone();
        }
        if value > max {
            max = value.clone();
        }
    }

    Some((min, max))
}

fn large_binary_min_max(array: &LargeBinaryArray) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut iter = array
        .iter()
        .filter_map(|value| value.map(|bytes| bytes.to_vec()));
    let first = iter.next()?;
    let mut min = first.clone();
    let mut max = first;

    for value in iter {
        if value < min {
            min = value.clone();
        }
        if value > max {
            max = value.clone();
        }
    }

    Some((min, max))
}

fn fixed_size_binary_min_max(array: &FixedSizeBinaryArray) -> Option<(Vec<u8>, Vec<u8>)> {
    if array.is_empty() {
        return None;
    }

    let mut min = array.value(0).to_vec();
    let mut max = min.clone();

    for i in 1..array.len() {
        if array.is_null(i) {
            continue;
        }
        let value = array.value(i);
        if value < min.as_slice() {
            min = value.to_vec();
        }
        if value > max.as_slice() {
            max = value.to_vec();
        }
    }

    Some((min, max))
}

fn decimal_min_max(array: &Decimal128Array) -> Option<(i128, i128)> {
    let mut iter = array.iter().flatten();
    let first = iter.next()?;
    let mut min = first;
    let mut max = first;

    for value in iter {
        if value < min {
            min = value;
        }
        if value > max {
            max = value;
        }
    }
    Some((min, max))
}
