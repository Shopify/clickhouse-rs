use chrono_tz::Tz;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{SqlType, Value, ValueRef},
};

use super::column_data::{BoxColumnData, ColumnData};

/// Column data for ClickHouse's `Nothing` type: zero bytes per row, used almost
/// exclusively inside `Nullable(Nothing)` for all-NULL columns.
///
/// Design note: since there's no `ValueRef::Nothing` variant, `at()` returns a
/// Nullable sentinel (`Nullable(Left(&SqlType::Nothing))`). This means standalone
/// Nothing columns have slightly unusual semantics — the `SqlType::Nothing` →
/// `Value::default()` → `SqlType::from()` roundtrip produces `Nullable(Nothing)`
/// instead of `Nothing`. This is an acceptable tradeoff given the type's narrow
/// real-world use case (always wrapped in Nullable).
pub(crate) struct NothingColumnData {
    pub(crate) len: usize,
}

impl NothingColumnData {
    pub(crate) fn load<R: ReadEx>(_reader: &mut R, size: usize) -> Result<Self> {
        // Nothing type has zero bytes per value, so we don't read anything
        Ok(NothingColumnData { len: size })
    }

    pub(crate) fn with_capacity(_capacity: usize) -> Self {
        NothingColumnData { len: 0 }
    }
}

impl ColumnData for NothingColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::Nothing
    }

    fn save(&self, _encoder: &mut Encoder, _start: usize, _end: usize) {
        // Nothing type has zero bytes per value, nothing to write
    }

    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, _value: Value) {
        self.len += 1;
    }

    fn at(&self, index: usize) -> ValueRef {
        assert!(index < self.len, "index out of bounds: {} >= {}", index, self.len);
        ValueRef::Nullable(either::Either::Left(&SqlType::Nothing))
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(NothingColumnData { len: self.len })
    }

    fn get_timezone(&self) -> Option<Tz> {
        None
    }
}
