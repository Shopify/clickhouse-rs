use chrono_tz::Tz;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{
        column::{column_data::BoxColumnData, list::List, numeric::save_data, ColumnData},
        SqlType, Value, ValueRef,
    },
};

/// Column data for ClickHouse's Date32 type.
///
/// Date32 stores dates as a signed 32-bit integer representing the number of days
/// since 1970-01-01 (Unix epoch). Supports range from 1900-01-01 to 2299-12-31.
pub(crate) struct Date32ColumnData {
    data: List<i32>,
}

impl Date32ColumnData {
    pub(crate) fn load<R: ReadEx>(reader: &mut R, size: usize) -> Result<Self> {
        let mut data = List::with_capacity(size);
        unsafe {
            data.set_len(size);
        }
        reader.read_bytes(data.as_mut())?;
        Ok(Date32ColumnData { data })
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Date32ColumnData {
            data: List::with_capacity(capacity),
        }
    }
}

impl ColumnData for Date32ColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::Date32
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        save_data::<i32>(self.data.as_ref(), encoder, start, end);
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn push(&mut self, value: Value) {
        if let Value::Date32(days) = value {
            self.data.push(days);
        } else {
            panic!("expected Date32 value");
        }
    }

    fn at(&self, index: usize) -> ValueRef {
        ValueRef::Date32(self.data.at(index))
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Date32ColumnData {
            data: self.data.clone(),
        })
    }

    fn get_timezone(&self) -> Option<Tz> {
        None
    }
}
