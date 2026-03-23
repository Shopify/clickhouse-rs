use chrono_tz::Tz;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{SqlType, Value, ValueRef},
};

use super::column_data::{BoxColumnData, ColumnData};

const BYTES_PER_VALUE: usize = 32;

/// Column data for ClickHouse's Int256 type (32-byte little-endian signed integer).
pub(crate) struct Int256ColumnData {
    data: Vec<u8>,
}

impl Int256ColumnData {
    pub(crate) fn load<R: ReadEx>(reader: &mut R, size: usize) -> Result<Self> {
        let total_bytes = size * BYTES_PER_VALUE;
        let mut data = vec![0u8; total_bytes];
        reader.read_bytes(&mut data)?;
        Ok(Int256ColumnData { data })
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Int256ColumnData {
            data: Vec::with_capacity(capacity * BYTES_PER_VALUE),
        }
    }
}

impl ColumnData for Int256ColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::Int256
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let start_byte = start * BYTES_PER_VALUE;
        let end_byte = end * BYTES_PER_VALUE;
        encoder.write_bytes(&self.data[start_byte..end_byte]);
    }

    fn len(&self) -> usize {
        self.data.len() / BYTES_PER_VALUE
    }

    fn push(&mut self, value: Value) {
        if let Value::Int256(bytes) = value {
            self.data.extend_from_slice(&bytes);
        } else {
            panic!("expected Int256 value");
        }
    }

    fn at(&self, index: usize) -> ValueRef {
        let start = index * BYTES_PER_VALUE;
        let mut bytes = [0u8; BYTES_PER_VALUE];
        bytes.copy_from_slice(&self.data[start..start + BYTES_PER_VALUE]);
        ValueRef::Int256(bytes)
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Int256ColumnData {
            data: self.data.clone(),
        })
    }

    fn get_timezone(&self) -> Option<Tz> {
        None
    }
}

/// Column data for ClickHouse's UInt256 type (32-byte little-endian unsigned integer).
pub(crate) struct UInt256ColumnData {
    data: Vec<u8>,
}

impl UInt256ColumnData {
    pub(crate) fn load<R: ReadEx>(reader: &mut R, size: usize) -> Result<Self> {
        let total_bytes = size * BYTES_PER_VALUE;
        let mut data = vec![0u8; total_bytes];
        reader.read_bytes(&mut data)?;
        Ok(UInt256ColumnData { data })
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        UInt256ColumnData {
            data: Vec::with_capacity(capacity * BYTES_PER_VALUE),
        }
    }
}

impl ColumnData for UInt256ColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::UInt256
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let start_byte = start * BYTES_PER_VALUE;
        let end_byte = end * BYTES_PER_VALUE;
        encoder.write_bytes(&self.data[start_byte..end_byte]);
    }

    fn len(&self) -> usize {
        self.data.len() / BYTES_PER_VALUE
    }

    fn push(&mut self, value: Value) {
        if let Value::UInt256(bytes) = value {
            self.data.extend_from_slice(&bytes);
        } else {
            panic!("expected UInt256 value");
        }
    }

    fn at(&self, index: usize) -> ValueRef {
        let start = index * BYTES_PER_VALUE;
        let mut bytes = [0u8; BYTES_PER_VALUE];
        bytes.copy_from_slice(&self.data[start..start + BYTES_PER_VALUE]);
        ValueRef::UInt256(bytes)
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(UInt256ColumnData {
            data: self.data.clone(),
        })
    }

    fn get_timezone(&self) -> Option<Tz> {
        None
    }
}
