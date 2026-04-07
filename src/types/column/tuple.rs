use std::sync::Arc;

use chrono_tz::Tz;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{
        column::{
            column_data::{ArcColumnData, BoxColumnData},
            ArcColumnWrapper, ColumnData, ColumnWrapper,
        },
        SqlType, Value, ValueRef,
    },
};

/// Column data for ClickHouse's Tuple(T1, T2, ..., TN) type.
///
/// Each element of the tuple is stored as a separate sub-column (columnar layout).
/// For a Tuple(UInt8, String) with N rows, we store one UInt8 column of N rows
/// and one String column of N rows.
pub(crate) struct TupleColumnData {
    pub(crate) columns: Vec<ArcColumnData>,
}

impl TupleColumnData {
    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        type_names: Vec<&str>,
        rows: usize,
        tz: Tz,
    ) -> Result<Self> {
        let mut columns = Vec::with_capacity(type_names.len());
        for type_name in type_names {
            let column =
                <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, type_name, rows, tz)?;
            columns.push(column);
        }
        Ok(TupleColumnData { columns })
    }

    pub(crate) fn with_capacity(
        element_types: &[&'static SqlType],
        tz: Tz,
        capacity: usize,
    ) -> Result<Self> {
        let mut columns = Vec::with_capacity(element_types.len());
        for sql_type in element_types {
            let column = <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                (*sql_type).clone(),
                tz,
                capacity,
            )?;
            columns.push(column);
        }
        Ok(TupleColumnData { columns })
    }
}

impl ColumnData for TupleColumnData {
    fn sql_type(&self) -> SqlType {
        let types: Vec<&'static SqlType> = self
            .columns
            .iter()
            .map(|c| {
                let sql_type = c.sql_type();
                let static_ref: &'static SqlType = sql_type.into();
                static_ref
            })
            .collect();
        SqlType::Tuple(types)
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        for column in &self.columns {
            column.save(encoder, start, end);
        }
    }

    fn len(&self) -> usize {
        self.columns.first().map_or(0, |c| c.len())
    }

    fn push(&mut self, value: Value) {
        if let Value::Tuple(values) = value {
            assert_eq!(
                values.len(),
                self.columns.len(),
                "tuple value length mismatch"
            );
            for (i, v) in values.iter().enumerate() {
                Arc::get_mut(&mut self.columns[i])
                    .unwrap()
                    .push(v.clone());
            }
        } else {
            panic!("expected Tuple value");
        }
    }

    fn at(&self, index: usize) -> ValueRef {
        let values: Vec<ValueRef> = self.columns.iter().map(|c| c.at(index)).collect();
        ValueRef::Tuple(Arc::new(values))
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(TupleColumnData {
            columns: self.columns.clone(),
        })
    }

    fn get_timezone(&self) -> Option<Tz> {
        self.columns.first().and_then(|c| c.get_timezone())
    }
}
