//! Defines primitive computations on arrays

use std::sync::Arc;

use arrow::array::*;
use arrow::builder::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;

/// A batch of column-oriented data
pub struct Table {
    schema: Arc<Schema>,
    columns: Vec<Arc<Array>>,
    // num_rows: usize
}

impl Table {
    pub fn new(schema: Arc<Schema>, columns: Vec<ArrayRef>) -> Self {
        // assert that there are some columns
        assert!(
            columns.len() > 0,
            "at least one column must be defined to create a record batch"
        );
        // assert that all columns have the same row count
        let len = columns[0].data().len();
        for i in 1..columns.len() {
            assert_eq!(
                len,
                columns[i].len(),
                "all columns in a record batch must have the same length"
            );
        }
        Table { schema, columns }
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.columns[0].data().len()
    }

    // keep fn
    pub fn column(&self, i: usize) -> &ArrayRef {
        &self.columns[i]
    }

    pub fn columns(&self) ->Vec<ArrayRef> {
        self.columns.clone()
    }

    // new fns
    fn add_column() {}

    // fn remove_column(&self, _i: usize) -> Self {
    //     Table {
    //         schema: self.schema,
    //         columns: self.columns
    //     }
    // }

    fn set_column() {}

    fn replace_schema_metadata() {}

    fn flatten() {}

    fn make() {}

    pub fn from_record_batches(schema: Arc<Schema>, record_batches: Vec<RecordBatch>) -> Self {
        // let mut columns: Vec<Arc<Array>> = vec![];
        let mut builders: Vec<Box<ArrayBuilder>> = vec![];
        // create array builders from the record batches
        if record_batches.len() == 0 {
            panic!("Error about record batches (copy from cpp)")
        }
        let first_batch = &record_batches[0];
        if schema.fields().len() != first_batch.num_columns() {
            panic!("First record batch length does not match that of the schema")
        }
        let capacity = first_batch.num_rows();
        for i in 0..schema.fields().len() {
            let field = schema.field(i);
            match field.data_type() {
                DataType::Boolean => {
                    let mut builder = BooleanArray::builder(capacity);
                    // add values from all record batches
                    for batch in &record_batches {
                        let array = batch.column(i).as_any()
                            .downcast_ref::<BooleanArray>().unwrap();
                        for j in 0..array.len() {
                            builder.append_value(array.value(j)).unwrap();
                        }
                    }
                    builders.push(Box::new(builder));
                },
                DataType::Int8 => {
                    let mut builder = Int8Array::builder(capacity);
                    // add values from all record batches
                    for batch in &record_batches {
                        let array = batch.column(i).as_any()
                            .downcast_ref::<Int8Array>().unwrap();
                        for j in 0..array.len() {
                            builder.append_value(array.value(j)).unwrap();
                        }
                    }
                    builders.push(Box::new(builder));
                },
                DataType::Int16 => {
                    let mut builder = Int16Array::builder(capacity);
                    // add values from all record batches
                    for batch in &record_batches {
                        let array = batch.column(i).as_any()
                            .downcast_ref::<Int16Array>().unwrap();
                        for j in 0..array.len() {
                            builder.append_value(array.value(j)).unwrap();
                        }
                    }
                    builders.push(Box::new(builder));
                },
                DataType::Int32 => {
                    let mut builder = Int32Array::builder(capacity);
                    // add values from all record batches
                    for batch in &record_batches {
                        let array = batch.column(i).as_any()
                            .downcast_ref::<Int32Array>().unwrap();
                        for j in 0..array.len() {
                            builder.append_value(array.value(j)).unwrap();
                        }
                    }
                    builders.push(Box::new(builder));
                },
                DataType::Int64 => {
                    let mut builder = Int64Array::builder(capacity);
                    // add values from all record batches
                    for batch in &record_batches {
                        let array = batch.column(i).as_any()
                            .downcast_ref::<Int64Array>().unwrap();
                        for j in 0..array.len() {
                            builder.append_value(array.value(j)).unwrap();
                        }
                    }
                    builders.push(Box::new(builder));
                },
                DataType::UInt8 => {
                    let mut builder = UInt8Array::builder(capacity);
                    // add values from all record batches
                    for batch in &record_batches {
                        let array = batch.column(i).as_any()
                            .downcast_ref::<UInt8Array>().unwrap();
                        for j in 0..array.len() {
                            builder.append_value(array.value(j)).unwrap();
                        }
                    }
                    builders.push(Box::new(builder));
                },
                DataType::UInt16 => {
                    let mut builder = UInt16Array::builder(capacity);
                    // add values from all record batches
                    for batch in &record_batches {
                        let array = batch.column(i).as_any()
                            .downcast_ref::<UInt16Array>().unwrap();
                        for j in 0..array.len() {
                            builder.append_value(array.value(j)).unwrap();
                        }
                    }
                    builders.push(Box::new(builder));
                },
                DataType::UInt32 => {
                    let mut builder = UInt32Array::builder(capacity);
                    // add values from all record batches
                    for batch in &record_batches {
                        let array = batch.column(i).as_any()
                            .downcast_ref::<UInt32Array>().unwrap();
                        for j in 0..array.len() {
                            builder.append_value(array.value(j)).unwrap();
                        }
                    }
                    builders.push(Box::new(builder));
                },
                DataType::UInt64 => {
                    let mut builder = UInt64Array::builder(capacity);
                    // add values from all record batches
                    for batch in &record_batches {
                        let array = batch.column(i).as_any()
                            .downcast_ref::<UInt64Array>().unwrap();
                        for j in 0..array.len() {
                            builder.append_value(array.value(j)).unwrap();
                        }
                    }
                    builders.push(Box::new(builder));
                },
                DataType::Float32 => {
                    let mut builder = Float32Array::builder(capacity);
                    // add values from all record batches
                    for batch in &record_batches {
                        let array = batch.column(i).as_any()
                            .downcast_ref::<Float32Array>().unwrap();
                        for j in 0..array.len() {
                            builder.append_value(array.value(j)).unwrap();
                        }
                    }
                    builders.push(Box::new(builder));
                },
                DataType::Float64 => {
                    let mut builder = Float64Array::builder(capacity);
                    // add values from all record batches
                    for batch in &record_batches {
                        let array = batch.column(i).as_any()
                            .downcast_ref::<Float64Array>().unwrap();
                        for j in 0..array.len() {
                            builder.append_value(array.value(j)).unwrap();
                        }
                    }
                    builders.push(Box::new(builder));
                },
                DataType::Utf8 => {
                    let mut builder = BinaryBuilder::new(capacity);
                    // add values from all record batches
                    for batch in &record_batches {
                        let array = batch.column(i).as_any()
                            .downcast_ref::<BinaryArray>().unwrap();
                        for j in 0..array.len() {
                            builder.append_string(std::str::from_utf8(array.value(j)).unwrap()).unwrap();
                        }
                    }
                    builders.push(Box::new(builder));
                },
                t @ _ => panic!("Data type {:?} is not currently supported", t),
            }
        }

        Table {
            schema,
            columns: builders.into_iter().map(|mut builder| builder.finish()).collect()
        }
    }

    fn concatenate_tables() {}

    fn to_record_batches() {}
}

unsafe impl Send for Table {}
unsafe impl Sync for Table {}

#[cfg(test)]
mod tests {
    // use super::*;
    // use crate::array::{Float64Array, Int32Array};

    #[test]
    fn test_supported() {
        println!("SSE: {}", is_x86_feature_detected!("sse"));
        println!("SSE2: {}", is_x86_feature_detected!("sse2"));
        println!("SSE3: {}", is_x86_feature_detected!("sse3"));
        println!("SSSE3: {}", is_x86_feature_detected!("ssse3"));
        println!("SSE4.1: {}", is_x86_feature_detected!("sse4.1"));
        println!("SSE4.2: {}", is_x86_feature_detected!("sse4.2"));
        println!("AVX: {}", is_x86_feature_detected!("avx"));
        println!("AVX2: {}", is_x86_feature_detected!("avx2"));
        println!("AVX512f: {}", is_x86_feature_detected!("avx512f"));
    }
}
