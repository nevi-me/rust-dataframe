use std::sync::Arc;

use arrow::array::*;
use arrow::builder::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;

pub struct ChunkedArray {
    chunks: Vec<Arc<Array>>,
    num_rows: usize,
    null_count: usize,
    // TODO: Go has data_type, is it worth storing, or getting from the first chunk?
}

impl ChunkedArray {
    /// Construct a `ChunkedArray` from a list of `Array`s.
    ///
    /// There must be at least 1 array, and all arrays must have the same data type.
    fn from_arrays(arrays: Vec<Arc<Array>>) -> Self {
        assert!(arrays.len() > 0);
        let mut num_rows = 0;
        let mut null_count = 0;
        // check that arrays have the same type
        let data_type = &arrays[0].data_type();
        arrays.iter().for_each(|array| {
            assert!(&array.data_type() == data_type);
            num_rows += array.len();
            null_count += array.null_count();
        });
        ChunkedArray {
            chunks: arrays,
            num_rows,
            null_count,
        }
    }

    /// Return the length of the arrays in the chunk. This value is pre-computed.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    fn null_count(&self) -> usize {
        self.null_count
    }

    pub fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    /// Get a chunk from the chunked array by index
    /// TODO: should this have bounds-chacking?
    pub fn chunk(&self, i: usize) -> &Arc<Array> {
        &self.chunks[i]
    }

    pub fn chunks(&self) -> &Vec<Arc<Array>> {
        &self.chunks
    }

    // pub fn as_arrays<T>(&self) -> &PrimitiveArray<T>
    // where
    //     T: ArrowPrimitiveType,
    //     T::Native: ArrowPrimitiveType
    // {
    //     match T::get_data_type() {
    //         // DataType::Boolean => self.chunks[0].as_any().downcast_ref::<BooleanArray>().unwrap(),
    //         DataType::Int8 => self.chunks[0].as_any().downcast_ref::<Int8Array>().unwrap(),
    //         // DataType::Int16 => self.chunks.iter().map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap()).collect(),
    //         // DataType::Int32 => self.chunks.iter().map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap()).collect(),
    //         // DataType::Int64 => self.chunks.iter().map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap()).collect(),
    //         // DataType::UInt8 => self.chunks.iter().map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap()).collect(),
    //         // DataType::UInt16 => self.chunks.iter().map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap()).collect(),
    //         // DataType::UInt32 => self.chunks.iter().map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap()).collect(),
    //         // DataType::UInt64 => self.chunks.iter().map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap()).collect(),
    //         // DataType::Float32 => self.chunks.iter().map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap()).collect(),
    //         // DataType::Float64 => self.chunks.iter().map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap()).collect(),
    //         // DataType::Utf8 => self.chunks.iter().map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap()).collect(),
    //         // DataType::List(_) => self.chunks.iter().map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap()).collect(),
    //         // DataType::Struct(_) => self.chunks.iter().map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap()).collect(),
    //         dt => panic!("Unexpected data type {:?}", dt),
    //     }

    // }

    /// Construct a zero-copy slice of the chunked array with the indicated offset and length.Arc
    ///
    /// The `offset` is the position of the first element in the constructed slice.
    /// `length` is the length of the slice. If there are not enough elements in the chunked array,
    /// the length will be adjusted accordingly.
    ///
    /// TODO: I've made length optional because CPP has 2 `slice` methods, with one being a slice
    /// to the end of the array.
    fn slice(&self, _offset: usize, length: Option<usize>) {
        unimplemented!("TODO: I need help here, this has to be a zero-copy slice among slices")
    }

    fn flatten(&self) {
        unimplemented!("This is for flattening struct columns, we aren't yet there")
    }
}

// pub trait Chunk {
//     fn test();
// }

// macro_rules! chunked_to_arrays {
//     ($name:ident, $b:ty, $chunks:expr) => {
//         impl Chunk for $name {
//             fn test(chunks: $chunks) -> Vec<$b< {
//                 //
//             }
//         }
//     }
// }

/// A column data structure consisting of a `Field` and `ChunkedArray`
pub struct Column {
    pub(crate) data: ChunkedArray,
    field: arrow::datatypes::Field,
}

impl Column {
    fn from_chunked_array(chunk: ChunkedArray, field: arrow::datatypes::Field) -> Self {
        // assert!()
        Column { data: chunk, field }
    }

    pub fn from_arrays(arrays: Vec<Arc<Array>>, field: arrow::datatypes::Field) -> Self {
        assert!(arrays.len() > 0);
        for ref array in &arrays {
            assert!(array.data_type() == field.data_type());
        }
        Column {
            data: ChunkedArray::from_arrays(arrays),
            field,
        }
    }

    fn name(&self) -> &str {
        self.field.name()
    }

    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
    }

    pub fn data(&self) -> &ChunkedArray {
        &self.data
    }

    pub(crate) fn field(&self) -> &Field {
        &self.field
    }

    /// TODO: slice seems the same as that of `ChunkedArray`
    fn slice() {}

    fn flatten() {}
}

/// Alogical table as a sequence of chunked arrays
pub struct Table {
    schema: Arc<Schema>,
    pub(crate) columns: Vec<Column>,
}

impl Table {
    // pub fn new(schema: Arc<Schema>, columns: Vec<Column>) -> Self {
    //     // assert that there are some columns
    //     assert!(
    //         columns.len() > 0,
    //         "at least one column must be defined to create a record batch"
    //     );
    //     // assert that all columns have the same row count
    //     let len = columns[0].data().len();
    //     for i in 1..columns.len() {
    //         assert_eq!(
    //             len,
    //             columns[i].len(),
    //             "all columns in a record batch must have the same length"
    //         );
    //     }
    //     Table { schema, columns }
    // }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.columns[0].data().num_rows()
    }

    // keep fn
    pub fn column(&self, i: usize) -> &Column {
        &self.columns[i]
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    // new fns
    fn add_column() {}

    // fn remove_column(&self, _i: usize) -> Self {
    //     Table {
    //         schema: self.schema.clone(),
    //         columns: self.columns
    //     }
    // }

    /// Replace a column in the table, producing a new `Table`
    fn set_column() {}

    fn replace_schema_metadata() {}

    /// Each column with a struct type is flattened into one column per struct field.
    /// Other columns are left unchanged.
    fn flatten() {}

    /// Construct a `Table` from a sequence of `Column`s and a schema
    fn make(columns: Vec<Column>) -> Self {
        let fields: Vec<Field> = columns.iter().map(|column| column.field.clone()).collect();
        Table {
            schema: Arc::new(Schema::new(fields)),
            columns,
        }
    }

    /// Construct a `Table` from a sequence of `Column`s and a schema
    fn make_with_schema(schema: Arc<Schema>, columns: Vec<Column>) -> Self {
        // TODO validate that schema and columns match
        Table { schema, columns }
    }

    /// Construct a `Table` from a sequence of Arrow `RecordBatch`es.
    ///
    /// Columns are first created from the `RecordBatch`es, with schema validations being performed.
    /// A table is then created
    pub fn from_record_batches(schema: Arc<Schema>, record_batches: Vec<RecordBatch>) -> Self {
        if record_batches.len() == 0 {
            panic!("Error about record batches (copy from cpp)")
        }
        let num_columns = record_batches[0].num_columns();
        // let mut arrays: Vec<Vec<&Arc<Array>>> = vec![vec![]; num_columns];
        let mut arrays: Vec<Vec<Arc<Array>>> = vec![vec![]; num_columns];
        // create columns from record batches
        for ref batch in record_batches {
            assert!(
                batch.num_columns() == num_columns,
                "Each record batch should have the same length as the first batch"
            );
            for i in 0..num_columns {
                arrays[i].push(batch.column(i).to_owned());
            }
        }
        let columns = arrays
            .iter()
            .enumerate()
            .map(|(i, array)| Column::from_arrays(array.to_owned(), schema.field(i).clone()))
            .collect();

        Table { schema, columns }
    }

    // pub fn from_record_batches_copy(schema: Arc<Schema>, record_batches: Vec<RecordBatch>) -> Self {
    //     // let mut columns: Vec<Arc<Array>> = vec![];
    //     let mut builders: Vec<Box<ArrayBuilder>> = vec![];
    //     // create array builders from the record batches
    //     if record_batches.len() == 0 {
    //         panic!("Error about record batches (copy from cpp)")
    //     }
    //     let first_batch = &record_batches[0];
    //     if schema.fields().len() != first_batch.num_columns() {
    //         panic!("First record batch length does not match that of the schema")
    //     }
    //     let capacity = first_batch.num_rows();
    //     for i in 0..schema.fields().len() {
    //         let field = schema.field(i);
    //         match field.data_type() {
    //             DataType::Boolean => {
    //                 let mut builder = BooleanArray::builder(capacity);
    //                 // add values from all record batches
    //                 for batch in &record_batches {
    //                     let array = batch.column(i).as_any()
    //                         .downcast_ref::<BooleanArray>().unwrap();
    //                     for j in 0..array.len() {
    //                         builder.append_value(array.value(j)).unwrap();
    //                     }
    //                 }
    //                 builders.push(Box::new(builder));
    //             },
    //             DataType::Int8 => {
    //                 let mut builder = Int8Array::builder(capacity);
    //                 // add values from all record batches
    //                 for batch in &record_batches {
    //                     let array = batch.column(i).as_any()
    //                         .downcast_ref::<Int8Array>().unwrap();
    //                     for j in 0..array.len() {
    //                         builder.append_value(array.value(j)).unwrap();
    //                     }
    //                 }
    //                 builders.push(Box::new(builder));
    //             },
    //             DataType::Int16 => {
    //                 let mut builder = Int16Array::builder(capacity);
    //                 // add values from all record batches
    //                 for batch in &record_batches {
    //                     let array = batch.column(i).as_any()
    //                         .downcast_ref::<Int16Array>().unwrap();
    //                     for j in 0..array.len() {
    //                         builder.append_value(array.value(j)).unwrap();
    //                     }
    //                 }
    //                 builders.push(Box::new(builder));
    //             },
    //             DataType::Int32 => {
    //                 let mut builder = Int32Array::builder(capacity);
    //                 // add values from all record batches
    //                 for batch in &record_batches {
    //                     let array = batch.column(i).as_any()
    //                         .downcast_ref::<Int32Array>().unwrap();
    //                     for j in 0..array.len() {
    //                         builder.append_value(array.value(j)).unwrap();
    //                     }
    //                 }
    //                 builders.push(Box::new(builder));
    //             },
    //             DataType::Int64 => {
    //                 let mut builder = Int64Array::builder(capacity);
    //                 // add values from all record batches
    //                 for batch in &record_batches {
    //                     let array = batch.column(i).as_any()
    //                         .downcast_ref::<Int64Array>().unwrap();
    //                     for j in 0..array.len() {
    //                         builder.append_value(array.value(j)).unwrap();
    //                     }
    //                 }
    //                 builders.push(Box::new(builder));
    //             },
    //             DataType::UInt8 => {
    //                 let mut builder = UInt8Array::builder(capacity);
    //                 // add values from all record batches
    //                 for batch in &record_batches {
    //                     let array = batch.column(i).as_any()
    //                         .downcast_ref::<UInt8Array>().unwrap();
    //                     for j in 0..array.len() {
    //                         builder.append_value(array.value(j)).unwrap();
    //                     }
    //                 }
    //                 builders.push(Box::new(builder));
    //             },
    //             DataType::UInt16 => {
    //                 let mut builder = UInt16Array::builder(capacity);
    //                 // add values from all record batches
    //                 for batch in &record_batches {
    //                     let array = batch.column(i).as_any()
    //                         .downcast_ref::<UInt16Array>().unwrap();
    //                     for j in 0..array.len() {
    //                         builder.append_value(array.value(j)).unwrap();
    //                     }
    //                 }
    //                 builders.push(Box::new(builder));
    //             },
    //             DataType::UInt32 => {
    //                 let mut builder = UInt32Array::builder(capacity);
    //                 // add values from all record batches
    //                 for batch in &record_batches {
    //                     let array = batch.column(i).as_any()
    //                         .downcast_ref::<UInt32Array>().unwrap();
    //                     for j in 0..array.len() {
    //                         builder.append_value(array.value(j)).unwrap();
    //                     }
    //                 }
    //                 builders.push(Box::new(builder));
    //             },
    //             DataType::UInt64 => {
    //                 let mut builder = UInt64Array::builder(capacity);
    //                 // add values from all record batches
    //                 for batch in &record_batches {
    //                     let array = batch.column(i).as_any()
    //                         .downcast_ref::<UInt64Array>().unwrap();
    //                     for j in 0..array.len() {
    //                         builder.append_value(array.value(j)).unwrap();
    //                     }
    //                 }
    //                 builders.push(Box::new(builder));
    //             },
    //             DataType::Float32 => {
    //                 let mut builder = Float32Array::builder(capacity);
    //                 // add values from all record batches
    //                 for batch in &record_batches {
    //                     let array = batch.column(i).as_any()
    //                         .downcast_ref::<Float32Array>().unwrap();
    //                     for j in 0..array.len() {
    //                         builder.append_value(array.value(j)).unwrap();
    //                     }
    //                 }
    //                 builders.push(Box::new(builder));
    //             },
    //             DataType::Float64 => {
    //                 let mut builder = Float64Array::builder(capacity);
    //                 // add values from all record batches
    //                 for batch in &record_batches {
    //                     let array = batch.column(i).as_any()
    //                         .downcast_ref::<Float64Array>().unwrap();
    //                     for j in 0..array.len() {
    //                         builder.append_value(array.value(j)).unwrap();
    //                     }
    //                 }
    //                 builders.push(Box::new(builder));
    //             },
    //             DataType::Utf8 => {
    //                 let mut builder = BinaryBuilder::new(capacity);
    //                 // add values from all record batches
    //                 for batch in &record_batches {
    //                     let array = batch.column(i).as_any()
    //                         .downcast_ref::<BinaryArray>().unwrap();
    //                     for j in 0..array.len() {
    //                         builder.append_string(std::str::from_utf8(array.value(j)).unwrap()).unwrap();
    //                     }
    //                 }
    //                 builders.push(Box::new(builder));
    //             },
    //             t @ _ => panic!("Data type {:?} is not currently supported", t),
    //         }
    //     }

    //     Table {
    //         schema,
    //         columns: builders.into_iter().map(|mut builder| builder.finish()).collect()
    //     }
    // }

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
