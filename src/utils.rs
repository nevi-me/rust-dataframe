use crate::table::Column;
use arrow::array;
use arrow::array::{Array, ArrayDataRef, ArrayRef};
use arrow::datatypes::*;
use std::sync::Arc;

pub fn make_array(data: ArrayDataRef) -> ArrayRef {
    // TODO: here data_type() needs to clone the type - maybe add a type tag enum to
    // avoid the cloning.
    match data.data_type().clone() {
        DataType::Boolean => Arc::new(array::BooleanArray::from(data)) as ArrayRef,
        DataType::Int8 => Arc::new(array::Int8Array::from(data)) as ArrayRef,
        DataType::Int16 => Arc::new(array::Int16Array::from(data)) as ArrayRef,
        DataType::Int32 => Arc::new(array::Int32Array::from(data)) as ArrayRef,
        DataType::Int64 => Arc::new(array::Int64Array::from(data)) as ArrayRef,
        DataType::UInt8 => Arc::new(array::UInt8Array::from(data)) as ArrayRef,
        DataType::UInt16 => Arc::new(array::UInt16Array::from(data)) as ArrayRef,
        DataType::UInt32 => Arc::new(array::UInt32Array::from(data)) as ArrayRef,
        DataType::UInt64 => Arc::new(array::UInt64Array::from(data)) as ArrayRef,
        DataType::Float32 => Arc::new(array::Float32Array::from(data)) as ArrayRef,
        DataType::Float64 => Arc::new(array::Float64Array::from(data)) as ArrayRef,
        DataType::Utf8 => Arc::new(array::StringArray::from(data)) as ArrayRef,
        DataType::List(_) => Arc::new(array::ListArray::from(data)) as ArrayRef,
        DataType::Struct(_) => Arc::new(array::StructArray::from(data)) as ArrayRef,
        dt => panic!("Unexpected data type {:?}", dt),
    }
}
