use crate::table::Column;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;

/// Constructs an array using the input `data`. Returns a reference-counted `Array`
/// instance.
pub fn make_array(data: ArrayDataRef) -> ArrayRef {
    match data.data_type() {
        DataType::Boolean => Arc::new(BooleanArray::from(data)) as ArrayRef,
        DataType::Int8 => Arc::new(Int8Array::from(data)) as ArrayRef,
        DataType::Int16 => Arc::new(Int16Array::from(data)) as ArrayRef,
        DataType::Int32 => Arc::new(Int32Array::from(data)) as ArrayRef,
        DataType::Int64 => Arc::new(Int64Array::from(data)) as ArrayRef,
        DataType::UInt8 => Arc::new(UInt8Array::from(data)) as ArrayRef,
        DataType::UInt16 => Arc::new(UInt16Array::from(data)) as ArrayRef,
        DataType::UInt32 => Arc::new(UInt32Array::from(data)) as ArrayRef,
        DataType::UInt64 => Arc::new(UInt64Array::from(data)) as ArrayRef,
        DataType::Float16 => panic!("Float16 datatype not supported"),
        DataType::Float32 => Arc::new(Float32Array::from(data)) as ArrayRef,
        DataType::Float64 => Arc::new(Float64Array::from(data)) as ArrayRef,
        DataType::Date32(DateUnit::Day) => Arc::new(Date32Array::from(data)) as ArrayRef,
        DataType::Date64(DateUnit::Millisecond) => Arc::new(Date64Array::from(data)) as ArrayRef,
        DataType::Time32(TimeUnit::Second) => Arc::new(Time32SecondArray::from(data)) as ArrayRef,
        DataType::Time32(TimeUnit::Millisecond) => {
            Arc::new(Time32MillisecondArray::from(data)) as ArrayRef
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            Arc::new(Time64MicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Arc::new(Time64NanosecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            Arc::new(TimestampSecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Arc::new(TimestampMillisecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Arc::new(TimestampMicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Arc::new(TimestampNanosecondArray::from(data)) as ArrayRef
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            Arc::new(IntervalYearMonthArray::from(data)) as ArrayRef
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            Arc::new(IntervalDayTimeArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Second) => {
            Arc::new(DurationSecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            Arc::new(DurationMillisecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            Arc::new(DurationMicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            Arc::new(DurationNanosecondArray::from(data)) as ArrayRef
        }
        DataType::Binary => Arc::new(BinaryArray::from(data)) as ArrayRef,
        DataType::FixedSizeBinary(_) => Arc::new(FixedSizeBinaryArray::from(data)) as ArrayRef,
        DataType::Utf8 => Arc::new(StringArray::from(data)) as ArrayRef,
        DataType::List(_) => Arc::new(ListArray::from(data)) as ArrayRef,
        DataType::Struct(_) => Arc::new(StructArray::from(data)) as ArrayRef,
        DataType::FixedSizeList(_, _) => Arc::new(FixedSizeListArray::from(data)) as ArrayRef,
        DataType::Dictionary(ref key_type, _) => match key_type.as_ref() {
            DataType::Int8 => Arc::new(DictionaryArray::<Int8Type>::from(data)) as ArrayRef,
            DataType::Int16 => Arc::new(DictionaryArray::<Int16Type>::from(data)) as ArrayRef,
            DataType::Int32 => Arc::new(DictionaryArray::<Int32Type>::from(data)) as ArrayRef,
            DataType::Int64 => Arc::new(DictionaryArray::<Int64Type>::from(data)) as ArrayRef,
            DataType::UInt8 => Arc::new(DictionaryArray::<UInt8Type>::from(data)) as ArrayRef,
            DataType::UInt16 => Arc::new(DictionaryArray::<UInt16Type>::from(data)) as ArrayRef,
            DataType::UInt32 => Arc::new(DictionaryArray::<UInt32Type>::from(data)) as ArrayRef,
            DataType::UInt64 => Arc::new(DictionaryArray::<UInt64Type>::from(data)) as ArrayRef,
            dt => panic!("Unexpected dictionary key type {:?}", dt),
        },
        dt => panic!("Unexpected data type {:?}", dt),
    }
}
