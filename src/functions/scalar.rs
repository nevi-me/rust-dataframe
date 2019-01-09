use arrow::array::{Array, PrimitiveArray};
use arrow::array_ops;
use arrow::builder::{ArrayBuilder, PrimitiveArrayBuilder};
use arrow::datatypes::{Float64Type, Int8Type};
use arrow::error::ArrowError;
use num::Zero;
use std::{ops::Add, ops::Div, ops::Mul, ops::Sub};
use arrow::datatypes::ArrowNumericType;
use arrow::array::Float64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::ArrowPrimitiveType;

pub struct ScalarFunctions;

impl ScalarFunctions {
    pub fn add<T>(
        left: &PrimitiveArray<T>,
        right: &PrimitiveArray<T>,
    ) -> Result<PrimitiveArray<T>, ArrowError>
        where
            T: ArrowNumericType,
            T::Native: Add<Output = T::Native>
            + Sub<Output = T::Native>
            + Mul<Output = T::Native>
            + Div<Output = T::Native>
            + Zero,
    {
        array_ops::add(left, right)
    }
    pub fn subtract<T>(
        left: &PrimitiveArray<T>,
        right: &PrimitiveArray<T>,
    ) -> Result<PrimitiveArray<T>, ArrowError>
        where
            T: ArrowNumericType,
            T::Native: Add<Output = T::Native>
            + Sub<Output = T::Native>
            + Mul<Output = T::Native>
            + Div<Output = T::Native>
            + Zero,
    {
        array_ops::subtract(left, right)
    }
    pub fn divide<T>(
        left: &PrimitiveArray<T>,
        right: &PrimitiveArray<T>,
    ) -> Result<PrimitiveArray<T>, ArrowError>
        where
            T: ArrowNumericType,
            T::Native: Add<Output = T::Native>
            + Sub<Output = T::Native>
            + Mul<Output = T::Native>
            + Div<Output = T::Native>
            + Zero,
    {
        array_ops::divide(left, right)
    }
    pub fn multiply<T>(
        left: &PrimitiveArray<T>,
        right: &PrimitiveArray<T>,
    ) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native>
            + Sub<Output = T::Native>
            + Mul<Output = T::Native>
            + Div<Output = T::Native>
            + Zero,
    {
        array_ops::multiply(left, right)
    }

//    pub fn abs<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
//        where
//            T: ArrowNumericType,
//            T::Native: Add<Output = T::Native>
//            + Sub<Output = T::Native>
//            + Mul<Output = T::Native>
//            + Div<Output = T::Native>
//            + Zero,
//    {
//        let mut b = PrimitiveArrayBuilder::<T>::new(array.len());
//        for i in 0..array.len() {
//            let index = i;
//            if array.is_null(i) {
//                b.push_null()?
//            } else {
//                let value: T::Native = array.value(i);
//                value.cos();
//                b.push(array.value(i).cos())?
//            }
//        }
//        Ok(b.finish())
//    }
    pub fn acos() {}
    pub fn add_months() {}
    // sort expression
    pub fn asc() {}
    pub fn asin() {}
    pub fn atan() {}
    pub fn atan2() {}
    pub fn base64() {}
    pub fn bitwise_not() {}
    pub fn cbrt() {}
    pub fn ceil() {}
    pub fn coalesce() {}
    pub fn concat() {}
    pub fn concat_ws() {}
    pub fn conv() {}
    pub fn corr() {}
    pub fn cos() {}
    pub fn cosh() {}
    pub fn crc32() {}
    pub fn current_date() {}
    pub fn current_timestamp() {}
    pub fn date_add() {}
    pub fn date_format() {}
    pub fn date_sub() {}
    pub fn date_trunc() {}
    pub fn date_diff() {}
    pub fn day_of_month() {}
    pub fn day_of_week() {}
    pub fn day_of_year() {}
    pub fn degrees() {}
    // sort expression
    pub fn desc() {}
    pub fn exp() {}

    // TODO might make sense as a DataFrame function
    pub fn explode() {}
    pub fn expm1() {}
    pub fn factorial() {}
    pub fn format_number() {}
    pub fn format_string() {}
    pub fn from_json() {}
    pub fn from_unix_time() {}
    pub fn from_utc_timestamp() {}
    pub fn greatest() {}
    pub fn hash() {}
    pub fn hex() {}
    pub fn hour() {}
    pub fn hypot() {}
    pub fn initcap() {}
    pub fn last_day() {}
    pub fn least() {}
    pub fn length() {}
}

pub fn cos<T>(array: &PrimitiveArray<T>) -> Result<Float64Array, ArrowError>
    where
        T: ArrowNumericType,
{
    let mut b = PrimitiveArrayBuilder::<Float64Type>::new(array.len());
    let data = array.data();
    for i in 0..data.len() {
        if data.is_null(i) {
            b.push_null()?
        } else {
//            let c = Float64Type::from(array.value(i));
            match T::get_data_type() {
                DataType::Int8 {} => {
                    let v = array.value(i);
                }
                _ => unimplemented!("Not implemented")
            }
            let aa = PrimitiveArray::<Float64Type>::from(array.value(i));
            b.push_null()?
        }
    }
    Ok(b.finish())
}
