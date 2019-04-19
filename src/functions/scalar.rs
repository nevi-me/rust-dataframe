use arrow::array::*;
use arrow::compute;
use arrow::builder::*;
use arrow::datatypes::*;
use arrow::error::ArrowError;
use num::{abs, Signed, Zero};
use num_traits::Float;
use std::{ops::Add, ops::Div, ops::Mul, ops::Sub};

extern crate test;

// use crate::error::{DataFrameError};

pub struct ScalarFunctions;

impl ScalarFunctions {
    /// Add two columns of `PrimitiveArray` type together
    pub fn add<T>(
        left: Vec<&PrimitiveArray<T>>,
        right: Vec<&PrimitiveArray<T>>,
    ) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native>
            + Sub<Output = T::Native>
            + Mul<Output = T::Native>
            + Div<Output = T::Native>
            + Zero,
    {
        left.iter()
            .zip(right.iter())
            .map(|(a, b)| compute::add(a, b).into())
            .collect()
    }
    /// Subtract two columns of `PrimitiveArray` type together
    pub fn subtract<T>(
        left: Vec<&PrimitiveArray<T>>,
        right: Vec<&PrimitiveArray<T>>,
    ) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native>
            + Sub<Output = T::Native>
            + Mul<Output = T::Native>
            + Div<Output = T::Native>
            + Zero,
    {
        left.iter()
            .zip(right.iter())
            .map(|(a, b)| compute::subtract(a, b).into())
            .collect()
    }
    pub fn divide<T>(
        left: Vec<&PrimitiveArray<T>>,
        right: Vec<&PrimitiveArray<T>>,
    ) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native>
            + Sub<Output = T::Native>
            + Mul<Output = T::Native>
            + Div<Output = T::Native>
            + Zero,
    {
        left.iter()
            .zip(right.iter())
            .map(|(a, b)| compute::divide(a, b).into())
            .collect()
    }
    pub fn multiply<T>(
        left: Vec<&PrimitiveArray<T>>,
        right: Vec<&PrimitiveArray<T>>,
    ) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native>
            + Sub<Output = T::Native>
            + Mul<Output = T::Native>
            + Div<Output = T::Native>
            + Zero,
    {
        left.iter()
            .zip(right.iter())
            .map(|(a, b)| compute::multiply(a, b).into())
            .collect()
    }

    /// Compute the absolute of a numeric array
    pub fn abs<T>(array: Vec<&PrimitiveArray<T>>) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Signed,
    {
        array.iter().map(|a| scalar_op(a, |a| Ok(abs(a)))).collect()
    }

    /// Compute the arccos of a decimal type array
    pub fn acos<T>(array: Vec<&PrimitiveArray<T>>) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: num_traits::Float,
    {
        array
            .iter()
            .map(|a| scalar_op(a, |a| Ok(num::Float::acos(a))))
            .collect()
    }
    pub fn add_months() {}
    // sort expression
    pub fn asc() {}
    pub fn asin<T>(array: Vec<&PrimitiveArray<T>>) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: num_traits::Float,
    {
        array
            .iter()
            .map(|a| scalar_op(a, |a| Ok(num::Float::asin(a))))
            .collect()
    }
    pub fn atan<T>(array: Vec<&PrimitiveArray<T>>) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: num_traits::Float,
    {
        array
            .iter()
            .map(|a| scalar_op(a, |a| Ok(num::Float::atan(a))))
            .collect()
    }
    pub fn atan2<T>(
        a: &PrimitiveArray<T>,
        b: &PrimitiveArray<T>,
    ) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        math_op(a, b, |a, b| Ok(num::Float::atan2(a, b)))
    }
    pub fn base64() {}
    pub fn bitwise_not() {}
    pub fn cbrt<T>(array: Vec<&PrimitiveArray<T>>) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: num_traits::Float,
    {
        array
            .iter()
            .map(|a| scalar_op(a, |a| Ok(num::Float::cbrt(a))))
            .collect()
    }
    pub fn ceil<T>(array: Vec<&PrimitiveArray<T>>) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: num_traits::Float,
    {
        array
            .iter()
            .map(|a| scalar_op(a, |a| Ok(num::Float::ceil(a))))
            .collect()
    }
    pub fn coalesce() {}
    pub fn concat() {}
    pub fn concat_ws() {}
    pub fn conv() {}
    pub fn corr() {}
    pub fn cos<T>(array: Vec<&PrimitiveArray<T>>) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: num_traits::Float,
    {
        array
            .iter()
            .map(|a| scalar_op(a, |a| Ok(num::Float::cos(a))))
            .collect()
    }
    pub fn cosh<T>(array: Vec<&PrimitiveArray<T>>) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: num_traits::Float,
    {
        array
            .iter()
            .map(|a| scalar_op(a, |a| Ok(num::Float::cosh(a))))
            .collect()
    }
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
    pub fn degrees<T>(array: Vec<&PrimitiveArray<T>>) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: num_traits::Float,
    {
        array
            .iter()
            .map(|a| scalar_op(a, |a| Ok(num::Float::to_degrees(a))))
            .collect()
    }
    // sort expression
    pub fn desc() {}
    pub fn exp<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::exp(array)))
    }

    // TODO might make sense as a DataFrame function
    pub fn explode() {}
    pub fn expm1<T>(array: Vec<&PrimitiveArray<T>>) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: num_traits::Float,
    {
        array
            .iter()
            .map(|a| scalar_op(a, |a| Ok(num::Float::exp_m1(a))))
            .collect()
    }
    pub fn factorial() {}
    pub fn floor<T>(array: Vec<&PrimitiveArray<T>>) -> Result<Vec<PrimitiveArray<T>>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: num_traits::Float,
    {
        array
            .iter()
            .map(|a| scalar_op(a, |a| Ok(num::Float::floor(a))))
            .collect()
    }
    pub fn format_number() {}
    pub fn format_string() {}
    pub fn from_json() {}
    pub fn from_unix_time() {}
    pub fn from_utc_timestamp() {}
    pub fn greatest() {}
    pub fn hash() {}
    pub fn hex() {}
    pub fn hour<T>(array: Vec<&PrimitiveArray<T>>) -> Result<Vec<Int32Array>, ArrowError>
    where
        T: ArrowNumericType + ArrowTemporalType,
        i64: std::convert::From<T::Native>
    {
        array
            .iter()
            .map(|a| compute::hour(a))
            .collect()
    }
    pub fn hypot<T>(
        a: &PrimitiveArray<T>,
        b: &PrimitiveArray<T>,
    ) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        math_op(a, b, |a, b| Ok(num::Float::hypot(a, b)))
    }
    pub fn initcap() {}
    pub fn last_day() {}
    pub fn least() {}
    pub fn length() {}
    pub fn levenshtein() {}
    pub fn lit() {}
    pub fn locate() {}
    pub fn log<T>(
        a: &PrimitiveArray<T>,
        b: &PrimitiveArray<T>,
    ) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        math_op(a, b, |a, b| Ok(num::Float::log(a, b)))
    }
    pub fn log10<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::log10(array)))
    }
    pub fn log2<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::log2(array)))
    }
    pub fn lower(arrays: Vec<&BinaryArray>) -> Result<Vec<BinaryArray>, ArrowError> {
        arrays
            .iter()
            .map(|array| {
                let mut b = BinaryBuilder::new(array.len());
                for i in 0..array.len() {
                    if array.is_null(i) {
                        b.append(false)?
                    } else {
                        match &::std::str::from_utf8(array.value(i)) {
                            Ok(string) => b.append_string(&string.to_lowercase())?,
                            _ => b.append(false)?,
                        }
                    }
                }
                Ok(b.finish())
            })
            .collect()
    }
    pub fn lpad() {}
    pub fn ltrim(array: Vec<&BinaryArray>) -> Result<Vec<BinaryArray>, ArrowError> {
        array
            .iter()
            .map(|a| string_op(a, |a| Ok(str::trim_start(a))))
            .collect()
    }
    pub fn md5() {}
    pub fn minute() {}
    fn monotonically_increasing_id() {}
    fn month() {}
    fn months_between() {}
    fn nanv1() {}
    fn next_day() {}
    fn ntile() {}
    // TODO pow requires usize, we might want to supply 2 arrays instead
    // fn pow<T>(a: &PrimitiveArray<T>, b: usize) -> Result<PrimitiveArray<T>, ArrowError>
    // where
    //     T: ArrowNumericType,
    //     T::Native: Mul<Output = T::Native>
    //         + num_traits::Num,
    // {
    //     math_op(a, b, |a, b| Ok(num::pow::pow(a, b)))
    // }
    fn quarter() {}
    fn radians<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::to_radians(array)))
    }
    fn rand() {}
    fn randn() {}
    fn regexp_extract() {}
    fn regexp_replace() {}
    fn repeat() {}
    // this can be a scalar and an array
    fn reverse() {}
    fn rint() {}
    // TODO Spark's round has a scale, whereas here we scale to 0
    fn round<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::round(array)))
    }
    fn rpad() {}
    pub fn rtrim(array: Vec<&BinaryArray>) -> Result<Vec<BinaryArray>, ArrowError> {
        array
            .iter()
            .map(|a| string_op(a, |a| Ok(str::trim_end(a))))
            .collect()
    }
    // fn schema_of_json() {}
    fn second() {}
    fn sequence() {}
    fn sha1() {}
    fn sha2() {}
    fn shift_left() {}
    fn shift_right() {}
    fn shift_right_unsigned() {}
    // collection function
    fn shuffle() {}
    fn signum() {}
    fn sin<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::sin(array)))
    }
    fn sinh<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::sinh(array)))
    }
    fn size() {}
    // collection function
    fn slice() {}
    fn sort_array() {}
    fn soundex() {}
    fn split() {}
    fn sqrt<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::sqrt(array)))
    }
    fn r#struct() {}
    fn substring(array: &BinaryArray, pos: usize, len: usize) -> Result<BinaryArray, ArrowError> {
        let mut b = BinaryBuilder::new(array.len());
        for i in 0..array.len() {
            let index = i;
            if array.is_null(i) {
                b.append(false)?;
            } else {
                match &::std::str::from_utf8(array.value(i)) {
                    Ok(string) => {
                        let s: String = string.chars().skip(pos).take(len).collect();
                        b.append_string(&s)?
                    }
                    _ => b.append(false)?,
                }
            }
        }
        Ok(b.finish())
    }
    fn substring_index() {}
    fn tan<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::tan(array)))
    }
    fn tanh<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::tanh(array)))
    }
    fn to_date() {}
    fn to_json() {}
    fn to_timestamp() {}
    fn to_utc_timestamp() {}
    fn translate() {}
    pub fn trim(array: Vec<&BinaryArray>) -> Result<Vec<BinaryArray>, ArrowError> {
        array
            .iter()
            .map(|a| string_op(a, |a| Ok(str::trim(a))))
            .collect()
    }
    fn trunc() {}
    fn unbase64() {}
    fn unhex() {}
    fn unix_timestamp() {}
    pub fn upper(arrays: Vec<&BinaryArray>) -> Result<Vec<BinaryArray>, ArrowError> {
        arrays
            .iter()
            .map(|array| {
                let mut b = BinaryBuilder::new(array.len());
                for i in 0..array.len() {
                    if array.is_null(i) {
                        b.append(false)?
                    } else {
                        match &::std::str::from_utf8(array.value(i)) {
                            Ok(string) => b.append_string(&string.to_uppercase())?,
                            _ => b.append(false)?,
                        }
                    }
                }
                Ok(b.finish())
            })
            .collect()
    }
    fn week_of_year() {}
    // this will be interesting to implement
    fn when() {}
    fn window() {}
    fn year() {}
}

/// Helper function to perform math lambda function on values from two arrays. If either left or
/// right value is null then the output value is also null, so `1 + null` is `null`.
fn math_op<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> Result<T::Native, ArrowError>,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform math operation on arrays of different length".to_string(),
        ));
    }
    let mut b = PrimitiveBuilder::<T>::new(left.len());
    for i in 0..left.len() {
        let index = i;
        if left.is_null(i) || right.is_null(i) {
            b.append_null()?;
        } else {
            b.append_value(op(left.value(index), right.value(index))?)?;
        }
    }
    Ok(b.finish())
}

fn scalar_op<T, F>(array: &PrimitiveArray<T>, op: F) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    F: Fn(T::Native) -> Result<T::Native, ArrowError>,
{
    let mut b = PrimitiveBuilder::<T>::new(array.len());
    for i in 0..array.len() {
        let index = i;
        if array.is_null(i) {
            b.append_null()?;
        } else {
            b.append_value(op(array.value(index))?)?;
        }
    }
    Ok(b.finish())
}

fn string_op<F>(array: &BinaryArray, op: F) -> Result<BinaryArray, ArrowError>
where
    F: Fn(&str) -> Result<&str, ArrowError>,
{
    let mut b = BinaryBuilder::new(array.len());
    for i in 0..array.len() {
        let index = i;
        if array.is_null(i) {
            b.append(false)?;
        } else {
            match &::std::str::from_utf8(array.value(i)) {
                Ok(string) => b.append_string(op(string)?)?,
                _ => b.append(false)?,
            }
        }
    }
    Ok(b.finish())
}

//pub fn cast<T, R>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<R>, ArrowError>
//    where
//        T: ArrowPrimitiveType,
//        R: ArrowPrimitiveType,
//        <R as arrow::datatypes::ArrowPrimitiveType>::Native: std::convert::From<<T as arrow::datatypes::ArrowPrimitiveType>::Native>
////        <R as arrow::datatypes::ArrowPrimitiveType>::Native: std::convert::From<<T as arrow::datatypes::ArrowPrimitiveType>::Native>,
//{
//    let mut b = PrimitiveBuilder::<R>::new(array.len());
//
//    let built = match (R::get_data_type(), T::get_data_type()) {
//        (_, DataType::Float64) => {
//            natural_cast(array)
//        }
//        (_, _) => {
//            unimplemented!()
//        }
//    };
//
//    built
//}

fn natural_cast<T, R>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<R>, ArrowError>
where
    T: ArrowNumericType,
    R: ArrowNumericType,
    <R as arrow::datatypes::ArrowPrimitiveType>::Native:
        std::convert::From<<T as arrow::datatypes::ArrowPrimitiveType>::Native>,
{
    let mut b = PrimitiveBuilder::<R>::new(array.len());

    for i in 0..array.len() {
        if array.is_null(i) {
            b.append_null()?;
        } else {
            let v = array.value(i);
            b.append_value(v.into())?;
        }
    }

    Ok(b.finish())
}

// pub fn cast(array: &Array, to_type: &str) -> Result<Array> {
//     // use different conversion functions depending on data types
//     use crate::datatypes::DataType::*;
//     match array.data_type() {
//         Int32 => panic!("Unsupported cast type"),
//         _ => panic!("Unsupported cast type")
//     }
// }

/// Casts the array from `ArrowNumericType<T>` to `ArrowNumericType<R>`.
///
/// If a value is null, the result is a null.
fn cast_numeric<T, R>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<R>, ArrowError>
where
    T: ArrowNumericType,
    R: ArrowNumericType,
    T::Native: num::NumCast,
    R::Native: num::NumCast,
{
    let mut b = PrimitiveBuilder::<R>::new(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            b.append_null()?;
        } else {
            // some casts return None, such as a negative value to u{8|16|32|64}
            match num::cast::cast(array.value(i)) {
                Some(v) => b.append_value(v)?,
                None => b.append_null()?,
            };
        }
    }

    Ok(b.finish())
}

fn cast_to_string<T>(array: &PrimitiveArray<T>) -> Result<BinaryArray, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ::std::string::ToString,
{
    let mut b = BinaryBuilder::new(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            b.append(false)?;
        } else {
            b.append_string(array.value(i).to_string().as_str())?
        }
    }

    Ok(b.finish())
}

fn cast_string_to_numeric<T>(array: &BinaryArray) -> crate::error::Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ::std::string::ToString,
    <T::Native as std::str::FromStr>::Err: std::fmt::Debug,
{
    let mut b = PrimitiveBuilder::<T>::new(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            b.append_null()?;
        } else {
            let string: &str = ::std::str::from_utf8(array.value(i))?;
            let value: T::Native = string.parse().unwrap();
            b.append_value(value)?
        }
    }

    Ok(b.finish())
}

fn cast_string_to_boolean(array: &BinaryArray) -> crate::error::Result<BooleanArray> {
    let mut b = BooleanBuilder::new(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            b.append_null()?;
        } else {
            let string: &str = ::std::str::from_utf8(array.value(i))?;
            let value: bool = string.to_lowercase().parse().unwrap();
            b.append_value(value)?
        }
    }

    Ok(b.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::scalar::test::Bencher;
    use arrow::array::*;

    #[test]
    fn test_primitive_array_cast_i32_to_f64() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        //        let b = Float64Array::from(vec![6.0, 7.0, 8.0, 9.0, 8.0]);
        let c: PrimitiveArray<Float64Type> = natural_cast(&a).unwrap();
        assert_eq!(5.0, c.value(0));
        assert_eq!(6.0, c.value(1));
        assert_eq!(7.0, c.value(2));
        assert_eq!(8.0, c.value(3));
        assert_eq!(9.0, c.value(4));
    }

    //    #[test]
    //    fn test_primitive_array_cast_f64_to_i32() {
    //        let a = Float64Array::from(vec![6.0, 7.0, 8.0, 9.0, 8.0]);
    ////        let b = Float64Array::from(vec![6.0, 7.0, 8.0, 9.0, 8.0]);
    //        let c: PrimitiveArray<Int32Type> = cast(&a).unwrap();
    //        assert_eq!(0, c.value(0));
    //        assert_eq!(6, c.value(1));
    //        assert_eq!(7, c.value(2));
    //        assert_eq!(8, c.value(3));
    //        assert_eq!(9, c.value(4));
    //    }

    #[test]
    fn test_primitive_array_abs_f64() {
        let a = Float64Array::from(vec![-5.2, -6.1, 7.3, -8.6, -0.0]);
        let c: &PrimitiveArray<Float64Type> = &ScalarFunctions::abs(vec![&a]).unwrap()[0];
        assert_eq!(5.2, c.value(0));
        assert_eq!(6.1, c.value(1));
        assert_eq!(7.3, c.value(2));
        assert_eq!(8.6, c.value(3));
        assert_eq!(0.0, c.value(4));
    }

    #[test]
    fn test_primitive_array_abs_i32() {
        let a = Int32Array::from(vec![-5, -6, 7, -8, -0]);
        let c: &PrimitiveArray<Int32Type> = &ScalarFunctions::abs(vec![&a]).unwrap()[0];
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert_eq!(7, c.value(2));
        assert_eq!(8, c.value(3));
        assert_eq!(0, c.value(4));
    }

    #[test]
    fn test_primitive_array_acos_f64() {
        let a = Float64Array::from(vec![-0.2, 0.25, 0.75]);
        let c: &PrimitiveArray<Float64Type> = &ScalarFunctions::acos(vec![&a]).unwrap()[0];
        assert_eq!(1.7721542475852274, c.value(0));
        assert_eq!(1.318116071652818, c.value(1));
        assert_eq!(0.7227342478134157, c.value(2));
    }

    #[test]
    fn test_primitive_array_acos_i32() {
        let a = Int32Array::from(vec![0, 1]);
        let c: &PrimitiveArray<Float64Type> =
            &ScalarFunctions::acos(vec![&natural_cast::<Int32Type, Float64Type>(&a).unwrap()])
                .unwrap()[0];
        assert_eq!(1.5707963267948966, c.value(0));
        assert_eq!(0.0, c.value(1));
    }

    #[test]
    fn test_primitive_array_cos_f64() {
        let a = Float64Array::from(vec![-0.2, 0.25, 0.75]);
        let c: &PrimitiveArray<Float64Type> = &ScalarFunctions::cos(vec![&a]).unwrap()[0];
        assert_eq!(0.9800665778412416, c.value(0));
        assert_eq!(0.9689124217106447, c.value(1));
        assert_eq!(0.7316888688738209, c.value(2));
    }

    #[test]
    fn test_primitive_array_cos_i32() {
        let a = Int32Array::from(vec![0, 1]);
        let c: &PrimitiveArray<Float64Type> =
            &ScalarFunctions::cos(vec![&natural_cast::<Int32Type, Float64Type>(&a).unwrap()])
                .unwrap()[0];
        assert_eq!(1.0, c.value(0));
        assert_eq!(0.5403023058681398, c.value(1));
    }

    #[test]
    fn test_i32_array_cast_f64() {
        let a = Int32Array::from(vec![None, Some(200), None, Some(-256)]);
        let b = cast_numeric::<Int32Type, Float64Type>(&a).unwrap();
        assert_eq!(true, b.is_null(0));
        assert_eq!(200f64, b.value(1));
        assert_eq!(true, b.is_null(2));
        assert_eq!(-256f64, b.value(3));
    }

    #[test]
    fn test_f32_array_cast_i64() {
        let a = Float32Array::from(vec![None, Some(200.0), None, Some(-256.0)]);
        let b: Int64Array = cast_numeric(&a).unwrap();
        assert_eq!(true, b.is_null(0));
        assert_eq!(200i64, b.value(1));
        assert_eq!(true, b.is_null(2));
        assert_eq!(-256i64, b.value(3));
    }

    #[test]
    fn test_f32_array_cast_uxx() {
        let a = Float32Array::from(vec![None, Some(200.0), None, Some(256.0)]);
        let b = cast_numeric::<Float32Type, Int32Type>(&a).unwrap();
        // dbg!(b);
        dbg!(b.value(0));
        dbg!(b.value(1));
        dbg!(b.value(2));
        dbg!(b.value(3));
        assert_eq!(true, b.is_null(0));
        assert_eq!(200i32, b.value(1));
        assert_eq!(true, b.is_null(2));
        assert_eq!(256i32, b.value(3));
    }

    #[test]
    fn test_i64_array_cast_u64() {
        let a = Int64Array::from(vec![None, Some(200), None, Some(256)]);
        let b = cast_numeric::<Int64Type, UInt64Type>(&a).unwrap();
        assert_eq!(true, b.is_null(0));
        assert_eq!(200u64, b.value(1));
        assert_eq!(true, b.is_null(2));
        assert_eq!(256u64, b.value(3));
    }

    #[test]
    fn test_str_upper_and_lower() {
        let mut builder = BinaryBuilder::new(14);
        builder.append_string("Hello").unwrap();
        builder.append_string("Arrow").unwrap();
        builder.append_string("农历新年").unwrap();
        let array = builder.finish();
        let lower = ScalarFunctions::lower(vec![&array]).unwrap();
        assert_eq!("hello", lower[0].get_string(0));
        assert_eq!("arrow", lower[0].get_string(1));
        assert_eq!("农历新年", lower[0].get_string(2));
        let upper = ScalarFunctions::upper(vec![&array]).unwrap();
        assert_eq!("HELLO", upper[0].get_string(0));
        assert_eq!("ARROW", upper[0].get_string(1));
        assert_eq!("农历新年", upper[0].get_string(2));
    }

    #[bench]
    fn bench_cos_f64(b: &mut Bencher) {
        let a = Int32Array::from(vec![None, Some(200), None, Some(-256), None]);
        b.iter(|| cast_numeric::<Int32Type, Float64Type>(&a).unwrap());
    }

    #[bench]
    fn bench_sin_f64(b: &mut Bencher) {
        let a = Int32Array::from(vec![None, Some(200), None, Some(-256), None]);
        b.iter(|| ScalarFunctions::multiply::<Int32Type>(vec![&a], vec![&a]).unwrap());
    }
}
