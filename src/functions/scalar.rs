use arrow::array::*;
use arrow::array_ops;
use arrow::builder::*;
use arrow::datatypes::*;
use arrow::error::ArrowError;
use num::{abs, Signed, Zero};
use num_traits::Float;
use std::{ops::Add, ops::Div, ops::Mul, ops::Sub};


pub struct ScalarFunctions;

impl ScalarFunctions {
    /// Add two columns of `PrimitiveArray` type together
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
        array_ops::add(left, right).into()
    }
    /// Subtract two columns of `PrimitiveArray` type together
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

    /// Compute the absolute of a numeric array
    pub fn abs<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + Signed,
    {
        scalar_op(array, |array| Ok(abs(array)))
    }
    pub fn acos<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        let mut b = PrimitiveBuilder::<T>::new(array.len());
        for i in 0..array.len() {
            let index = i;
            if array.is_null(i) {
                b.append_null()?
            } else {
                let value: T::Native = array.value(i);
                b.append_value(num::Float::acos(array.value(i)))?
            }
        }
        Ok(b.finish())
    }
    pub fn add_months() {}
    // sort expression
    pub fn asc() {}
    pub fn asin<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::asin(array)))
    }
    pub fn atan<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::atan(array)))
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
    pub fn cbrt<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::cbrt(array)))
    }
    pub fn ceil<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::ceil(array)))
    }
    pub fn coalesce() {}
    pub fn concat() {}
    pub fn concat_ws() {}
    pub fn conv() {}
    pub fn corr() {}
    pub fn cos<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::cos(array)))
    }
    pub fn cosh<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::cosh(array)))
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
    pub fn degrees<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::to_degrees(array)))
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
    pub fn expm1<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::exp_m1(array)))
    }
    pub fn factorial() {}
    pub fn floor<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native> + num_traits::Float,
    {
        scalar_op(array, |array| Ok(num::Float::floor(array)))
    }
    pub fn format_number() {}
    pub fn format_string() {}
    pub fn from_json() {}
    pub fn from_unix_time() {}
    pub fn from_utc_timestamp() {}
    pub fn greatest() {}
    pub fn hash() {}
    pub fn hex() {}
    pub fn hour() {}
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
    pub fn lower() {}
    pub fn lpad() {}
    pub fn ltrim() {}
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
    fn rtrim() {}
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
    fn substring() {}
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
    fn trim() {}
    fn trunc() {}
    fn unbase64() {}
    fn unhex() {}
    fn unix_timestamp() {}
    fn upper() {}
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

#[cfg(test)]
mod tests {
    use super::*;
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
        let c: PrimitiveArray<Float64Type> = ScalarFunctions::abs(&a).unwrap();
        assert_eq!(5.2, c.value(0));
        assert_eq!(6.1, c.value(1));
        assert_eq!(7.3, c.value(2));
        assert_eq!(8.6, c.value(3));
        assert_eq!(0.0, c.value(4));
    }

    #[test]
    fn test_primitive_array_abs_i32() {
        let a = Int32Array::from(vec![-5, -6, 7, -8, -0]);
        let c: PrimitiveArray<Int32Type> = ScalarFunctions::abs(&a).unwrap();
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert_eq!(7, c.value(2));
        assert_eq!(8, c.value(3));
        assert_eq!(0, c.value(4));
    }

    #[test]
    fn test_primitive_array_acos_f64() {
        let a = Float64Array::from(vec![-0.2, 0.25, 0.75]);
        let c: PrimitiveArray<Float64Type> = ScalarFunctions::acos(&a).unwrap();
        assert_eq!(1.7721542475852274, c.value(0));
        assert_eq!(1.318116071652818, c.value(1));
        assert_eq!(0.7227342478134157, c.value(2));
    }

    #[test]
    fn test_primitive_array_acos_i32() {
        let a = Int32Array::from(vec![0, 1]);
        let c: PrimitiveArray<Float64Type> =
            ScalarFunctions::acos(&natural_cast::<Int32Type, Float64Type>(&a).unwrap()).unwrap();
        assert_eq!(1.5707963267948966, c.value(0));
        assert_eq!(0.0, c.value(1));
    }

    #[test]
    fn test_primitive_array_cos_f64() {
        let a = Float64Array::from(vec![-0.2, 0.25, 0.75]);
        let c: PrimitiveArray<Float64Type> = ScalarFunctions::cos(&a).unwrap();
        assert_eq!(0.9800665778412416, c.value(0));
        assert_eq!(0.9689124217106447, c.value(1));
        assert_eq!(0.7316888688738209, c.value(2));
    }

    #[test]
    fn test_primitive_array_cos_i32() {
        let a = Int32Array::from(vec![0, 1]);
        let c: PrimitiveArray<Float64Type> =
            ScalarFunctions::cos(&natural_cast::<Int32Type, Float64Type>(&a).unwrap()).unwrap();
        assert_eq!(1.0, c.value(0));
        assert_eq!(0.5403023058681398, c.value(1));
    }

}
