use arrow::array::Array;
use arrow::array::{Int64Array, PrimitiveArray};
use arrow::array_ops;
use arrow::datatypes::ArrowNumericType;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::datatypes::Int64Type;
use std::ops::Add;

struct AggregateFunctions;

impl AggregateFunctions {
    pub fn max<T>(arrays: Vec<&PrimitiveArray<T>>) -> Option<T::Native>
    where
        T: ArrowNumericType,
        T::Native: std::cmp::Ord,
    {
        arrays
            .iter()
            .map(|array| array_ops::max(array).unwrap())
            .max()
    }
    pub fn min<T>(arrays: Vec<&PrimitiveArray<T>>) -> Option<T::Native>
    where
        T: ArrowNumericType,
        T::Native: std::cmp::Ord,
    {
        arrays
            .iter()
            .map(|array| array_ops::max(array).unwrap())
            .max()
    }
    // pub fn avg<T>(array: &PrimitiveArray<T>) -> Option<f64>
    // where
    //     T: ArrowNumericType
    // {
    //     let sum = array_ops::sum(array);
    //     match sum {
    //         None => None,
    //         Some(sum) => {
    //             let count = AggregateFunctions::count(array).unwrap();
    //             let sum = sum as f64;
    //             Some(sum / count as f64)
    //         }
    //     }
    // }

    /// Count returns the number of non-null values in the array/column.
    ///
    /// For the number of all values, use `len()`
    pub fn count<T>(arrays: Vec<&PrimitiveArray<T>>) -> Option<i64>
    where
        T: ArrowPrimitiveType,
    {
        let mut sum = 0;
        arrays
            .iter()
            .for_each(|array| sum += (array.len() - array.null_count()) as i64);

        Some(sum)
    }
    fn count_distinct() {}
    pub fn sum<T>(arrays: Vec<&PrimitiveArray<T>>) -> Option<T::Native>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native>,
    {
        let mut sum = T::default_value();
        arrays
            .iter()
            .for_each(|array| sum = sum + array_ops::sum(array).unwrap_or(T::default_value()));

        Some(sum)
    }
    pub fn first() {}
    pub fn kurtosis() {}
    pub fn last() {}
    pub fn skewness() {}
    pub fn stddev() {}
    // TODO population and sample stddevs
    pub fn sum_distinct() {}
    pub fn variance() {}
    // TODO population and sample variances
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array};
    use arrow::datatypes::Int32Type;

    #[test]
    fn testit() {
        let a = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let b = Int32Array::from(vec![7, 6, 8, 9, 10]);
        let c = b.value(0);
        let d = Int32Array::from(vec![c]);
        //        assert_eq!(7.0, c);
        assert_eq!(a.value(0), b.value(1));
        assert_eq!(b.value(0), d.value(0));
    }

    #[test]
    fn test_aggregate_count() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let c = AggregateFunctions::count(vec![&a]).unwrap();
        assert_eq!(5, c);
    }
}
