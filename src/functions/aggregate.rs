use arrow::array::PrimitiveArray;
use arrow::array_ops;
use arrow::datatypes::ArrowNumericType;
use std::ops::Add;

struct AggregateFunctions;

impl AggregateFunctions {
    pub fn max<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
    where
        T: ArrowNumericType,
    {
        array_ops::max(array)
    }
    pub fn min<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
    where
        T: ArrowNumericType,
    {
        array_ops::min(array)
    }
    pub fn avg() {}
    pub fn count() {}
    pub fn sum<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
    where
        T: ArrowNumericType,
        T::Native: Add<Output = T::Native>,
    {
        array_ops::sum(array)
    }
    pub fn first() {}
    pub fn floor() {}
    pub fn kurtosis() {}
    pub fn last() {}
    pub fn skewness() {}
    pub fn stddev() {}
    // TODO population and sample stddevs
    pub fn sum_distinct() {}
    pub fn variance() {}
    // TODO population and sample variances
}
