use array_tool::vec::*;
use arrow::array::*;
use arrow::builder::*;
use arrow::datatypes::*;
use arrow::error::ArrowError;

struct ArrayFunctions;

impl ArrayFunctions {
    // pub fn array<T>(arrays: Vec<&PrimitiveArray<T>>) -> Result<ListArray, ArrowError> {

    // }
    /// Checks whether the array contains the given value.
    ///
    /// Returns null if the array is null, otherwise a true or false.
    pub fn array_contains<T>(array: &ListArray, val: T::Native) -> Result<BooleanArray, ArrowError>
    where
        T: ArrowPrimitiveType + ArrowNumericType,
    {
        let mut b = BooleanBuilder::new(array.len());
        // get array datatype so we can downcast appropriately
        let data_type = array.value_type();
        for i in 0..array.len() {
            if array.is_null(i) {
                b.append_null()?
            } else {
                let values = array.values();
                let values = values.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                let values = values.value_slice(
                    array.value_offset(i) as usize,
                    array.value_length(i) as usize,
                );
                let contains: bool = values.contains(&val);
                b.append_value(contains)?;
            }
        }
        Ok(b.finish())
    }
    fn array_join() {}
    fn array_distinct<T>(array: &ListArray) -> Result<ListArray, ArrowError>
    where
        T: ArrowPrimitiveType + ArrowNumericType,
    {
        let values_builder: PrimitiveBuilder<T> = PrimitiveBuilder::new(array.values().len());
        let mut b = ListBuilder::new(values_builder);
        // get array datatype so we can downcast appropriately
        let data_type = array.value_type();
        for i in 0..array.len() {
            if array.is_null(i) {
                b.append(true)?
            } else {
                let values = array.values();
                let values = values.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                let values = values
                    .value_slice(
                        array.value_offset(i) as usize,
                        array.value_length(i) as usize,
                    )
                    .to_vec();
                let u = values.unique();
                // TODO check how nulls are treated here
                u.iter().for_each(|x| b.values().append_value(*x).unwrap());
            }
        }
        Ok(b.finish())
    }
    pub fn array_except<T>(a: &ListArray, b: &ListArray) -> Result<ListArray, ArrowError>
    where
        T: ArrowPrimitiveType + ArrowNumericType,
        T::Native: std::cmp::PartialEq<T::Native> + std::cmp::Ord,
    {
        // check that lengths of both arrays are equal
        if a.len() != b.len() {
            return Err(ArrowError::ComputeError(
                "Expected array a and b to have the same length".to_string(),
            ));
        }
        let values_builder: PrimitiveBuilder<T> = PrimitiveBuilder::new(a.values().len());
        let mut c = ListBuilder::new(values_builder);
        // get array datatype so we can downcast appropriately
        let data_type = a.value_type();
        for i in 0..a.len() {
            if a.is_null(i) {
                c.append(true)?
            } else {
                let a_values = a.values();
                let a_values = a_values
                    .as_any()
                    .downcast_ref::<PrimitiveArray<T>>()
                    .unwrap();
                let a_values = a_values
                    .value_slice(a.value_offset(i) as usize, a.value_length(i) as usize)
                    .to_vec();
                let b_values = b.values();
                let b_values = b_values
                    .as_any()
                    .downcast_ref::<PrimitiveArray<T>>()
                    .unwrap();
                let b_values = b_values
                    .value_slice(b.value_offset(i) as usize, b.value_length(i) as usize)
                    .to_vec();

                let u = a_values.uniq(b_values);
                // TODO check how nulls are treated here
                u.iter().for_each(|x| c.values().append_value(*x).unwrap());
                c.append(true)?;
            }
        }
        Ok(c.finish())
    }
    pub fn array_intersect<T>(a: &ListArray, b: &ListArray) -> Result<ListArray, ArrowError>
    where
        T: ArrowPrimitiveType + ArrowNumericType,
        T::Native: std::cmp::PartialEq<T::Native> + std::cmp::Ord,
    {
        // check that lengths of both arrays are equal
        if a.len() != b.len() {
            return Err(ArrowError::ComputeError(
                "Expected array a and b to have the same length".to_string(),
            ));
        }
        let values_builder: PrimitiveBuilder<T> = PrimitiveBuilder::new(a.values().len());
        let mut c = ListBuilder::new(values_builder);
        // get array datatype so we can downcast appropriately
        let data_type = a.value_type();
        for i in 0..a.len() {
            if a.is_null(i) {
                c.append(true)?
            } else {
                let a_values = a.values();
                let a_values = a_values
                    .as_any()
                    .downcast_ref::<PrimitiveArray<T>>()
                    .unwrap();
                let a_values = a_values
                    .value_slice(a.value_offset(i) as usize, a.value_length(i) as usize)
                    .to_vec();
                let b_values = b.values();
                let b_values = b_values
                    .as_any()
                    .downcast_ref::<PrimitiveArray<T>>()
                    .unwrap();
                let b_values = b_values
                    .value_slice(b.value_offset(i) as usize, b.value_length(i) as usize)
                    .to_vec();

                let u = a_values.intersect(b_values);
                // TODO check how nulls are treated here
                u.iter().for_each(|x| c.values().append_value(*x).unwrap());
                c.append(true)?;
            }
        }
        Ok(c.finish())
    }
    pub fn array_max<T>(array: &ListArray) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowPrimitiveType + ArrowNumericType,
        T::Native: ::std::cmp::Ord,
    {
        let mut b = PrimitiveBuilder::<T>::new(array.len());
        // get array datatype so we can downcast appropriately
        let data_type = array.value_type();
        for i in 0..array.len() {
            if array.is_null(i) {
                b.append_null()?
            } else {
                let values = array.values();
                let values = values.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                let values = values.value_slice(
                    array.value_offset(i) as usize,
                    array.value_length(i) as usize,
                );
                let max: T::Native = *values.iter().max().unwrap();
                b.append_value(max)?;
            }
        }
        Ok(b.finish())
    }
    pub fn array_min<T>(array: &ListArray) -> Result<PrimitiveArray<T>, ArrowError>
    where
        T: ArrowPrimitiveType + ArrowNumericType,
        T::Native: ::std::cmp::Ord,
    {
        let mut b = PrimitiveBuilder::<T>::new(array.len());
        // get array datatype so we can downcast appropriately
        let data_type = array.value_type();
        for i in 0..array.len() {
            if array.is_null(i) {
                b.append_null()?
            } else {
                let values = array.values();
                let values = values.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                let values = values.value_slice(
                    array.value_offset(i) as usize,
                    array.value_length(i) as usize,
                );
                let min: T::Native = *values.iter().min().unwrap();
                b.append_value(min)?;
            }
        }
        Ok(b.finish())
    }

    /// Locates the position of the first occurrence of the given value in the given array.
    /// Returns 0 if element is not found, otherwise a 1-based index with the position in the array.
    pub fn array_position<T>(array: &ListArray, val: T::Native) -> Result<Int32Array, ArrowError>
    where
        T: ArrowPrimitiveType + ArrowNumericType,
        T::Native: std::cmp::PartialEq<T::Native>,
    {
        let mut b = Int32Builder::new(array.len());
        // get array datatype so we can downcast appropriately
        let data_type = array.value_type();
        for i in 0..array.len() {
            if array.is_null(i) {
                b.append_value(0)?
            } else {
                let values = array.values();
                let values = values.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                let values = values.value_slice(
                    array.value_offset(i) as usize,
                    array.value_length(i) as usize,
                );
                let pos = values.iter().position(|x| x == &val);
                match pos {
                    Some(pos) => b.append_value((pos + 1) as i32)?,
                    None => b.append_value(0)?,
                };
            }
        }
        Ok(b.finish())
    }

    /// Remove all elements that equal the given element in the array
    pub fn array_remove<T>(array: &ListArray, val: T::Native) -> Result<ListArray, ArrowError>
    where
        T: ArrowPrimitiveType + ArrowNumericType,
        T::Native: std::cmp::PartialEq<T::Native>,
    {
        let values_builder: PrimitiveBuilder<T> = PrimitiveBuilder::new(array.values().len());
        let mut b = ListBuilder::new(values_builder);
        // get array datatype so we can downcast appropriately
        let data_type = array.value_type();
        for i in 0..array.len() {
            if array.is_null(i) {
                b.append(true)?
            } else {
                let values = array.values();
                let values = values.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                let values = values.value_slice(
                    array.value_offset(i) as usize,
                    array.value_length(i) as usize,
                );
                values.iter().for_each(|x| {
                    // append value if it should not be removed
                    if x != &val {
                        b.values().append_value(*x).unwrap();
                    }
                });
                b.append(true)?;
            }
        }
        Ok(b.finish())
    }

    /// TODO: extract repetitive code and share with other array fns that use `array_tool` crate
    pub fn array_repeat<T>(array: &ListArray, count: i32) -> Result<ListArray, ArrowError>
    where
        T: ArrowPrimitiveType + ArrowNumericType,
        T::Native: std::cmp::PartialEq<T::Native> + std::cmp::Ord,
    {
        let values_builder: PrimitiveBuilder<T> = PrimitiveBuilder::new(array.values().len());
        let mut c = ListBuilder::new(values_builder);
        // get array datatype so we can downcast appropriately
        let data_type = array.value_type();
        for i in 0..array.len() {
            if array.is_null(i) {
                c.append(true)?
            } else {
                let values = array.values();
                let values = values.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                let values = values
                    .value_slice(
                        array.value_offset(i) as usize,
                        array.value_length(i) as usize,
                    )
                    .to_vec();

                let u = values.times(count);
                // TODO check how nulls are treated here
                u.iter().for_each(|x| c.values().append_value(*x).unwrap());
                c.append(true)?;
            }
        }
        Ok(c.finish())
    }

    /// Sorts the input array in ascending order.
    ///
    /// TODO: document null treatment, and make it behave like Spark does.
    fn array_sort<T>(array: &ListArray) -> Result<ListArray, ArrowError>
    where
        T: ArrowPrimitiveType + ArrowNumericType,
        T::Native: std::cmp::PartialEq<T::Native> + std::cmp::Ord,
    {
        let values_builder: PrimitiveBuilder<T> = PrimitiveBuilder::new(array.values().len());
        let mut b = ListBuilder::new(values_builder);
        // get array datatype so we can downcast appropriately
        let data_type = array.value_type();
        for i in 0..array.len() {
            if array.is_null(i) {
                b.append(true)?
            } else {
                let values = array.values();
                let values = values.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                let values = values.value_slice(
                    array.value_offset(i) as usize,
                    array.value_length(i) as usize,
                );
                let mut v = values.to_vec();
                v.sort();
                // TODO check how nulls are treated here
                v.iter().for_each(|x| b.values().append_value(*x).unwrap());
                b.append(true)?;
            }
        }
        Ok(b.finish())
    }
    pub fn array_union<T>(a: &ListArray, b: &ListArray) -> Result<ListArray, ArrowError>
    where
        T: ArrowPrimitiveType + ArrowNumericType,
        T::Native: std::cmp::PartialEq<T::Native> + std::cmp::Ord,
    {
        // check that lengths of both arrays are equal
        if a.len() != b.len() {
            return Err(ArrowError::ComputeError(
                "Expected array a and b to have the same length".to_string(),
            ));
        }
        let values_builder: PrimitiveBuilder<T> = PrimitiveBuilder::new(a.values().len());
        let mut c = ListBuilder::new(values_builder);
        // get array datatype so we can downcast appropriately
        let data_type = a.value_type();
        for i in 0..a.len() {
            if a.is_null(i) {
                c.append(true)?
            } else {
                let a_values = a.values();
                let a_values = a_values
                    .as_any()
                    .downcast_ref::<PrimitiveArray<T>>()
                    .unwrap();
                let a_values = a_values
                    .value_slice(a.value_offset(i) as usize, a.value_length(i) as usize)
                    .to_vec();
                let b_values = b.values();
                let b_values = b_values
                    .as_any()
                    .downcast_ref::<PrimitiveArray<T>>()
                    .unwrap();
                let b_values = b_values
                    .value_slice(b.value_offset(i) as usize, b.value_length(i) as usize)
                    .to_vec();

                let u = a_values.union(b_values);
                // TODO check how nulls are treated here
                u.iter().for_each(|x| c.values().append_value(*x).unwrap());
                c.append(true)?;
            }
        }
        Ok(c.finish())
    }
    fn arrays_overlap() {}
    fn arrays_zip() {}

    // maybe aggregate
    fn collect_list() {}
    fn collect_set() {}

    // maps and unions
    fn map_concat() {}
    fn map_from_arrays() {}
    fn map_from_entries() {}
    fn map_keys() {}
    fn map_values() {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::array_data::*;
    use arrow::buffer::Buffer;
    use arrow::datatypes::*;
    use std::sync::Arc;

    #[test]
    fn test_array_contains_i32s() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(16)
            .add_buffer(Buffer::from(
                &[0, 0, 0, 1, 2, 1, 3, 4, 5, 1, 3, 2, 3, 2, 8, 3].to_byte_slice(),
            ))
            .build();

        let value_offsets = Buffer::from(&[0, 3, 6, 8, 12, 14, 16].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type = DataType::List(Box::new(DataType::Int32));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(6)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        let list_array = ListArray::from(list_data);

        let bools = ArrayFunctions::array_contains::<Int32Type>(&list_array, 2).unwrap();
        assert_eq!(false, bools.value(0));
        assert_eq!(true, bools.value(1));
        assert_eq!(false, bools.value(2));
        assert_eq!(true, bools.value(3));
        assert_eq!(true, bools.value(4));
        assert_eq!(false, bools.value(5));
    }

    #[test]
    fn test_array_contains_i64s() {
        // Construct a value array
        let value_data =
            Int64Array::from(vec![0, 0, 0, 1, 2, 1, 3, 4, 5, 1, 3, 2, 3, 2, 8, 3]).data();

        let value_offsets = Buffer::from(&[0, 3, 6, 8, 12, 14, 16].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type = DataType::List(Box::new(DataType::Int64));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(6)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        let list_array = ListArray::from(list_data);

        let bools = ArrayFunctions::array_contains::<Int64Type>(&list_array, 2).unwrap();
        assert_eq!(false, bools.value(0));
        assert_eq!(true, bools.value(1));
        assert_eq!(false, bools.value(2));
        assert_eq!(true, bools.value(3));
        assert_eq!(true, bools.value(4));
        assert_eq!(false, bools.value(5));
    }

    #[test]
    fn test_array_contains_f64s() {
        // Construct a value array
        let value_data = Float64Array::from(vec![
            0.0, 0.0, 0.0, 1.0, 2.0, 1.0, 3.0, 4.0, 5.0, 1.0, 3.0, 2.0, 3.0, 2.0, 8.0, 3.0,
        ])
        .data();

        let value_offsets = Buffer::from(&[0, 3, 6, 8, 12, 14, 16].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type = DataType::List(Box::new(DataType::Float64));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(6)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        let list_array = ListArray::from(list_data);

        let bools = ArrayFunctions::array_contains::<Float64Type>(&list_array, 2.0).unwrap();
        assert_eq!(false, bools.value(0));
        assert_eq!(true, bools.value(1));
        assert_eq!(false, bools.value(2));
        assert_eq!(true, bools.value(3));
        assert_eq!(true, bools.value(4));
        assert_eq!(false, bools.value(5));
    }

    #[test]
    fn test_array_position() {
        // Construct a value array
        let value_data =
            Int64Array::from(vec![0, 0, 0, 1, 2, 1, 3, 4, 5, 1, 3, 2, 3, 2, 8, 3]).data();

        let value_offsets = Buffer::from(&[0, 3, 6, 8, 12, 14, 16].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type = DataType::List(Box::new(DataType::Int64));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(6)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        let list_array = ListArray::from(list_data);

        let bools = ArrayFunctions::array_position::<Int64Type>(&list_array, 2).unwrap();
        assert_eq!(0, bools.value(0));
        assert_eq!(2, bools.value(1));
        assert_eq!(0, bools.value(2));
        assert_eq!(4, bools.value(3));
        assert_eq!(2, bools.value(4));
        assert_eq!(0, bools.value(5));
    }

    #[test]
    fn test_array_remove() {
        // Construct a value array
        let value_data =
            Int64Array::from(vec![0, 0, 0, 1, 2, 1, 3, 4, 5, 1, 3, 2, 3, 2, 8, 3]).data();

        let value_offsets = Buffer::from(&[0, 3, 6, 8, 12, 14, 16].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type = DataType::List(Box::new(DataType::Int64));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(6)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        let list_array = ListArray::from(list_data);

        let b = ArrayFunctions::array_remove::<Int64Type>(&list_array, 2).unwrap();
        let values = b.values();
        let values = values
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap();

        assert_eq!(6, b.len());
        assert_eq!(13, values.len());
        assert_eq!(0, b.value_offset(0));
        assert_eq!(3, b.value_offset(1));
        assert_eq!(5, b.value_offset(2));
        assert_eq!(7, b.value_offset(3));
        assert_eq!(10, b.value_offset(4));
        assert_eq!(11, b.value_offset(5));
    }

    #[test]
    fn test_array_sort() {
        // Construct a value array
        let value_data =
            Int64Array::from(vec![0, 0, 0, 1, 2, 1, 3, 4, 5, 1, 3, 2, 3, 2, 8, 3]).data();

        let value_offsets = Buffer::from(&[0, 3, 6, 8, 12, 14, 16].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type = DataType::List(Box::new(DataType::Int64));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(6)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        let list_array = ListArray::from(list_data);

        let b = ArrayFunctions::array_sort::<Int64Type>(&list_array).unwrap();
        let values = b.values();
        let values = values
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap();

        assert_eq!(6, b.len());
        assert_eq!(16, values.len());
        assert_eq!(0, b.value_offset(0));
        assert_eq!(3, b.value_offset(1));
        assert_eq!(6, b.value_offset(2));
        assert_eq!(8, b.value_offset(3));
        assert_eq!(12, b.value_offset(4));
        assert_eq!(14, b.value_offset(5));

        let expected = Int64Array::from(vec![0, 0, 0, 1, 1, 2, 3, 4, 1, 2, 3, 5, 2, 3, 3, 8]);
        for i in 0..b.len() {
            let x = values.value_slice(b.value_offset(i) as usize, b.value_length(i) as usize);
            let d = expected.value_slice(b.value_offset(i) as usize, b.value_length(i) as usize);
            assert_eq!(x, d);
        }
    }

    #[test]
    fn test_array_union() {
        // Construct a value array
        let value_data =
            Int64Array::from(vec![0, 0, 0, 1, 2, 1, 3, 4, 5, 1, 3, 2, 3, 2, 8, 3]).data();

        let value_offsets = Buffer::from(&[0, 3, 6, 8, 12, 14, 16].to_byte_slice());

        let value_data =
            Int64Array::from(vec![0, 0, 0, 1, 2, 1, 3, 4, 5, 1, 3, 2, 3, 2, 8, 3]).data();

        let value_offsets = Buffer::from(&[0, 3, 6, 8, 12, 14, 16].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type = DataType::List(Box::new(DataType::Int64));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(6)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        let list_array = ListArray::from(list_data);

        let b = ArrayFunctions::array_sort::<Int64Type>(&list_array).unwrap();
        let values = b.values();
        let values = values
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap();

        assert_eq!(6, b.len());
        assert_eq!(16, values.len());
        assert_eq!(0, b.value_offset(0));
        assert_eq!(3, b.value_offset(1));
        assert_eq!(6, b.value_offset(2));
        assert_eq!(8, b.value_offset(3));
        assert_eq!(12, b.value_offset(4));
        assert_eq!(14, b.value_offset(5));

        let expected = Int64Array::from(vec![0, 0, 0, 1, 1, 2, 3, 4, 1, 2, 3, 5, 2, 3, 3, 8]);
        for i in 0..b.len() {
            let x = values.value_slice(b.value_offset(i) as usize, b.value_length(i) as usize);
            let d = expected.value_slice(b.value_offset(i) as usize, b.value_length(i) as usize);
            assert_eq!(x, d);
        }
    }
}
