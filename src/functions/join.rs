//! Join algorithms

use std::collections::{HashMap, HashSet};

use arrow::array::*;
use arrow::datatypes::*;
use byteorder::{LittleEndian, WriteBytesExt};

use crate::{
    dataframe::DataFrame,
    expression::{JoinCriteria, JoinType},
    table::{col_to_prim_arrays, Column},
};

/// Calculate matching indices for equality joins
///
/// Might return incorrect results if the comparison columns do not have the same type,
///  it is the caller's responsibility to cast data to appropriate types first.
pub(crate) fn calc_equijoin_indices(
    left: &DataFrame,
    right: &DataFrame,
    criteria: &JoinCriteria,
) -> (Vec<Option<u32>>, Vec<Option<u32>>) {
    // how about operating on dataframes?

    let left_columns = criteria
        .criteria
        .iter()
        .map(|(l, _)| left.column_by_name(l.as_str()).to_array().unwrap())
        .collect::<Vec<ArrayRef>>();

    let right_columns = criteria
        .criteria
        .iter()
        .map(|(_, r)| right.column_by_name(r.as_str()).to_array().unwrap())
        .collect::<Vec<ArrayRef>>();

    // build hash inputs for left criteria
    let (left_hash, left_nulls) = build_hash_inputs(left_columns, left.num_rows());
    // build hash inputs for right criteria
    let (right_hash, right_nulls) = build_hash_inputs(right_columns, right.num_rows());

    let mut left_indices = vec![];
    let mut right_indices = vec![];
    match criteria.join_type {
        JoinType::LeftJoin => {
            left_hash
                .iter()
                .for_each(|(k, left): (&Vec<u8>, &Vec<usize>)| {
                    // find indices in right index
                    let right = right_hash.get(k).cloned().unwrap_or_default();
                    for l in left {
                        if right.is_empty() {
                            left_indices.push(Some(*l as u32));
                            right_indices.push(None);
                        } else {
                            for r in right.clone() {
                                left_indices.push(Some(*l as u32));
                                right_indices.push(Some(r as u32));
                            }
                        }
                    }
                });
            left_nulls
                .iter()
                .for_each(|v| left_indices.push(Some(*v as u32)));
            let mut r = vec![None; left_nulls.len()];
            right_indices.append(&mut r);
        }
        JoinType::RightJoin => {
            dbg!(&right_hash.values());
            right_hash
                .iter()
                .for_each(|(k, right): (&Vec<u8>, &Vec<usize>)| {
                    // find indices in left index
                    let left = left_hash.get(k).cloned().unwrap_or_default();
                    for r in right {
                        if left.is_empty() {
                            left_indices.push(None);
                            right_indices.push(Some(*r as u32));
                        } else {
                            for l in left.clone() {
                                left_indices.push(Some(l as u32));
                                right_indices.push(Some(*r as u32));
                            }
                        }
                    }
                });
            right_nulls
                .iter()
                .for_each(|v| right_indices.push(Some(*v as u32)));
            let mut r = vec![None; right_nulls.len()];
            left_indices.append(&mut r);
        }
        JoinType::InnerJoin => {
            left_hash
                .iter()
                .for_each(|(k, left): (&Vec<u8>, &Vec<usize>)| {
                    // find indices in right index
                    if let Some(v) = right_hash.get(k) {
                        for l in left {
                            for r in v {
                                left_indices.push(Some(*l as u32));
                                right_indices.push(Some(*r as u32));
                            }
                        }
                    }
                });
        }
        JoinType::FullJoin => {
            left_hash
                .iter()
                .for_each(|(k, left): (&Vec<u8>, &Vec<usize>)| {
                    // find indices in right index
                    if let Some(v) = right_hash.get(k) {
                        for l in left {
                            for r in v {
                                left_indices.push(Some(*l as u32));
                                right_indices.push(Some(*r as u32));
                            }
                        }
                    }
                });
            left_nulls
                .iter()
                .for_each(|v| left_indices.push(Some(*v as u32)));
            let mut r = vec![None; left_nulls.len()];
            right_indices.append(&mut r);
            right_nulls
                .iter()
                .for_each(|v| right_indices.push(Some(*v as u32)));
            let mut r = vec![None; right_nulls.len()];
            left_indices.append(&mut r);
        }
    };

    (left_indices, right_indices)
}

fn build_hash_inputs(
    arrays: Vec<ArrayRef>,
    table_len: usize,
) -> (HashMap<Vec<u8>, Vec<usize>>, HashSet<usize>) {
    // create hashmaps for left and right
    let mut hash = HashMap::with_capacity(table_len);
    let mut null_set = HashSet::new();
    let mut bytes = (0..table_len)
        .map(|i| (i, vec![]))
        .collect::<Vec<(usize, Vec<u8>)>>();

    arrays
        .into_iter()
        .for_each(|col: ArrayRef| match col.data_type() {
            DataType::Boolean => {
                populate_primitive_bytes::<BooleanType>(col, &mut bytes, &mut null_set);
            }
            DataType::Int8 => {
                populate_primitive_bytes::<Int8Type>(col, &mut bytes, &mut null_set);
            }
            DataType::Int16 => {
                populate_primitive_bytes::<Int16Type>(col, &mut bytes, &mut null_set);
            }
            DataType::Int32 => {
                populate_primitive_bytes::<Int32Type>(col, &mut bytes, &mut null_set);
            }
            DataType::Int64 => {
                populate_primitive_bytes::<Int64Type>(col, &mut bytes, &mut null_set);
            }
            DataType::UInt8 => {
                populate_primitive_bytes::<UInt8Type>(col, &mut bytes, &mut null_set);
            }
            DataType::UInt16 => {
                populate_primitive_bytes::<UInt16Type>(col, &mut bytes, &mut null_set);
            }
            DataType::UInt32 => {
                populate_primitive_bytes::<UInt32Type>(col, &mut bytes, &mut null_set);
            }
            DataType::UInt64 => {
                populate_primitive_bytes::<UInt64Type>(col, &mut bytes, &mut null_set);
            }
            DataType::Float16 => unreachable!(),
            DataType::Float32 => {
                populate_primitive_bytes::<Float32Type>(col, &mut bytes, &mut null_set);
            }
            DataType::Float64 => {
                populate_primitive_bytes::<Float64Type>(col, &mut bytes, &mut null_set);
            }
            DataType::Timestamp(_, _) => {}
            DataType::Date32(_) => {}
            DataType::Date64(_) => {}
            DataType::Time32(_) => {}
            DataType::Time64(_) => {}
            DataType::Duration(_) => {}
            DataType::Interval(_) => {}
            DataType::Binary => {}
            DataType::FixedSizeBinary(_) => {}
            DataType::Utf8 => {}
            DataType::List(_) => {}
            DataType::FixedSizeList(_, _) => {}
            DataType::Struct(_) => {}
            DataType::Dictionary(_, _) => {}
            DataType::Union(_) => {}
            DataType::Null => {}
            DataType::LargeBinary => {}
            DataType::LargeUtf8 => {}
            DataType::LargeList(_) => {}
        });

    // populate hashmap
    bytes.into_iter().for_each(|(index, bytes)| {
        hash.entry(bytes).or_insert_with(Vec::new).push(index);
    });

    // return results
    (hash, null_set)
}

fn populate_primitive_bytes<T: ArrowPrimitiveType>(
    array: ArrayRef,
    bytes: &mut Vec<(usize, Vec<u8>)>,
    null_set: &mut HashSet<usize>,
) {
    let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    // check if array element is null?
    for i in 0..array.len() {
        if !array.is_null(i) && !null_set.contains(&i) {
            // array contains value, append bytes
            bytes[i]
                .1
                .append(&mut array.value(i).to_byte_slice().to_vec());
        } else {
            null_set.insert(i);
        }
    }
}
