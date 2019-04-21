//!
//! Lazy evaluation for DataFrames
//!
//! This is an experimental initial implementation

use crate::dataframe::DataFrame;
use crate::expression::*;
use crate::functions::scalar::ScalarFunctions as Scalar;
use crate::table;
use ::std::sync::Arc;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::error::ArrowError;
use num::{abs, Signed, Zero};
use num_traits::Float;
use std::{ops::Add, ops::Div, ops::Mul, ops::Sub};

fn eval_numeric_scalar_op<T, F>(
    a: Vec<&PrimitiveArray<T>>,
    b: Vec<&PrimitiveArray<T>>,
    op: F,
) -> Vec<ArrayRef>
where
    T: ArrowNumericType,
    F: Fn(
        Vec<&PrimitiveArray<T>>,
        Vec<&PrimitiveArray<T>>,
    ) -> Result<Vec<PrimitiveArray<T>>, ArrowError>,
{
    op(a, b)
        .unwrap()
        .into_iter()
        .map(|arr| Arc::new(arr) as ArrayRef)
        .collect()
}

pub trait Evaluate {
    /// Evaluate an operation against a data source
    fn evaluate(self, operation: &Operation) -> Self;
}

impl Evaluate for DataFrame {
    fn evaluate(self, operation: &Operation) -> Self {
        // get the input columns from the dataframe
        let columns: Vec<&table::Column> = operation
            .inputs
            .clone()
            .into_iter()
            .map(|col: Column| self.column_by_name(&col.name))
            .collect();
        match &operation.expression {
            Expression::Scalar(expr) => match expr {
                //scalars that take 2 variables
                ScalarExpression::Add | ScalarExpression::Subtract | ScalarExpression::Divide | ScalarExpression::Multiply => {
                    // we are adding 2 columns together to create a third
                    let column: Vec<ArrayRef> =
                        if let ColumnType::Scalar(dtype) = &operation.output.column_type {
                            match dtype {
                                DataType::UInt16 => {
                                    // assign op to use
                                    let op = match expr {
                                        ScalarExpression::Add => Scalar::add,
                                        ScalarExpression::Subtract => Scalar::subtract,
                                        ScalarExpression::Divide => Scalar::divide,
                                        ScalarExpression::Multiply => Scalar::multiply
                                    };
                                    let a = table::col_to_prim_arrays::<UInt16Type>(columns.get(0).unwrap());
                                    let b = table::col_to_prim_arrays::<UInt16Type>(columns.get(1).unwrap());
                                    eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                                }
                                DataType::UInt32 => {
                                    let op = match expr {
                                        ScalarExpression::Add => Scalar::add,
                                        ScalarExpression::Subtract => Scalar::subtract,
                                        ScalarExpression::Divide => Scalar::divide,
                                        ScalarExpression::Multiply => Scalar::multiply
                                    };
                                    let a = table::col_to_prim_arrays::<UInt32Type>(columns.get(0).unwrap());
                                    let b = table::col_to_prim_arrays::<UInt32Type>(columns.get(1).unwrap());
                                    eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                                }
                                DataType::UInt64 => {
                                    let op = match expr {
                                        ScalarExpression::Add => Scalar::add,
                                        ScalarExpression::Subtract => Scalar::subtract,
                                        ScalarExpression::Divide => Scalar::divide,
                                        ScalarExpression::Multiply => Scalar::multiply
                                    };
                                    let a = table::col_to_prim_arrays::<UInt64Type>(columns.get(0).unwrap());
                                    let b = table::col_to_prim_arrays::<UInt64Type>(columns.get(1).unwrap());
                                    eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                                }
                                DataType::Int16 => {
                                    let op = match expr {
                                        ScalarExpression::Add => Scalar::add,
                                        ScalarExpression::Subtract => Scalar::subtract,
                                        ScalarExpression::Divide => Scalar::divide,
                                        ScalarExpression::Multiply => Scalar::multiply
                                    };
                                    let a = table::col_to_prim_arrays::<Int16Type>(columns.get(0).unwrap());
                                    let b = table::col_to_prim_arrays::<Int16Type>(columns.get(1).unwrap());
                                    eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                                }
                                DataType::Int32 => {
                                    let op = match expr {
                                        ScalarExpression::Add => Scalar::add,
                                        ScalarExpression::Subtract => Scalar::subtract,
                                        ScalarExpression::Divide => Scalar::divide,
                                        ScalarExpression::Multiply => Scalar::multiply
                                    };
                                    let a = table::col_to_prim_arrays::<Int32Type>(columns.get(0).unwrap());
                                    let b = table::col_to_prim_arrays::<Int32Type>(columns.get(1).unwrap());
                                    eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                                }
                                DataType::Int64 => {
                                    let op = match expr {
                                        ScalarExpression::Add => Scalar::add,
                                        ScalarExpression::Subtract => Scalar::subtract,
                                        ScalarExpression::Divide => Scalar::divide,
                                        ScalarExpression::Multiply => Scalar::multiply
                                    };
                                    let a = table::col_to_prim_arrays::<Int64Type>(columns.get(0).unwrap());
                                    let b = table::col_to_prim_arrays::<Int64Type>(columns.get(1).unwrap());
                                    eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                                }
                                DataType::Float32 => {
                                    let op = match expr {
                                        ScalarExpression::Add => Scalar::add,
                                        ScalarExpression::Subtract => Scalar::subtract,
                                        ScalarExpression::Divide => Scalar::divide,
                                        ScalarExpression::Multiply => Scalar::multiply
                                    };
                                    let a = table::col_to_prim_arrays::<Float32Type>(columns.get(0).unwrap());
                                    let b = table::col_to_prim_arrays::<Float32Type>(columns.get(1).unwrap());
                                    eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                                },
                                DataType::Float64 => {
                                    let op = match expr {
                                        ScalarExpression::Add => Scalar::add,
                                        ScalarExpression::Subtract => Scalar::subtract,
                                        ScalarExpression::Divide => Scalar::divide,
                                        ScalarExpression::Multiply => Scalar::multiply
                                    };
                                    let a = table::col_to_prim_arrays::<Float64Type>(columns.get(0).unwrap());
                                    let b = table::col_to_prim_arrays::<Float64Type>(columns.get(1).unwrap());
                                    eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                                },
                                _ => panic!("Unsupported operation"),
                            }
                        } else {
                            unreachable!()
                        };
                    self.with_column(
                        &operation.output.name,
                        table::Column::from_arrays(column, operation.output.clone().into()),
                    )
                }
                // _ => panic!("Scalar Expression {:?} not supported", expr),
            },
            Expression::Cast => {
                let input_col: &table::Column = columns.get(0).unwrap();
                let input_col = self.column_by_name(input_col.name());
                let arrays: Vec<ArrayRef> = input_col.data().chunks().iter().map(|array_ref: &ArrayRef| {
                    arrow::compute::cast(array_ref, &DataType::from(operation.output.column_type.clone())).unwrap()
                }).collect();
                self.with_column(&operation.output.name, table::Column::from_arrays(arrays, operation.output.clone().into()))
            }
            // expr @ _ => panic!("Expression {:?} not supported", expr),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::operation::{AddOperation, CastOperation, ScalarOperation};

    #[test]
    fn test_evaluation() {
        let dataframe = DataFrame::from_csv("./test/data/uk_cities_with_headers.csv", None);
        let a = Column {
            name: "lat".to_owned(),
            column_type: ColumnType::Scalar(DataType::Float64),
        };
        let b = Column {
            name: "lng".to_owned(),
            column_type: ColumnType::Scalar(DataType::Float64),
        };

        let add = AddOperation::transform(vec![a, b], Some("lat_lng".to_owned()), None).unwrap();

        let mut out_dataframe = dataframe;
        for op in add {
            out_dataframe = out_dataframe.evaluate(&op);
        }

        // cast lat_lng to string
        let lat_lng = out_dataframe.expr_column_by_name("lat_lng");
        let cast = CastOperation::transform(
            vec![lat_lng],
            Some("lat_lng".to_owned()),
            Some(DataType::Utf8),
        )
        .unwrap();

        for op in cast {
            out_dataframe = out_dataframe.evaluate(&op);
        }

        assert_eq!(
            "Schema { fields: [Field { name: \"city\", data_type: Utf8, nullable: false }, Field { name: \"lat\", data_type: Float64, nullable: false }, Field { name: \"lng\", data_type: Float64, nullable: false }, Field { name: \"lat_lng\", data_type: Utf8, nullable: true }] }",
            format!("{:?}", out_dataframe.schema())
        );
    }
}
