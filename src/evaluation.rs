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

/// Evaluate a numeric scalar op that takes 2 input arrays of the same type
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

/// Evaluate a float scalar op that takes 1 input array
fn eval_float1_scalar_op<T, F>(a: Vec<&PrimitiveArray<T>>, op: F) -> Vec<ArrayRef>
where
    T: ArrowNumericType,
    T::Native: num_traits::Float,
    F: Fn(Vec<&PrimitiveArray<T>>) -> Result<Vec<PrimitiveArray<T>>, ArrowError>,
{
    op(a)
        .unwrap()
        .into_iter()
        .map(|arr| Arc::new(arr) as ArrayRef)
        .collect()
}

pub trait Evaluate {
    /// Evaluate an overall transformation against a transformation
    fn evaluate(self, comp: &Vec<Computation>) -> Self;
    /// Evaluate a calculation transformation
    fn calculate(self, operation: &Operation) -> Self;
    /// Evaluate a `Read` operation, returning the read data
    fn read(reader: &Reader) -> Self;
}

impl Evaluate for DataFrame {
    fn evaluate(self, comp: &Vec<Computation>) -> Self {
        use Transformation::*;
        let mut frame = self;
        // get the input columns from the dataframe
        for c in comp.iter().rev() {
            for transform in &c.transformations {
                frame = match transform {
                    Aggregate => panic!("aggregations not supported"),
                    Calculate(operation) => frame.calculate(&operation),
                    Group => panic!(),
                    Join(_, _, _) => panic!(),
                    Project => panic!(),
                    Read(reader) => Self::read(&reader),
                };
            }
        }

        frame
    }
    fn calculate(self, operation: &Operation) -> Self {
        let columns: Vec<&table::Column> = operation
            .inputs
            .clone()
            .into_iter()
            .map(|col: Column| self.column_by_name(&col.name))
            .collect();
        match &operation.function {
            Function::Scalar(expr) => match expr {
                // scalars that take 2 variables
                ScalarFunction::Add
                | ScalarFunction::Subtract
                | ScalarFunction::Divide
                | ScalarFunction::Multiply => {
                    // we are adding 2 columns together to create a third
                    let column: Vec<ArrayRef> = if let ColumnType::Scalar(dtype) =
                        &operation.output.column_type
                    {
                        match dtype {
                            DataType::UInt16 => {
                                // assign op to use
                                let op = match expr {
                                    ScalarFunction::Add => Scalar::add,
                                    ScalarFunction::Subtract => Scalar::subtract,
                                    ScalarFunction::Divide => Scalar::divide,
                                    ScalarFunction::Multiply => Scalar::multiply,
                                    _ => unreachable!(),
                                };
                                let a = table::col_to_prim_arrays::<UInt16Type>(
                                    columns.get(0).unwrap(),
                                );
                                let b = table::col_to_prim_arrays::<UInt16Type>(
                                    columns.get(1).unwrap(),
                                );
                                eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                            }
                            DataType::UInt32 => {
                                let op = match expr {
                                    ScalarFunction::Add => Scalar::add,
                                    ScalarFunction::Subtract => Scalar::subtract,
                                    ScalarFunction::Divide => Scalar::divide,
                                    ScalarFunction::Multiply => Scalar::multiply,
                                    _ => unreachable!(),
                                };
                                let a = table::col_to_prim_arrays::<UInt32Type>(
                                    columns.get(0).unwrap(),
                                );
                                let b = table::col_to_prim_arrays::<UInt32Type>(
                                    columns.get(1).unwrap(),
                                );
                                eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                            }
                            DataType::UInt64 => {
                                let op = match expr {
                                    ScalarFunction::Add => Scalar::add,
                                    ScalarFunction::Subtract => Scalar::subtract,
                                    ScalarFunction::Divide => Scalar::divide,
                                    ScalarFunction::Multiply => Scalar::multiply,
                                    _ => unreachable!(),
                                };
                                let a = table::col_to_prim_arrays::<UInt64Type>(
                                    columns.get(0).unwrap(),
                                );
                                let b = table::col_to_prim_arrays::<UInt64Type>(
                                    columns.get(1).unwrap(),
                                );
                                eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                            }
                            DataType::Int16 => {
                                let op = match expr {
                                    ScalarFunction::Add => Scalar::add,
                                    ScalarFunction::Subtract => Scalar::subtract,
                                    ScalarFunction::Divide => Scalar::divide,
                                    ScalarFunction::Multiply => Scalar::multiply,
                                    _ => unreachable!(),
                                };
                                let a =
                                    table::col_to_prim_arrays::<Int16Type>(columns.get(0).unwrap());
                                let b =
                                    table::col_to_prim_arrays::<Int16Type>(columns.get(1).unwrap());
                                eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                            }
                            DataType::Int32 => {
                                let op = match expr {
                                    ScalarFunction::Add => Scalar::add,
                                    ScalarFunction::Subtract => Scalar::subtract,
                                    ScalarFunction::Divide => Scalar::divide,
                                    ScalarFunction::Multiply => Scalar::multiply,
                                    _ => unreachable!(),
                                };
                                let a =
                                    table::col_to_prim_arrays::<Int32Type>(columns.get(0).unwrap());
                                let b =
                                    table::col_to_prim_arrays::<Int32Type>(columns.get(1).unwrap());
                                eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                            }
                            DataType::Int64 => {
                                let op = match expr {
                                    ScalarFunction::Add => Scalar::add,
                                    ScalarFunction::Subtract => Scalar::subtract,
                                    ScalarFunction::Divide => Scalar::divide,
                                    ScalarFunction::Multiply => Scalar::multiply,
                                    _ => unreachable!(),
                                };
                                let a =
                                    table::col_to_prim_arrays::<Int64Type>(columns.get(0).unwrap());
                                let b =
                                    table::col_to_prim_arrays::<Int64Type>(columns.get(1).unwrap());
                                eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                            }
                            DataType::Float32 => {
                                let op = match expr {
                                    ScalarFunction::Add => Scalar::add,
                                    ScalarFunction::Subtract => Scalar::subtract,
                                    ScalarFunction::Divide => Scalar::divide,
                                    ScalarFunction::Multiply => Scalar::multiply,
                                    _ => unreachable!(),
                                };
                                let a = table::col_to_prim_arrays::<Float32Type>(
                                    columns.get(0).unwrap(),
                                );
                                let b = table::col_to_prim_arrays::<Float32Type>(
                                    columns.get(1).unwrap(),
                                );
                                eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                            }
                            DataType::Float64 => {
                                let op = match expr {
                                    ScalarFunction::Add => Scalar::add,
                                    ScalarFunction::Subtract => Scalar::subtract,
                                    ScalarFunction::Divide => Scalar::divide,
                                    ScalarFunction::Multiply => Scalar::multiply,
                                    _ => unreachable!(),
                                };
                                let a = table::col_to_prim_arrays::<Float64Type>(
                                    columns.get(0).unwrap(),
                                );
                                let b = table::col_to_prim_arrays::<Float64Type>(
                                    columns.get(1).unwrap(),
                                );
                                eval_numeric_scalar_op(a, b, |a, b| op(a, b))
                            }
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
                // scalars that take 1 variable
                ScalarFunction::Cosine | ScalarFunction::Sine | ScalarFunction::Tangent => {
                    let column: Vec<ArrayRef> = if let ColumnType::Scalar(dtype) =
                        &operation.output.column_type
                    {
                        match dtype {
                            DataType::Float32 => {
                                // assign op to use
                                let op = match expr {
                                    ScalarFunction::Sine => Scalar::sin,
                                    ScalarFunction::Cosine => Scalar::cos,
                                    ScalarFunction::Tangent => Scalar::tan,
                                    // ScalarFunction::Multiply => Scalar::multiply,
                                    _ => panic!("unsupported function {:?}", expr),
                                };
                                let a = table::col_to_prim_arrays::<Float32Type>(
                                    columns.get(0).unwrap(),
                                );
                                eval_float1_scalar_op(a, |a| op(a))
                            }
                            DataType::Float64 => {
                                let op = match expr {
                                    ScalarFunction::Sine => Scalar::sin,
                                    ScalarFunction::Cosine => Scalar::cos,
                                    ScalarFunction::Tangent => Scalar::tan,
                                    // ScalarFunction::Multiply => Scalar::multiply,
                                    _ => panic!("unsupported function {:?}", expr),
                                };
                                let a = table::col_to_prim_arrays::<Float64Type>(
                                    columns.get(0).unwrap(),
                                );
                                eval_float1_scalar_op(a, |a| op(a))
                            }
                            _ => {
                                panic!("Expecting float datatype for operation, found {:?}", dtype)
                            }
                        }
                    } else {
                        unreachable!()
                    };
                    self.with_column(
                        &operation.output.name,
                        table::Column::from_arrays(column, operation.output.clone().into()),
                    )
                }
                _ => panic!("Scalar Function {:?} not supported", expr),
            },
            Function::Cast => {
                let input_col: &table::Column = columns.get(0).unwrap();
                let input_col = self.column_by_name(input_col.name());
                let arrays: Vec<ArrayRef> = input_col
                    .data()
                    .chunks()
                    .iter()
                    .map(|array_ref: &ArrayRef| {
                        arrow::compute::cast(
                            array_ref,
                            &DataType::from(operation.output.column_type.clone()),
                        )
                        .unwrap()
                    })
                    .collect();
                self.with_column(
                    &operation.output.name,
                    table::Column::from_arrays(arrays, operation.output.clone().into()),
                )
            }
            Function::Rename => self.with_column_renamed(
                &operation.inputs.first().unwrap().name,
                &operation.output.name,
            ),
            expr @ _ => panic!("Function {:?} not supported", expr),
        }
    }
    fn read(reader: &Reader) -> Self {
        use DataSourceType::*;
        match &reader.source {
            // TODO build with options, good first issue
            Csv(path, options) => DataFrame::from_csv(&path, None),
            Json(path) => DataFrame::from_json(&path, None),
            Parquet(path) => unimplemented!("Parquet data sources not ye supported"),
            Feather(path) => DataFrame::from_feather(&path).unwrap(),
            Sql(table, options) => match &options.db {
                SqlDatabase::Postgres => DataFrame::from_sql(&options.connection_string, &table),
                t @ _ => unimplemented!("SQL Protocol {:?} not supported", t),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::io::datasource::DataSourceEval;
    use crate::lazyframe::LazyFrame;
    use crate::operation::{AddOperation, CastOperation, ScalarOperation};

    #[test]
    fn test_lazy_evaluation() {
        let reader = Reader {
            source: DataSourceType::Csv(
                "./test/data/uk_cities_with_headers.csv".to_owned(),
                CsvReadOptions {
                    has_headers: true,
                    batch_size: 1024,
                    delimiter: None,
                    max_records: Some(1024),
                    projection: None,
                },
            ),
        };
        let compute = Computation::compute_read(&reader);
        // read data
        let mut frame = LazyFrame::read(compute);
        // rename a column
        frame = frame.with_column_renamed("city", "town");
        // add a column as a calculation of 2 columns
        frame = frame
            .with_column(
                "sin_lat",
                Function::Scalar(ScalarFunction::Sine),
                vec!["lat"],
                None,
            )
            .unwrap();
        frame = frame
            .with_column(
                "sin_lng",
                Function::Scalar(ScalarFunction::Sine),
                vec!["lng"],
                None,
            )
            .unwrap();
        let ops = frame.expression.unroll();

        let mut dataframe = DataFrame::empty();
        dataframe = dataframe.evaluate(&ops);
        dataframe.to_csv("./target/test_output.csv").unwrap();
    }
}
