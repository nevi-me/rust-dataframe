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

pub trait Evaluate {
    /// Evaluate an overall transformation against a transformation
    fn evaluate(self, transformation: &Transformation) -> Self;
    /// Evaluate a calculation transformation
    fn calculate(self, operation: &Operation) -> Self;
    /// Evaluate a `Read` operation, returning the read data
    fn read(reader: &Reader) -> Self;
}

impl Evaluate for DataFrame {
    fn evaluate(self, transformation: &Transformation) -> Self {
        use Transformation::*;
        // get the input columns from the dataframe
        match transformation {
            Aggregate => panic!("aggregations not supported"),
            Calculate(operation) => self.calculate(operation),
            Group => panic!(),
            Join(_, _, _) => panic!(),
            Project => panic!(),
            Read(reader) => Self::read(&reader),
        }
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
                //scalars that take 2 variables
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
    use crate::operation::{AddOperation, CastOperation, ScalarOperation};

    // #[test]
    // fn test_lazy_ops() {
    //     let mut computation: Computation = Computation::empty();
    //     let mut transforms: Vec<Transformation> = vec![];
    //     // read data
    //     let reader = Reader {
    //         source: DataSourceType::Csv(
    //             "./test/data/uk_cities_with_headers.csv".to_owned(),
    //             CsvReadOptions {
    //                 has_headers: true,
    //                 delimiter: None,
    //                 max_records: Some(1024),
    //                 batch_size: 1024,
    //                 projection: None,
    //             },
    //         ),
    //     };
    //     // let in_dataset = reader.get_dataset().unwrap();
    //     // transforms.push(Transformation::Read(reader));
    //     computation = Computation::compute_transform(vec![], vec![Transformation::Read(reader)]);

    //     let chain_ds = computation.output.clone();

    //     let a = chain_ds.get_column("lat").unwrap().1;
    //     let b = chain_ds.get_column("lng").unwrap().1;

    //     let add =
    //         AddOperation::transform(vec![a.clone(), b.clone()], Some("lat_lng".to_owned()), None)
    //             .unwrap();

    //     for op in add {
    //         transforms.push(Transformation::Calculate(op));
    //     }

    //     // TODO I should ideally have a dataset as an output, having lat_lng as one of the columns

    //     // cast lat_lng to string
    //     let lat_lng = out_dataframe.expr_column_by_name("lat_lng");
    //     let cast = CastOperation::transform(
    //         vec![lat_lng],
    //         Some("lat_lng".to_owned()),
    //         Some(DataType::Utf8),
    //     )
    //     .unwrap();

    //     for op in cast {
    //         out_dataframe = out_dataframe.evaluate(&Transformation::Calculate(op.clone()));
    //     }

    //     assert_eq!(
    //         "Schema { fields: [Field { name: \"city\", data_type: Utf8, nullable: false }, Field { name: \"lat\", data_type: Float64, nullable: false }, Field { name: \"lng\", data_type: Float64, nullable: false }, Field { name: \"lat_lng\", data_type: Utf8, nullable: true }] }",
    //         format!("{:?}", out_dataframe.schema())
    //     );
    // }
}
