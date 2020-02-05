//!
//! Lazy evaluation for DataFrames
//!
//! This is an experimental initial implementation

use crate::dataframe::DataFrame;
use crate::error::DataFrameError;
use crate::expression::*;
use crate::functions::scalar::ScalarFunctions as ScalarFn;
use crate::table;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::error::ArrowError;
use num::{abs, Signed, Zero};
use num_traits::Float;
use std::sync::Arc;
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

pub trait Evaluate: Sized {
    /// Evaluate a list of computations
    fn evaluate(self, comp: &Vec<Computation>) -> Self;
    /// Evaluate a calculation transformation
    fn calculate(self, calculation: &Calculation) -> Self;
    /// Evaluate a `Read` operation, returning the read data
    fn read(reader: &Reader) -> Self;
    /// Evaluate a write operation, and write the data to the writer
    fn write(self, writer: &Writer) -> Result<(), DataFrameError>;
}

impl Evaluate for DataFrame {
    fn evaluate(self, comp: &Vec<Computation>) -> Self {
        use Transformation::*;
        let mut frame = self;
        // get the input columns from the dataframe
        for c in comp.iter().rev() {
            for transform in &c.transformations {
                frame = match transform {
                    GroupAggregate(_, _) => panic!("aggregations not supported"),
                    Calculate(operation) => frame.calculate(&operation),
                    Join(a, b, criteria) => {
                        let mut frame_a = DataFrame::empty();
                        frame_a = frame_a.evaluate(a);
                        let mut frame_b = DataFrame::empty();
                        frame_b = frame_b.evaluate(b);
                        // TODO: make sure that joined names follow same logic as LazyFrame
                        frame_a.join(&frame_b, &criteria)
                    }
                    Select(cols) => frame.select(cols.iter().map(|s| s.as_str()).collect()),
                    Drop(cols) => frame.drop(cols.iter().map(|s| s.as_str()).collect()),
                    Read(reader) => Self::read(&reader),
                    Filter(cond) => frame.filter(cond),
                    Limit(size) => frame.limit(*size),
                    Sort(criteria) => frame.sort(criteria),
                };
            }
        }

        frame
    }
    fn calculate(self, calculation: &Calculation) -> Self {
        let columns: Vec<&table::Column> = calculation
            .inputs
            .clone()
            .into_iter()
            .map(|col: Column| self.column_by_name(&col.name))
            .collect();
        match &calculation.function {
            Function::Scalar(expr) => match expr {
                // scalars that take 2 variables
                ScalarFunction::Add
                | ScalarFunction::Subtract
                | ScalarFunction::Divide
                | ScalarFunction::Multiply => {
                    // we are adding 2 columns together to create a third
                    let column: Vec<ArrayRef> = if let ColumnType::Scalar(dtype) =
                        &calculation.output.column_type
                    {
                        match dtype {
                            DataType::UInt16 => {
                                // assign op to use
                                let op = match expr {
                                    ScalarFunction::Add => ScalarFn::add,
                                    ScalarFunction::Subtract => ScalarFn::subtract,
                                    ScalarFunction::Divide => ScalarFn::divide,
                                    ScalarFunction::Multiply => ScalarFn::multiply,
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
                                    ScalarFunction::Add => ScalarFn::add,
                                    ScalarFunction::Subtract => ScalarFn::subtract,
                                    ScalarFunction::Divide => ScalarFn::divide,
                                    ScalarFunction::Multiply => ScalarFn::multiply,
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
                                    ScalarFunction::Add => ScalarFn::add,
                                    ScalarFunction::Subtract => ScalarFn::subtract,
                                    ScalarFunction::Divide => ScalarFn::divide,
                                    ScalarFunction::Multiply => ScalarFn::multiply,
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
                                    ScalarFunction::Add => ScalarFn::add,
                                    ScalarFunction::Subtract => ScalarFn::subtract,
                                    ScalarFunction::Divide => ScalarFn::divide,
                                    ScalarFunction::Multiply => ScalarFn::multiply,
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
                                    ScalarFunction::Add => ScalarFn::add,
                                    ScalarFunction::Subtract => ScalarFn::subtract,
                                    ScalarFunction::Divide => ScalarFn::divide,
                                    ScalarFunction::Multiply => ScalarFn::multiply,
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
                                    ScalarFunction::Add => ScalarFn::add,
                                    ScalarFunction::Subtract => ScalarFn::subtract,
                                    ScalarFunction::Divide => ScalarFn::divide,
                                    ScalarFunction::Multiply => ScalarFn::multiply,
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
                                    ScalarFunction::Add => ScalarFn::add,
                                    ScalarFunction::Subtract => ScalarFn::subtract,
                                    ScalarFunction::Divide => ScalarFn::divide,
                                    ScalarFunction::Multiply => ScalarFn::multiply,
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
                                    ScalarFunction::Add => ScalarFn::add,
                                    ScalarFunction::Subtract => ScalarFn::subtract,
                                    ScalarFunction::Divide => ScalarFn::divide,
                                    ScalarFunction::Multiply => ScalarFn::multiply,
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
                        &calculation.output.name,
                        table::Column::from_arrays(column, calculation.output.clone().into()),
                    )
                }
                // scalars that take 1 variable
                ScalarFunction::Cosine | ScalarFunction::Sine | ScalarFunction::Tangent => {
                    let column: Vec<ArrayRef> = if let ColumnType::Scalar(dtype) =
                        &calculation.output.column_type
                    {
                        match dtype {
                            DataType::Float32 => {
                                // assign op to use
                                let op = match expr {
                                    ScalarFunction::Sine => ScalarFn::sin,
                                    ScalarFunction::Cosine => ScalarFn::cos,
                                    ScalarFunction::Tangent => ScalarFn::tan,
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
                                    ScalarFunction::Sine => ScalarFn::sin,
                                    ScalarFunction::Cosine => ScalarFn::cos,
                                    ScalarFunction::Tangent => ScalarFn::tan,
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
                        &calculation.output.name,
                        table::Column::from_arrays(column, calculation.output.clone().into()),
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
                            &DataType::from(calculation.output.column_type.clone()),
                        )
                        .unwrap()
                    })
                    .collect();
                self.with_column(
                    &calculation.output.name,
                    table::Column::from_arrays(arrays, calculation.output.clone().into()),
                )
            }
            Function::Rename => self.with_column_renamed(
                &calculation.inputs.first().unwrap().name,
                &calculation.output.name,
            ),
            Function::Limit(limit) => self.limit(*limit),
            Function::Filter(filter) => self.filter(filter),
            expr @ _ => panic!("Function {:?} not supported", expr),
        }
    }
    fn read(reader: &Reader) -> Self {
        use DataSourceType::*;
        match &reader.source {
            // TODO build with options, good first issue
            Csv(path, options) => DataFrame::from_csv(&path, None),
            Json(path) => DataFrame::from_json(&path, None),
            Parquet(path) => unimplemented!("Parquet data sources not yet supported"),
            Arrow(path) => DataFrame::from_arrow(&path).unwrap(),
            Sql(table, options) => match &options.db {
                SqlDatabase::Postgres => {
                    DataFrame::from_sql_table(&options.connection_string, &table)
                }
                t @ _ => unimplemented!("SQL Protocol {:?} not yet supported", t),
            },
        }
    }
    fn write(self, writer: &Writer) -> Result<(), DataFrameError> {
        use DataSinkType::*;
        match &writer.sink {
            Csv(path, _options) => self.to_csv(&path),
            Arrow(path) => self.to_arrow(&path),
        }
        .map_err(|arrow_err| arrow_err.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::io::datasource::DataSourceEval;
    use crate::lazyframe::LazyFrame;
    use crate::operation::scalar::{AddOperation, CastOperation, ScalarOperation};

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
        frame = frame.limit(25);
        // filter for where (lat > 55 or (sin_lat <= sin_lng or lng > -10))
        // frame = frame.filter(BooleanFilter::Or(
        //     Box::new(BooleanFilter::Gt(
        //         Box::new(BooleanFilter::Input(BooleanInput::Column(Column {
        //             name: "lat".to_owned(),
        //             column_type: ColumnType::Scalar(DataType::Float64),
        //         }))),
        //         Box::new(BooleanFilter::Input(BooleanInput::Scalar(Scalar::Float64(
        //             55.0,
        //         )))),
        //     )),
        //     Box::new(BooleanFilter::Or(
        //         Box::new(BooleanFilter::Le(
        //             Box::new(BooleanFilter::Input(BooleanInput::Column(Column {
        //                 name: "sin_lat".to_owned(),
        //                 column_type: ColumnType::Scalar(DataType::Float64),
        //             }))),
        //             Box::new(BooleanFilter::Input(BooleanInput::Column(Column {
        //                 name: "sin_lng".to_owned(),
        //                 column_type: ColumnType::Scalar(DataType::Float64),
        //             }))),
        //         )),
        //         Box::new(BooleanFilter::Gt(
        //             Box::new(BooleanFilter::Input(BooleanInput::Column(Column {
        //                 name: "lng".to_owned(),
        //                 column_type: ColumnType::Scalar(DataType::Float64),
        //             }))),
        //             Box::new(BooleanFilter::Input(BooleanInput::Scalar(Scalar::Float64(
        //                 -10.0,
        //             )))),
        //         )),
        //     )),
        // ));
        let ops = frame.expression.unroll();

        let mut dataframe = DataFrame::empty();
        dataframe = dataframe.evaluate(&ops);
        dataframe.to_csv("./target/test_output.csv").unwrap();
        assert_eq!(25, dataframe.num_rows());
    }

    #[test]
    fn test_join() {}
}
