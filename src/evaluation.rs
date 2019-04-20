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
use arrow::datatypes::DataType;
use arrow::error::ArrowError;

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
                ScalarExpression::Add => {
                    // we are adding 2 columns together to create a third
                    let column: Vec<ArrayRef> =
                        if let ColumnType::Scalar(dtype) = &operation.output.column_type {
                            match dtype {
                                DataType::Int16 => Scalar::add(
                                    table::column_to_arrays_i16(columns.get(0).unwrap()),
                                    table::column_to_arrays_i16(columns.get(1).unwrap()),
                                )
                                .unwrap()
                                .into_iter()
                                .map(|arr| Arc::new(arr) as ArrayRef)
                                .collect(),
                                DataType::Float64 => Scalar::add(
                                    table::column_to_arrays_f64(columns.get(0).unwrap()),
                                    table::column_to_arrays_f64(columns.get(1).unwrap()),
                                )
                                .unwrap()
                                .into_iter()
                                .map(|arr| Arc::new(arr) as ArrayRef)
                                .collect(),
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
                _ => panic!("Scalar Expression {:?} not supported", expr),
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
