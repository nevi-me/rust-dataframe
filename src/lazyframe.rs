//! Lazy dataframe

use crate::dataframe::DataFrame;
use crate::error::DataFrameError;
use crate::evaluation::Evaluate;
use crate::expression::*;

use arrow::datatypes::{DataType, Schema};
use arrow::error::ArrowError;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A lazy dataframe
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LazyFrame {
    id: String,
    pub(crate) expression: Expression,
    output: Dataset,
}

impl LazyFrame {
    /// lazily read a dataset
    ///
    /// This should eventually take the inputs that make up the computation
    pub fn read(computation: Computation) -> Self {
        Self {
            id: "".to_owned(),
            output: computation.output.clone(),
            expression: Expression::Read(computation),
        }
    }

    /// Materialise and write the frame to the writer
    pub fn write(&self, writer: &Writer) -> Result<(), DataFrameError> {
        // a write operation evaluates the expression, and returns a write status
        let dataframe = self.evaluate();
        // write the resulting dataframe
        dataframe.write(writer)
    }

    /// Prints a subset of the data frame to console
    pub fn display(&self, limit: usize) -> Result<(), DataFrameError> {
        // display is like write, except it just shows results as a table
        let limited = self.limit(limit);
        let dataframe = limited.evaluate();
        dataframe.display()
    }

    pub fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    pub fn column(&self, name: &str) -> Option<(usize, &Column)> {
        self.output.get_column(name)
    }

    /// Create a column from the operation
    pub fn with_column(
        &self,
        col_name: &str,
        function: Function,
        input_col_names: Vec<&str>,
        as_type: Option<DataType>,
    ) -> Result<Self, DataFrameError> {
        // the columns that make the output dataset
        let ops = Calculation::calculate(
            &self.output,
            input_col_names,
            function,
            Some(col_name.to_owned()),
            as_type,
        )?;
        let mut out_dataset: Dataset = self.output.clone();
        // this transformation only works with calculations
        for tfm in &ops {
            match tfm {
                Transformation::Calculate(op) => {
                    out_dataset = out_dataset.append_column(op.output.clone())
                }
                _ => panic!("can't create column from {:?} transformation", tfm),
            }
        }
        Ok(Self {
            id: self.id.clone(),
            output: out_dataset.clone(),
            expression: Expression::Compute(
                Box::new(self.expression.clone()),
                Computation {
                    input: vec![self.output.clone()],
                    transformations: ops,
                    output: out_dataset,
                },
            ),
        })
    }

    pub fn with_column_renamed(&self, old_name: &str, new_name: &str) -> Self {
        // create a transformation that renames a column's name
        let column = self.output.get_column(old_name);
        match column {
            Some((index, column)) => {
                let mut columns = self.output.columns.clone();
                // rename column
                let rename = Calculation::rename(column, new_name);
                columns[index] = Column {
                    name: new_name.to_owned(),
                    column_type: column.column_type.clone(),
                };
                let output = Dataset {
                    name: "renamed_dataset".to_owned(),
                    columns,
                };

                let computation = Computation {
                    input: vec![self.output.clone()],
                    transformations: vec![Transformation::Calculate(rename)],
                    output: output.clone(),
                };
                let expression =
                    Expression::Compute(Box::new(self.expression.clone()), computation);
                Self {
                    id: "renamed_frame".to_owned(),
                    expression,
                    output,
                }
            }
            None => self.clone(),
        }
    }

    /// Limit data to the specified number of records.
    ///
    /// If the specified limit is greater than the total records, the total records are returned
    pub fn limit(&self, limit: usize) -> Self {
        let computation = Computation {
            input: vec![self.output.clone()],
            transformations: vec![Transformation::Limit(limit)],
            output: self.output.clone(),
        };
        let expression = Expression::Compute(Box::new(self.expression.clone()), computation);
        Self {
            id: "limited_frame".to_owned(),
            expression,
            output: self.output.clone(),
        }
    }

    /// Apply a filter using a `BooleanFilter` which evaluates to a `BooleanArray`
    pub fn filter(&self, condition: BooleanFilter) -> Self {
        let computation = Computation {
            input: vec![self.output.clone()],
            transformations: vec![Transformation::Filter(condition)],
            output: self.output.clone(),
        };
        let expression = Expression::Compute(Box::new(self.expression.clone()), computation);
        Self {
            id: "filtered_frame".to_owned(),
            expression,
            output: self.output.clone(),
        }
    }

    /// project columns
    pub fn select(&self, col_names: Vec<&str>) -> Result<Self, DataFrameError> {
        //check that columns exist, then return the columns that exist
        let mut columns = vec![];
        let mut projected = vec![];
        for col in col_names {
            let column = self.output.get_column(col).ok_or_else(|| {
                DataFrameError::ComputeError(format!(
                    "Column {:?} cannot be selected as it does not exist",
                    col
                ))
            })?;
            columns.push(column.1.clone());
            projected.push(col.to_string());
        }
        let out_dataset = Dataset {
            name: "".to_string(),
            columns,
        };
        let computation = Computation {
            input: vec![self.output.clone()],
            transformations: vec![Transformation::Select(projected)],
            output: out_dataset.clone(),
        };
        let expression = Expression::Compute(Box::new(self.expression.clone()), computation);
        Ok(Self {
            id: "projected_frame".to_owned(),
            expression,
            output: out_dataset,
        })
    }

    /// drop columns
    ///
    /// TODO: drop should be infallible as it should just filter for non-existent columns
    pub fn drop(&self, col_names: Vec<&str>) -> Result<Self, DataFrameError> {
        let mut columns = vec![];
        let mut projected = vec![];
        for col in self.output.columns.clone() {
            if col_names.contains(&col.name.as_str()) {
                projected.push(col.name);
                continue;
            }
            columns.push(col.clone());
        }
        let out_dataset = Dataset {
            name: "".to_string(),
            columns,
        };
        let computation = Computation {
            input: vec![self.output.clone()],
            transformations: vec![Transformation::Drop(projected)],
            output: out_dataset.clone(),
        };
        let expression = Expression::Compute(Box::new(self.expression.clone()), computation);
        Ok(Self {
            id: "projected_frame".to_owned(),
            expression,
            output: out_dataset,
        })
    }

    pub fn join(&self, other: &Self, join_criteria: &JoinCriteria) -> Result<Self, DataFrameError> {
        // in order to join, we need to check that the join columns exist on both sides,
        //  and that they are compatible

        self.output
            .try_join(
                &other.output,
                join_criteria
                    .criteria
                    .iter()
                    .map(|(a, b)| (a.as_str(), b.as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .map(|dataset| {
                let expression = Expression::Join(
                    Box::new(self.expression.clone()),
                    Box::new(other.expression.clone()),
                    join_criteria.clone(),
                    dataset.clone(),
                );
                Self {
                    id: "joined_frame".to_owned(),
                    expression,
                    output: dataset,
                }
            })
    }

    pub fn sort(&self, sort_criteria: &[SortCriteria]) -> Result<Self, DataFrameError> {
        // in order to sort by a column, it has to exist and be sortable by
        // existence is easier to check, but sortability depends on what Arrow supports
        if sort_criteria.is_empty() {
            return Err(DataFrameError::ComputeError(
                "Sort criteria cannot be empty".to_string(),
            ));
        }
        // check that columns exist (, and optionally have sortable column types)
        // let mut columns = vec![];
        for criteria in sort_criteria {
            let col = self.output.get_column(&criteria.column).ok_or_else(|| {
                DataFrameError::ComputeError(format!(
                    "Column {:?} used in sort expression does not exist in dataframe",
                    &criteria.column
                ))
            })?;
            // TODO: check for sortability
        }
        let computation = Computation {
            input: vec![self.output.clone()],
            transformations: vec![Transformation::Sort(sort_criteria.to_vec())],
            output: self.output.clone(),
        };
        let expression = Expression::Compute(Box::new(self.expression.clone()), computation);
        Ok(Self {
            id: "sorted_frame".to_owned(),
            expression,
            output: self.output.clone(),
        })
    }

    pub fn aggregate(
        &self,
        groups: Vec<&str>,
        aggr: Vec<Aggregation>,
    ) -> Result<Self, DataFrameError> {
        // check if groupby columns are in the dataframe
        let out_dataset = self.output.try_aggregate(&groups, &aggr).unwrap();

        Ok(Self {
            id: self.id.clone(),
            output: out_dataset.clone(),
            expression: Expression::Compute(
                Box::new(self.expression.clone()),
                Computation {
                    input: vec![self.output.clone()],
                    transformations: vec![Transformation::GroupAggregate(
                        groups.iter().map(|s| s.to_string()).collect(),
                        aggr,
                    )],
                    output: out_dataset,
                },
            ),
        })
    }

    /// Helper function to evaluate the frame's expressions, and return a dataframe
    pub(crate) fn evaluate(&self) -> DataFrame {
        let ops = self.expression.unroll();
        let dataframe = DataFrame::empty();
        dataframe.evaluate(&ops)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;

    #[test]
    fn test_lazy_pipeline() {
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
        // materialise the frame
        let dataframe = frame.evaluate();
        assert_eq!(dataframe.num_columns(), 5);
        assert_eq!(dataframe.num_rows(), 37);
    }

    #[test]
    fn test_with_columns() {
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
        // add a column as a calculation of 2 columns
        frame = frame
            .with_column(
                "sum",
                Function::Scalar(ScalarFunction::Add),
                vec!["lat", "lng"],
                None,
            )
            .unwrap();
        // materialise the frame
        let dataframe = frame.evaluate();
        assert!(
            Float64Array::from(
                dataframe
                    .column_by_name("sum")
                    .data()
                    .chunks()
                    .iter()
                    .next()
                    .unwrap()
                    .data()
            )
            .value_slice(0, 1)[0]
                - (57.653484 - 3.335724)
                < f64::EPSILON,
        );
    }

    #[test]
    fn test_lazy_join() {
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
        let frame2 = LazyFrame::read(compute);
        frame = frame
            .join(
                &frame2,
                &JoinCriteria {
                    criteria: vec![("town".to_owned(), "city".to_owned())],
                    join_type: JoinType::InnerJoin,
                },
            )
            .unwrap();
        dbg!(frame.expression.unroll());
    }

    #[test]
    fn test_projection() {
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
        frame = frame.select(vec!["town", "sin_lat", "sin_lng"]).unwrap();
        frame = frame.drop(vec!["town"]).unwrap();
        dbg!(frame.expression.unroll());
    }

    #[test]
    fn test_group_aggregate() {
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
        frame = frame
            .aggregate(
                vec!["city"],
                vec![Aggregation {
                    function: AggregateFunction::Max,
                    columns: vec!["lat".to_string(), "lng".to_string()],
                }],
            )
            .unwrap();
        dbg!(frame.expression.unroll());
    }
}
