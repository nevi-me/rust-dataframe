//! Operations

use crate::evaluation::*;
use crate::expression::*;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;

pub trait ScalarOperation {
    fn name() -> &'static str;
    fn transform(
        inputs: Vec<Column>,
        name: Option<String>,
        to_type: Option<DataType>,
    ) -> Result<Vec<Operation>, ArrowError>;
}

/// Operation to add two numeric columns together
pub struct AddOperation;

impl ScalarOperation for AddOperation {
    fn name() -> &'static str {
        "add"
    }

    fn transform(
        inputs: Vec<Column>,
        name: Option<String>,
        to_type: Option<DataType>,
    ) -> Result<Vec<Operation>, ArrowError> {
        // add n columns together provided that they are of the same data type
        // for now we support 2 inputs at a time
        // the output data type is also ignored
        if inputs.len() != 2 {
            Err(ArrowError::ComputeError(
                "Add operation expects 2 inputs".to_string(),
            ))
        } else {
            let a = &inputs[0];
            let b = &inputs[1];
            match (&a.column_type, &b.column_type) {
                (ColumnType::Array(_), _) | (_, ColumnType::Array(_)) => {
                    Err(ArrowError::ComputeError(
                        "Add operation only works on scalar columns".to_string(),
                    ))
                }
                (ColumnType::Scalar(from_type), ColumnType::Scalar(to_type)) => {
                    if from_type != to_type {
                        Err(ArrowError::ComputeError(
                            "Add operation currently only works on the same data types".to_string(),
                        ))
                    } else {
                        Ok(vec![Operation {
                            name: Self::name().to_string(),
                            inputs: inputs.clone(),
                            output: Column {
                                name: name.unwrap_or(format!(
                                    "{}({}, {})",
                                    Self::name(),
                                    &a.name,
                                    &b.name
                                )),
                                column_type: ColumnType::Scalar(from_type.clone()),
                            },
                            expression: Expression::Scalar(ScalarExpression::Add),
                        }])
                    }
                }
            }
        }
    }
}

pub struct CastOperation;

impl ScalarOperation for CastOperation {
    fn name() -> &'static str {
        "cast"
    }

    fn transform(
        inputs: Vec<Column>,
        name: Option<String>,
        to_type: Option<DataType>,
    ) -> Result<Vec<Operation>, ArrowError> {
        // cast columns to the output type
        // we've made provision for casting more than 1 column at a time, but for now we only cast 1
        if inputs.len() != 1 {
            Err(ArrowError::ComputeError(
                "Cast operation expects 1 input".to_string(),
            ))
        } else {
            let a = &inputs[0];
            let to_type = to_type.ok_or(ArrowError::InvalidArgumentError(
                "Cast requires a target output datatype".to_string(),
            ))?;

            match &a.column_type {
                ColumnType::Array(_) => Err(ArrowError::ComputeError(
                    "Cast operation is currently only supported on scalar columns".to_string(),
                )),
                _ => Ok(vec![Operation {
                    name: Self::name().to_string(),
                    inputs: inputs.clone(),
                    output: Column {
                        name: name.unwrap_or(format!("{}({} as datatype)", Self::name(), &a.name)),
                        column_type: ColumnType::Scalar(to_type.clone()),
                    },
                    expression: Expression::Cast,
                }]),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_operations() {
        let a = Column {
            name: "a".to_owned(),
            column_type: ColumnType::Scalar(DataType::Int64),
        };
        let b = Column {
            name: "b".to_owned(),
            column_type: ColumnType::Scalar(DataType::Int64),
        };

        let add = AddOperation::transform(vec![a, b], None, None).unwrap();

        assert_eq!(
            "[Operation { name: \"add\", inputs: [Column { name: \"a\", column_type: Scalar(Int64) }, Column { name: \"b\", column_type: Scalar(Int64) }], output: Column { name: \"add(a, b)\", column_type: Scalar(Int64) }, expression: Scalar(Add) }]",
            format!("{:?}", add)
        );
    }
}
