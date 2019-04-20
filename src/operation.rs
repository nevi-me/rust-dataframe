//! Operations

use crate::evaluation::*;
use crate::expression::*;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;

pub trait ScalarOperation {
    fn name() -> &'static str;
    fn transform(inputs: Vec<Column>, name: Option<String>) -> Result<Operation, ArrowError>;
}

/// Operation to add two numeric columns together
pub struct AddOperation;

impl ScalarOperation for AddOperation {
    fn name() -> &'static str {
        "add"
    }

    fn transform(inputs: Vec<Column>, name: Option<String>) -> Result<Operation, ArrowError> {
        // add n columns together provided that they are of the same data type
        // for now we support 2 inputs at a time
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
                        Ok(Operation {
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
                        })
                    }
                }
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

        let add = AddOperation::transform(vec![a, b], None).unwrap();

        assert_eq!(
            "Operation { name: \"add\", inputs: [Column { name: \"a\", column_type: Scalar(Int64) }, Column { name: \"b\", column_type: Scalar(Int64) }], output: Column { name: \"add(a, b)\", column_type: Scalar(Int64) }, expression: Scalar(Add) }",
            format!("{:?}", add)
        );
    }
}
