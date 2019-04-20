//! Expressions that generate operations and computations

use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ColumnType {
    Array(DataType),
    Scalar(DataType),
}

impl From<DataType> for ColumnType {
    fn from(dtype: DataType) -> Self {
        match dtype {
            DataType::Struct(_) => panic!("struct array conversion not yet supported"),
            DataType::List(inner) => ColumnType::Array(*inner.clone()),
            _ => ColumnType::Scalar(dtype.clone()),
        }
    }
}

impl From<ColumnType> for DataType {
    fn from(from: ColumnType) -> Self {
        match from {
            ColumnType::Array(dtype) => DataType::List(Box::new(dtype)),
            ColumnType::Scalar(dtype) => dtype.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
}

impl From<arrow::datatypes::Field> for Column {
    fn from(field: arrow::datatypes::Field) -> Self {
        Column {
            name: field.name().clone(),
            column_type: field.data_type().clone().into(),
        }
    }
}

impl From<Column> for arrow::datatypes::Field {
    fn from(column: Column) -> Self {
        arrow::datatypes::Field::new(column.name.as_str(), column.column_type.into(), true)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Frame {
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Transformation {
    Aggregate,
    Calculate(Operation),
    Join,
    Group,
    // can add other transform types here
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Operation {
    pub(crate) name: String,
    pub(crate) inputs: Vec<Column>,
    pub(crate) output: Column,
    pub(crate) expression: Expression,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Computation {
    pub(crate) input: Vec<Frame>,
    pub(crate) operations: Vec<Operation>,
    pub(crate) output: Frame,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Expression {
    Scalar(ScalarExpression),
    Cast,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ScalarExpression {
    Add,
    Subtract,
    Divide,
    Multiply,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug() {
        let frame = Frame {
            name: "Input Table 1".to_owned(),
            columns: vec![Column {
                name: "id".to_owned(),
                column_type: ColumnType::Scalar(DataType::Int64),
            }],
        };

        assert_eq!("Frame { name: \"Input Table 1\", columns: [Column { name: \"id\", column_type: Scalar(Int64) }] }", format!("{:?}", frame));
        let as_json = serde_json::to_string(&frame).unwrap();
        assert_eq!("{\"name\":\"Input Table 1\",\"columns\":[{\"name\":\"id\",\"column_type\":{\"Scalar\":\"Int64\"}}]}", as_json);
    }
}
