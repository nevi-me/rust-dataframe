//! Expressions that generate operations and computations

use crate::io::datasource::DataSourceEval;
use ::std::sync::Arc;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
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
pub struct Dataset {
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
}

impl Dataset {
    pub fn get_column(&self, name: &str) -> Option<(usize, &Column)> {
        let column = self
            .columns
            .iter()
            .enumerate()
            .find(|(index, col): &(usize, &Column)| &col.name == name);
        return column;
    }

    pub fn empty() -> Self {
        Dataset {
            name: "empty".to_owned(),
            columns: vec![],
        }
    }

    // overrides or appends a column
    pub fn append_column(&self, column: Column) -> Self {
        let existing = self.get_column(&column.name);
        let mut columns = self.columns.clone();
        match existing {
            Some((index, _)) => {
                columns[index] = column;
            }
            None => {
                columns.push(column);
            }
        };
        Self {
            name: self.name.clone(),
            columns,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Transformation {
    Aggregate,
    Calculate(Operation),
    Join(Dataset, Dataset, JoinCriteria),
    Group,
    Project,
    Read(Reader),
    Limit(usize),
    Filter(BooleanFilter),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JoinCriteria {
    join_type: JoinType,
    criteria: (),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum JoinType {
    LeftJoin,
    RightJoin,
    InnerJoin,
    FullJoin,
}

/// A read expression defines how a data source should be read
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Reader {
    pub(crate) source: DataSourceType,
}

/// data source types (and options)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataSourceType {
    Csv(String, CsvReadOptions),
    Json(String),
    Feather(String),
    // TODO provide an option between a table name and a SQL query
    Sql(String, SqlReadOptions),
    Parquet(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CsvReadOptions {
    pub(crate) has_headers: bool,
    pub(crate) delimiter: Option<u8>,
    pub(crate) max_records: Option<usize>,
    pub(crate) batch_size: usize,
    pub(crate) projection: Option<Vec<usize>>,
}

/// The different database protocols that can be supported, used to generate queries at runtime
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SqlDatabase {
    Postgres,
    MsSql,
    MySql,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SqlReadOptions {
    pub(crate) connection_string: String,
    pub(crate) db: SqlDatabase,
    pub(crate) limit: Option<usize>,
}

/// An operation represents a calculation on one or many columns, producing an output column
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Operation {
    // TODO move operation to operation.rs
    pub(crate) name: String,
    pub(crate) inputs: Vec<Column>,
    pub(crate) output: Column,
    pub(crate) function: Function,
}

impl Operation {
    pub(crate) fn rename(col: &Column, to: &str) -> Self {
        Self {
            name: "rename".to_owned(),
            inputs: vec![col.clone()],
            output: Column {
                name: to.to_owned(),
                column_type: col.column_type.clone(),
            },
            function: Function::Rename,
        }
    }

    /// create a calculation operation
    ///
    /// The operation can return one or more transformations (e.g. if a column needs casting before the required operation)
    pub(crate) fn calculate(
        ds: &Dataset,
        // we search the dataset for names
        in_col_names: Vec<&str>,
        function: Function,
        out_col_name: Option<String>,
        out_col_type: Option<DataType>,
    ) -> Result<Vec<Transformation>, ArrowError> {
        use crate::operation::scalar::ScalarOperation;
        use Function::*;
        // get columns
        let mut inputs = vec![];
        for name in in_col_names {
            let col = ds.get_column(name);
            match col {
                Some((index, col)) => {
                    inputs.push(col.clone());
                }
                None => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Column {} not found",
                        name
                    )));
                }
            }
        }
        match function {
            Rename => panic!("Please use rename function directly for now"),
            Cast => unimplemented!("cast op"),
            Scalar(s) => {
                use ScalarFunction::*;
                let operations = match s {
                    ScalarFunction::Abs => panic!(),
                    ScalarFunction::Add => crate::operation::scalar::AddOperation::transform(
                        inputs,
                        out_col_name,
                        out_col_type,
                    )?,
                    ScalarFunction::Subtract => {
                        crate::operation::scalar::SubtractOperation::transform(
                            inputs,
                            out_col_name,
                            out_col_type,
                        )?
                    }
                    ScalarFunction::Multiply => panic!(),
                    ScalarFunction::Divide => panic!(),
                    ScalarFunction::Sine => crate::operation::scalar::SinOperation::transform(
                        inputs,
                        out_col_name,
                        out_col_type,
                    )?,
                    ScalarFunction::Cosine => panic!(),
                    ScalarFunction::Tangent => panic!(),
                    ScalarFunction::Cosecant => panic!(),
                    ScalarFunction::Secant => panic!(),
                    ScalarFunction::Cotangent => panic!(),
                };
                Ok(operations
                    .into_iter()
                    .map(|op| Transformation::Calculate(op))
                    .collect())
            }
            Array(a) => unimplemented!("array op"),
            Limit(limit) => Ok(vec![Transformation::Limit(limit)]),
            Filter(cond) => Ok(vec![Transformation::Filter(cond)]),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Expression {
    Read(Computation),
    Compute(Box<Expression>, Computation),
    Write(Box<Expression>),
    Output,
}

impl Expression {
    /// unroll the expression into a number of computations
    ///
    /// This is used for optimising queries
    pub fn unroll(&self) -> Vec<Computation> {
        let mut computations = vec![];
        match self {
            Expression::Read(c) => computations.push(c.clone()),
            Expression::Compute(expr, c) => {
                computations.push(c.clone());
                computations.append(&mut expr.unroll());
            }
            Expression::Write(expr) => computations.append(&mut expr.unroll()),
            Expression::Output => {}
        };
        computations
    }
}

/// A computation determines the impact of one or more transformations on inputs, producing a single output.
///
/// Transformations can be various types, such as reading data, calculating columns, or aggregating, etc.
/// A read computation takes no inputs, and is expected to be able to inspect an input data source to determine
/// its schema (if one is not provided explicitly).
///
/// All other transformations would then take input dataset.
///
/// In the future, we expect a Write computation to potentially return a Read against the saved data.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Computation {
    pub(crate) input: Vec<Dataset>,
    pub(crate) transformations: Vec<Transformation>,
    pub(crate) output: Dataset,
}

impl Computation {
    /// Returns an empty computation which transformations can be added on
    pub fn empty() -> Self {
        Self {
            input: vec![],
            transformations: vec![],
            output: Dataset::empty(),
        }
    }

    pub fn compute_read(read: &Reader) -> Self {
        let dataset = read.get_dataset().unwrap();
        Self {
            input: vec![],
            transformations: vec![Transformation::Read(read.clone())],
            output: dataset,
        }
    }

    /// An operation takes a number of input columns, and produces an output.
    /// If the output column name already exists in the input, the current default behaviour
    /// is to override the existing column with the output column.
    ///
    /// Thus to compute a calculation, we only need to compute how the output dataset looks like.
    /// TODO(Neville) hide the operation struct members behind a function to guarantee the above.
    fn compute_calculation(input: &Dataset, operation: &Operation) -> Dataset {
        let mut columns = input.columns.clone();
        let out_column = &operation.output;
        match input.get_column(&out_column.name) {
            Some((index, column)) => columns[index] = out_column.clone(),
            None => columns.push(out_column.clone()),
        };
        Dataset {
            name: "unnamed_dataset".to_owned(),
            columns: columns.to_vec(),
        }
    }

    /// Compute how a dataset looks like after applying transformations to it
    pub fn compute_transform(inputs: Vec<Dataset>, transforms: Vec<Transformation>) -> Self {
        use Transformation::*;
        if transforms.is_empty() {
            panic!("Cannot compute with 0 transformations")
        }
        let mut output: Dataset = Dataset::empty();
        match inputs.len() {
            0 => {
                // a read transform takes 0 inputs
                let mut has_read_data = false;
                for transform in &transforms {
                    match (transform, has_read_data) {
                        (Read(reader), false) => {
                            // read the data
                            output = Self::compute_read(&reader).output;
                            has_read_data = true;
                        }
                        (t @ _, false) => panic!("Transformation {:?} requires input data", t),
                        (Read(_), true) => panic!("Chained reads are currently not supported, a read has taken place already"),
                        (_, _) => unimplemented!(),
                    }
                }
            }
            1 => {
                output = inputs.get(0).unwrap().clone();
                for transform in &transforms {
                    match transform {
                        Transformation::Read(_) => {
                            panic!("A read transformation that has inputs is not supported")
                        }
                        Transformation::Calculate(operation) => {
                            output = Self::compute_calculation(&output, &operation);
                        }
                        _ => unimplemented!(
                            "TODO transformations not yet implemented for single data input"
                        ),
                    }
                }
            }
            2 => panic!("two input transformations aren't supported yet"),
            _ => panic!("unsupported number of input datasets: {}", inputs.len()),
        }
        Self {
            input: inputs,
            transformations: transforms,
            output,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Function {
    Scalar(ScalarFunction),
    Array(ArrayFunction),
    Cast,
    Rename,
    Filter(BooleanFilter),
    Limit(usize),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ScalarFunction {
    Add,
    Subtract,
    Divide,
    Multiply,
    Abs,
    Sine,
    Cosine,
    Tangent,
    Cotangent,
    Secant,
    Cosecant,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ArrayFunction {
    Contains,
    Join,
    Distinct,
    Except,
    Intersect,
    Max,
    Min,
    Position,
    Remove,
    Repeat,
    Sort,
    Union,
    Overlap,
    Zip,
    CollectList,
    CollectSet,
}

// TODO: This is a temporary work-around until there are scalars in Arrow
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Scalar {
    Null,
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    String(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum BooleanInput {
    Scalar(Scalar),
    Column(Column),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum BooleanFilter {
    Input(BooleanInput),
    Not(Box<BooleanFilter>),
    And(Box<BooleanFilter>, Box<BooleanFilter>),
    Or(Box<BooleanFilter>, Box<BooleanFilter>),
    Gt(Box<BooleanFilter>, Box<BooleanFilter>),
    Ge(Box<BooleanFilter>, Box<BooleanFilter>),
    Eq(Box<BooleanFilter>, Box<BooleanFilter>),
    Ne(Box<BooleanFilter>, Box<BooleanFilter>),
    Lt(Box<BooleanFilter>, Box<BooleanFilter>),
    Le(Box<BooleanFilter>, Box<BooleanFilter>),
}

impl BooleanFilter {
    pub fn eval_to_array(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<arrow::array::ArrayRef, ArrowError> {
        use ::std::sync::Arc;
        use arrow::array::*;
        use BooleanFilter::*;
        let len = batch.num_rows();
        match self {
            // either extract a column or create a scalar with batch length
            Input(input) => match input {
                BooleanInput::Scalar(scalar) => match scalar {
                    Scalar::Boolean(v) => {
                        Ok(Arc::new(arrow::array::BooleanArray::from(vec![*v; len])) as ArrayRef)
                    }
                    Scalar::Float32(v) => {
                        Ok(Arc::new(arrow::array::Float32Array::from(vec![*v; len])) as ArrayRef)
                    }
                    Scalar::Float64(v) => {
                        Ok(Arc::new(arrow::array::Float64Array::from(vec![*v; len])) as ArrayRef)
                    }
                    Scalar::Int32(v) => {
                        Ok(Arc::new(arrow::array::Int32Array::from(vec![*v; len])) as ArrayRef)
                    }
                    Scalar::Int64(v) => {
                        Ok(Arc::new(arrow::array::Int64Array::from(vec![*v; len])) as ArrayRef)
                    }
                    Scalar::Null => {
                        Ok(Arc::new(arrow::array::BooleanArray::from(vec![false; len]))
                            as ArrayRef)
                    }
                    Scalar::String(v) => {
                        Ok(
                            Arc::new(arrow::array::BinaryArray::from(vec![v.as_str(); len]))
                                as ArrayRef,
                        )
                    }
                },
                BooleanInput::Column(column) => {
                    let col = batch.schema().column_with_name(&column.name);
                    match col {
                        Some((num, field)) => {
                            let col = batch.column(num);
                            return Ok(col.clone());
                        }
                        None => {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "Cannot find column {}",
                                &column.name
                            )));
                        }
                    }
                }
            },
            Not(ref array) => {
                let a = arrow::compute::cast(&array.eval_to_array(batch)?, &DataType::Boolean)?;
                Ok(Arc::new(arrow::compute::not(&BooleanArray::from(a.data()))?) as ArrayRef)
            }
            And(ref left, ref right) | Or(ref left, ref right) => {
                let l = arrow::compute::cast(&left.eval_to_array(batch)?, &DataType::Float64)?;
                let r = arrow::compute::cast(&right.eval_to_array(batch)?, &DataType::Float64)?;
                let op = match self {
                    And(_, _) => arrow::compute::and,
                    Or(_, _) => arrow::compute::or,
                    _ => unreachable!(),
                };
                Ok(Arc::new(op(
                    &BooleanArray::from(l.data()),
                    &BooleanArray::from(r.data()),
                )?) as ArrayRef)
            }
            Gt(ref left, ref right)
            | Ge(ref left, ref right)
            | Eq(ref left, ref right)
            | Ne(ref left, ref right)
            | Lt(ref left, ref right)
            | Le(ref left, ref right) => {
                // cast arrays to compatible types, then calculate `gt`
                // TODO determine types to cast to, using f64 for expediency
                let l = arrow::compute::cast(&left.eval_to_array(batch)?, &DataType::Float64)?;
                let r = arrow::compute::cast(&right.eval_to_array(batch)?, &DataType::Float64)?;
                let op = match self {
                    Gt(_, _) => arrow::compute::gt,
                    Ge(_, _) => arrow::compute::gt_eq,
                    Eq(_, _) => arrow::compute::eq,
                    Ne(_, _) => arrow::compute::neq,
                    Lt(_, _) => arrow::compute::lt,
                    Le(_, _) => arrow::compute::lt_eq,
                    _ => unreachable!(),
                };
                Ok(Arc::new(op(
                    &Float64Array::from(l.data()),
                    &Float64Array::from(r.data()),
                )?) as ArrayRef)
            }
        }
    }
}

pub trait BooleanFilterEval {
    fn eval(&self, filter: BooleanFilter) -> arrow::array::BooleanArray;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug() {
        let dataset = Dataset {
            name: "Input Table 1".to_owned(),
            columns: vec![Column {
                name: "id".to_owned(),
                column_type: ColumnType::Scalar(DataType::Int64),
            }],
        };

        assert_eq!("Dataset { name: \"Input Table 1\", columns: [Column { name: \"id\", column_type: Scalar(Int64) }] }", format!("{:?}", dataset));
        let as_json = serde_json::to_string(&dataset).unwrap();
        assert_eq!("{\"name\":\"Input Table 1\",\"columns\":[{\"name\":\"id\",\"column_type\":{\"Scalar\":\"Int64\"}}]}", as_json);
    }
}
