use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use arrow::array::*;
use arrow::builder::*;
use arrow::datatypes::*;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JsonType {
    Bool,
    Int,
    Float,
    Str,
    BoolList,
    IntList,
    FloatList,
    StrList,
    Struct,
}

fn json_to_datatype(dtype: &JsonType) -> DataType {
    use JsonType::*;
    match dtype {
        Bool => DataType::Boolean,
        Int => DataType::Int64,
        Float => DataType::Float64,
        Str => DataType::Utf8,
        // BoolList | IntList | FloatList | StrList => DataType::List(_),
        // Struct => DataType::Struct(_),
        _ => {
            // lists and structs, return an error
            unimplemented!("Lists and structs not yet supported")
        }
    }
}

fn generate_schema(spec: HashMap<String, HashSet<JsonType>>) -> Arc<Schema> {
    let fields = spec
        .iter()
        .map(|(k, hs)| {
            let v: Vec<&JsonType> = hs.iter().collect();
            match v.len() {
                1 => {
                    let dtype = json_to_datatype(v[0]);
                    Field::new(k, dtype, true)
                }
                2 => {
                    if v.contains(&&JsonType::Float) && v.contains(&&JsonType::Int) {
                        Field::new(k, DataType::Float64, true)
                    // } else if v.contains(JsonType::Bool) || v.contains(JsonType::Str) {
                    //     Field::new(k, DataType::Utf8, true)
                    } else {
                        Field::new(k, DataType::Utf8, true)
                    }
                }
                _ => {
                    // return as string as there are too many conflicting types
                    // TODO: separate treatment for values that have lists and scalars
                    Field::new(k, DataType::Utf8, true)
                }
            }
        })
        .collect();
    let schema = Schema::new(fields);
    Arc::new(schema)
}

/// We originally tried creating this with `<T: Read + Seek>`, but the `BufReader::lines()` consumes the 
/// reader, making it impossible to seek back to the start.
/// `std::fs::File` has the `try_clone()` method, which allows us to overcome this limitation.
fn infer_json_schema(
    file: File,
    max_read_records: Option<usize>,
) -> Result<Arc<Schema>, ArrowError> {
    let mut values: HashMap<String, HashSet<JsonType>> = HashMap::new();
    let mut reader = BufReader::new(file.try_clone()?);

    let mut line = String::new();
    for i in 0..max_read_records.unwrap_or(std::usize::MAX) {
        &reader.read_line(&mut line)?;
        if line.is_empty() {
            break;
        }
        let record: Value = serde_json::from_str(&line.trim()).unwrap();

        line = String::new();

        match record {
            Value::Object(map) => {
                map.iter().for_each(|(k, v)| {
                    match v {
                        Value::Array(a) => {
                            // TODO: loop through list and determine the type of list to use
                        }
                        Value::Bool(b) => {
                            if values.contains_key(k) {
                                let x = values.get_mut(k).unwrap();
                                x.insert(JsonType::Bool);
                            } else {
                                // create hashset and add value type
                                let mut hs = HashSet::new();
                                hs.insert(JsonType::Bool);
                                values.insert(k.to_string(), hs);
                            }
                        }
                        Value::Null => {
                            // do nothing, we treat json as nullable by default
                        }
                        Value::Number(n) => {
                            if n.is_f64() {
                                if values.contains_key(k) {
                                    let x = values.get_mut(k).unwrap();
                                    x.insert(JsonType::Float);
                                } else {
                                    // create hashset and add value type
                                    let mut hs = HashSet::new();
                                    hs.insert(JsonType::Float);
                                    values.insert(k.to_string(), hs);
                                }
                            } else {
                                // default to i64
                                if values.contains_key(k) {
                                    let x = values.get_mut(k).unwrap();
                                    x.insert(JsonType::Int);
                                } else {
                                    // create hashset and add value type
                                    let mut hs = HashSet::new();
                                    hs.insert(JsonType::Int);
                                    values.insert(k.to_string(), hs);
                                }
                            }
                        }
                        Value::String(_) => {
                            if values.contains_key(k) {
                                let x = values.get_mut(k).unwrap();
                                x.insert(JsonType::Str);
                            } else {
                                // create hashset and add value type
                                let mut hs = HashSet::new();
                                hs.insert(JsonType::Str);
                                values.insert(k.to_string(), hs);
                            }
                        }
                        Value::Object(o) => {
                            // TODO
                        }
                    }
                });
            }
            _ => {
                // return an error, we expect a value
            }
        };
    }

    let schema = generate_schema(values);

    // return the reader seek back to the start
    &reader.into_inner().seek(SeekFrom::Start(0))?;

    Ok(schema)
}

pub struct Reader<R: Read> {
    schema: Arc<Schema>,
    projection: Option<Vec<String>>,
    reader: BufReader<R>,
    batch_size: usize,
}

impl<R: Read> Reader<R> {
    pub fn new(
        reader: BufReader<R>,
        schema: Arc<Schema>,
        batch_size: usize,
        projection: Option<Vec<String>>,
    ) -> Self {
        Self {
            schema,
            projection,
            reader,
            batch_size,
        }
    }

    // pub fn from_buf_reader(
    //     reader: BufReader<R>,
    //     schema: Arc<Schema>,
    //     batch_size: usize,
    //     projection: Option<Vec<String>>,
    // ) -> Self {
    //     reader: BufReader<R>,
    //     schema: Arc<Schema>,
    //     batch_size: usize,
    //     projection: Option<Vec<String>>,
    // }

    /// Read the next batch of records
    pub fn next(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut rows: Vec<Value> = Vec::with_capacity(self.batch_size);
        let mut line = String::new();
        for _ in 0..self.batch_size {
            self.reader.read_line(&mut line).unwrap();
            if !line.is_empty() {
                rows.push(serde_json::from_str(&line).unwrap());
                line = String::new();
            } else {
                break;
            }
        }

        // TODO: projection
        let rows = &rows[..];
        let arrays: Result<Vec<ArrayRef>, ArrowError> =
            self.schema.clone().fields().iter().map(|field| {
                match field.data_type() {
                    DataType::Boolean => self.build_boolean_array(rows, field.name()),
                    DataType::Float64 => self.build_primitive_array::<Float64Type>(rows, field.name()),
                    DataType::Int64 => self.build_primitive_array::<Int64Type>(rows, field.name()),
                    DataType::Utf8 => {
                        let mut builder = BinaryBuilder::new(rows.len());
                        for row_index in 0..rows.len() {
                            match rows[row_index].get(field.name()) {
                                Some(value) => {
                                    match value.as_str() {
                                        Some(v) => builder.append_string(v)?,
                                        // TODO: value exists as something else, coerce so we don't lose it
                                        None => builder.append(false)?
                                    }
                                },
                                None => builder.append(false)?
                            }
                        };
                        Ok(Arc::new(builder.finish()) as ArrayRef)
                    },
                    _ => unimplemented!("lists and other types are not yet supported")
                }
            }).collect();

        match arrays {
            Ok(arr) => Ok(Some(RecordBatch::new(self.schema.clone(), arr))),
            Err(e) => Err(e)
        }
    }

    fn build_boolean_array(&self, rows: &[Value], col_name: &str) -> Result<ArrayRef, ArrowError> {
        let mut builder = BooleanBuilder::new(rows.len());
        for row_index in 0..rows.len() {
            match rows[row_index].get(col_name) {
                Some(value) => {
                    match value.as_bool() {
                        Some(v) => builder.append_value(v)?,
                        None => builder.append_null()?,
                    }
                }
                None => {
                    builder.append_null()?;
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn build_primitive_array<T: ArrowPrimitiveType>(
        &self,
        rows: &[Value],
        col_name: &str,
    ) -> Result<ArrayRef, ArrowError>
    where
        // T::Native: std::convert::From<i64> + std::convert::From<f64> + std::convert::From<bool>,
        T::Native: num::NumCast,
    {
        let mut builder = PrimitiveBuilder::<T>::new(rows.len());
        let t = T::get_data_type();
        for row_index in 0..rows.len() {
            match rows[row_index].get(col_name) {
                Some(value) => {
                    // check that value is of expected datatype
                    match t {
                        DataType::Int64 => match value.as_i64() {
                            Some(v) => {
                                match num::cast::cast(v) {
                                    Some(v) => builder.append_value(v)?,
                                    None => builder.append_null()?
                                }
                            },
                            None => builder.append_null()?,
                        },
                        DataType::Float64 => match value.as_f64() {
                            Some(v) => {
                                match num::cast::cast(v) {
                                    Some(v) => builder.append_value(v)?,
                                    None => builder.append_null()?
                                }
                            },
                            None => builder.append_null()?,
                        },
                        _ => {
                            // TODO: replace with JsonError
                            return Err(ArrowError::ParseError("Cannot use primitive builder for this type of field".to_string()))
                        }
                    }
                }
                None => {
                    builder.append_null()?;
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

pub struct ReaderBuilder {
    schema: Option<Arc<Schema>>,
    max_records: Option<usize>,
    batch_size: usize,
    projection: Option<Vec<String>>,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        Self {
            schema: None,
            max_records: None,
            batch_size: 1024,
            projection: None,
        }
    }
}

impl ReaderBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the JSON file's schema
    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the JSON reader to infer the schema of the file
    pub fn infer_schema(mut self, max_records: Option<usize>) -> Self {
        // remove any schema that is set
        self.schema = None;
        self.max_records = max_records;
        self
    }

    /// Set the batch size (number of records to load at one time)
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the reader's column projection
    pub fn with_projection(mut self, projection: Vec<String>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Create a new `Reader` from the `ReaderBuilder`
    pub fn build<R: Read>(self, file: File) -> Result<Reader<File>, ArrowError> {
        // check if schema should be inferred
        let schema = match self.schema {
            Some(schema) => schema,
            None => {
                let inferred = infer_json_schema(
                    file.try_clone()?,
                    self.max_records,
                )?;

                inferred
            }
        };
        let buf_reader = BufReader::new(file);
        Ok(Reader::new(buf_reader, schema, self.batch_size, self.projection))
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder.build::<File>(File::open("test/data/numbers.json").unwrap()).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(9, batch.num_columns());
        assert_eq!(64, batch.num_rows());

        let schema = batch.schema();

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int64, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(&DataType::Int64, b.1.data_type());
        let id = schema.column_with_name("id").unwrap();
        assert_eq!(&DataType::Int64, id.1.data_type());
        let id_str = schema.column_with_name("id_str").unwrap();
        assert_eq!(&DataType::Utf8, id_str.1.data_type());

        let aa = batch.column(a.0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(1, aa.value(0));

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(9, batch.num_columns());
        assert_eq!(36, batch.num_rows());

    }
}
