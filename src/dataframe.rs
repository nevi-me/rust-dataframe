use arrow::array;
use arrow::array::{Array, ArrayRef};
use arrow::array_data::ArrayDataBuilder;
use arrow::array_data::ArrayDataRef;
use arrow::csv::Reader as CsvReader;
use arrow::csv::ReaderBuilder as CsvReaderBuilder;
use arrow::datatypes::*;
//use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::fs::File;

fn make_array(data: ArrayDataRef) -> ArrayRef {
    // TODO: here data_type() needs to clone the type - maybe add a type tag enum to
    // avoid the cloning.
    match data.data_type().clone() {
        DataType::Boolean => Arc::new(array::BooleanArray::from(data)) as ArrayRef,
        DataType::Int8 => Arc::new(array::Int8Array::from(data)) as ArrayRef,
        DataType::Int16 => Arc::new(array::Int16Array::from(data)) as ArrayRef,
        DataType::Int32 => Arc::new(array::Int32Array::from(data)) as ArrayRef,
        DataType::Int64 => Arc::new(array::Int64Array::from(data)) as ArrayRef,
        DataType::UInt8 => Arc::new(array::UInt8Array::from(data)) as ArrayRef,
        DataType::UInt16 => Arc::new(array::UInt16Array::from(data)) as ArrayRef,
        DataType::UInt32 => Arc::new(array::UInt32Array::from(data)) as ArrayRef,
        DataType::UInt64 => Arc::new(array::UInt64Array::from(data)) as ArrayRef,
        DataType::Float32 => Arc::new(array::Float32Array::from(data)) as ArrayRef,
        DataType::Float64 => Arc::new(array::Float64Array::from(data)) as ArrayRef,
        DataType::Utf8 => Arc::new(array::BinaryArray::from(data)) as ArrayRef,
        DataType::List(_) => Arc::new(array::ListArray::from(data)) as ArrayRef,
        DataType::Struct(_) => Arc::new(array::StructArray::from(data)) as ArrayRef,
        dt => panic!("Unexpected data type {:?}", dt),
    }
}

//impl From<&ArrayRef> for &PrimitiveArray<BooleanType> {
//    fn from(array: &ArrayRef) -> Self {
//        array.as_any().downcast_ref::<BooleanArray>().unwrap()
//    }
//}

//impl<T: ArrowPrimitiveType> From<&Array> for &PrimitiveArray<T> {
//    fn from(array: &Array) -> Self {
//        match array.data_type() {
//            DataType::Boolean => array.as_any().downcast_ref::<T>().unwrap()
//        }
////        _ => unimplemented!("Casting array to other primitive types is not implemented")
//    }
//}

//fn array_to_primitive<T>(array: &Array) -> &PrimitiveArray<T>
//    where
//        T: ArrowPrimitiveType,
//{
//    match array.data_type() {
//        DataType::Boolean => {
//            array.as_any().downcast_ref::<BooleanArray>().unwrap()
//        }
//        _ => unimplemented!("Casting for other array types is not implemented")
//    }
//}

pub struct DataFrame {
    schema: Arc<Schema>,
    columns: Vec<ArrayRef>,
}

struct CsvDataSource {
    reader: CsvReader,
}

//impl Iterator for CsvDataSource {
//    type Item = Result<RecordBatch, ArrowError>;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        Some(self.reader.next())
//    }
//}

impl DataFrame {
    /// Create an empty `DataFrame`
    fn empty() -> Self {
        DataFrame {
            schema: Arc::new(Schema::empty()),
            columns: vec![],
        }
    }

    fn new(schema: Arc<Schema>, columns: Vec<Arc<Array>>) -> Self {
        DataFrame { schema, columns }
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.columns[0].data().len()
    }

    /// Get a column from schema by index.
    pub fn column(&self, i: usize) -> &ArrayRef {
        &self.columns[i]
    }

    pub fn column_by_name(&self, name: &str) -> &ArrayRef {
        let column_number = self.schema.column_with_name(name).unwrap();
        let column = &self.columns[column_number.0];
        let field = self.schema.field(column_number.0);
        column
    }

    /// Returns a new `DataFrame` with column appended.
    pub fn with_column(mut self, name: &str, column: ArrayRef) -> Self {
        let mut fields = self.schema.fields().clone();
        fields.push(Field::new(
            name,
            column.data_type().clone(),
            column.null_count() > 0,
        ));
        self.schema = Arc::new(Schema::new(fields));
        self.columns.push(column);
        self
    }

    /// Returns the `DataFrame` with the specified column renamed
    pub fn with_column_renamed(mut self, old_name: &str, new_name: &str) -> Self {
        // this should only modify the schema
        let (index, mut field) = self.schema.column_with_name(old_name).unwrap();
        let new_field = Field::new(new_name, field.data_type().clone(), field.is_nullable());
        let mut fields = self.schema.fields().clone();
        fields[index] = new_field;
        self.schema = Arc::new(Schema::new(fields.to_vec()));
        self
    }

    /// Returns dataframe as an Arrow `RecordBatch`
    fn as_record_batch(&self) -> RecordBatch {
        RecordBatch::new(self.schema.clone(), self.columns.clone())
    }

    /// Returns dataframe with the first n records selected
    fn take(&self, count: usize) -> Self {
        DataFrame::new(
            self.schema.clone(),
            self.columns
                .clone()
                .into_iter()
                .map(|col| {
                    ArrayDataBuilder::new(col.data_type().clone())
                        .child_data(
                            col.data()
                                .child_data()
                                .iter()
                                .take(count)
                                .into_iter()
                                .map(|x| x.clone())
                                .collect(),
                        )
                        .build()
                })
                .map(|col| make_array(col))
                .collect(),
        )
    }

    fn intersect(&self, other: &DataFrame) -> Self {
        unimplemented!("Intersect not yet implemented")
    }

    /// Returns dataframe with specified columns selected.
    ///
    /// If a column name does not exist, it is omitted
    pub fn select(&self, col_names: Vec<&str>) -> Self {
        // get the names of columns from the schema, and match them with supplied
        let mut col_num: i16 = -1;
        let schema = self.schema.clone();
        let field_names: Vec<(usize, &str)> = schema
            .fields()
            .into_iter()
            .map(|c| {
                col_num += 1;
                (col_num as usize, c.name().as_str())
            })
            .collect();

        // filter names
        let filter_cols: Vec<(usize, &str)> = if col_names.contains(&"*") {
            field_names
        } else {
            // TODO follow the order of user-supplied column names
            field_names
                .into_iter()
                .filter(|(col, name)| col_names.contains(name))
                .collect()
        };

        // construct dataframe with selected columns
        DataFrame {
            schema: Arc::new(Schema::new(
                filter_cols
                    .iter()
                    .map(|c| schema.field(c.0).clone())
                    .collect(),
            )),
            columns: filter_cols
                .into_iter()
                .map(move |c| self.columns[c.0].clone())
                .collect(),
        }
    }

    pub fn drop(&self, col_names: Vec<&str>) -> Self {
        // get the names of columns from the schema, and match them with supplied
        let mut col_num: i16 = -1;
        let schema = self.schema.clone();
        let field_names: Vec<(usize, &str)> = schema
            .fields()
            .into_iter()
            .map(|c| {
                col_num += 1;
                (col_num as usize, c.name().as_str())
            })
            .collect();

        // filter names
        let filter_cols: Vec<(usize, &str)> = {
            // TODO follow the order of user-supplied column names
            field_names
                .into_iter()
                .filter(|(col, name)| !col_names.contains(name))
                .collect()
        };

        // construct dataframe with selected columns
        DataFrame {
            schema: Arc::new(Schema::new(
                filter_cols
                    .iter()
                    .map(|c| schema.field(c.0).clone())
                    .collect(),
            )),
            columns: filter_cols
                .into_iter()
                .map(move |c| self.columns[c.0].clone())
                .collect(),
        }
    }

    pub fn from_table(table: arrow::table::Table) -> Self {
        DataFrame {
            schema: table.schema().clone(),
            columns: table.columns()
        }
    }

    pub fn from_csv(path: &str, schema: Option<Arc<Schema>>) -> Self {
        let file = File::open(path).unwrap();
        let mut reader = match schema {
            Some(schema) => CsvReader::new(file, schema, true, 1024, None),
            None => {
                let builder = CsvReaderBuilder::new().infer_schema(None).has_headers(true).with_batch_size(6);
                builder.build(file).unwrap()
            }
        };
        let mut batches: Vec<RecordBatch> = vec![];
        let mut has_next = true;
        while has_next {
            match reader.next() {
                Ok(batch) => {
                    match batch {
                        Some(batch) => {
                            batches.push(batch);
                        },
                        None => {
                            has_next = false;
                        }
                    }
                },
                Err(e) => {
                    has_next = false;
                }
            }
        }

        let schema: Arc<Schema> = batches[0].schema().clone();

        // convert to an arrow table
        let table = arrow::table::Table::from_record_batches(schema, batches);

        DataFrame::from_table(table)
    }
}

mod tests {
    use arrow::array::Float64Array;
    use crate::dataframe::DataFrame;
    use crate::functions::scalar::ScalarFunctions;
    use std::sync::Arc;

    #[test]
    fn create_empty_dataframe() {
        let dataframe = DataFrame::empty();

        assert_eq!(0, dataframe.num_columns());
        assert_eq!(0, dataframe.schema().fields().len());
    }

    #[test]
    fn read_csv_to_dataframe() {
        let dataframe = DataFrame::from_csv("./test/data/uk_cities_with_headers.csv", None);

        assert_eq!(3, dataframe.num_columns());
        assert_eq!(37, dataframe.num_rows());
    }

    #[test]
    fn dataframe_ops() {
        let mut dataframe = DataFrame::from_csv("./test/data/uk_cities_with_headers.csv", None);
        let a = dataframe.column_by_name("lat").as_ref();
        let b = dataframe.column_by_name("lng").as_ref();
        let sum = ScalarFunctions::add(
            a.as_any().downcast_ref::<Float64Array>().unwrap(), 
            b.as_any().downcast_ref::<Float64Array>().unwrap()
        );
        dataframe = dataframe.with_column("lat_lng_sum", Arc::new(sum.unwrap()));

        assert_eq!(4, dataframe.num_columns());
        assert_eq!(4, dataframe.schema().fields().len());
        assert_eq!(54.31776, dataframe.column_by_name("lat_lng_sum").as_any().downcast_ref::<Float64Array>().unwrap().value(0));

        dataframe = dataframe.with_column_renamed("lat_lng_sum", "ll_sum");

        assert_eq!("ll_sum", dataframe.schema().field(3).name());

        dataframe = dataframe.select(vec!["*"]);

        assert_eq!(4, dataframe.num_columns());
        assert_eq!(4, dataframe.schema().fields().len());

        let df2 = dataframe.select(vec!["lat", "lng"]);

        assert_eq!(2, df2.num_columns());
        assert_eq!(2, df2.schema().fields().len());

        // drop columns from `dataframe`
        let df3 = dataframe.drop(vec!["city", "ll_sum"]);

        assert_eq!(df2.schema().fields(), df3.schema().fields());

        // calculate absolute value of `lng`
        let abs: PrimitiveArray<Float64Type> = ScalarFunctions::abs(
            dataframe.column_by_name("lng").as_ref()
                .as_any().downcast_ref::<Float64Array>().unwrap()
        ).unwrap();

        assert_eq!(3.335724, abs.value(0));
    }
}
