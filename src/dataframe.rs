use std::fs::{metadata, read_dir, File};
use std::io::BufReader;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use arrow::array::{Array, ArrayDataBuilder, ArrayDataRef, ArrayRef, UInt32Array, UInt64Array};
use arrow::compute;
use arrow::csv::{Reader as CsvReader, ReaderBuilder as CsvReaderBuilder};
use arrow::datatypes::*;
use arrow::error::ArrowError;
use arrow::ipc::{reader::FileReader as IpcFileReader, writer::FileWriter as IpcFileWriter};
use arrow::json::{Reader as JsonReader, ReaderBuilder as JsonReaderBuilder};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow::util::pretty;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::error::{DataFrameError, Result};
use crate::expression::{
    BooleanFilter, BooleanFilterEval, BooleanInput, JoinCriteria, SortCriteria, SqlDatabase,
    SqlWriteOptions,
};
use crate::io::sql::{self, SqlDataSink, SqlDataSource};
use crate::table::Column;
use crate::utils;

use compute::{SortColumn, SortOptions};

pub struct DataFrame {
    schema: Arc<Schema>,
    columns: Vec<Column>,
}

impl DataFrame {
    /// Create an empty `DataFrame`
    pub fn empty() -> Self {
        DataFrame {
            schema: Arc::new(Schema::empty()),
            columns: vec![],
        }
    }

    fn new(schema: Arc<Schema>, columns: Vec<Column>) -> Self {
        DataFrame { schema, columns }
    }

    fn from_arrays(schema: Arc<Schema>, arrays: Vec<Arc<dyn Array>>) -> Self {
        let columns = arrays
            .into_iter()
            .enumerate()
            .map(|(i, array)| Column::from_arrays(vec![array], schema.field(i).clone()))
            .collect();
        DataFrame { schema, columns }
    }

    pub fn from_columns(schema: Arc<Schema>, columns: Vec<Column>) -> Self {
        DataFrame { schema, columns }
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_chunks(&self) -> usize {
        self.column(0).data.num_chunks()
    }

    pub fn num_rows(&self) -> usize {
        self.columns[0].data.num_rows()
    }

    /// Get a column from schema by index.
    pub fn column(&self, i: usize) -> &Column {
        &self.columns[i]
    }

    pub fn column_by_name(&self, name: &str) -> &Column {
        let column_number = self
            .schema
            .column_with_name(name)
            .expect("Column not found by name");
        let column = &self.columns[column_number.0];
        let field = self.schema.field(column_number.0);
        column
    }

    /// Returns a new `DataFrame` with column appended.
    pub fn with_column(mut self, name: &str, column: Column) -> Self {
        let mut fields = self.schema.fields().clone();
        // check if field exists, and overwrite it
        let col = self.schema.column_with_name(name);
        match col {
            Some((i, field)) => {
                self = self.drop(vec![name]);
                fields = self.schema.fields().clone();
            }
            None => {}
        }
        fields.push(Field::new(
            name,
            column.data_type().clone(),
            column.field().is_nullable(),
        ));
        self.schema = Arc::new(Schema::new(fields));
        self.columns.push(column);
        self
    }

    /// Returns the `DataFrame` with the specified column renamed
    pub fn with_column_renamed(mut self, old_name: &str, new_name: &str) -> Self {
        // this should only modify the schema
        let (index, field) = self.schema.column_with_name(old_name).unwrap();
        let new_field = Field::new(new_name, field.data_type().clone(), field.is_nullable());
        let mut fields = self.schema.fields().clone();
        fields[index] = new_field;
        self.schema = Arc::new(Schema::new(fields.to_vec()));
        self
    }

    /// Returns dataframe as an Arrow `RecordBatch`
    /// TODO: add a method to break into smaller batches
    pub fn to_record_batches(&self) -> Vec<RecordBatch> {
        let num_chunks = self.column(0).data().num_chunks();
        let num_columns = self.num_columns();
        let mut batches: Vec<RecordBatch> = Vec::with_capacity(num_chunks);
        let mut arrays: Vec<Vec<ArrayRef>> = Vec::with_capacity(num_chunks);
        // for i in 0..self.num_columns() {
        //     let column = self.column(i);
        //     if i == 0 {
        //         arrays.push(vec![]);
        //     }
        //     for j in 0..column.data().num_chunks() {
        //         arrays[i].push(column.data().chunk(j).to_owned());
        //     }
        // }

        for i in 0..num_chunks {
            let mut arr = vec![];

            // if i == 0 {
            //     arrays.push(vec![]);
            // }
            for j in 0..num_columns {
                let column = self.column(j);
                arr.push(column.data().chunk(i).to_owned());
            }

            arrays.push(arr);
        }

        arrays.into_iter().for_each(|array| {
            // the unwrap is infallible as we're passing data that's already been verified
            batches.push(RecordBatch::try_new(self.schema.clone(), array).unwrap());
        });

        batches
    }

    /// Returns dataframe with the first n records selected
    pub fn limit(&self, count: usize) -> Self {
        Self::new(
            self.schema.clone(),
            self.columns
                .clone()
                .into_iter()
                .map(|col| col.slice(0, Some(count)))
                .collect(),
        )
    }

    /// Filter the dataframe
    pub fn filter(&self, condition: &BooleanFilter) -> Self {
        // create boolean array to filter with
        let mask = self.evaluate_boolean_filter(condition).unwrap();
        Self::new(
            self.schema.clone(),
            self.columns
                .clone()
                .into_iter()
                .map(|col| col.filter(&mask))
                .collect(),
        )
    }

    /// Sort the dataframe by specified criteria
    ///
    /// Note that the signature will be changed to return `Self` if all Arrow types can be sortable
    pub fn sort(&self, criteria: &Vec<SortCriteria>) -> Result<Self> {
        if criteria.is_empty() {
            return Err(DataFrameError::ComputeError(
                "Sort criteria cannot be empty".to_string(),
            ));
        }
        let mut lex_criteria = vec![];
        for c in criteria {
            let column = self.column_by_name(c.column.as_ref());
            let array = column.to_array()?;
            lex_criteria.push(SortColumn {
                values: array,
                options: Some(SortOptions {
                    descending: c.descending,
                    nulls_first: false,
                }),
            });
        }
        let sort_indices = compute::kernels::sort::lexsort_to_indices(&lex_criteria)?;
        self.sort_by_indices(&sort_indices)
    }

    fn sort_by_indices(&self, indices: &UInt32Array) -> Result<Self> {
        let mut columns = Vec::with_capacity(self.num_columns());
        for col in &self.columns {
            columns.push(col.take(indices, 4096)?);
        }
        Ok(Self::from_columns(self.schema().clone(), columns))
    }

    /// Prints the data frame to console
    ///
    /// It is the caller's responsibility to limit the dataframe if wishing to print a subset
    pub fn display(&self) -> Result<()> {
        Ok(pretty::print_batches(&self.to_record_batches())?)
    }

    /// Return the dataframe with an id that monotonically increases
    ///
    /// The id is a 64-bit array
    pub fn with_id(self, name: &str) -> Self {
        let distribution = &self.column(0).data.chunk_counts();
        dbg!(&distribution);
        let arrays = distribution
            .iter()
            .zip(0..distribution.len())
            .map(|(count, index)| {
                // assumption that we won't create record batches that have 100k rows
                let multiple = (100_000 * index) as u64;
                let range: Vec<u64> = ((multiple + 1)..(multiple + *count as u64 + 1)).collect();
                Arc::new(UInt64Array::from(range)) as ArrayRef
            })
            .collect();
        let column = Column::from_arrays(arrays, Field::new(name, DataType::UInt64, false));
        self.with_column(name, column)
    }

    fn intersect(&self, other: &DataFrame) -> Self {
        unimplemented!("Intersect not yet implemented")
    }

    /// Returns dataframe with specified columns selected.
    ///
    /// If a column name does not exist, it is omitted.
    pub fn select(&mut self, col_names: Vec<&str>) -> Self {
        // get the names of columns from the schema, and match them with supplied
        let mut col_num: i16 = -1;
        let schema = &self.schema.clone();
        let field_names: Vec<(usize, &str)> = schema
            .fields()
            .iter()
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

        let mut columns = vec![];

        for (i, u) in filter_cols.clone() {
            let c = &self.columns[i];
            columns.push(c.clone());
        }

        let new_schema = Arc::new(Schema::new(
            filter_cols
                .iter()
                .map(|c| schema.field(c.0).clone())
                .collect(),
        ));

        DataFrame::from_columns(new_schema, columns)
    }

    /// Returns a dataframe with specified columns dropped.
    ///
    /// If a column name does not exist, it is omitted.
    pub fn drop(&mut self, col_names: Vec<&str>) -> Self {
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
                .iter()
                .map(move |c| self.columns[c.0].clone())
                .collect(),
        }
    }

    /// Create a dataframe from an Arrow Table.
    ///
    /// Arrow Tables are not yet in the Rust library, and we are hashing them out here
    pub fn from_table(table: crate::table::Table) -> Self {
        DataFrame {
            schema: table.schema().clone(),
            columns: table.columns().to_vec(),
        }
    }

    pub fn from_csv(path: &str, schema: Option<Arc<Schema>>) -> Self {
        let file = File::open(path).unwrap();
        let mut reader = match schema {
            Some(schema) => CsvReader::new(file, schema, true, None, 1024, None),
            None => {
                let builder = CsvReaderBuilder::new()
                    .infer_schema(None)
                    .has_header(true)
                    .with_batch_size(1024);
                builder.build(file).unwrap()
            }
        };
        let mut batches: Vec<RecordBatch> = vec![];
        let mut has_next = true;
        while has_next {
            match reader.next() {
                Ok(batch) => match batch {
                    Some(batch) => {
                        batches.push(batch);
                    }
                    None => {
                        has_next = false;
                    }
                },
                Err(e) => {
                    has_next = false;
                }
            }
        }

        let schema: Arc<Schema> = batches[0].schema().clone();

        // convert to an arrow table
        let table = crate::table::Table::from_record_batches(schema.clone(), batches);

        // DataFrame::from_table(table)
        DataFrame {
            schema,
            columns: table.columns,
        }
    }

    pub fn from_arrow(path: &str) -> Result<Self> {
        let mut reader = IpcFileReader::try_new(File::open(path)?)?;

        let schema = reader.schema();
        let mut batches = vec![];

        while let Some(batch) = reader.next()? {
            batches.push(batch);
        }

        let table = crate::table::Table::from_record_batches(schema.clone(), batches);

        Ok(DataFrame {
            schema,
            columns: table.columns,
        })
    }

    pub fn from_json(path: &str, schema: Option<Arc<Schema>>) -> Self {
        let file = File::open(path).unwrap();
        let mut reader = match schema {
            Some(schema) => JsonReader::new(BufReader::new(file), schema, 1024, None),
            None => {
                let builder = JsonReaderBuilder::new()
                    .infer_schema(None)
                    .with_batch_size(1024);
                builder.build::<File>(file).unwrap()
            }
        };
        let mut batches: Vec<RecordBatch> = vec![];
        let mut has_next = true;
        while has_next {
            match reader.next() {
                Ok(batch) => match batch {
                    Some(batch) => {
                        batches.push(batch);
                    }
                    None => {
                        has_next = false;
                    }
                },
                Err(e) => {
                    has_next = false;
                }
            }
        }

        let schema: Arc<Schema> = batches[0].schema().clone();

        // convert to an arrow table
        let table = crate::table::Table::from_record_batches(schema.clone(), batches);

        // DataFrame::from_table(table)
        DataFrame {
            schema,
            columns: table.columns,
        }
    }

    pub fn from_parquet(path: &str) -> Result<Self> {
        let attr = metadata(path)?;
        let paths;
        if attr.is_dir() {
            let readdir = read_dir(path)?;
            paths = readdir
                .into_iter()
                .filter_map(|r| r.ok())
                .map(|entry| entry.path())
                .collect();
        } else {
            paths = vec![PathBuf::from(path)];
        }

        let mut schema = None;
        let mut batches = vec![];
        for path in paths.iter() {
            let file = File::open(path)?;
            let file_reader = SerializedFileReader::new(file)?;
            if file_reader.metadata().num_row_groups() == 0 {
                // skip empty parquet files
                continue;
            }

            let mut arrow_reader = ParquetFileArrowReader::new(Rc::new(file_reader));
            if schema.is_none() {
                schema = Some(Arc::new(arrow_reader.get_schema()?));
            }

            let mut record_batch_reader = arrow_reader.get_record_reader(1024)?;
            while let Ok(Some(batch)) = record_batch_reader.next_batch() {
                batches.push(batch);
            }
        }

        let table = crate::table::Table::from_record_batches(schema.unwrap(), batches);

        Ok(Self {
            schema: table.schema().clone(),
            columns: table.columns,
        })
    }

    /// Create a DataFrame from a SQL table
    ///
    /// Note: Only PostgreSQL is currently supported, and data is buffered in-memory before creating dataframe.
    /// This might be undesirable if reading large tables. However, note that this library currently performs
    /// eager evaluation, so the DataFrame would still be created and held in-memory. We will improve this with
    /// a better execution model in future.
    pub fn from_sql_table(connection_string: &str, table_name: &str) -> Self {
        let batches =
            sql::postgres::Postgres::read_table(connection_string, table_name, None, 1024).unwrap();
        if batches.is_empty() {
            DataFrame::empty()
        } else {
            let schema = batches.get(0).unwrap().schema().clone();
            let table = crate::table::Table::from_record_batches(schema.clone(), batches);
            DataFrame {
                schema,
                columns: table.columns,
            }
        }
    }

    /// Write dataframe to an Arrow IPC file
    ///
    /// TOOO: caller must supply extension as there is no common extension yet
    pub fn to_arrow(&self, path: &str) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = IpcFileWriter::try_new(file, self.schema())?;
        let record_batches = self.to_record_batches();

        for batch in record_batches {
            writer.write(&batch)?;
        }

        Ok(())
    }

    pub fn to_csv(&self, path: &str) -> Result<()> {
        // use csv::error::Error;
        use arrow::csv::Writer;

        let file = File::create(path)?;

        let mut wrt = Writer::new(file);

        let batches = self.to_record_batches();
        let results: Result<Vec<_>> = batches
            .iter()
            .map(|b| wrt.write(b).map_err(|e| e.into()))
            .collect();

        results?;

        Ok(())
    }

    pub fn to_sql(
        &self,
        table_name: &str,
        options: &crate::expression::SqlWriteOptions,
    ) -> Result<()> {
        match options.db {
            SqlDatabase::Postgres => {
                if options.overwrite {
                    sql::postgres::Postgres::create_table(
                        options.connection_string.as_str(),
                        table_name,
                        self.schema(),
                    )?;
                }
                sql::postgres::Postgres::write_to_table(
                    options.connection_string.as_str(),
                    table_name,
                    &self.to_record_batches(),
                )?;
            }
            SqlDatabase::MsSql => {
                return Err(DataFrameError::SqlError(
                    "MSSQL database not yet supported".to_string(),
                ));
            }
            SqlDatabase::MySql => {
                return Err(DataFrameError::SqlError(
                    "MySQL database not yet supported".to_string(),
                ))
            }
        }
        Ok(())
    }

    /// get a column from table as a column expression
    pub fn expr_column(&self, i: usize) -> crate::expression::Column {
        self.schema().field(i).clone().into()
    }

    /// Get a column from table by name as a column expression
    pub fn expr_column_by_name(&self, name: &str) -> crate::expression::Column {
        self.schema()
            .column_with_name(name)
            .unwrap()
            .1
            .clone()
            .into()
    }

    /// Evaluate a `BooleanFilter` against dataframe, returning a column with chunked `BooleanArray` masks
    fn evaluate_boolean_filter(&self, filter: &BooleanFilter) -> Result<crate::table::Column> {
        // create a new column with filter
        let bools: Result<Vec<ArrayRef>> = self
            .to_record_batches()
            .iter()
            .map(|batch| filter.eval_to_array(batch))
            .collect();
        let column = crate::table::Column::from_arrays(
            bools?,
            Field::new("bool_filter", DataType::Boolean, true),
        );
        Ok(column)
    }

    pub fn join(&self, other: &Self, criteria: &JoinCriteria) -> Result<Self> {
        // get join indices
        let (left_indices, right_indices) =
            crate::functions::join::calc_equijoin_indices(self, other, criteria);
        // partition dataframes into chunk boundaries, and collect them
        let mut offset = 0;
        let left_boundaries = self
            .column(0)
            .data()
            .chunks()
            .iter()
            .zip(0..self.num_chunks())
            .map(|(a, i)| {
                let start = offset as u32;
                offset += a.len();
                (start, offset as u32, i)
            })
            .collect::<Vec<(u32, u32, usize)>>();

        let mut offset = 0;
        let right_boundaries = other
            .column(0)
            .data()
            .chunks()
            .iter()
            .zip(0..other.num_chunks())
            .map(|(a, i)| {
                let start = offset as u32;
                offset += a.len();
                (start, offset as u32, i)
            })
            .collect::<Vec<(u32, u32, usize)>>();

        let left_last = left_boundaries.len() - 1;
        let right_last = right_boundaries.len() - 1;

        // calculate cut-off points to ensure that left and right have the same lengths of columns
        let left_bound = left_boundaries;
        let right_bound = right_boundaries;
        let mut min_left;
        let mut min_right;
        let mut max_left;
        let mut max_right;
        let mut seen_left = 0;
        let mut seen_right = 0;
        let mut merged_boundaries = vec![];
        while seen_left <= left_last && seen_right <= right_last {
            let l: &(u32, u32, usize) = left_bound.get(seen_left).unwrap();
            let r: &(u32, u32, usize) = right_bound.get(seen_right).unwrap();
            min_left = l.0;
            max_left = l.1;
            min_right = r.0;
            max_right = r.1;
            let v = (
                if min_left <= min_right {
                    min_left
                } else {
                    min_right
                },
                if max_left <= max_right {
                    max_left
                } else {
                    max_right
                },
                l.2,
                r.2,
            );
            if v.1 >= max_left {
                seen_left += 1;
            }
            if v.1 >= max_right {
                seen_right += 1;
            }
            merged_boundaries.push(v);
        }

        // reconstruct the record batches from both sides
        let left = UInt32Array::from(left_indices);
        let right = UInt32Array::from(right_indices);
        let mut joined_columns = Vec::with_capacity(self.num_columns() + other.num_columns());
        for col in &self.columns {
            joined_columns.push(col.take(&left, 4096)?);
                }
        for col in &other.columns {
            joined_columns.push(col.take(&right, 4096)?);
                }

        // create merged schema
        let mut merged_fields = self.schema().fields().clone();
        merged_fields.append(&mut other.schema().fields().clone());
        let merged_schema = Arc::new(Schema::new(merged_fields));

        Ok(Self::from_columns(merged_schema, joined_columns))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Float64Type, Schema};
    use std::sync::Arc;

    use crate::functions::scalar::ScalarFunctions;
    use crate::{
        expression::{JoinCriteria, JoinType, SqlDatabase, SqlWriteOptions},
        table::*,
    };

    #[test]
    fn test_create_empty_dataframe() {
        let dataframe = DataFrame::empty();

        assert_eq!(0, dataframe.num_columns());
        assert_eq!(0, dataframe.schema().fields().len());
    }

    #[test]
    fn test_read_csv_to_dataframe() {
        let dataframe = DataFrame::from_csv("./test/data/uk_cities_with_headers.csv", None);

        assert_eq!(3, dataframe.num_columns());
        assert_eq!(37, dataframe.num_rows());
    }

    #[test]
    fn test_read_postgres_table_to_then_save() {
        let connection_string = "postgres://postgres:password@localhost:5432/postgres";
        let mut dataframe = DataFrame::from_sql_table(connection_string, "arrow_data_types");
        assert!(dataframe.num_columns() > 3);
        assert!(dataframe.num_rows() > 0);

        // invert booleans
        let strings = dataframe.column_by_name("strings");
        let lower = ScalarFunctions::lower(col_to_string_arrays(strings)).unwrap();
        let lower: Vec<ArrayRef> = lower.into_iter().map(|a| Arc::new(a) as ArrayRef).collect();
        dataframe = dataframe.with_column(
            "strings_lower",
            Column::from_arrays(lower, Field::new("strings_lower", DataType::Utf8, true)),
        );

        dataframe
            .to_sql(
                "t2_strings_lower",
                &SqlWriteOptions {
                    connection_string: connection_string.to_string(),
                    overwrite: true,
                    db: SqlDatabase::Postgres,
                },
            )
            .unwrap();
    }

    #[test]
    fn test_dataframe_ops() {
        let mut dataframe = DataFrame::from_csv("./test/data/uk_cities_with_headers.csv", None);
        let a = dataframe.column_by_name("lat");
        let b = dataframe.column_by_name("lng");
        let sum = ScalarFunctions::add(
            col_to_prim_arrays::<Float64Type>(a),
            col_to_prim_arrays::<Float64Type>(b),
        );
        // TODO, make this better
        let sum: Vec<ArrayRef> = sum
            .unwrap()
            .into_iter()
            .map(|p| Arc::new(p) as ArrayRef)
            .collect();
        dataframe = dataframe.with_column(
            "lat_lng_sum",
            Column::from_arrays(sum, Field::new("lat_lng_sum", DataType::Float64, true)),
        );

        assert_eq!(4, dataframe.num_columns());
        assert_eq!(4, dataframe.schema().fields().len());
        assert_eq!(
            54.31776,
            col_to_prim_arrays::<Float64Type>(dataframe.column_by_name("lat_lng_sum"))[0].value(0)
        );

        dataframe = dataframe.with_column_renamed("lat_lng_sum", "ll_sum");

        assert_eq!("ll_sum", dataframe.schema().field(3).name());

        // dataframe = dataframe.select(vec!["*"]);

        // assert_eq!(4, dataframe.num_columns());
        // assert_eq!(4, dataframe.schema().fields().len());

        // let df2 = dataframe.select(vec!["lat", "lng"]);

        // assert_eq!(2, df2.num_columns());
        // assert_eq!(2, df2.schema().fields().len());

        // // drop columns from `dataframe`
        // let df3 = dataframe.drop(vec!["city", "ll_sum"]);

        // assert_eq!(df2.schema().fields(), df3.schema().fields());

        // calculate absolute value of `lng`
        let abs = ScalarFunctions::abs(col_to_prim_arrays::<Float64Type>(
            dataframe.column_by_name("lng"),
        ))
        .unwrap();

        assert_eq!(3.335724, abs[0].value(0));
    }

    #[test]
    fn test_arrow_io() {
        let mut dataframe = DataFrame::from_csv("./test/data/uk_cities_with_headers.csv", None);
        let a = dataframe.column_by_name("lat");
        let b = dataframe.column_by_name("lng");
        let sum = ScalarFunctions::add(
            col_to_prim_arrays::<Float64Type>(a),
            col_to_prim_arrays::<Float64Type>(b),
        );
        // TODO, make this better
        let sum: Vec<ArrayRef> = sum
            .unwrap()
            .into_iter()
            .map(|p| Arc::new(p) as ArrayRef)
            .collect();
        dataframe = dataframe.with_column(
            "lat_lng_sum",
            Column::from_arrays(sum, Field::new("lat_lng_sum", DataType::Float64, true)),
        );

        let city = dataframe.column_by_name("city");
        let lowercase = ScalarFunctions::lower(col_to_string_arrays(city));
        let lowercase: Vec<ArrayRef> = lowercase
            .unwrap()
            .into_iter()
            .map(|p| Arc::new(p) as ArrayRef)
            .collect();
        dataframe = dataframe.with_column(
            "city_lower",
            Column::from_arrays(lowercase, Field::new("city_lower", DataType::Utf8, true)),
        );

        let write = dataframe.to_arrow("./test/data/uk_cities.arrow");
        assert!(write.is_ok());
    }

    #[test]
    fn test_csv_io() {
        let mut dataframe = DataFrame::from_csv("./test/data/uk_cities_with_headers.csv", None);
        let a = dataframe.column_by_name("lat");
        let b = dataframe.column_by_name("lng");
        let sum = ScalarFunctions::add(
            col_to_prim_arrays::<Float64Type>(a),
            col_to_prim_arrays::<Float64Type>(b),
        );
        // TODO, make this better
        let sum: Vec<ArrayRef> = sum
            .unwrap()
            .into_iter()
            .map(|p| Arc::new(p) as ArrayRef)
            .collect();
        dataframe = dataframe.with_column(
            "lat_lng_sum",
            Column::from_arrays(sum, Field::new("lat_lng_sum", DataType::Float64, true)),
        );

        let city = dataframe.column_by_name("city");
        let lowercase = ScalarFunctions::lower(col_to_string_arrays(city));
        let lowercase: Vec<ArrayRef> = lowercase
            .unwrap()
            .into_iter()
            .map(|p| Arc::new(p) as ArrayRef)
            .collect();
        dataframe = dataframe.with_column(
            "city_lower",
            Column::from_arrays(lowercase, Field::new("city_lower", DataType::Utf8, true)),
        );

        let write = dataframe.to_csv("/tmp/uk_cities_out.csv");
        assert!(write.is_ok());
    }

    #[test]
    fn test_increasing_id() {
        let mut dataframe = DataFrame::from_csv("./test/data/uk_cities_with_headers.csv", None);
        dataframe = dataframe.limit(10);
        dataframe = dataframe.with_id("id");
        let id = dataframe.column_by_name("id");
        assert_eq!(id.data_type(), &DataType::UInt64);
        assert_eq!(id.name(), "id");
        assert_eq!(id.num_rows(), 10);
        let array = id.data().chunk(0);
        let array: UInt64Array = UInt64Array::from(array.data());
        assert_eq!(
            array.value_slice(0, 10),
            &(1u64..11u64).collect::<Vec<u64>>()[..]
        );
    }

    #[test]
    fn test_sort() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::UInt8, false),
        ]);
        let a = Int32Array::from(vec![Some(1), Some(1), None, Some(3), Some(3), Some(4)]);
        let b = UInt8Array::from(vec![9, 5, 6, 7, 4, 8]);
        let frame = DataFrame::from_arrays(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]);
        let sort_criteria_a = SortCriteria {
            column: "a".to_string(),
            descending: true,
            nulls_first: false,
        };
        let sort_criteria_b = SortCriteria {
            column: "b".to_string(),
            descending: false,
            nulls_first: false,
        };
        let sorted = frame.sort(&vec![sort_criteria_a, sort_criteria_b]).unwrap();
        let a = sorted.column(0);
        let a_chunks = a.data().chunks();
        assert_eq!(a_chunks.len(), 1);
        let a_array = a_chunks[0].as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(a_array.is_null(5));
        assert_eq!(a_array.value(0), 4);
        assert_eq!(a_array.value(1), 3);
        assert_eq!(a_array.value(2), 3);
        assert_eq!(a_array.value(3), 1);
        assert_eq!(a_array.value(4), 1);

        let b = sorted.column(1);
        let b_chunks = b.data().chunks();
        assert_eq!(b_chunks.len(), 1);
        let b_array = b_chunks[0].as_any().downcast_ref::<UInt8Array>().unwrap();
        assert_eq!(b_array.value(0), 8);
        assert_eq!(b_array.value(1), 4);
        assert_eq!(b_array.value(2), 7);
        assert_eq!(b_array.value(3), 5);
        assert_eq!(b_array.value(4), 9);
        assert_eq!(b_array.value(5), 6);
    }

    #[test]
    fn test_left_join() {
        let connection_string = "postgres://postgres:password@localhost:5432/postgres";
        let a = DataFrame::from_sql_table(connection_string, "join_test_j1");
        let b = DataFrame::from_sql_table(connection_string, "join_test_j2");
        let joined = a
            .join(
            &b,
            &JoinCriteria {
                join_type: JoinType::LeftJoin,
                    criteria: vec![("b".to_string(), "d".to_string())],
                },
            )
            .unwrap();
        joined.display().unwrap();
        assert_eq!(joined.num_columns(), 6);
        assert_eq!(joined.num_rows(), 9);
    }

    #[test]
    fn test_right_join() {
        let connection_string = "postgres://postgres:password@localhost:5432/postgres";
        let a = DataFrame::from_sql_table(connection_string, "join_test_j1");
        let b = DataFrame::from_sql_table(connection_string, "join_test_j2");
        let joined = a
            .join(
                &b,
                &JoinCriteria {
                    join_type: JoinType::RightJoin,
                criteria: vec![("a".to_string(), "d".to_string())],
            },
            )
            .unwrap();
        joined.display().unwrap();
        assert_eq!(joined.num_rows(), 10);
        assert_eq!(joined.num_columns(), 6);
    }

    #[test]
    fn test_inner_join() {
        let connection_string = "postgres://postgres:password@localhost:5432/postgres";
        let a = DataFrame::from_sql_table(connection_string, "join_test_j1");
        let b = DataFrame::from_sql_table(connection_string, "join_test_j2");
        let joined = a
            .join(
                &b,
                &JoinCriteria {
                    join_type: JoinType::InnerJoin,
                    criteria: vec![("a".to_string(), "d".to_string())],
                },
            )
            .unwrap();
        joined.display().unwrap();
        assert_eq!(joined.num_rows(), 4);
        assert_eq!(joined.num_columns(), 6);
    }
}
