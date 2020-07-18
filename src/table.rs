use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;

use crate::error::*;

#[derive(Clone)]
pub struct ChunkedArray {
    chunks: Vec<Arc<dyn Array>>,
    num_rows: usize,
    null_count: usize,
}

impl ChunkedArray {
    /// Construct a `ChunkedArray` from a list of `Array`s.
    ///
    /// There must be at least 1 array, and all arrays must have the same data type.
    fn from_arrays(arrays: Vec<Arc<dyn Array>>) -> Self {
        assert!(!arrays.is_empty());
        let mut num_rows = 0;
        let mut null_count = 0;
        // check that arrays have the same type
        let data_type = &arrays[0].data_type();
        arrays.iter().for_each(|array| {
            assert!(&array.data_type() == data_type);
            num_rows += array.len();
            null_count += array.null_count();
        });
        ChunkedArray {
            chunks: arrays,
            num_rows,
            null_count,
        }
    }

    /// Return the length of the arrays in the chunk. This value is pre-computed.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn null_count(&self) -> usize {
        self.null_count
    }

    pub fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    /// Get the count per chunk
    ///
    /// This is useful for repartitioning
    pub(crate) fn chunk_counts(&self) -> Vec<usize> {
        self.chunks().iter().map(|chunk| chunk.len()).collect()
    }

    /// Get a chunk from the chunked array by index
    /// TODO: should this have bounds-chacking?
    pub fn chunk(&self, i: usize) -> &Arc<dyn Array> {
        &self.chunks[i]
    }

    pub fn chunks(&self) -> &Vec<Arc<dyn Array>> {
        &self.chunks
    }

    /// Construct a zero-copy slice of the chunked array with the indicated offset and length.
    ///
    /// The `offset` is the position of the first element in the constructed slice.
    /// `length` is the length of the slice. If there are not enough elements in the chunked array,
    /// the length will be adjusted accordingly.
    fn slice(&self, offset: usize, length: Option<usize>) -> Self {
        let mut offset = offset;
        let mut length = length.unwrap_or(std::usize::MAX);
        length = std::cmp::min(length, self.num_rows());
        let mut current_chunk: usize = 0;
        let mut new_chunks: Vec<ArrayRef> = vec![];
        // compute the first offset. If offset > whole chunks' lengths, skip those chunks
        while current_chunk < self.num_chunks() && offset >= self.chunk(current_chunk).len() {
            offset -= self.chunk(current_chunk).len();
            current_chunk += 1;
        }
        while current_chunk < self.num_chunks() && length > 0 {
            new_chunks.push(self.chunk(current_chunk).slice(offset, length));
            length -= std::cmp::min(length, self.chunk(current_chunk).len() - offset);
            offset = 0;
            current_chunk += 1;
        }
        Self::from_arrays(new_chunks)
    }

    fn filter(&self, condition: &Self) -> Self {
        let filtered: arrow::error::Result<Vec<ArrayRef>> = self
            .chunks()
            .iter()
            .zip(condition.chunks())
            .map(|(a, b): (&ArrayRef, &ArrayRef)| {
                arrow::compute::filter(a.as_ref(), &BooleanArray::from(b.data()))
            })
            .collect();
        Self::from_arrays(filtered.unwrap())
    }

    fn flatten(&self) {
        unimplemented!("This is for flattening struct columns, we aren't yet there")
    }
}

pub fn col_to_prim_arrays<T>(column: &Column) -> Vec<&PrimitiveArray<T>>
where
    T: ArrowPrimitiveType,
{
    let mut arrays: Vec<&PrimitiveArray<T>> = vec![];
    for chunk in column.data().chunks() {
        arrays.push(chunk.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap())
    }
    arrays
}

pub fn col_to_string_arrays(column: &Column) -> Vec<&StringArray> {
    let mut arrays = vec![];
    for chunk in column.data().chunks() {
        arrays.push(chunk.as_any().downcast_ref::<StringArray>().unwrap())
    }
    arrays
}

/// A column data structure consisting of a `Field` and `ChunkedArray`
#[derive(Clone)]
pub struct Column {
    pub(crate) data: ChunkedArray,
    field: arrow::datatypes::Field,
}

fn col_to_numeric_array<T>(column: &Column) -> Result<ArrayRef>
where
    T: ArrowNumericType,
{
    let len = column.num_rows();
    let mut builder = PrimitiveBuilder::<T>::new(len);
    let arrays = column
        .data()
        .chunks()
        .iter()
        .map(|array| array.data())
        .collect::<Vec<ArrayDataRef>>();
    builder.append_data(&arrays[..])?;
    Ok(Arc::new(builder.finish()))
}

impl Column {
    pub fn from_chunked_array(chunk: ChunkedArray, field: arrow::datatypes::Field) -> Self {
        Column { data: chunk, field }
    }

    pub fn from_arrays(arrays: Vec<Arc<dyn Array>>, field: arrow::datatypes::Field) -> Self {
        assert!(!arrays.is_empty());
        for array in &arrays {
            assert!(array.data_type() == field.data_type());
        }
        Column {
            data: ChunkedArray::from_arrays(arrays),
            field,
        }
    }

    /// Merge the chunk arrays into a single array
    pub fn to_array(&self) -> Result<ArrayRef> {
        let len = self.num_rows();
        match self.data_type() {
            DataType::Null => Ok(Arc::new(NullArray::new(len))),
            DataType::Boolean => todo!("find an efficient way of appending boolean arrays"),
            DataType::Int8 => col_to_numeric_array::<Int8Type>(self),
            DataType::Int16 => col_to_numeric_array::<Int16Type>(self),
            DataType::Int32 => col_to_numeric_array::<Int32Type>(self),
            DataType::Int64 => col_to_numeric_array::<Int64Type>(self),
            DataType::UInt8 => col_to_numeric_array::<UInt8Type>(self),
            DataType::UInt16 => col_to_numeric_array::<UInt16Type>(self),
            DataType::UInt32 => col_to_numeric_array::<UInt32Type>(self),
            DataType::UInt64 => col_to_numeric_array::<UInt64Type>(self),
            DataType::Float16 => Err(DataFrameError::ComputeError(
                "Not yet implemented".to_string(),
            )),
            DataType::Float32 => col_to_numeric_array::<Float32Type>(self),
            DataType::Float64 => col_to_numeric_array::<Float64Type>(self),
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => col_to_numeric_array::<TimestampSecondType>(self),
                TimeUnit::Millisecond => col_to_numeric_array::<TimestampMillisecondType>(self),
                TimeUnit::Microsecond => col_to_numeric_array::<TimestampMicrosecondType>(self),
                TimeUnit::Nanosecond => col_to_numeric_array::<TimestampNanosecondType>(self),
            },
            DataType::Date32(unit) => col_to_numeric_array::<Date32Type>(self),
            DataType::Date64(_) => col_to_numeric_array::<Date64Type>(self),
            DataType::Time32(unit) => match unit {
                TimeUnit::Second => col_to_numeric_array::<Time32SecondType>(self),
                TimeUnit::Millisecond => col_to_numeric_array::<Time32MillisecondType>(self),
                _ => unreachable!(),
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Microsecond => col_to_numeric_array::<Time64MicrosecondType>(self),
                TimeUnit::Nanosecond => col_to_numeric_array::<Time64NanosecondType>(self),
                _ => unreachable!(),
            },
            DataType::Duration(unit) => match unit {
                TimeUnit::Second => col_to_numeric_array::<DurationSecondType>(self),
                TimeUnit::Millisecond => col_to_numeric_array::<DurationMillisecondType>(self),
                TimeUnit::Microsecond => col_to_numeric_array::<DurationMicrosecondType>(self),
                TimeUnit::Nanosecond => col_to_numeric_array::<DurationNanosecondType>(self),
            },
            DataType::Interval(unit) => match unit {
                IntervalUnit::YearMonth => col_to_numeric_array::<IntervalYearMonthType>(self),
                IntervalUnit::DayTime => col_to_numeric_array::<IntervalDayTimeType>(self),
            },
            DataType::Binary => {
                let mut builder = BinaryBuilder::new(1);
                let data: Vec<ArrayDataRef> =
                    self.data().chunks().iter().map(|a| a.data()).collect();
                builder.append_data(data.as_slice())?;
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            DataType::FixedSizeBinary(width) => {
                let mut builder = FixedSizeBinaryBuilder::new(1, *width);
                let data: Vec<ArrayDataRef> =
                    self.data().chunks().iter().map(|a| a.data()).collect();
                builder.append_data(data.as_slice())?;
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::new(1);
                let data: Vec<ArrayDataRef> =
                    self.data().chunks().iter().map(|a| a.data()).collect();
                builder.append_data(data.as_slice())?;
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::LargeBinary
            | DataType::LargeUtf8 => Err(DataFrameError::ComputeError(
                "Not yet implemented".to_string(),
            )),
            DataType::FixedSizeList(_, _) => Err(DataFrameError::ComputeError(
                "Not yet implemented".to_string(),
            )),
            DataType::Struct(_) => Err(DataFrameError::ComputeError(
                "Not yet implemented".to_string(),
            )),
            DataType::Union(_) => Err(DataFrameError::ComputeError(
                "Not yet implemented".to_string(),
            )),
            DataType::Dictionary(_, _) => Err(DataFrameError::ComputeError(
                "Not yet implemented".to_string(),
            )), // t => {
                //     return Err(DataFrameError::ComputeError(
                //         format!("Merging arrays not yet implemented for {:?}", t),
                //     ))
                // }
                // DataType::LargeBinary => {
                //     return Err(DataFrameError::ComputeError(
                //         "Not yet implemented".to_string(),
                //     ))
                // }
                // DataType::LargeUtf8 => {
                //     return Err(DataFrameError::ComputeError(
                //         "Not yet implemented".to_string(),
                //     ))
                // }
                // DataType::LargeList(_) => {
                //     return Err(DataFrameError::ComputeError(
                //         "Not yet implemented".to_string(),
                //     ))
                // }
        }
    }

    pub fn name(&self) -> &str {
        self.field.name()
    }

    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
    }

    pub fn data(&self) -> &ChunkedArray {
        &self.data
    }

    pub(crate) fn field(&self) -> &Field {
        &self.field
    }

    pub fn slice(&self, offset: usize, length: Option<usize>) -> Self {
        Self::from_chunked_array(self.data().slice(offset, length), self.field().clone())
    }

    pub fn null_count(&self) -> usize {
        self.data().null_count()
    }

    pub fn num_rows(&self) -> usize {
        self.data().num_rows()
    }

    /// Filter this column using a Boolean column as the mask
    pub fn filter(&self, condition: &Self) -> Self {
        Self::from_chunked_array(self.data.filter(condition.data()), self.field.clone())
    }

    /// Create a new column by taking values at indices, while repartitioning to the chunk size
    pub fn take(&self, indices: &UInt32Array, chunk_size: usize) -> Result<Self> {
        let mut consumed_len = 0;
        let total_len = indices.len();
        let values = self.to_array()?;
        let mut outputs = vec![];
        while consumed_len < total_len {
            let bounded_len = if total_len < chunk_size {
                total_len
            } else if consumed_len + chunk_size > total_len {
                chunk_size
            } else {
                total_len - consumed_len
            };
            let slice = indices.slice(consumed_len, bounded_len);
            let slice = slice.as_any().downcast_ref::<UInt32Array>().unwrap();
            let taken = arrow::compute::take(&values, slice, None)?;
            outputs.push(taken);
            consumed_len += bounded_len;
        }
        Ok(Self {
            data: ChunkedArray::from_arrays(outputs),
            field: self.field.clone(),
        })
    }

    fn flatten() {}
}

/// Alogical table as a sequence of chunked arrays
pub struct Table {
    schema: Arc<Schema>,
    pub(crate) columns: Vec<Column>,
}

impl Table {
    pub fn new(schema: Arc<Schema>, columns: Vec<Column>) -> Self {
        // assert that there are some columns
        assert!(
            !columns.is_empty(),
            "at least one column must be defined to create a record batch"
        );
        // assert that all columns have the same row count
        let len = columns[0].data().num_rows();
        for column in &columns {
            assert_eq!(
                len,
                column.data().num_rows(),
                "all columns in a record batch must have the same length"
            );
        }
        Table { schema, columns }
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.columns[0].data().num_rows()
    }

    // keep fn
    pub fn column(&self, i: usize) -> &Column {
        &self.columns[i]
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    // new fns
    fn add_column() {}

    // fn remove_column(&self, _i: usize) -> Self {
    //     Table {
    //         schema: self.schema.clone(),
    //         columns: self.columns
    //     }
    // }

    /// Replace a column in the table, producing a new `Table`
    fn set_column() {}

    fn replace_schema_metadata() {}

    /// Each column with a struct type is flattened into one column per struct field.
    /// Other columns are left unchanged.
    fn flatten() {}

    /// Construct a `Table` from a sequence of `Column`s and a schema
    fn make(columns: Vec<Column>) -> Self {
        let fields: Vec<Field> = columns.iter().map(|column| column.field.clone()).collect();
        Self {
            schema: Arc::new(Schema::new(fields)),
            columns,
        }
    }

    /// Construct a `Table` from a sequence of `Column`s and a schema
    fn make_with_schema(schema: Arc<Schema>, columns: Vec<Column>) -> Self {
        // TODO validate that schema and columns match
        Self { schema, columns }
    }

    /// Slice the table from an offset
    pub fn slice(&self, offset: usize, limit: usize) -> Self {
        Self {
            schema: self.schema.clone(),
            columns: self
                .columns
                .clone()
                .into_iter()
                .map(|col| col.slice(offset, Some(limit)))
                .collect(),
        }
    }

    pub fn filter(&self, condition: &Column) -> Self {
        Self {
            schema: self.schema.clone(),
            columns: self
                .columns
                .clone()
                .into_iter()
                .map(|col| col.filter(condition))
                .collect(),
        }
    }

    /// Construct a `Table` from a sequence of Arrow `RecordBatch`es.
    ///
    /// Columns are first created from the `RecordBatch`es, with schema validations being performed.
    /// A table is then created
    pub fn from_record_batches(schema: Arc<Schema>, record_batches: Vec<RecordBatch>) -> Self {
        if record_batches.is_empty() {
            panic!("Error about record batches (copy from cpp)")
        }
        let num_columns = record_batches[0].num_columns();
        // let mut arrays: Vec<Vec<&Arc<Array>>> = vec![vec![]; num_columns];
        let mut arrays: Vec<Vec<Arc<dyn Array>>> = vec![vec![]; num_columns];
        // create columns from record batches
        for batch in &record_batches {
            assert!(
                batch.num_columns() == num_columns,
                "Each record batch should have the same length as the first batch"
            );
            batch.columns().iter().enumerate().for_each(|(i, array)| {
                arrays[i].push(array.to_owned());
            });
        }
        let columns = arrays
            .iter()
            .enumerate()
            .map(|(i, array)| Column::from_arrays(array.to_owned(), schema.field(i).clone()))
            .collect();

        Table { schema, columns }
    }

    fn concatenate_tables() {}

    fn to_record_batches() {}
}

unsafe impl Send for Table {}
unsafe impl Sync for Table {}

#[cfg(test)]
mod tests {}
