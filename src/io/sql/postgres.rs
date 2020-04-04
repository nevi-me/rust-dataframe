//! An experimental interface for reading and writing record batches to and from PostgreSQL

use std::convert::TryFrom;
use std::{
    io::{BufRead, BufReader, Cursor, Read, Seek},
    sync::Arc,
};

use arrow::array::*;
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, DateUnit, Field, IntervalUnit, Schema, TimeUnit, ToByteSlice};
use arrow::record_batch::RecordBatch;
use byteorder::{LittleEndian, NetworkEndian, ReadBytesExt};
use chrono::Timelike;
use postgres::types::*;
use postgres::{Client, NoTls, Row};

use crate::io::sql::SqlDataSource;

const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";

pub struct Postgres;

impl SqlDataSource for Postgres {
    fn get_table_schema(connection_string: &str, table_name: &str) -> Result<Schema, ()> {
        let (table_schema, table_name) = if table_name.contains(".") {
            let split = table_name.split(".").collect::<Vec<&str>>();
            if split.len() != 2 {
                eprintln!("table name must have schema and table name only, or just table name");
                return Err(());
            }
            (
                format!("table_schema = '{}'", split[0]),
                format!(" and table_name = '{}'", split[1]),
            )
        } else {
            (String::new(), format!("table_name = '{}'", table_name))
        };
        let mut client = Client::connect(connection_string, NoTls).unwrap();
        let results = client
            .query(format!("select column_name, ordinal_position, is_nullable, data_type, character_maximum_length, numeric_precision, datetime_precision from information_schema.columns where {}{}", table_schema, table_name).as_str(), &[])
            .unwrap();
        let fields: Result<Vec<Field>, ()> = results
            .iter()
            .map(|row| PgDataType {
                column_name: row.get("column_name"),
                ordinal_position: row.get("ordinal_position"),
                is_nullable: row.get("is_nullable"),
                data_type: row.get("data_type"),
                char_max_length: row.get("character_maximum_length"),
                numeric_precision: row.get("numeric_precision"),
                datetime_precision: row.get("datetime_precision"),
            })
            .map(|t| Field::try_from(t))
            .collect();
        Ok(Schema::new(fields?))
    }

    fn read_table(
        connection: &str,
        table_name: &str,
        limit: usize,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>, ()> {
        // read_table_by_rows(connection, table_name, limit, batch_size)
        // create connection
        let mut client = Client::connect(connection, NoTls).unwrap();
        // get schema
        // TODO: reuse connection
        // TODO: split read into multiple batches, using limit and skip
        let schema = Postgres::get_table_schema(connection, table_name)?;
        let reader = client
            .copy_out(
                format!(
                    "COPY (select * from {}) TO stdout with (format binary)",
                    table_name
                )
                .as_str(),
            )
            .unwrap();
        read_from_binary(reader, &schema)
            .map(|batch| vec![batch])
            .map_err(|e| eprintln!("Dataframe Error: {:?}", e))
    }
}

struct PgDataType {
    column_name: String,
    ordinal_position: i32,
    is_nullable: String,
    data_type: String,
    char_max_length: Option<i32>,
    numeric_precision: Option<i32>,
    datetime_precision: Option<i32>,
}

impl TryFrom<PgDataType> for Field {
    type Error = ();
    fn try_from(field: PgDataType) -> Result<Self, Self::Error> {
        let data_type = match field.data_type.as_str() {
            "integer" => match field.numeric_precision {
                Some(8) => Ok(DataType::Int8),
                Some(16) => Ok(DataType::Int16),
                Some(32) => Ok(DataType::Int32),
                Some(64) => Ok(DataType::Int64),
                _ => Err(()),
            },
            "bigint" => Ok(DataType::Int64),
            "\"char\"" => Ok(DataType::UInt8),
            // "anyarray" | "ARRAY" => Err(()),
            "boolean" => Ok(DataType::Boolean),
            "bytea" => Ok(DataType::Binary),
            "character varying" => Ok(DataType::Utf8),
            "date" => Ok(DataType::Date32(DateUnit::Day)),
            "double precision" => Ok(DataType::Float64),
            // "inet" => Err(()),
            "interval" => Ok(DataType::Interval(IntervalUnit::DayTime)), // TODO: use appropriate unit
            // "name" => Err(()),
            "numeric" => Ok(DataType::Float64),
            // "oid" => Err(()),
            "real" => Ok(DataType::Float32),
            "smallint" => Ok(DataType::Int8),
            "text" => Ok(DataType::Utf8),
            "time without time zone" => Ok(DataType::Time64(TimeUnit::Microsecond)), // TODO: use datetime_precision to determine correct type
            "timestamp with time zone" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
            "timestamp without time zone" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
            "uuid" => Ok(DataType::Binary), // TODO: use a more specialised data type
            t @ _ => {
                eprintln!("Conversion not set for data type: {:?}", t);
                Err(())
            }
        };
        Ok(Field::new(
            &field.column_name,
            data_type?,
            &field.is_nullable == "YES",
        ))
    }
}

/// Convert Postgres Type to Arrow DataType
///
/// Not all types are covered, but can be easily added
fn pg_to_arrow_type(dt: &Type) -> Option<DataType> {
    match dt {
        &Type::BOOL => Some(DataType::Boolean),
        &Type::BYTEA | &Type::CHAR | &Type::NAME | &Type::TEXT | &Type::VARCHAR => {
            Some(DataType::Utf8)
        }
        &Type::INT2 => Some(DataType::Int16),
        &Type::INT4 => Some(DataType::Int32),
        &Type::INT8 => Some(DataType::Int64),
        // &Type::NUMERIC => Some(DataType::Float64),
        //        &OID => None,
        //        &JSON => None,
        &Type::FLOAT4 => Some(DataType::Float32),
        &Type::FLOAT8 => Some(DataType::Float64),
        //        &ABSTIME => None,
        //        &RELTIME => None,
        //        &TINTERVAL => None,
        //        &MONEY => None,
        &Type::BOOL_ARRAY => Some(DataType::List(Box::new(DataType::Boolean))),
        &Type::BYTEA_ARRAY | &Type::CHAR_ARRAY | &Type::NAME_ARRAY => {
            Some(DataType::List(Box::new(DataType::Utf8)))
        }
        //        &INT2_ARRAY => None,
        //        &INT2_VECTOR => None,
        //        &INT2_VECTOR_ARRAY => None,
        //        &INT4_ARRAY => None,
        //        &TEXT_ARRAY => None,
        //        &INT8_ARRAY => None,
        //        &FLOAT4_ARRAY => None,
        //        &FLOAT8_ARRAY => None,
        //        &ABSTIME_ARRAY => None,
        //        &RELTIME_ARRAY => None,
        //        &TINTERVAL_ARRAY => None,
        &Type::DATE => Some(DataType::Date32(DateUnit::Day)),
        &Type::TIME => Some(DataType::Time64(TimeUnit::Microsecond)),
        &Type::TIMESTAMP => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
        //        &TIMESTAMP_ARRAY => None,
        &Type::DATE_ARRAY => Some(DataType::List(Box::new(DataType::Date32(DateUnit::Day)))),
        //        &TIME_ARRAY => None,
        //        &TIMESTAMPTZ => None,
        //        &TIMESTAMPTZ_ARRAY => None,
        //        &INTERVAL => None,
        //        &INTERVAL_ARRAY => None,
        //        &NUMERIC_ARRAY => None,
        //        &TIMETZ => None,
        //        &BIT => None,
        //        &BIT_ARRAY => None,
        //        &VARBIT => None,
        //        &NUMERIC => None,
        &Type::UUID => Some(DataType::FixedSizeBinary(16)),
        t @ _ => panic!("Postgres type {:?} not supported", t),
    }
}

fn read_table_by_rows(
    connection_string: &str,
    table_name: &str,
    limit: usize,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, ()> {
    // create connection
    let mut client = Client::connect(connection_string, NoTls).unwrap();
    let results = client
        .query(format!("SELECT * FROM {}", table_name).as_str(), &[])
        .unwrap();
    if results.is_empty() {
        return Ok(vec![]);
    }

    // TODO: replace with metadata version of call
    let schema = row_to_schema(results.get(0).unwrap()).unwrap();
    let field_len = schema.fields().len();
    let mut builder = StructBuilder::from_schema(schema.clone(), batch_size);
    let chunks = results.chunks(batch_size);
    let mut batches = vec![];
    chunks.for_each(|chunk: &[Row]| {
        for j in 0..field_len {
            match schema.field(j).data_type() {
                DataType::Int32 => {
                    let field_builder = builder.field_builder::<Int32Builder>(j).unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        match row.try_get(j) {
                            Ok(value) => field_builder.append_value(value).unwrap(),
                            Err(_) => field_builder.append_null().unwrap(),
                        };
                    }
                }
                DataType::Int64 => {
                    let field_builder = builder.field_builder::<Int64Builder>(j).unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        match row.try_get(j) {
                            Ok(value) => field_builder.append_value(value).unwrap(),
                            Err(_) => field_builder.append_null().unwrap(),
                        };
                    }
                }
                DataType::Float64 => {
                    let field_builder = builder.field_builder::<Float64Builder>(j).unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        match row.try_get(j) {
                            Ok(value) => field_builder.append_value(value).unwrap(),
                            Err(_) => field_builder.append_null().unwrap(),
                        };
                    }
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    let field_builder = builder
                        .field_builder::<TimestampMillisecondBuilder>(j)
                        .unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        let timestamp: chrono::NaiveDateTime = row.get(j);
                        field_builder
                            .append_value(timestamp.timestamp_millis())
                            .unwrap();
                    }
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    let field_builder = builder
                        .field_builder::<Time64MicrosecondBuilder>(j)
                        .unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        let time: chrono::NaiveTime = row.get(j);
                        field_builder
                            .append_value(
                                time.num_seconds_from_midnight() as i64 * 1000000
                                    + time.nanosecond() as i64 / 1000,
                            )
                            .unwrap();
                    }
                }
                DataType::Boolean => {
                    let field_builder = builder.field_builder::<BooleanBuilder>(j).unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        field_builder.append_value(row.get(j)).unwrap();
                    }
                }
                DataType::Utf8 => {
                    let field_builder = builder.field_builder::<StringBuilder>(j).unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        field_builder.append_value(row.get(j)).unwrap();
                    }
                }
                t @ _ => panic!("Field builder for {:?} not yet supported", t),
            }
        }
        // TODO perhaps change the order of processing so we can do this earlier
        for i in 0..chunk.len() {
            builder.append(true).unwrap();
        }
        let batch: RecordBatch = RecordBatch::from(&builder.finish());
        batches.push(batch);
    });
    Ok(batches)
}

/// Generate Arrow schema from a row
fn row_to_schema(row: &postgres::Row) -> Result<Schema, ()> {
    let mut metadata = std::collections::HashMap::new();
    let fields = row
        .columns()
        .iter()
        .map(|col: &postgres::Column| {
            metadata.insert(col.name().to_string(), col.type_().to_string());
            Field::new(col.name(), pg_to_arrow_type(col.type_()).unwrap(), true)
        })
        .collect();
    Ok(Schema::new_with_metadata(fields, metadata))
}

fn read_from_binary<R>(mut reader: R, schema: &Schema) -> crate::error::Result<RecordBatch>
where
    R: Read + BufRead,
{
    // read signature
    let mut bytes = [0u8; 11];
    reader.read_exact(&mut bytes).unwrap();
    if bytes != MAGIC {
        eprintln!("Unexpected binary format type");
        return Err(crate::error::DataFrameError::IoError(
            "Unexpected Postgres binary type".to_string(),
        ));
    }
    // read flags
    let mut bytes: [u8; 4] = [0; 4];
    reader.read_exact(&mut bytes).unwrap();
    let _size = u32::from_be_bytes(bytes);

    // header extension area length
    let mut bytes: [u8; 4] = [0; 4];
    reader.read_exact(&mut bytes).unwrap();
    let _size = u32::from_be_bytes(bytes);

    read_rows(&mut reader, schema)
}

/// Read row tuples
fn read_rows<R>(mut reader: R, schema: &Schema) -> crate::error::Result<RecordBatch>
where
    R: Read + BufRead,
{
    let mut is_done = false;
    let field_len = schema.fields().len();

    let mut buffers: Vec<Vec<u8>> = vec![vec![]; field_len];
    let mut null_buffers: Vec<Vec<bool>> = vec![vec![]; field_len];
    let mut offset_buffers: Vec<Vec<i32>> = vec![vec![]; field_len];

    let mut record_num = -1;
    while !is_done {
        record_num += 1;
        // tuple length
        let mut bytes: [u8; 2] = [0; 2];
        reader.read_exact(&mut bytes).unwrap();
        let size = i16::from_be_bytes(bytes);
        if size == -1 {
            // trailer
            is_done = true;
            continue;
        }
        let size = size as usize;
        // in almost all cases, the tuple length should equal schema field length
        assert_eq!(size, field_len);
        for i in 0..field_len {
            let mut bytes = [0u8; 4];
            reader.read_exact(&mut bytes).unwrap();
            let col_length = i32::from_be_bytes(bytes);
            // populate offsets for types that need them
            match schema.field(i).data_type() {
                DataType::Binary | DataType::Utf8 => {
                    offset_buffers[i].push(col_length);
                }
                DataType::FixedSizeBinary(binary_size) => {
                    offset_buffers[i].push(record_num * binary_size);
                }
                DataType::List(_) => {}
                DataType::FixedSizeList(_, _) => {}
                DataType::Struct(_) => {}
                _ => {}
            }
            // populate values
            if col_length == -1 {
                // null value
                null_buffers[i].push(false);
            } else {
                null_buffers[i].push(true);
                // big endian data, needs to be converted to little endian
                let mut data = read_col(
                    &mut reader,
                    schema.field(i).data_type(),
                    col_length as usize,
                )
                .expect("Unable to read data");
                buffers[i].append(&mut data);
            }
        }
    }

    let mut arrays = vec![];
    // build record batches
    buffers
        .into_iter()
        .zip(null_buffers.into_iter())
        .zip(schema.fields().iter())
        .enumerate()
        .for_each(|(i, ((b, n), f))| {
            let null_count = n.iter().filter(|v| v == &&false).collect::<Vec<_>>().len();
            let mut null_buffer = BooleanBufferBuilder::new(n.len() / 8 + 1);
            null_buffer.append_slice(&n[..]).unwrap();
            let null_buffer = null_buffer.finish();
            match f.data_type() {
                DataType::Boolean => {
                    let bools = b.iter().map(|v| v == &0).collect::<Vec<bool>>();
                    let mut bool_buffer = BooleanBufferBuilder::new(bools.len());
                    bool_buffer.append_slice(&bools[..]).unwrap();
                    let bool_buffer = bool_buffer.finish();
                    let data = ArrayData::new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer), // TODO: add null_bit_buffer
                        0,
                        vec![bool_buffer],
                        vec![],
                    );
                    arrays.push(Arc::new(BooleanArray::from(Arc::new(data))) as ArrayRef)
                }
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Timestamp(_, _)
                | DataType::Date32(_)
                | DataType::Date64(_)
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_) => {
                    let data = ArrayData::new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer), // TODO: add null_bit_buffer
                        0,
                        vec![Buffer::from(b)],
                        vec![],
                    );
                    arrays.push(crate::utils::make_array(Arc::new(data)))
                }
                DataType::FixedSizeBinary(size) => {
                    // TODO: do we need to calculate any offsets?
                    let data = ArrayData::new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer), // TODO: add null_bit_buffer
                        0,
                        vec![Buffer::from(b)],
                        vec![],
                    );
                    arrays.push(crate::utils::make_array(Arc::new(data)))
                }
                DataType::Binary | DataType::Utf8 => {
                    // recontruct offsets
                    let mut offset = 0;
                    let mut offsets = vec![0];
                    offset_buffers[i].iter().for_each(|o| {
                        offsets.push(offset + o);
                        offset += o;
                    });
                    let data = ArrayData::new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer),
                        0,
                        vec![Buffer::from(offsets.to_byte_slice()), Buffer::from(b)],
                        vec![],
                    );
                    arrays.push(crate::utils::make_array(Arc::new(data)))
                }
                DataType::List(_) => {
                    let data = ArrayData::new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer), // TODO: add null_bit_buffer
                        0,
                        vec![Buffer::from(b)],
                        vec![],
                    );
                    arrays.push(crate::utils::make_array(Arc::new(data)))
                }
                DataType::FixedSizeList(_, _) => {
                    let data = ArrayData::new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer), // TODO: add null_bit_buffer
                        0,
                        vec![Buffer::from(b)],
                        vec![],
                    );
                    arrays.push(crate::utils::make_array(Arc::new(data)))
                }
                DataType::Float16 => panic!("Float16 not yet implemented"),
                DataType::Struct(_) => panic!("Reading struct arrays not implemented"),
                DataType::Dictionary(_, _) => panic!("Reading dictionary arrays not implemented"),
            }
        });
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), arrays)?)
}

fn read_col<R: Read>(reader: &mut R, data_type: &DataType, length: usize) -> Result<Vec<u8>, ()> {
    match data_type {
        DataType::Boolean => read_bool(reader),
        DataType::Int8 => read_i8(reader),
        DataType::Int16 => read_i16(reader),
        DataType::Int32 => read_i32(reader),
        DataType::Int64 => read_i64(reader),
        DataType::UInt8 => read_u8(reader),
        DataType::UInt16 => read_u16(reader),
        DataType::UInt32 => read_u32(reader),
        DataType::UInt64 => read_u64(reader),
        DataType::Float16 => Err(()),
        DataType::Float32 => read_f32(reader),
        DataType::Float64 => read_f64(reader),
        DataType::Timestamp(_, _) => read_time64(reader),
        DataType::Date32(_) => read_date32(reader),
        DataType::Date64(_) => unreachable!(),
        DataType::Time32(_) => read_i32(reader),
        DataType::Time64(_) => read_i64(reader),
        DataType::Duration(_) => read_i64(reader),
        DataType::Interval(_) => read_i64(reader),
        DataType::Binary => read_string(reader, length), // TODO we'd need the length of the binary
        DataType::FixedSizeBinary(_) => read_string(reader, length),
        DataType::Utf8 => read_string(reader, length),
        DataType::List(_) => Err(()),
        DataType::FixedSizeList(_, _) => Err(()),
        DataType::Struct(_) => Err(()),
        DataType::Dictionary(_, _) => Err(()),
    }
}

fn read_u8<R: Read>(reader: &mut R) -> Result<Vec<u8>, ()> {
    reader
        .read_u8()
        .map(|v| vec![v])
        .map_err(|e| eprintln!("Error: {:?}", e))
}

fn read_i8<R: Read>(reader: &mut R) -> Result<Vec<u8>, ()> {
    reader
        .read_i8()
        .map(|v| vec![v as u8])
        .map_err(|e| eprintln!("Error: {:?}", e))
}

fn read_u16<R: Read>(reader: &mut R) -> Result<Vec<u8>, ()> {
    reader
        .read_u16::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| eprintln!("Error: {:?}", e))
}
fn read_i16<R: Read>(reader: &mut R) -> Result<Vec<u8>, ()> {
    reader
        .read_i16::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| eprintln!("Error: {:?}", e))
}
fn read_u32<R: Read>(reader: &mut R) -> Result<Vec<u8>, ()> {
    reader
        .read_u32::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| eprintln!("Error: {:?}", e))
}
fn read_i32<R: Read>(reader: &mut R) -> Result<Vec<u8>, ()> {
    reader
        .read_i32::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| eprintln!("Error: {:?}", e))
}
fn read_u64<R: Read>(reader: &mut R) -> Result<Vec<u8>, ()> {
    reader
        .read_u64::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| eprintln!("Error: {:?}", e))
}
fn read_i64<R: Read>(reader: &mut R) -> Result<Vec<u8>, ()> {
    reader
        .read_i64::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| eprintln!("Error: {:?}", e))
}
fn read_bool<R: Read>(reader: &mut R) -> Result<Vec<u8>, ()> {
    reader
        .read_u8()
        .map(|v| vec![v])
        .map_err(|e| eprintln!("Error: {:?}", e))
}
fn read_string<R: Read>(reader: &mut R, len: usize) -> Result<Vec<u8>, ()> {
    let mut buf = vec![0; len];
    reader
        .read_exact(&mut buf)
        .map_err(|e| eprintln!("Error: {:?}", e))?;
    Ok(buf)
}
fn read_f32<R: Read>(reader: &mut R) -> Result<Vec<u8>, ()> {
    reader
        .read_f32::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| eprintln!("Error: {:?}", e))
}
fn read_f64<R: Read>(reader: &mut R) -> Result<Vec<u8>, ()> {
    reader
        .read_f64::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| eprintln!("Error: {:?}", e))
}

/// Postgres dates are days since epoch of 01-01-2000, so we add 10957 days
fn read_date32<R: Read>(reader: &mut R) -> Result<Vec<u8>, ()> {
    reader
        .read_i32::<NetworkEndian>()
        .map(|v| { 10957 + v }.to_le_bytes().to_vec())
        .map_err(|e| eprintln!("Error: {:?}", e))
}

fn read_time64<R: Read>(reader: &mut R) -> Result<Vec<u8>, ()> {
    reader
        .read_i64::<NetworkEndian>()
        .map(|v| { 946684800000000 + v }.to_le_bytes().to_vec())
        .map_err(|e| eprintln!("Error: {:?}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_table_schema() {
        let result = Postgres::get_table_schema(
            "postgres://postgres:password@localhost:5432/postgres",
            "arrow_data",
        )
        .expect("Unable to get table schema");
        dbg!(result);
    }

    #[test]
    fn test_read_table() {
        let result = Postgres::read_table(
            "postgres://postgres:password@localhost:5432/postgres",
            "arrow_data",
            0,
            1024,
        )
        .unwrap();
        dbg!(result.len());
        let schema = result.get(0).map(|rb| rb.schema()).unwrap();
        // create dataframe and write the batches to it
        let table = crate::table::Table::from_record_batches(schema.clone(), result);
        let df = crate::dataframe::DataFrame::from_table(table);
        df.to_csv("target/debug/arrow_data_from_sql.csv").unwrap();
    }
}
