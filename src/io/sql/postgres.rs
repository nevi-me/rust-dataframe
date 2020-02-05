//! An experimental interface for reading and writing record batches to and from PostgreSQL

use std::convert::TryFrom;
use std::io::{BufRead, BufReader, Cursor, Read, Seek};

use arrow::array::*;
use arrow::datatypes::{DataType, DateUnit, Field, IntervalUnit, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::Timelike;
use postgres::types::*;
use postgres::{Client, NoTls, Row};

use crate::io::sql::SqlDataSource;

const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";

pub struct Postgres;

impl SqlDataSource for Postgres {
    fn get_table_schema(connection_string: &str, table_name: &str) -> Result<Schema, ()> {
        let mut client = Client::connect(connection_string, NoTls).unwrap();
        let results = client
            .query(format!("select column_name, ordinal_position, is_nullable, data_type, character_maximum_length, numeric_precision, datetime_precision from information_schema.columns where table_name = '{}'", table_name).as_str(), &[])
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
        read_table_by_rows(connection, table_name, limit, batch_size)
        // // create connection
        // let mut client = Client::connect(connection, NoTls).unwrap();
        // // get schema
        // // TODO: reuse connection
        // let schema = Postgres::get_table_schema(connection, table_name)?;
        // let reader = client
        //     .copy_out(format!("COPY {} TO stdout with (format binary)", table_name).as_str())
        //     .unwrap();
        // read_from_binary(reader, &schema)?;
        // panic!()
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
            "date" => Ok(DataType::Date64(DateUnit::Millisecond)),
            "double precision" => Ok(DataType::Float64),
            // "inet" => Err(()),
            "interval" => Ok(DataType::Interval(IntervalUnit::DayTime)), // TODO: use appropriate unit
            // "name" => Err(()),
            "numeric" => Ok(DataType::Float64),
            // "oid" => Err(()),
            "real" => Ok(DataType::Float32),
            "smallint" => Ok(DataType::Int8),
            "text" => Ok(DataType::Utf8),
            "time without time zone" => Ok(DataType::Time64(TimeUnit::Millisecond)), // TODO: use datetime_precision to determine correct type
            "timestamp with time zone" => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
            "timestamp without time zone" => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
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
        &Type::NUMERIC => Some(DataType::Float64),
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
        &Type::DATE => Some(DataType::Date64(DateUnit::Millisecond)),
        &Type::TIME => Some(DataType::Time64(TimeUnit::Microsecond)),
        &Type::TIMESTAMP => Some(DataType::Timestamp(TimeUnit::Millisecond, None)),
        //        &TIMESTAMP_ARRAY => None,
        &Type::DATE_ARRAY => Some(DataType::List(Box::new(DataType::Date64(
            DateUnit::Millisecond,
        )))),
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
    let fields = row
        .columns()
        .iter()
        .map(|col: &postgres::Column| {
            Field::new(col.name(), pg_to_arrow_type(col.type_()).unwrap(), true)
        })
        .collect();
    Ok(Schema::new(fields))
}

// fn read_from_binary<R>(mut reader: R, schema: &Schema) -> Result<RecordBatch, ()>
// where
//     R: Read + BufRead,
// {
//     let mut bytes = [0u8; 11];
//     reader.read_exact(&mut bytes).unwrap();
//     if bytes != MAGIC {
//         eprintln!("Unexpected binary format type");
//         return Err(());
//     }
//     let mut bytes: [u8; 4] = [0; 4];
//     reader.read_exact(&mut bytes).unwrap();
//     let size = u32::from_be_bytes(bytes);
//     // TODO: do something with size

//     read_column(&mut reader, schema)?;
//     panic!()
// }

// fn read_column<R>(mut reader: R, schema: &Schema) -> Result<(), ()>
// where
//     R: Read + BufRead,
// {
//     let mut is_done = false;
//     let col_length = schema.fields().len();

//     let mut buffers: Vec<Vec<u8>> = vec![vec![]; col_length];
//     let mut null_buffers: Vec<Vec<bool>> = vec![vec![]; col_length];

//     while !is_done {
//         let mut bytes: [u8; 4] = [0; 4];
//         reader.read_exact(&mut bytes).unwrap();
//         let size = u32::from_be_bytes(bytes) as usize;
//         dbg!(size);
//         // TODO: check if fields are the same length as schema
//         for i in 0..size {
//             let mut bytes = [0u8; 2];
//             reader.read_exact(&mut bytes).unwrap();
//             let col_length = i16::from_be_bytes(bytes);
//             dbg!(col_length);
//             if col_length == -1 {
//                 // null value
//                 null_buffers[i].push(false);
//             } else {
//                 null_buffers[i].push(true);
//                 // read byte data into buffer
//                 let mut buf = vec![0; col_length as usize];
//                 reader.read_exact(&mut buf).unwrap();
//                 // big endian data, needs to be converted to little endian
//             }
//         }
//     }

//     Ok(())
// }

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
    }
}
