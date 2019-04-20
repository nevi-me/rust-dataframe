//! An experimental interface for reading and writing record batches to and from PostgreSQL

use arrow::builder::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::Timelike;
use postgres::types::*;
use postgres::{Client, NoTls, Row};

/// Convert Postgres Type to Arrow DataType
///
/// Not all types are covered, but can be easily added
fn pg_to_arrow_type(dt: &Type) -> Option<DataType> {
    match dt {
        &Type::BOOL => Some(DataType::Boolean),
        &Type::BYTEA | &Type::CHAR | &Type::NAME | &Type::TEXT | &Type::VARCHAR => {
            Some(DataType::Utf8)
        }
        &Type::INT8 => Some(DataType::Int64),
        &Type::INT2 => Some(DataType::Int16),
        &Type::INT4 => Some(DataType::Int32),
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
        //        &DATE => None,
        &Type::TIME => Some(DataType::Time64(TimeUnit::Microsecond)),
        &Type::TIMESTAMP => Some(DataType::Timestamp(TimeUnit::Millisecond)),
        //        &TIMESTAMP_ARRAY => None,
        //        &DATE_ARRAY => None,
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
        //        &UUID => None,
        t @ _ => panic!("Postgres type {:?} not supported", t),
    }
}

// TODO can make this a common trait for DB sources
pub fn read_table(
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
                DataType::Timestamp(TimeUnit::Millisecond) => {
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
                    let field_builder = builder.field_builder::<BinaryBuilder>(j).unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        field_builder.append_string(row.get(j)).unwrap();
                    }
                }
                t @ _ => panic!("Field builder for {:?} not yet supported", t),
            }
        }
        builder.append(true).unwrap();
        batches.push(RecordBatch::from(&builder.finish()));
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
