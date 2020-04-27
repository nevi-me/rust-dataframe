//! An interface for reading and writing record batches to and from PostgreSQL

pub mod reader;
pub mod writer;

/// PGCOPY header
pub const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";
pub const EPOCH_DAYS: i32 = 10957;
pub const EPOCH_MICROS: i64 = 946684800000000;

pub struct Postgres;

/// A Postgres reader that returns an iterator of record batches
pub struct PostgresReadIterator {
    client: postgres::Client,
    query: String,
    limit: usize,
    batch_size: usize,
    schema: arrow::datatypes::Schema,
    read_records: usize,
    is_complete: bool,
}
