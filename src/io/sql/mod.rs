pub mod postgres;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use std::sync::Arc;
pub trait SqlDataSource {
    fn get_table_schema(connection: &str, table_name: &str) -> Result<Schema>;
    fn read_table(
        connection: &str,
        table_name: &str,
        limit: Option<usize>,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>>;
    fn read_query(
        connection: &str,
        query: &str,
        limit: Option<usize>,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>>;
}

pub trait SqlDataSink {
    fn create_table(connection: &str, table_name: &str, schema: &Arc<Schema>) -> Result<()>;
    fn write_to_table(connection: &str, table_name: &str, batches: Vec<&RecordBatch>)
        -> Result<()>;
}
