pub mod postgres;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::error::Result;
pub trait SqlDataSource {
    fn get_table_schema(connection: &str, table_name: &str) -> Result<Schema>;
    fn read_table(
        connection: &str,
        table_name: &str,
        limit: Option<usize>,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>>;
}
