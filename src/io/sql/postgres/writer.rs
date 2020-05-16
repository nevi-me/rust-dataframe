use std::io::Write;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, DateUnit, Field, Schema, TimeUnit};
use arrow::{compute::cast, record_batch::RecordBatch};
use byteorder::{LittleEndian, NetworkEndian, WriteBytesExt};
use postgres::{Client, CopyInWriter, NoTls};

use super::{Postgres, EPOCH_DAYS, EPOCH_MICROS, MAGIC};
use crate::error::{DataFrameError, Result};
use crate::io::sql::{SqlDataSink, SqlDataSource};

impl SqlDataSink for Postgres {
    fn create_table(connection: &str, table_name: &str, schema: &Arc<Schema>) -> Result<()> {
        let mut table = String::new();
        let field_len = schema.fields().len();
        for i in 0..field_len {
            table.push_str(
                format!(
                    "{} {}",
                    schema.field(i).name(),
                    get_postgres_type(schema.field(i))?
                )
                .as_str(),
            );
            if i + 1 < field_len {
                table.push_str(",\n");
            }
        }

        let mut client = Client::connect(connection, NoTls)?;
        client.execute(format!("DROP TABLE IF EXISTS {}", table_name).as_str(), &[])?;
        client.execute(
            format!("CREATE TABLE IF NOT EXISTS {} ({})", table_name, table).as_str(),
            &[],
        )?;
        Ok(())
    }
    fn write_to_table(
        connection: &str,
        table_name: &str,
        batches: &Vec<RecordBatch>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Err(DataFrameError::IoError(
                "At least one batch should be provided to the SQL writer".to_string(),
            ));
        }

        let mut client = Client::connect(connection, NoTls)?;
        let mut writer = get_binary_writer(&mut client, table_name)?;

        for batch in batches {
            // write out batch
            write_to_binary(&mut writer, batch)?;
        }
        writer.write_i16::<NetworkEndian>(-1)?;
        println!("Done writing values, finishing");

        writer.finish()?;
        Ok(())
    }
}

fn get_postgres_type(field: &Field) -> Result<String> {
    let nullable = if !field.is_nullable() {
        " not null"
    } else {
        ""
    };
    let dtype = match field.data_type() {
        DataType::Boolean => "boolean",
        DataType::Int8 => {
            return Err(DataFrameError::SqlError(
                "Int8 not mapped to any PostgreSQL type".to_string(),
            ))
        }
        DataType::Int16 => "smallint",
        DataType::Int32 => "integer",
        DataType::Int64 => "bigint",
        DataType::UInt8 => "character",
        DataType::UInt16 => "bigint",
        DataType::UInt32 => "bigint",
        DataType::UInt64 => "bigint",
        DataType::Float16 => {
            return Err(DataFrameError::ArrowError(
                "Float16 not supported".to_string(),
            ))
        }
        DataType::Float32 => "real",
        DataType::Float64 => "double precision",
        DataType::Timestamp(_, timezone) => match timezone {
            Some(_) => "timestamp with time zone",
            None => "timestamp",
        },
        DataType::Date32(_) | DataType::Date64(_) => "date",
        DataType::Time32(_) | DataType::Time64(_) => "time",
        DataType::Duration(_) => {
            return Err(DataFrameError::SqlError(
                "Duration write not support not yet implemented".to_string(),
            ))
        }
        DataType::Interval(_) => {
            return Err(DataFrameError::SqlError(
                "Interval write not support not yet implemented".to_string(),
            ))
        }
        DataType::Binary => "bytea",
        DataType::FixedSizeBinary(_) => {
            return Err(DataFrameError::SqlError(
                "FixedSizeBinary write not support not yet implemented".to_string(),
            ))
        }
        DataType::Utf8 => "text",
        DataType::List(_) => {
            return Err(DataFrameError::SqlError(
                "List write not support not yet implemented".to_string(),
            ))
        }
        DataType::FixedSizeList(_, _) => {
            return Err(DataFrameError::SqlError(
                "FixedSizeList write not support not yet implemented".to_string(),
            ))
        }
        DataType::Struct(_) => {
            return Err(DataFrameError::SqlError(
                "Struct write not support not yet implemented".to_string(),
            ))
        }
        DataType::Dictionary(_, _) => {
            return Err(DataFrameError::SqlError(
                "Dictionary write not support not yet implemented".to_string(),
            ))
        }
        arrow::datatypes::DataType::Union(_) => {
            return Err(DataFrameError::SqlError(
                "Union type not yet supported".to_string(),
            ));
        }
    };
    Ok(format!("{} {}", dtype, nullable))
}

fn get_binary_writer<'a>(client: &'a mut Client, table_name: &str) -> Result<CopyInWriter<'a>> {
    Ok(client.copy_in(format!("COPY {} FROM stdin with (format binary)", table_name).as_str())?)
}

fn write_to_binary(writer: &mut CopyInWriter, batch: &RecordBatch) -> Result<u64> {
    // write header
    writer.write(MAGIC)?;
    // write flags
    writer.write(&[0; 4])?;
    // write header extension
    writer.write(&[0; 4])?;

    let column_len = batch.num_columns();

    // start writing rows
    for row in 0..batch.num_rows() {
        // write row/tuple length
        writer.write_i16::<NetworkEndian>(column_len as i16)?;

        for (arr, field) in batch.columns().iter().zip(batch.schema().fields().iter()) {
            match field.data_type() {
                arrow::datatypes::DataType::Boolean => {
                    let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Int8 => {
                    let arr = arr.as_any().downcast_ref::<Int8Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Int16 => {
                    let arr = arr.as_any().downcast_ref::<Int16Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Int32 => {
                    let arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Int64 => {
                    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::UInt8 => {
                    let arr = arr.as_any().downcast_ref::<UInt8Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::UInt16 => {
                    return Err(DataFrameError::SqlError(
                        "No PostgreSQL data type has been mapped to UInt16Type".to_string(),
                    ));
                }
                arrow::datatypes::DataType::UInt32 => {
                    let arr = arr.as_any().downcast_ref::<UInt32Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::UInt64 => {
                    let arr = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Float16 => {
                    return Err(DataFrameError::ArrowError(
                        "Float16 type not yet supported by Arrow".to_string(),
                    ));
                }
                arrow::datatypes::DataType::Float32 => {
                    let arr = arr.as_any().downcast_ref::<Float32Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Float64 => {
                    let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Date32(_) | arrow::datatypes::DataType::Date64(_) => {
                    let arr = cast(arr, &DataType::Date32(DateUnit::Day))?;
                    let arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Timestamp(_, _) => {
                    let arr = cast(arr, &DataType::Timestamp(TimeUnit::Microsecond, None))?;
                    let arr = arr
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Time32(_) | arrow::datatypes::DataType::Time64(_) => {
                    let arr = cast(arr, &DataType::Time64(TimeUnit::Microsecond))?;
                    let arr = arr
                        .as_any()
                        .downcast_ref::<Time64MicrosecondArray>()
                        .unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Duration(_) => {
                    return Err(DataFrameError::SqlError(
                        "Duration type not yet supported by PostgreSQL writer".to_string(),
                    ));
                }
                arrow::datatypes::DataType::Interval(_) => {
                    return Err(DataFrameError::SqlError(
                        "Writing Intervals not yet implemented".to_string(),
                    ));
                }
                arrow::datatypes::DataType::Binary => {
                    let arr = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::FixedSizeBinary(_) => {
                    return Err(DataFrameError::SqlError(
                        "Duration type not yet supported by PostgreSQL writer".to_string(),
                    ));
                }
                arrow::datatypes::DataType::Utf8 => {
                    let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::List(_) => {
                    return Err(DataFrameError::SqlError(
                        "Duration type not yet supported by PostgreSQL writer".to_string(),
                    ));
                }
                arrow::datatypes::DataType::FixedSizeList(_, _) => {
                    return Err(DataFrameError::SqlError(
                        "Duration type not yet supported by PostgreSQL writer".to_string(),
                    ));
                }
                arrow::datatypes::DataType::Struct(_) => {
                    return Err(DataFrameError::SqlError(
                        "Duration type not yet supported by PostgreSQL writer".to_string(),
                    ));
                }
                arrow::datatypes::DataType::Dictionary(_, _) => {
                    return Err(DataFrameError::SqlError(
                        "Duration type not yet supported by PostgreSQL writer".to_string(),
                    ));
                }
                arrow::datatypes::DataType::Union(_) => {
                    return Err(DataFrameError::SqlError(
                        "Union type not yet supported by PostgreSQL writer".to_string(),
                    ));
                }
            }
        }
    }

    Ok(0)
}

/// A trait to write various supported array values to PostgreSQL's binary format
///
/// This assumes that nullness has been checked by the caller, and thus does not check this.
/// It is also the responsibility of the caller to ensure that the array has been converted to
///  a compatible array type for the Postgres column type
trait WriteToBinary {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()>;
}

fn write_null_to_binary<W: Write>(writer: &mut W) -> Result<()> {
    writer.write_i32::<NetworkEndian>(-1)?;
    Ok(())
}

fn write_length_to_binary<W: Write>(writer: &mut W, length: usize) -> Result<()> {
    writer.write_i32::<NetworkEndian>(length as i32)?;
    Ok(())
}

impl WriteToBinary for BooleanArray {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<u8>())?;
        match self.value(index) {
            true => writer.write_u8(1),
            false => writer.write_u8(0),
        }?;
        Ok(())
    }
}

impl WriteToBinary for UInt8Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<u8>())?;
        writer.write_u8(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for Int8Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<i8>())?;
        writer.write_i8(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for Int16Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<i16>())?;
        writer.write_i16::<NetworkEndian>(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for Int32Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<i32>())?;
        writer.write_i32::<NetworkEndian>(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for UInt32Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<u32>())?;
        writer.write_u32::<NetworkEndian>(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for UInt64Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<u32>())?;
        writer.write_u64::<NetworkEndian>(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for Int64Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<i64>())?;
        writer.write_i64::<NetworkEndian>(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for Float32Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<f32>())?;
        writer.write_f32::<NetworkEndian>(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for Float64Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<f64>())?;
        writer.write_f64::<NetworkEndian>(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for TimestampMicrosecondArray {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<i64>())?;
        writer.write_i64::<NetworkEndian>(self.value(index) - EPOCH_MICROS)?;
        Ok(())
    }
}

impl WriteToBinary for Date32Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<i32>())?;
        writer.write_i32::<NetworkEndian>(self.value(index) - EPOCH_DAYS)?;
        Ok(())
    }
}

impl WriteToBinary for Time64MicrosecondArray {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<i32>())?;
        writer.write_i32::<NetworkEndian>(self.value(index) as i32)?;
        Ok(())
    }
}

impl WriteToBinary for StringArray {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        let value = self.value(index).as_bytes();
        dbg!(&value);
        write_length_to_binary(writer, value.len())?;
        writer.write(value)?;
        Ok(())
    }
}

impl WriteToBinary for BinaryArray {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        let value = self.value(index);
        write_length_to_binary(writer, value.len())?;
        writer.write(value)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::Field;

    #[test]
    fn test_create_table() -> Result<()> {
        let connection = "postgres://postgres:password@localhost:5432/postgres";
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Boolean, true),
            Field::new("c2", DataType::Boolean, false),
            // Field::new("c3", DataType::Int8, true),
            // Field::new("c4", DataType::Int8, false),
            Field::new("c5", DataType::Int16, true),
            Field::new("c6", DataType::Int16, false),
            Field::new("c7", DataType::Int32, true),
            Field::new("c8", DataType::Int32, false),
            Field::new("c9", DataType::Int64, true),
            Field::new("c10", DataType::Int64, false),
            Field::new("c11", DataType::Float32, true),
            Field::new("c12", DataType::Float32, false),
            Field::new("c13", DataType::Float64, true),
            Field::new("c14", DataType::Float64, false),
            Field::new("c15", DataType::Binary, true),
            Field::new("c16", DataType::Binary, false),
            Field::new("c17", DataType::Utf8, true),
            Field::new("c18", DataType::Utf8, false),
        ]);
        Postgres::create_table(connection, "t1", &Arc::new(schema.clone()))?;
        let read_schema = Postgres::get_table_schema(connection, "t1")?;
        assert_eq!(schema, read_schema);
        Ok(())
    }

    #[test]
    fn test_write_table() -> Result<()> {
        let connection = "postgres://postgres:password@localhost:5432/postgres";
        // read an existing table
        let batches =
            Postgres::read_query(connection, "select * from arrow_data_types", None, 1024)?;
        assert!(batches.len() > 0);
        // create a table
        let schema = batches[0].schema();
        Postgres::create_table(connection, "t2", &schema)?;
        Postgres::write_to_table(connection, "t2", &batches)?;
        Ok(())
    }
}
