# Rust DataFrame

A dataframe implementation in Rust.

This project currently exists as a prototype that uses the Apache Arrow Rust library. 
Its goal is to act as an additional user of Arrow as we develop the library, in order to pick up potential difficulties from Arrow's downstream consumers (this dataframe being one).

## Functionality

This project is inspired by Pandas and other dataframe libraries, but specifically *borrows* functions from Apache Spark.

It mainly focuses on computation, and aims to include:

* Scalar functions
* Aggregate function
* Window functions
* Array functions

As a point of reference, we use [Apache Spark Python functions](http://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#module-pyspark.sql.functions) for function parity, and aim to be compatible with Apache Spark functions.

## Status

- DataFrame Operations
  - [x] Read CSV into dataframe
  - [ ] Select single column, subset of columns, drop columns
  - [ ] Add or remove columns
  - [ ] Rename columns
  - [ ] Create dataframe from record batches (a `Vec<RecordBatch>` as well as an iterator)
  - [ ] Write dataframe to CSV (and other formats as and when Arrow supports them)

## Performance

We plan on providing simple benchmarks in the near future, especially after we gain the ability to save dataframes to disk.