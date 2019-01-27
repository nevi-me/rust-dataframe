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

## Non-Goals

Although we use Apache Spark as a reference, we do not intend on:

- Creating deferred computation kernels (we'll leverage Arrow Rust)
- Creating distributed computation kernels

One can think of this library partly as a playground for features that could form part of Arrow.

## Status

- DataFrame Operations
  - [x] Read CSV into dataframe
  - [X] Select single column
  - [ ] Select subset of columns, drop columns
  - [X] Add or remove columns
  - [X] Rename columns
  - [ ] Create dataframe from record batches (a `Vec<RecordBatch>` as well as an iterator)
  - [ ] Write dataframe to CSV (and other formats as and when Arrow supports them)

- Scalar Functions
  - [X] Trig functions (sin, cos, tan, asin, asinh, ...) (using the `num` crate where possible)
  - [X] Basic arithmetic (add, mul, divide, subtract) **Implemented from Arrow**
  - [ ] Date/Time functions
  - [ ] String functions (in progress, subset implemented)
  - [ ] Crypto/hash functions (md5, crc32, sha{x}, ...)
  - [ ] Other functions (that we haven't classified)

- Aggregate Functions
  - [X] Sum, max, min
  - [X] Count
  - [ ] Statistical aggregations (mean, mode, median, stddev, ...)

- Window Functions
  - [ ] Lead, lag
  - [ ] Rank, percent rank
  - [ ] Other

- Array Functions
  - [ ] Compatibility with Spark 2.4 functions

## Performance

We plan on providing simple benchmarks in the near future, especially after we gain the ability to save dataframes to disk.