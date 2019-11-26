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

## Eager vs Lazy Evaluation

The initial experiments of this project were to see if it's possible to create some form of dataframe. We're happy that this condition is met, however the initial version relied on eager evaluation, which would make it difficult to use in a REPL fashion, and make it slow.

We are mainly focusing on creating a process for lazy evaluation (the current `LazyFrame`), which involves reading an input's schema, then applying transformations on that schema until a materialising action is required.
While still figuring this out, there might not be much progress on the surface, as most of this exercise is happening offline.

The plan is to provide a reasonable API for lazily transforming data, and the ability to apply some optimisations on the computation graph (e.g. predicate pushdown, rearranging computations).

In the future, `LazyFrame` will probably be renamed to `DataFrame`, and the current `DataFrame` with eager evaluation removed/made private.

The ongoing experiments on lazy evaluation are in the `master` branch, and we would appreciate some help 🙏🏾.

## Non-Goals

Although we use Apache Spark as a reference, we do not intend on:

- Creating deferred computation kernels (we'll leverage Arrow Rust)
- Creating distributed computation kernels

Spark is a convenience to reduce bikeshedding, but we will probably provide a more Rust idiomatic API in future.

## Status

### Roadmap

- [ ] Lazy evaluation (Q1 2020)
- [ ] Adding compute `fn`s (Q2 2020)
- [ ] SQL support (Q3 2020)
- [ ] Python bindings (Q4 2020)

### IO

IO support isn't great yet, but this is not the best place to implement it. We are contributing to the effort in Apache Arrow's Rust implementation, and more contributors would be welcome there.

For now, we're trying to support CSV, JSON, and perhaps other simpler file formats.
**Note on Feather:** The Feather file format support can be considered as deprecated in favour of Arrow IPC. Though we have implemented Feather, it's meant to be a stop-gap measure until Arrow supports IPC (in Rust, anticipated at `1.0.0`).

- IO Support
  - [X] CSV (using Arrow)
    - [X] Read
    - [X] Write
  - [ ] JSON
    - [X] Read (submitted to Arrow)
    - [ ] Write
  - [ ] Feather
    - [X] Read
    - [X] Write (**do not use**, we write each record batch as a partitioned file, instead of a single file for all the data)
  - [ ] Arrow IPC
    - [ ] Read File
    - [ ] Write FIle
  - [ ] Parquet (relying on Arrow)
    - [ ] Read File
    - [ ] Write File
  - [ ] SQL
    - [ ] PostgreSQL
      - [X] Read (ongoing, reading of most columns possible)
      - [ ] Write
    - [ ] MSSQL (using tiberius)
      - [ ] Read
      - [ ] Write

### Functionality

- DataFrame Operations
  - [X] Select single column
  - [X] Select subset of columns, drop columns
  - [X] Add or remove columns
  - [X] Rename columns
  - [ ] Create dataframe from record batches (a `Vec<RecordBatch>` as well as an iterator)
  - [ ] Sort dataframes
  - [ ] Grouped operations
  - [ ] Filter dataframes
  - [ ] Join dataframes

- Scalar Functions
  - [X] Trig functions (sin, cos, tan, asin, asinh, ...) (using the `num` crate where possible)
  - [X] Basic arithmetic (add, mul, divide, subtract) **Implemented from Arrow**
  - [ ] Date/Time functions
  - [ ] String functions
    - [ ] Basic string manipulation
    - [ ] Regular expressions (leveraging `regex`)
    - [ ] Casting to and from strings
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

We plan on providing simple benchmarks in the near future, especially after we gain the ability to save dataframes to disk. Specifically, after we implement CSV and JSON writers.