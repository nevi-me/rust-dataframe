use std::error::Error;
use arrow::error::ArrowError;

#[derive(Debug, Clone, PartialEq)]
pub enum DataFrameError {
    MemoryError(String),
    ParseError(String),
    ComputeError(String),
    DivideByZero,
    IoError(String),
    NoneError,
    ArrowError
}

impl From<ArrowError> for DataFrameError {
    fn from(_error: ArrowError) -> Self {
        DataFrameError::ArrowError
    }
}

impl From<::std::io::Error> for DataFrameError {
    fn from(error: ::std::io::Error) -> Self {
        DataFrameError::IoError(error.description().to_string())
    }
}

impl From<std::option::NoneError> for DataFrameError {
    fn from(_error: ::std::option::NoneError) -> Self {
        DataFrameError::NoneError
    }
}

impl From<std::str::Utf8Error> for DataFrameError {
    fn from(error: ::std::str::Utf8Error) -> Self {
        DataFrameError::ParseError(error.description().to_string())
    }
}

pub type Result<T> = ::std::result::Result<T, DataFrameError>;
