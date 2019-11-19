//! Interfaces for creating and managing windows

/*
    The goal here is to be able to get SQL-like window compatibility, though sticking close
    to what Spark does.
*/

pub struct WindowSpec {}

pub trait Window {}

impl Window for WindowSpec {
    /// We intentionally take columns so we can extract their names and know that they exist in
    /// the dataframe.
    fn order_by(columns: Vec<&Column>) {}

    fn partition_by(columns: Vec<&Column>) {}

    fn range_between() {}

    fn rows_between() {}
}