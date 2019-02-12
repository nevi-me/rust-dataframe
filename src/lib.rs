#![allow(unused_imports)]
#![allow(unused_variables)]
#![feature(try_trait)]
// required for matching box in lists
#![feature(box_syntax, box_patterns)]
#![allow(dead_code)]
#![feature(test)]

pub mod dataframe;
pub mod error;
pub mod functions;
pub mod io;
pub mod table;
pub mod utils;
