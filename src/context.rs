//! Context allows registering custom data sources, sinks, functions, and customising behaviour

use std::collections::HashMap;

use crate::{io::datasource::DataSource, error::{Result, DataFrameError}};

#[derive(Default)]
pub struct Context {
    sources: HashMap<String, Box<dyn DataSource>>,
    sinks: HashMap<String, String>,
    functions: HashMap<String, String>
}

impl Context {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn register_datasource(&mut self, name: &str, source: Box<dyn DataSource>) {
        self.sources.insert(name.to_string(), source);
    }
}