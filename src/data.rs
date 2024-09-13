use crate::errors::SQLError;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub struct TableSchema {
    pub columns: HashMap<String, usize>, // Column name to index
}

pub fn read_table_schema(file_path: &str) -> Result<TableSchema, SQLError> {
    let file = File::open(file_path)
        .map_err(|_| SQLError::InvalidTable(format!("Cannot open table file '{}'", file_path)))?;
    let mut reader = BufReader::new(file);
    let mut header_line = String::new();
    reader
        .read_line(&mut header_line)
        .map_err(|_| SQLError::InvalidTable("Failed to read table header".to_string()))?;

    let columns: HashMap<String, usize> = header_line
        .trim_end()
        .split(',')
        .enumerate()
        .map(|(idx, col_name)| (col_name.to_string(), idx))
        .collect();

    Ok(TableSchema { columns })
}
