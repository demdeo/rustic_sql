use std::env;

mod errors;
use errors::SQLError;

mod data;
mod executor;
mod parser;

use crate::executor::execute_query;
use crate::parser::{parse, tokenize};

fn main() {
    // Collect command-line arguments
    let args: Vec<String> = env::args().collect();

    // Ensure the correct number of arguments are provided
    if args.len() != 3 {
        eprintln!("Usage: cargo run -- <path_to_tables> \"<SQL_query>\"");
        std::process::exit(1);
    }

    let tables_path = &args[1];
    let sql_query = &args[2];

    // Proceed to parse and execute the SQL query
    println!("Tables path: {}", tables_path);
    println!("SQL query: {}", sql_query);

    // Tokenize and parse the query
    let tokens = match tokenize(sql_query) {
        Ok(t) => t,
        Err(e) => {
            println!("{}", e);
            std::process::exit(1);
        }
    };

    let parsed_query = match parse(&tokens) {
        Ok(q) => q,
        Err(e) => {
            println!("{}", e);
            std::process::exit(1);
        }
    };

    // Execute the query
    if let Err(e) = execute_query(parsed_query, tables_path) {
        println!("{}", e);
        std::process::exit(1);
    }
}
