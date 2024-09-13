use std::fmt;

#[derive(Debug)]
pub enum SQLError {
    InvalidTable(String),
    InvalidColumn(String),
    InvalidSyntax(String),
    GenericError(String),
}

impl fmt::Display for SQLError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SQLError::InvalidTable(msg) => write!(f, "INVALID_TABLE: {}", msg),
            SQLError::InvalidColumn(msg) => write!(f, "INVALID_COLUMN: {}", msg),
            SQLError::InvalidSyntax(msg) => write!(f, "INVALID_SYNTAX: {}", msg),
            SQLError::GenericError(msg) => write!(f, "ERROR: {}", msg),
        }
    }
}

impl std::error::Error for SQLError {}
