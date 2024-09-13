use crate::SQLError;

#[derive(Debug)]
pub enum SQLQuery {
    Select(SelectQuery),
    Insert(InsertQuery),
    Update(UpdateQuery),
    Delete(DeleteQuery),
}

#[derive(Debug)]
pub struct SelectQuery {
    pub columns: Vec<String>,
    pub table: String,
    pub where_clause: Option<Expression>,
    pub order_by: Option<OrderBy>,
}

#[derive(Debug)]
pub struct InsertQuery {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<String>,
}

#[derive(Debug)]
pub struct UpdateQuery {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug)]
pub struct DeleteQuery {
    pub table: String,
    pub where_clause: Option<Expression>,
}

#[derive(Debug)]
pub struct Assignment {
    pub column: String,
    pub value: String,
}

#[derive(Debug)]
pub enum Expression {
    BinaryOp {
        left: Box<Expression>,
        op: String,
        right: Box<Expression>,
    },
    Literal(String),
    Column(String),
}

#[derive(Debug)]
pub struct OrderBy {
    pub column: String,
    pub ascending: bool,
}

#[derive(Debug, PartialEq)]
pub enum Token {
    Keyword(String),
    Identifier(String),
    Operator(String),
    Literal(String),
    Comma,
    Semicolon,
    Asterisk,
    OpenParen,
    CloseParen,
    EOF,
}

pub fn tokenize(input: &str) -> Result<Vec<Token>, crate::errors::SQLError> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
            }
            ',' => {
                chars.next();
                tokens.push(Token::Comma);
            }
            ';' => {
                chars.next();
                tokens.push(Token::Semicolon);
            }
            '*' => {
                chars.next();
                tokens.push(Token::Asterisk);
            }
            '(' => {
                chars.next();
                tokens.push(Token::OpenParen);
            }
            ')' => {
                chars.next();
                tokens.push(Token::CloseParen);
            }
            '=' | '>' | '<' => {
                let mut op = ch.to_string();
                chars.next();
                if let Some(&'=') = chars.peek() {
                    op.push('=');
                    chars.next();
                }
                tokens.push(Token::Operator(op));
            }
            '\'' => {
                chars.next();
                let mut literal = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch == '\'' {
                        chars.next();
                        break;
                    } else {
                        literal.push(ch);
                        chars.next();
                    }
                }
                tokens.push(Token::Literal(literal));
            }
            _ if ch.is_alphabetic() => {
                let mut ident = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch.is_alphanumeric() || ch == '_' {
                        ident.push(ch);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let upper_ident = ident.to_uppercase();
                match upper_ident.as_str() {
                    "SELECT" | "FROM" | "WHERE" | "ORDER" | "BY" | "ASC" | "DESC" | "INSERT"
                    | "INTO" | "VALUES" | "UPDATE" | "SET" | "DELETE" | "AND" | "OR" | "NOT" => {
                        tokens.push(Token::Keyword(upper_ident))
                    }
                    _ => tokens.push(Token::Identifier(ident)),
                }
            }
            _ if ch.is_digit(10) => {
                let mut number = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch.is_digit(10) {
                        number.push(ch);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(Token::Literal(number));
            }
            _ => {
                return Err(crate::errors::SQLError::InvalidSyntax(format!(
                    "Unexpected character: '{}'",
                    ch
                )));
            }
        }
    }

    tokens.push(Token::EOF);
    Ok(tokens)
}

pub fn parse(tokens: &[Token]) -> Result<SQLQuery, SQLError> {
    let mut index = 0;
    match tokens.get(index) {
        Some(Token::Keyword(k)) if k == "SELECT" => parse_select(tokens, &mut index),
        Some(Token::Keyword(k)) if k == "INSERT" => parse_insert(tokens, &mut index),
        Some(Token::Keyword(k)) if k == "UPDATE" => parse_update(tokens, &mut index),
        Some(Token::Keyword(k)) if k == "DELETE" => parse_delete(tokens, &mut index),
        _ => Err(SQLError::InvalidSyntax(
            "Expected a SQL command".to_string(),
        )),
    }
}

fn parse_select(tokens: &[Token], index: &mut usize) -> Result<SQLQuery, crate::errors::SQLError> {
    *index += 1; // Skip 'SELECT'

    let columns = parse_select_list(tokens, index)?;

    // Expect 'FROM'
    match tokens.get(*index) {
        Some(Token::Keyword(k)) if k == "FROM" => *index += 1,
        _ => {
            return Err(crate::errors::SQLError::InvalidSyntax(
                "Expected 'FROM' keyword".to_string(),
            ))
        }
    }

    // Expect table name
    let table = match tokens.get(*index) {
        Some(Token::Identifier(name)) => {
            *index += 1;
            name.clone()
        }
        _ => {
            return Err(crate::errors::SQLError::InvalidSyntax(
                "Expected table name".to_string(),
            ))
        }
    };

    // Parse optional WHERE clause
    let where_clause = if let Some(Token::Keyword(k)) = tokens.get(*index) {
        if k == "WHERE" {
            *index += 1;
            Some(parse_expression(tokens, index)?)
        } else {
            None
        }
    } else {
        None
    };

    // Parse optional ORDER BY clause
    let order_by = if let Some(Token::Keyword(k)) = tokens.get(*index) {
        if k == "ORDER" {
            *index += 1;
            match tokens.get(*index) {
                Some(Token::Keyword(k)) if k == "BY" => *index += 1,
                _ => {
                    return Err(crate::errors::SQLError::InvalidSyntax(
                        "Expected 'BY' after 'ORDER'".to_string(),
                    ))
                }
            }
            Some(parse_order_by(tokens, index)?)
        } else {
            None
        }
    } else {
        None
    };

    // Expect semicolon or EOF
    match tokens.get(*index) {
        Some(Token::Semicolon) | Some(Token::EOF) => {}
        _ => {
            return Err(crate::errors::SQLError::InvalidSyntax(
                "Expected ';' at the end of query".to_string(),
            ))
        }
    }

    Ok(SQLQuery::Select(SelectQuery {
        columns,
        table,
        where_clause,
        order_by,
    }))
}

fn parse_select_list(
    tokens: &[Token],
    index: &mut usize,
) -> Result<Vec<String>, crate::errors::SQLError> {
    let mut columns = Vec::new();

    loop {
        match tokens.get(*index) {
            Some(Token::Asterisk) => {
                *index += 1;
                columns.push("*".to_string());
            }
            Some(Token::Identifier(name)) => {
                *index += 1;
                columns.push(name.clone());
            }
            _ => {
                return Err(crate::errors::SQLError::InvalidSyntax(
                    "Expected column name or '*'".to_string(),
                ))
            }
        }

        match tokens.get(*index) {
            Some(Token::Comma) => *index += 1,
            _ => break,
        }
    }

    Ok(columns)
}

fn parse_insert(tokens: &[Token], index: &mut usize) -> Result<SQLQuery, SQLError> {
    *index += 1; // Skip 'INSERT'

    // Expect 'INTO'
    match tokens.get(*index) {
        Some(Token::Keyword(k)) if k == "INTO" => *index += 1,
        _ => {
            return Err(SQLError::InvalidSyntax(
                "Expected 'INTO' keyword".to_string(),
            ))
        }
    }

    // Expect table name
    let table = match tokens.get(*index) {
        Some(Token::Identifier(name)) => {
            *index += 1;
            name.clone()
        }
        _ => {
            return Err(SQLError::InvalidSyntax(
                "Expected table name after 'INTO'".to_string(),
            ))
        }
    };

    // Parse optional column list
    let columns = if let Some(Token::OpenParen) = tokens.get(*index) {
        *index += 1; // Skip '('
        let cols = parse_column_list(tokens, index)?;
        match tokens.get(*index) {
            Some(Token::CloseParen) => *index += 1, // Skip ')'
            _ => {
                return Err(SQLError::InvalidSyntax(
                    "Expected ')' after column list".to_string(),
                ))
            }
        }
        cols
    } else {
        Vec::new()
    };

    // Expect 'VALUES' keyword
    match tokens.get(*index) {
        Some(Token::Keyword(k)) if k == "VALUES" => *index += 1,
        _ => {
            return Err(SQLError::InvalidSyntax(
                "Expected 'VALUES' keyword".to_string(),
            ))
        }
    }

    // Parse values list
    match tokens.get(*index) {
        Some(Token::OpenParen) => *index += 1, // Skip '('
        _ => {
            return Err(SQLError::InvalidSyntax(
                "Expected '(' before values list".to_string(),
            ))
        }
    }

    let values = parse_values_list(tokens, index)?;

    match tokens.get(*index) {
        Some(Token::CloseParen) => *index += 1, // Skip ')'
        _ => {
            return Err(SQLError::InvalidSyntax(
                "Expected ')' after values list".to_string(),
            ))
        }
    }

    // Expect semicolon or EOF
    match tokens.get(*index) {
        Some(Token::Semicolon) | Some(Token::EOF) => {}
        _ => {
            return Err(SQLError::InvalidSyntax(
                "Expected ';' at the end of the query".to_string(),
            ))
        }
    }

    Ok(SQLQuery::Insert(InsertQuery {
        table,
        columns,
        values,
    }))
}

fn parse_column_list(tokens: &[Token], index: &mut usize) -> Result<Vec<String>, SQLError> {
    let mut columns = Vec::new();
    loop {
        match tokens.get(*index) {
            Some(Token::Identifier(name)) => {
                columns.push(name.clone());
                *index += 1;
            }
            _ => {
                return Err(SQLError::InvalidSyntax(
                    "Expected column name in column list".to_string(),
                ))
            }
        }

        match tokens.get(*index) {
            Some(Token::Comma) => *index += 1,
            Some(Token::CloseParen) => break,
            _ => {
                return Err(SQLError::InvalidSyntax(
                    "Expected ',' or ')' in column list".to_string(),
                ))
            }
        }
    }
    Ok(columns)
}

fn parse_values_list(tokens: &[Token], index: &mut usize) -> Result<Vec<String>, SQLError> {
    let mut values = Vec::new();
    loop {
        match tokens.get(*index) {
            Some(Token::Literal(value)) => {
                values.push(value.clone());
                *index += 1;
            }
            _ => {
                return Err(SQLError::InvalidSyntax(
                    "Expected literal value in values list".to_string(),
                ))
            }
        }

        match tokens.get(*index) {
            Some(Token::Comma) => *index += 1,
            Some(Token::CloseParen) => break,
            _ => {
                return Err(SQLError::InvalidSyntax(
                    "Expected ',' or ')' in values list".to_string(),
                ))
            }
        }
    }
    Ok(values)
}

fn parse_update(tokens: &[Token], index: &mut usize) -> Result<SQLQuery, SQLError> {
    *index += 1; // Skip 'UPDATE'

    // Expect table name
    let table = match tokens.get(*index) {
        Some(Token::Identifier(name)) => {
            *index += 1;
            name.clone()
        }
        _ => {
            return Err(SQLError::InvalidSyntax(
                "Expected table name after 'UPDATE'".to_string(),
            ))
        }
    };

    // Expect 'SET' keyword
    match tokens.get(*index) {
        Some(Token::Keyword(k)) if k == "SET" => *index += 1,
        _ => {
            return Err(SQLError::InvalidSyntax(
                "Expected 'SET' keyword".to_string(),
            ))
        }
    }

    // Parse assignments
    let assignments = parse_assignments(tokens, index)?;

    // Parse optional WHERE clause
    let where_clause = if let Some(Token::Keyword(k)) = tokens.get(*index) {
        if k == "WHERE" {
            *index += 1;
            Some(parse_expression(tokens, index)?)
        } else {
            None
        }
    } else {
        None
    };

    // Expect semicolon or EOF
    match tokens.get(*index) {
        Some(Token::Semicolon) | Some(Token::EOF) => {}
        _ => {
            return Err(SQLError::InvalidSyntax(
                "Expected ';' at the end of the query".to_string(),
            ))
        }
    }

    Ok(SQLQuery::Update(UpdateQuery {
        table,
        assignments,
        where_clause,
    }))
}

fn parse_assignments(tokens: &[Token], index: &mut usize) -> Result<Vec<Assignment>, SQLError> {
    let mut assignments = Vec::new();
    loop {
        // Expect column name
        let column = match tokens.get(*index) {
            Some(Token::Identifier(name)) => {
                *index += 1;
                name.clone()
            }
            _ => {
                return Err(SQLError::InvalidSyntax(
                    "Expected column name in assignment".to_string(),
                ))
            }
        };

        // Expect '=' operator
        match tokens.get(*index) {
            Some(Token::Operator(op)) if op == "=" => *index += 1,
            _ => {
                return Err(SQLError::InvalidSyntax(
                    "Expected '=' in assignment".to_string(),
                ))
            }
        }

        // Expect literal value
        let value = match tokens.get(*index) {
            Some(Token::Literal(val)) => {
                *index += 1;
                val.clone()
            }
            _ => {
                return Err(SQLError::InvalidSyntax(
                    "Expected literal value in assignment".to_string(),
                ))
            }
        };

        assignments.push(Assignment { column, value });

        // Check for comma or end
        match tokens.get(*index) {
            Some(Token::Comma) => *index += 1,
            _ => break,
        }
    }
    Ok(assignments)
}

fn parse_delete(tokens: &[Token], index: &mut usize) -> Result<SQLQuery, SQLError> {
    *index += 1; // Skip 'DELETE'

    // Expect 'FROM'
    match tokens.get(*index) {
        Some(Token::Keyword(k)) if k == "FROM" => *index += 1,
        _ => {
            return Err(SQLError::InvalidSyntax(
                "Expected 'FROM' keyword after 'DELETE'".to_string(),
            ))
        }
    }

    // Expect table name
    let table = match tokens.get(*index) {
        Some(Token::Identifier(name)) => {
            *index += 1;
            name.clone()
        }
        _ => {
            return Err(SQLError::InvalidSyntax(
                "Expected table name after 'FROM'".to_string(),
            ))
        }
    };

    // Parse optional WHERE clause
    let where_clause = if let Some(Token::Keyword(k)) = tokens.get(*index) {
        if k == "WHERE" {
            *index += 1;
            Some(parse_expression(tokens, index)?)
        } else {
            None
        }
    } else {
        None
    };

    // Expect semicolon or EOF
    match tokens.get(*index) {
        Some(Token::Semicolon) | Some(Token::EOF) => {}
        _ => {
            return Err(SQLError::InvalidSyntax(
                "Expected ';' at the end of the query".to_string(),
            ))
        }
    }

    Ok(SQLQuery::Delete(DeleteQuery {
        table,
        where_clause,
    }))
}

fn parse_order_by(tokens: &[Token], index: &mut usize) -> Result<OrderBy, crate::errors::SQLError> {
    // Expect column name
    let column = match tokens.get(*index) {
        Some(Token::Identifier(name)) => {
            *index += 1;
            name.clone()
        }
        _ => {
            return Err(crate::errors::SQLError::InvalidSyntax(
                "Expected column name in ORDER BY".to_string(),
            ))
        }
    };

    // Optional ASC/DESC
    let ascending = match tokens.get(*index) {
        Some(Token::Keyword(k)) if k == "ASC" => {
            *index += 1;
            true
        }
        Some(Token::Keyword(k)) if k == "DESC" => {
            *index += 1;
            false
        }
        _ => true, // Default to ASC
    };

    Ok(OrderBy { column, ascending })
}

fn parse_expression(
    tokens: &[Token],
    index: &mut usize,
) -> Result<Expression, crate::errors::SQLError> {
    // For simplicity, parse expressions of the form: column operator literal
    let left = match tokens.get(*index) {
        Some(Token::Identifier(name)) => {
            *index += 1;
            Expression::Column(name.clone())
        }
        _ => {
            return Err(crate::errors::SQLError::InvalidSyntax(
                "Expected column name in expression".to_string(),
            ))
        }
    };

    // Expect operator
    let op = match tokens.get(*index) {
        Some(Token::Operator(op)) => {
            *index += 1;
            op.clone()
        }
        _ => {
            return Err(crate::errors::SQLError::InvalidSyntax(
                "Expected operator in expression".to_string(),
            ))
        }
    };

    // Expect literal
    let right = match tokens.get(*index) {
        Some(Token::Literal(value)) => {
            *index += 1;
            Expression::Literal(value.clone())
        }
        _ => {
            return Err(crate::errors::SQLError::InvalidSyntax(
                "Expected literal value in expression".to_string(),
            ))
        }
    };

    Ok(Expression::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_simple() {
        let query = "SELECT id, name FROM users;";
        let tokens = tokenize(query).unwrap();
        let parsed_query = parse(&tokens).unwrap();

        if let SQLQuery::Select(select_query) = parsed_query {
            assert_eq!(select_query.columns, vec!["id", "name"]);
            assert_eq!(select_query.table, "users");
            assert!(select_query.where_clause.is_none());
            assert!(select_query.order_by.is_none());
        } else {
            panic!("Expected SELECT query");
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let query = "SELECT * FROM customers WHERE age > 30;";
        let tokens = tokenize(query).unwrap();
        let parsed_query = parse(&tokens).unwrap();

        if let SQLQuery::Select(select_query) = parsed_query {
            assert_eq!(select_query.columns, vec!["*"]);
            assert_eq!(select_query.table, "customers");
            assert!(select_query.where_clause.is_some());
        } else {
            panic!("Expected SELECT query");
        }
    }
}
