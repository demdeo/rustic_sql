use crate::data::{read_table_schema, TableSchema};
use crate::errors::SQLError;
use crate::parser::{DeleteQuery, Expression, InsertQuery, SQLQuery, SelectQuery, UpdateQuery};
use std::fs::File;
use std::io::{BufRead, BufReader};

pub fn execute_query(query: SQLQuery, tables_path: &str) -> Result<(), SQLError> {
    match query {
        SQLQuery::Select(select_query) => execute_select(select_query, tables_path),
        SQLQuery::Insert(insert_query) => execute_insert(insert_query, tables_path),
        SQLQuery::Update(update_query) => execute_update(update_query, tables_path),
        SQLQuery::Delete(delete_query) => execute_delete(delete_query, tables_path),
        // Other query types...
        _ => Err(SQLError::GenericError("Unsupported query type".to_string())),
    }
}

fn execute_select(select_query: SelectQuery, tables_path: &str) -> Result<(), SQLError> {
    // Construct file path
    let table_file = format!("{}/{}.csv", tables_path, select_query.table);

    // Read table schema
    let schema = read_table_schema(&table_file)?;

    // Validate selected columns
    let selected_indices = get_selected_indices(&select_query, &schema)?;

    // Open the table file
    let file = File::open(&table_file)
        .map_err(|_| SQLError::InvalidTable(format!("Cannot open table file '{}'", table_file)))?;
    let reader = BufReader::new(file);

    // Process rows
    process_rows(reader, &schema, &select_query, selected_indices)
}

fn get_selected_indices(
    select_query: &SelectQuery,
    schema: &TableSchema,
) -> Result<Vec<usize>, SQLError> {
    let mut indices = Vec::new();
    if select_query.columns.len() == 1 && select_query.columns[0] == "*" {
        let mut all_indices: Vec<usize> = schema.columns.values().cloned().collect();
        all_indices.sort();
        indices = all_indices;
    } else {
        for col in &select_query.columns {
            if let Some(&idx) = schema.columns.get(col) {
                indices.push(idx);
            } else {
                return Err(SQLError::InvalidColumn(format!(
                    "Column '{}' does not exist",
                    col
                )));
            }
        }
    }
    Ok(indices)
}

use std::fs::OpenOptions;
// use std::io::Write;
use std::io::{BufWriter, Write};

fn execute_insert(insert_query: InsertQuery, tables_path: &str) -> Result<(), SQLError> {
    // Construct the file path
    let table_file = format!("{}/{}.csv", tables_path, insert_query.table);

    // Read the table schema
    let schema = read_table_schema(&table_file)?;

    // Determine the columns to insert into
    let columns_to_insert = if insert_query.columns.is_empty() {
        // No columns specified, use all columns
        let mut cols: Vec<String> = schema.columns.keys().cloned().collect();
        cols.sort_by_key(|k| schema.columns[k]);
        cols
    } else {
        insert_query.columns.clone()
    };

    // Validate that all specified columns exist
    for col in &columns_to_insert {
        if !schema.columns.contains_key(col) {
            return Err(SQLError::InvalidColumn(format!(
                "Column '{}' does not exist",
                col
            )));
        }
    }

    // Ensure the number of values matches the number of columns
    if columns_to_insert.len() != insert_query.values.len() {
        return Err(SQLError::InvalidSyntax(
            "Number of columns and values do not match".to_string(),
        ));
    }

    // Prepare the new row with empty strings
    let num_columns = schema.columns.len();
    let mut new_row = vec!["".to_string(); num_columns];

    // Fill in the values for the specified columns
    for (col, val) in columns_to_insert.iter().zip(insert_query.values.iter()) {
        let idx = schema.columns[col];
        new_row[idx] = val.clone();
    }

    // Open the CSV file in append mode
    let mut file = OpenOptions::new()
        .append(true)
        .open(&table_file)
        .map_err(|_| SQLError::InvalidTable(format!("Cannot open table '{}'", table_file)))?;

    // Write the new row to the file
    let row_line = new_row.join(",") + "\n";
    file.write_all(row_line.as_bytes())
        .map_err(|_| SQLError::GenericError("Failed to write to table file".to_string()))?;

    Ok(())
}

fn execute_update(update_query: UpdateQuery, tables_path: &str) -> Result<(), SQLError> {
    // Construct the file paths
    let table_file = format!("{}/{}.csv", tables_path, update_query.table);
    let temp_file = format!("{}/{}.tmp", tables_path, update_query.table);

    // Read the table schema
    let schema = read_table_schema(&table_file)?;

    // Validate assignment columns
    for assignment in &update_query.assignments {
        if !schema.columns.contains_key(&assignment.column) {
            return Err(SQLError::InvalidColumn(format!(
                "Column '{}' does not exist",
                assignment.column
            )));
        }
    }

    // Open the table file for reading
    let file = File::open(&table_file)
        .map_err(|_| SQLError::InvalidTable(format!("Cannot open table '{}'", table_file)))?;
    let reader = BufReader::new(file);

    // Open a temporary file for writing
    let temp_file_handle = File::create(&temp_file)
        .map_err(|_| SQLError::GenericError("Failed to create temporary file".to_string()))?;
    let mut writer = BufWriter::new(temp_file_handle);

    let mut lines = reader.lines();
    // Write the header line
    if let Some(Ok(header_line)) = lines.next() {
        writer
            .write_all(header_line.as_bytes())
            .map_err(|_| SQLError::GenericError("Failed to write to temporary file".to_string()))?;
        writer.write_all(b"\n").map_err(|_| {
            SQLError::GenericError("Failed to write newline to temporary file".to_string())
        })?;
    } else {
        return Err(SQLError::InvalidTable(
            "Table is empty or corrupted".to_string(),
        ));
    }

    // Process each row
    for line_result in lines {
        let line = line_result
            .map_err(|_| SQLError::InvalidTable("Failed to read table row".to_string()))?;
        let mut row_values: Vec<String> = line.split(',').map(|s| s.to_string()).collect();

        let mut should_update = true;
        if let Some(ref where_clause) = update_query.where_clause {
            should_update = evaluate_where_clause(where_clause, &schema, &row_values)?;
        }

        if should_update {
            // Apply the assignments
            for assignment in &update_query.assignments {
                let idx = schema.columns[&assignment.column];
                row_values[idx] = assignment.value.clone();
            }
        }

        // Write the updated (or original) row to the temp file
        let updated_line = row_values.join(",") + "\n";
        writer
            .write_all(updated_line.as_bytes())
            .map_err(|_| SQLError::GenericError("Failed to write to temporary file".to_string()))?;
    }

    // Replace the original file with the temp file
    std::fs::rename(&temp_file, &table_file)
        .map_err(|_| SQLError::GenericError("Failed to replace original table file".to_string()))?;

    Ok(())
}

fn execute_delete(delete_query: DeleteQuery, tables_path: &str) -> Result<(), SQLError> {
    // Construct the file paths
    let table_file = format!("{}/{}.csv", tables_path, delete_query.table);
    let temp_file = format!("{}/{}.tmp", tables_path, delete_query.table);

    // Read the table schema
    let schema = read_table_schema(&table_file)?;

    // Open the table file for reading
    let file = File::open(&table_file)
        .map_err(|_| SQLError::InvalidTable(format!("Cannot open table '{}'", table_file)))?;
    let reader = BufReader::new(file);

    // Open a temporary file for writing
    let temp_file_handle = File::create(&temp_file)
        .map_err(|_| SQLError::GenericError("Failed to create temporary file".to_string()))?;
    let mut writer = BufWriter::new(temp_file_handle);

    let mut lines = reader.lines();
    // Write the header line
    if let Some(Ok(header_line)) = lines.next() {
        writer
            .write_all(header_line.as_bytes())
            .map_err(|_| SQLError::GenericError("Failed to write to temporary file".to_string()))?;
        writer.write_all(b"\n").map_err(|_| {
            SQLError::GenericError("Failed to write newline to temporary file".to_string())
        })?;
    } else {
        return Err(SQLError::InvalidTable(
            "Table is empty or corrupted".to_string(),
        ));
    }

    // Process each row
    for line_result in lines {
        let line = line_result
            .map_err(|_| SQLError::InvalidTable("Failed to read table row".to_string()))?;
        let row_values: Vec<String> = line.split(',').map(|s| s.to_string()).collect();

        let mut should_delete = false;
        if let Some(ref where_clause) = delete_query.where_clause {
            should_delete = evaluate_where_clause(where_clause, &schema, &row_values)?;
        }

        if !should_delete {
            // Write the row to the temp file
            let row_line = row_values.join(",") + "\n";
            writer.write_all(row_line.as_bytes()).map_err(|_| {
                SQLError::GenericError("Failed to write to temporary file".to_string())
            })?;
        }
    }

    // Replace the original file with the temp file
    std::fs::rename(&temp_file, &table_file)
        .map_err(|_| SQLError::GenericError("Failed to replace original table file".to_string()))?;

    Ok(())
}

fn process_rows(
    reader: BufReader<File>,
    schema: &TableSchema,
    select_query: &SelectQuery,
    selected_indices: Vec<usize>,
) -> Result<(), SQLError> {
    let mut lines = reader.lines().skip(1); // Skip header
    let mut results = Vec::new();

    while let Some(Ok(line)) = lines.next() {
        let row_values: Vec<String> = line.split(',').map(|s| s.to_string()).collect();

        let mut include_row = true;
        if let Some(ref where_clause) = select_query.where_clause {
            include_row = evaluate_where_clause(where_clause, schema, &row_values)?;
        }

        if include_row {
            let selected_values: Vec<String> = selected_indices
                .iter()
                .map(|&idx| row_values[idx].clone())
                .collect();
            results.push(selected_values);
        }
    }

    // Handle ORDER BY if present
    if let Some(ref order_by) = select_query.order_by {
        sort_results(&mut results, &selected_indices, schema, order_by)?;
    }

    // Output the results
    output_results(&selected_indices, schema, &results)?;

    Ok(())
}

fn evaluate_where_clause(
    expr: &Expression,
    schema: &TableSchema,
    row_values: &[String],
) -> Result<bool, SQLError> {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            let left_value = get_value(left, schema, row_values)?;
            let right_value = get_value(right, schema, row_values)?;

            match op.as_str() {
                "=" => Ok(left_value == right_value),
                "<" => Ok(left_value < right_value),
                ">" => Ok(left_value > right_value),
                "<=" => Ok(left_value <= right_value),
                ">=" => Ok(left_value >= right_value),
                "<>" => Ok(left_value != right_value),
                _ => Err(SQLError::InvalidSyntax(format!(
                    "Unknown operator '{}'",
                    op
                ))),
            }
        }
        _ => Err(SQLError::InvalidSyntax(
            "Unsupported expression in WHERE clause".to_string(),
        )),
    }
}

fn get_value(
    expr: &Expression,
    schema: &TableSchema,
    row_values: &[String],
) -> Result<String, SQLError> {
    match expr {
        Expression::Literal(val) => Ok(val.clone()),
        Expression::Column(col_name) => {
            if let Some(&idx) = schema.columns.get(col_name) {
                Ok(row_values[idx].clone())
            } else {
                Err(SQLError::InvalidColumn(format!(
                    "Column '{}' does not exist",
                    col_name
                )))
            }
        }
        _ => Err(SQLError::InvalidSyntax(
            "Unsupported expression".to_string(),
        )),
    }
}

fn sort_results(
    results: &mut [Vec<String>],
    selected_indices: &[usize],
    schema: &TableSchema,
    order_by: &crate::parser::OrderBy,
) -> Result<(), SQLError> {
    let order_idx = schema.columns.get(&order_by.column).ok_or_else(|| {
        SQLError::InvalidColumn(format!("Column '{}' does not exist", order_by.column))
    })?;

    let pos_in_selected = selected_indices
        .iter()
        .position(|&idx| idx == *order_idx)
        .ok_or_else(|| {
            SQLError::InvalidColumn(format!(
                "Column '{}' is not in the selected columns",
                order_by.column
            ))
        })?;

    if order_by.ascending {
        results.sort_by(|a, b| a[pos_in_selected].cmp(&b[pos_in_selected]));
    } else {
        results.sort_by(|a, b| b[pos_in_selected].cmp(&a[pos_in_selected]));
    }

    Ok(())
}

fn output_results(
    selected_indices: &[usize],
    schema: &TableSchema,
    results: &[Vec<String>],
) -> Result<(), SQLError> {
    // Print header
    let headers: Vec<_> = selected_indices
        .iter()
        .map(|&idx| {
            schema
                .columns
                .iter()
                .find(|&(_, &i)| i == idx)
                .map(|(name, _)| name.clone())
                .unwrap()
        })
        .collect();
    println!("{}", headers.join(","));

    // Print rows
    for row in results {
        println!("{}", row.join(","));
    }

    Ok(())
}
