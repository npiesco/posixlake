//! Query pruning and predicate extraction for efficient file scanning

use tracing::debug;

/// Extract simple predicates from SQL WHERE clause using DataFusion's parser
/// Returns: Vec<(column_name, operator, value)>
pub fn extract_predicates(sql: &str) -> Vec<(String, String, serde_json::Value)> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

    let mut predicates = Vec::new();

    // Parse SQL using DataFusion's SQL parser
    let dialect = GenericDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        debug!("Failed to parse SQL: {}", sql);
        return predicates;
    };

    if statements.is_empty() {
        return predicates;
    }

    // Extract WHERE clause from SELECT statement
    if let datafusion::sql::sqlparser::ast::Statement::Query(query) = &statements[0] {
        if let datafusion::sql::sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
            if let Some(selection) = &select.selection {
                extract_predicates_from_expr(selection, &mut predicates);
            }
        }
    }

    predicates
}

/// Recursively extract predicates from WHERE expression
fn extract_predicates_from_expr(
    expr: &datafusion::sql::sqlparser::ast::Expr,
    predicates: &mut Vec<(String, String, serde_json::Value)>,
) {
    use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr};

    if let Expr::BinaryOp { left, op, right } = expr {
        // Handle AND/OR recursively
        match op {
            BinaryOperator::And | BinaryOperator::Or => {
                extract_predicates_from_expr(left, predicates);
                extract_predicates_from_expr(right, predicates);
            }
            // Handle comparison operators
            BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Eq => {
                // Extract column name and value
                if let (Expr::Identifier(ident), Expr::Value(val)) = (left.as_ref(), right.as_ref())
                {
                    let col_name = ident.value.clone();
                    let operator = match op {
                        BinaryOperator::Gt => ">",
                        BinaryOperator::GtEq => ">=",
                        BinaryOperator::Lt => "<",
                        BinaryOperator::LtEq => "<=",
                        BinaryOperator::Eq => "=",
                        _ => return,
                    }
                    .to_string();

                    // Extract value from ValueWithSpan
                    let value_str = &val.value;
                    let json_value = match value_str {
                        datafusion::sql::sqlparser::ast::Value::Number(n, _) => {
                            // Try to parse as i64 first, then f64
                            if let Ok(i) = n.parse::<i64>() {
                                serde_json::json!(i)
                            } else if let Ok(f) = n.parse::<f64>() {
                                serde_json::json!(f)
                            } else {
                                return;
                            }
                        }
                        datafusion::sql::sqlparser::ast::Value::SingleQuotedString(s)
                        | datafusion::sql::sqlparser::ast::Value::DoubleQuotedString(s) => {
                            serde_json::json!(s)
                        }
                        _ => return,
                    };

                    predicates.push((col_name, operator, json_value));
                }
            }
            _ => {}
        }
    }
}

/// Helper function to compare JSON values (less than)
pub fn is_value_less_than(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Number(a_num), serde_json::Value::Number(b_num)) => {
            if let (Some(a_i), Some(b_i)) = (a_num.as_i64(), b_num.as_i64()) {
                a_i < b_i
            } else if let (Some(a_f), Some(b_f)) = (a_num.as_f64(), b_num.as_f64()) {
                a_f < b_f
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Helper function to compare JSON values (greater than)
pub fn is_value_greater_than(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Number(a_num), serde_json::Value::Number(b_num)) => {
            if let (Some(a_i), Some(b_i)) = (a_num.as_i64(), b_num.as_i64()) {
                a_i > b_i
            } else if let (Some(a_f), Some(b_f)) = (a_num.as_f64(), b_num.as_f64()) {
                a_f > b_f
            } else {
                false
            }
        }
        _ => false,
    }
}
