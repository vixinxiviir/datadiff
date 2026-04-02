use super::ConnectorError;
use polars::prelude::*;
use rusqlite::{Connection, types::ValueRef};

/// Open a SQLite database file and execute a query, returning the result as a Polars DataFrame.
///
/// `query` may be a bare table name (`"customers"`) or a full SELECT statement.
/// All column values are returned as String series.
pub fn load(path: &str, query: &str) -> Result<DataFrame, ConnectorError> {
    let conn = Connection::open(path)
        .map_err(|e| ConnectorError::ConnectionFailed(format!("Cannot open '{}': {}", path, e)))?;

    let sql = normalize_query(query);

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| ConnectorError::QueryFailed(e.to_string()))?;

    let col_count = stmt.column_count();
    let col_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

    let mut string_cols: Vec<Vec<Option<String>>> = vec![Vec::new(); col_count];

    let mut rows = stmt
        .query([])
        .map_err(|e| ConnectorError::QueryFailed(e.to_string()))?;

    while let Some(row) = rows
        .next()
        .map_err(|e| ConnectorError::QueryFailed(e.to_string()))?
    {
        for i in 0..col_count {
            let val = match row
                .get_ref(i)
                .map_err(|e| ConnectorError::QueryFailed(e.to_string()))?
            {
                ValueRef::Null => None,
                ValueRef::Integer(n) => Some(n.to_string()),
                ValueRef::Real(f) => Some(f.to_string()),
                ValueRef::Text(s) => Some(String::from_utf8_lossy(s).into_owned()),
                ValueRef::Blob(b) => Some(format!("<blob {} bytes>", b.len())),
            };
            string_cols[i].push(val);
        }
    }

    if string_cols[0].is_empty() {
        return Ok(DataFrame::empty());
    }

    let series_vec: Vec<Series> = col_names
        .iter()
        .zip(string_cols)
        .map(|(name, vals)| Series::new(name.as_str(), vals))
        .collect();

    DataFrame::new(series_vec).map_err(ConnectorError::Polars)
}

/// Wrap a bare table name in `SELECT * FROM <table>`.
/// Full SELECT / WITH statements are passed through unchanged.
fn normalize_query(query: &str) -> String {
    let trimmed = query.trim();
    let upper = trimmed.to_uppercase();
    if upper.starts_with("SELECT") || upper.starts_with("WITH") {
        trimmed.to_string()
    } else {
        format!("SELECT * FROM {}", trimmed)
    }
}
