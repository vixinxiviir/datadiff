use super::ConnectorError;
use mysql_async::{prelude::*, Pool, Opts, OptsBuilder, Row, Value};
use polars::prelude::*;

/// Connect to MySQL / MariaDB and execute a query, returning the result as a Polars DataFrame.
///
/// `query` may be a bare table/schema reference (`"mydb.customers"`) or a full SELECT statement.
/// All column values are returned as String series.
pub async fn load_async(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
    query: &str,
) -> Result<DataFrame, ConnectorError> {
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port)
        .db_name(Some(database))
        .user(Some(username))
        .pass(Some(password));

    let pool = Pool::new(Opts::from(opts));
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("Cannot connect to {}:{}/{}: {}", host, port, database, e)))?;

    let sql = normalize_query(query);

    let result = conn
        .exec_iter(&sql, ())
        .await
        .map_err(|e| ConnectorError::QueryFailed(e.to_string()))?;

    let col_names: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|c| c.name_str().into_owned())
        .collect();

    let col_count = col_names.len();

    // Collect as Row — Row implements FromRow
    let rows: Vec<Row> = result
        .collect_and_drop()
        .await
        .map_err(|e| ConnectorError::QueryFailed(e.to_string()))?;

    // Disconnect cleanly; ignore errors (pool cleanup is best-effort).
    drop(conn);
    pool.disconnect().await.ok();

    if rows.is_empty() {
        return Ok(DataFrame::empty());
    }

    let mut string_cols: Vec<Vec<Option<String>>> = vec![Vec::with_capacity(rows.len()); col_count];

    for mut row in rows {
        for i in 0..col_count {
            // take() converts the cell to Value (identity FromValue impl) and removes it from the row
            let val: Option<Value> = row.take(i);
            string_cols[i].push(val.map(|v| mysql_value_to_string(&v)).unwrap_or(None));
        }
    }

    let series_vec: Vec<Series> = col_names
        .iter()
        .zip(string_cols)
        .map(|(name, vals)| Series::new(name.as_str(), vals))
        .collect();

    DataFrame::new(series_vec).map_err(ConnectorError::Polars)
}

fn mysql_value_to_string(val: &Value) -> Option<String> {
    match val {
        Value::NULL => None,
        Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        Value::Int(n) => Some(n.to_string()),
        Value::UInt(n) => Some(n.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Double(f) => Some(f.to_string()),
        Value::Date(y, mo, d, h, mi, s, us) => Some(format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            y, mo, d, h, mi, s, us
        )),
        Value::Time(neg, days, h, mi, s, us) => {
            let sign = if *neg { "-" } else { "" };
            Some(format!(
                "{}{:02}:{:02}:{:02}.{:06}",
                sign,
                days * 24 + *h as u32,
                mi,
                s,
                us
            ))
        }
    }
}

/// Wrap a bare table/schema reference in `SELECT * FROM`.
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
