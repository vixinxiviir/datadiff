use super::ConnectorError;
use polars::prelude::*;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

/// Connect to SQL Server and execute a query, returning the result as a Polars DataFrame.
///
/// `query` may be either a bare table reference (`"dbo.customers"`) or a full SELECT statement.
/// All column values are fetched as UTF-8 strings via `simple_query`; Polars stores them as
/// String series. Type-casting can be applied in a follow-up operation if needed.
///
/// Connections use `trust_cert()` - suitable for self-signed/dev certificates.
pub async fn load_async(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
    query: &str,
) -> Result<DataFrame, ConnectorError> {
    let mut config = Config::new();
    config.host(host);
    config.port(port);
    config.database(database);
    config.authentication(AuthMethod::sql_server(username, password));
    // Trust server certificate - change to a TLS-verified config for production.
    config.trust_cert();

    let addr = config.get_addr();
    let tcp = TcpStream::connect(&addr)
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("Cannot reach {}: {}", addr, e)))?;
    tcp.set_nodelay(true)
        .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

    let mut client = Client::connect(config, tcp.compat_write())
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

    let sql = normalize_query(query);

    // simple_query returns all column values as strings, avoiding type dispatch.
    let rows = client
        .simple_query(sql.as_str())
        .await
        .map_err(|e| ConnectorError::QueryFailed(e.to_string()))?
        .into_first_result()
        .await
        .map_err(|e| ConnectorError::QueryFailed(e.to_string()))?;

    if rows.is_empty() {
        return Ok(DataFrame::empty());
    }

    let col_count = rows[0].columns().len();
    let col_names: Vec<String> = rows[0]
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();

    // Transpose row-major to column-major, collecting each cell as Option<String>.
    let mut string_cols: Vec<Vec<Option<String>>> =
        vec![Vec::with_capacity(rows.len()); col_count];
    for row in &rows {
        for col_idx in 0..col_count {
            let v: Option<&str> = row.get(col_idx);
            string_cols[col_idx].push(v.map(|s| s.to_string()));
        }
    }

    let series_vec: Vec<Series> = col_names
        .iter()
        .zip(string_cols)
        .map(|(name, vals)| Series::new(name.as_str(), vals))
        .collect();

    DataFrame::new(series_vec).map_err(ConnectorError::Polars)
}

/// Wrap bare table references in `SELECT * FROM <table>`.
/// Full SELECT / WITH / EXEC statements are passed through unchanged.
fn normalize_query(query: &str) -> String {
    let trimmed = query.trim();
    let upper = trimmed.to_uppercase();
    if upper.starts_with("SELECT")
        || upper.starts_with("WITH")
        || upper.starts_with("EXEC")
        || upper.starts_with("EXECUTE")
    {
        trimmed.to_string()
    } else {
        format!("SELECT * FROM {}", trimmed)
    }
}