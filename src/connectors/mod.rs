pub mod csv;
pub mod sqlserver;

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Describes a data source — either a local file or a database connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    File {
        path: String,
    },
    SqlServer {
        host: String,
        /// Defaults to 1433 when omitted.
        port: Option<u16>,
        database: String,
        username: String,
        password: String,
        /// Either a bare table reference ("dbo.customers") or a full SELECT query.
        query: String,
    },
}

impl SourceConfig {
    /// Short human-readable label used in error messages.
    pub fn label(&self) -> String {
        match self {
            SourceConfig::File { path } => path.clone(),
            SourceConfig::SqlServer { host, port, database, query, .. } => {
                format!("{}:{}/{}/{}", host, port.unwrap_or(1433), database, query)
            }
        }
    }
}

#[derive(Debug)]
pub enum ConnectorError {
    ConnectionFailed(String),
    QueryFailed(String),
    TypeConversion(String),
    Polars(polars::error::PolarsError),
    Io(std::io::Error),
}

impl fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectorError::ConnectionFailed(m) => write!(f, "Connection failed: {}", m),
            ConnectorError::QueryFailed(m) => write!(f, "Query failed: {}", m),
            ConnectorError::TypeConversion(m) => write!(f, "Type conversion error: {}", m),
            ConnectorError::Polars(e) => write!(f, "Polars error: {}", e),
            ConnectorError::Io(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl std::error::Error for ConnectorError {}

impl From<polars::error::PolarsError> for ConnectorError {
    fn from(e: polars::error::PolarsError) -> Self {
        ConnectorError::Polars(e)
    }
}

impl From<std::io::Error> for ConnectorError {
    fn from(e: std::io::Error) -> Self {
        ConnectorError::Io(e)
    }
}

/// Load a DataFrame from any SourceConfig.
pub async fn load_source(config: &SourceConfig) -> Result<DataFrame, ConnectorError> {
    match config {
        SourceConfig::File { path } => csv::load(path),
        SourceConfig::SqlServer { host, port, database, username, password, query } => {
            sqlserver::load_async(host, port.unwrap_or(1433), database, username, password, query)
                .await
        }
    }
}
