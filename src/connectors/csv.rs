use super::ConnectorError;
use polars::prelude::*;

/// Load a CSV file into a Polars DataFrame with header detection and schema inference.
pub fn load(path: &str) -> Result<DataFrame, ConnectorError> {
    CsvReader::from_path(path)
        .map_err(ConnectorError::Polars)?
        .infer_schema(Some(100))
        .has_header(true)
        .finish()
        .map_err(ConnectorError::Polars)
}
