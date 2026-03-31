#[tauri::command]
fn run_diff(
    path1: String,
    path2: String,
    keys: Vec<String>,
    exclude_columns: Option<String>,
    only_columns: Option<String>,
    numeric_tolerance: Option<f64>,
) -> Result<serde_json::Value, String> {
    datadiff::data::run_diff(
        &path1,
        &path2,
        &keys,
        exclude_columns.as_deref(),
        only_columns.as_deref(),
        numeric_tolerance,
    )
    .map_err(|e| e.to_string())
}

#[tauri::command]
fn run_schema_diff(
    path1: String,
    path2: String,
) -> Result<datadiff::schema::SchemaDiffResult, String> {
    datadiff::schema::run_schema_diff(&path1, &path2, None)
        .map_err(|e| e.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .invoke_handler(tauri::generate_handler![run_diff, run_schema_diff])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
