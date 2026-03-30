# datadiff

A fast, schema-aware CLI tool for comparing CSV files and detecting structural and data differences.

Compare schemas for breaking changes, find modified rows with configurable tolerance, and run batch comparisons across multiple file pairs with a single manifest.

## Features

- **Schema Comparison** — detect added/removed columns, type changes, and compatibility issues with optional policy validation
- **Data Diffing** — identify source-only, target-only, and modified rows with configurable keys and numeric tolerance
- **Batch Operations** — compare multiple file pairs in one run with a JSON or CSV manifest
- **Policy-Driven Validation** — enforce schema contracts (required columns, forbidden removals, allowed type promotions)
- **Flexible Output** — export results as JSON or CSV for downstream automation
- **Scalable** — optimized for large datasets with early termination and column filtering

## Installation

### From Source

```bash
git clone https://github.com/<your-org>/datadiff.git
cd datadiff
cargo install --path .
```

This builds and installs the `datadiff` binary to your Cargo bin directory (usually `~/.cargo/bin`).

### Verify Installation

```bash
datadiff --version
datadiff --help
```

## Quick Start

### 1. Basic Schema Comparison

Compare two CSV files to see what columns changed:

```bash
datadiff schema \
  --source gold_customers.csv \
  --target silver_customers.csv
```

Output includes:
- Columns added in target
- Columns removed from source
- Type changes and impact classification (SafePromotion, RiskyConversion, Breaking)
- Backward and forward compatibility assessment

### 2. Data Diffing with Primary Keys

Find which rows were added, removed, or modified:

```bash
datadiff data \
  --source gold_customers.csv \
  --target silver_customers.csv \
  --key customer_id
```

Options:
- `--key` — one or more column names for row matching (can repeat: `--key id --key date`)
- `--exclude-columns` — skip comparing certain columns (comma-separated: `--exclude-columns created_at,updated_at`)
- `--only-columns` — compare only specific columns
- `--numeric-tolerance` — allow numeric values to differ by this amount (e.g., `0.01` for 1%)
- `--diffs-only` — show only modified rows, skip summary tables (much faster)
- `--output` — directory to write JSON/CSV exports
- `--format` — export format: `json` or `csv`
- `--temp` — use a timestamped temp directory instead of `--output`

Example with filters:

```bash
datadiff data \
  --source raw_events.csv \
  --target processed_events.csv \
  --key event_id \
  --exclude-columns processing_timestamp \
  --numeric-tolerance 0.001 \
  --output ./reports \
  --format json \
  --diffs-only
```

### 3. Batch Comparisons with Manifest

Run multiple file pair comparisons and get an aggregate summary:

```bash
datadiff batch \
  --manifest pairs.json \
  --key id \
  --output ./batch_results \
  --format json
```

#### Manifest Format (JSON)

```json
[
  {
    "name": "customers_v1_to_v2",
    "source": "data/customers_v1.csv",
    "target": "data/customers_v2.csv",
    "key": "customer_id"
  },
  {
    "name": "orders_daily_check",
    "source": "data/orders_daily.csv",
    "target": "data/orders_staging.csv",
    "key": "order_id,order_date",
    "exclude_columns": "processing_notes",
    "numeric_tolerance": 0.01,
    "diffs_only": true
  }
]
```

Entries can override global settings:
- `key` (string) — override `--key` for this pair
- `exclude_columns` (string) — comma-separated columns to skip
- `only_columns` (string) — comma-separated columns to include only
- `numeric_tolerance` (float) — tolerance for this pair
- `diffs_only` (bool) — show only diffs for this pair
- `output_base` (string) — per-pair output directory

#### Manifest Format (CSV)

```csv
name,source,target,key,exclude_columns,numeric_tolerance,diffs_only
customers_v1_to_v2,data/customers_v1.csv,data/customers_v2.csv,customer_id,,
orders_daily_check,data/orders_daily.csv,data/orders_staging.csv,"order_id,order_date",processing_notes,0.01,true
```

## Schema Policy & Validation

Enforce structural contracts with a JSON policy file:

```bash
datadiff schema \
  --source gold_schema.csv \
  --target silver_schema.csv \
  --policy schema-contract.json
```

### Policy File Format

```json
{
  "required_columns_source": ["id", "created_at"],
  "required_columns_target": ["id", "created_at", "modified_at"],
  "forbidden_removals": ["id", "customer_id"],
  "max_new_columns": 5,
  "allowed_type_changes": [
    { "from": "Int32", "to": "Int64" },
    { "from": "Float32", "to": "Float64" },
    { "from": "Int32", "to": "Int32" }
  ],
  "fail_on_breaking": true
}
```

- `required_columns_source` — columns that must exist in source
- `required_columns_target` — columns that must exist in target
- `forbidden_removals` — columns that cannot be removed
- `max_new_columns` — reject if more than N columns are added
- `allowed_type_changes` — list of type conversions to permit
- `fail_on_breaking` — if true, exit with error on breaking/risky changes

## Output & Exports

### Schema Comparison Output (terminal + optional export)

```
Schema Comparison Results
---------------------------
Source file: gold_schema.csv
Target file: silver_schema.csv

Columns added in target (1): ["new_field"]
Columns removed from source (0): []

Type changes in shared columns (1):
  - customer_id: Int32 -> Int64 (SafePromotion)

Potential renames: none

Compatibility:
  - Backward compatible: true
  - Forward compatible: false
  - Breaking reasons:
    - Added column: new_field

Policy check: passed (schema-contract.json)
```

### Data Diff Output (terminal + JSON/CSV export)

Terminal shows:
- Summary of row counts (total, source-only, target-only, modified)
- Column-level statistics (nulls, unique values, numeric min/max/mean)
- Most-changed columns

Export JSON includes structured diff results for automation.

### Batch Summary Output

```
Batch Results: 3 pairs
- customers_v1_to_v2: ✓ (5 modified rows)
- orders_daily_check: ✓ (120 target-only rows)
- transactions_staging: ✗ (missing source file)

Total: 2 succeeded, 1 failed
Total rows modified across all pairs: 125
```

## Examples

### Example 1: Validate a Data Warehouse Schema Change

```bash
# Check if a new table version is backward compatible
datadiff schema \
  --source warehouse/events_v2.csv \
  --target warehouse/events_v3.csv \
  --policy warehouse/schema-policies.json
```

### Example 2: Find Unexpected Changes in ETL Output

```bash
# Compare daily ETL inputs to see what changed
datadiff data \
  --source raw/daily_2026-03-28.csv \
  --target raw/daily_2026-03-29.csv \
  --key transaction_id \
  --diffs-only \
  --output ./etl_check
```

### Example 3: Batch Validation After Release

```bash
# Run schema checks on all updated tables after a deployment
datadiff schema \
  --source prod_snapshot.csv \
  --target staging_snapshot.csv \
  --policy prod-schema-contract.json

# If schema is OK, check data integrity
datadiff batch \
  --manifest prod_validation_pairs.json \
  --key id \
  --output ./release_validation \
  --format json
```

## Performance Tips

- Use `--diffs-only` to skip expensive statistics computation
- Use `--exclude-columns` or `--only-columns` to reduce comparison scope
- For multi-column keys, use only the minimal key set needed for matching
- Test policy files on small samples before batch runs

## Troubleshooting

**Error: "No columns added in target"**  
Normal when schemas match. Check file paths and CSV encoding.

**Error: "CSV parsing failed"**  
Verify CSV is valid (correct delimiters, quotes, encoding). Polars infers delimiter automatically but defaults to comma.

**Batch run fails on one pair but not others**  
Add `--verbose` for debugging (if supported in your version), or check individual pair with `datadiff data` and the same filters.

**Type classification seems wrong**  
Polars infers types from the first 100 rows. If your CSV has mixed types, ensure consistent formatting.

## Contributing

Contributions welcome! Please open issues for bugs or feature requests.

## License

(Add your license here—e.g., MIT, Apache 2.0)

## Roadmap

- [ ] Database integration (Postgres, Snowflake, Databricks)
- [ ] Desktop GUI for interactive exploration
- [ ] Streaming mode for files larger than RAM
- [ ] Plugin system for custom diff rules
- [ ] Scheduled reports and alerting
