# Custom Indexing Patterns

Build domain-specific indexers by filtering and decoding events from Sui checkpoints.

## Getting Started

Start with the main example and add filtering:

```bash
# Index all events
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 --end 239600500 \
  --output-dir ./parquet_output

# Filter by package (e.g., DeepBook)
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 --end 239600500 \
  --output-dir ./deepbook_output \
  --package 0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809
```

## Output Schema

The indexer produces 10 Parquet tables. For event-focused analysis, key columns are:

| Column | Type | Description |
|--------|------|-------------|
| `checkpoint_num` | u64 | Checkpoint sequence number |
| `timestamp_ms` | u64 | Checkpoint timestamp |
| `tx_digest` | string | Transaction digest |
| `event_index` | u32 | Event index within transaction |
| `package_id` | string | Move package ID |
| `module` | string | Move module name |
| `event_type` | string | Event type name |
| `sender` | string | Transaction sender |
| `event_json` | string | Event data as JSON |
| `bcs_data` | binary | Raw BCS-encoded event |

## Creating a Package-Specific Indexer

### 1. Filter by Package

Use the `--package` flag to filter events:

```bash
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 --end 239600500 \
  --package 0xYOUR_PACKAGE_ID \
  --output-dir ./my_package_output
```

### 2. Decode Events (Optional)

For custom event decoding, see `src/deepbook.rs` as a reference:

```rust
// Example: decode a custom event type
pub fn decode_my_event(bcs_data: &[u8]) -> serde_json::Value {
    match bcs::from_bytes::<MyEventStruct>(bcs_data) {
        Ok(event) => serde_json::json!({
            "field1": event.field1,
            "field2": event.field2.to_string(),
        }),
        Err(_) => serde_json::json!({
            "decode_error": "failed to decode event"
        }),
    }
}
```

### 3. Query with DuckDB

After indexing, analyze with SQL:

```sql
-- Events by type for your package
SELECT event_type, COUNT(*) as count
FROM read_parquet('./my_package_output/events.parquet')
GROUP BY event_type ORDER BY count DESC;

-- Parse JSON fields
SELECT
  tx_digest,
  json_extract_string(event_json, '$.field1') as field1
FROM read_parquet('./my_package_output/events.parquet')
WHERE event_type = 'MyEvent';
```

## Design Principles

1. **Preserve raw data**: Always keep `bcs_data` for future re-decoding
2. **Stable output**: Use consistent column names across indexers
3. **Portable format**: Parquet works with DuckDB, Polars, Spark, etc.
4. **Decode errors**: Return JSON error objects instead of failing

## Example: DeepBook Analysis

```bash
# Index DeepBook events
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 --end 239700000 \
  --package 0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809 \
  --output-dir ./deepbook_output

# Analyze order flow
duckdb -c "
  SELECT event_type, COUNT(*) as count
  FROM read_parquet('./deepbook_output/events.parquet')
  GROUP BY event_type ORDER BY count DESC
"
```
