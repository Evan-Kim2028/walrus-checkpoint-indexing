**Custom Indexing (Patterns + Examples)**
- Goal: show how to filter and decode events for domain-specific datasets.
- Design: keep output stable and portable (Parquet + JSON fields).

**Where to Start**
- `examples/alt_checkpoint_parquet.rs`: all events to parquet (baseline)
- `examples/deepbook_alt_parquet.rs`: DeepBook-only events (decoded)
- `src/deepbook.rs`: decoding helper + output JSON shape

**Recommended Output Shape**
- Always include:
  - `checkpoint_num`, `timestamp_ms`, `tx_digest`, `event_index`
  - `package_id`, `module`, `event_type`, `sender`
  - `event_json` (decoded or error object)
  - `bcs_data` (raw BCS bytes)
- This preserves raw data while providing a friendly JSON payload.

**DeepBook Example**
- `event_json` contains decoded fields for each DeepBook event.
- `bcs_data` always preserves raw bytes for future re-decode.
- Unknown types or decode failures return:
```json
{"event_type":"<name>","decode_error":"unsupported_event_type_or_decode_failure"}
```

**Creating a New Package Indexer**
1) Start from `alt_checkpoint_parquet.rs` and add a package filter.
2) Implement a decode helper (like `src/deepbook.rs`) for your event types.
3) Emit decoded `event_json` and keep `bcs_data` intact.
4) Add a fixture test that asserts the JSON shape.

**Fixture Test Guidance**
- Use a small event struct with known values.
- Encode with `bcs::to_bytes`.
- Decode via your helper and assert JSON fields.

**Example CLI Run**
```bash
cargo run --release --example deepbook_alt_parquet --features parquet-output,alt-framework -- \
  --start 239600000 \
  --end 239600050 \
  --output ./deepbook_events.parquet
```
