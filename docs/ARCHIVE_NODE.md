**Archive Node (Walrus-Backed)**
- Goal: use Walrus as the durable backend and build a local archive view without running a full Sui node.
- Sources: HTTP archival/aggregator endpoints or the Walrus CLI (official or forked).

**Data Flow**
```
Walrus (archival/aggregator/CLI)
        │
        ▼
Checkpoint blobs (BCS + index)
        │
        ▼
Local spool (.chk files, optional)
        │
        ▼
Indexer (custom or Sui alt framework)
        │
        ▼
Parquet / DuckDB / downstream analytics
```

**Why a Local Spool**
- `.chk` files are the exact Sui checkpoint format expected by the Sui indexer alt framework.
- Spooling lets you iterate locally without re-downloading blobs each run.
- You can keep it ephemeral (temp dir) or cached (repeatable runs).

**Minimal Archive Workflow**
1) Fetch checkpoints from Walrus
2) Spool to local `.chk`
3) Process locally (alt framework or custom)
4) Persist to Parquet or your own sink

**Example: Full-range Spool + Parquet**
```bash
cargo run --release --example alt_checkpoint_parquet --features parquet-output,alt-framework -- \
  --start 239600000 \
  --end 239600050 \
  --spool-mode cache \
  --spool-dir ./checkpoint-spool \
  --output ./checkpoint_events.parquet
```

**Repeatable Runs**
- Cache Walrus blobs: `--cache-enabled --cache-dir ./.walrus-cache`
- Reuse spool: `--spool-mode cache --spool-dir ./checkpoint-spool`

**Validation (Quick)**
```bash
cargo run --release --example alt_checkpoint_parquet --features parquet-output,alt-framework,duckdb -- \
  --start 239600000 \
  --end 239600050 \
  --duckdb-summary
```

**Notes**
- This repo intentionally keeps the architecture light: spool → index → parquet.
- You can swap the sink with your own database, but Parquet keeps examples portable.
