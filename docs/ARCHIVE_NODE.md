# Archive Node (Walrus-Backed)

Use Walrus decentralized storage as a durable backend for Sui checkpoint data without running a full Sui node.

## Data Flow

```
Walrus Storage (archival/aggregator)
        │
        ▼
Checkpoint blobs (BCS + index)
        │
        ▼
Local spool (.chk files)
        │
        ▼
Sui Alt Framework (indexer)
        │
        ▼
Parquet / DuckDB / Analytics
```

## Why Local Spooling?

- `.chk` files match the Sui checkpoint format expected by the indexer framework
- Spooling enables iteration without re-downloading blobs each run
- Choose ephemeral (temp dir, auto-cleanup) or cached (persistent) mode

## Quick Start

### Ephemeral Mode (Default)

Checkpoints stored in temp directory, cleaned up after run:

```bash
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 \
  --end 239600500 \
  --output-dir ./parquet_output
```

### Cached Mode (Reusable)

Checkpoints persisted for faster re-runs:

```bash
# First run: download and spool
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 \
  --end 239600500 \
  --spool-mode cache \
  --spool-dir ./checkpoint-spool \
  --cache-enabled \
  --output-dir ./parquet_output

# Subsequent runs: instant (no re-download)
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 \
  --end 239600500 \
  --spool-mode cache \
  --spool-dir ./checkpoint-spool \
  --output-dir ./parquet_output
```

## Caching Options

| Flag | Description | Disk Usage |
|------|-------------|------------|
| `--cache-enabled` | Cache Walrus blobs | ~3 GB per blob |
| `--spool-mode cache` | Persist checkpoint files | ~2 KB per checkpoint |
| `--spool-dir <path>` | Custom spool location | - |

### Storage Estimates

| Checkpoints | Blobs | Blob Cache | Spool Size |
|-------------|-------|------------|------------|
| 10,000 | 1-2 | ~6 GB | ~20 MB |
| 100,000 | ~11 | ~33 GB | ~200 MB |
| 1,000,000 | ~110 | ~330 GB | ~2 GB |

## Validation

Verify output with DuckDB summary:

```bash
cargo run --release --example alt_checkpoint_parquet --features duckdb -- \
  --start 239600000 \
  --end 239600500 \
  --output-dir ./parquet_output \
  --duckdb-summary
```

## Large-Scale Indexing

For 100k+ checkpoints, enable parallel blob prefetching:

```bash
cargo run --release --example alt_checkpoint_parquet -- \
  --start 238954764 \
  --end 239086998 \
  --output-dir ./parquet_output \
  --cache-enabled \
  --parallel-prefetch \
  --prefetch-concurrency 4
```

This downloads multiple blobs concurrently (2x faster than sequential).

## Architecture Notes

- **Light design**: spool → index → parquet
- **Portable output**: Parquet works with DuckDB, Polars, Spark
- **Extensible**: Swap the sink for your own database if needed
