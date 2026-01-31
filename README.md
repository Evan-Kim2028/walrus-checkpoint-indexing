# Walrus Checkpoint Indexing

Index Sui blockchain checkpoints from Walrus decentralized storage into queryable Parquet files.

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Sui](https://img.shields.io/badge/Sui-mainnet--v1.64.2-4DA2FF)](https://github.com/MystenLabs/sui)

## Why This Project?

Sui checkpoint data is archived on [Walrus](https://walrus.xyz) decentralized storage. This library provides efficient tools to:

- **Stream checkpoints** from Walrus at high throughput
- **Export to Parquet** for analysis with DuckDB, Polars, or Spark
- **Match official schemas** compatible with `sui-indexer-alt`

**Performance:** 132k checkpoints in 11 minutes, 24.5 GB → 1.84 GB Parquet (13x compression)

## Quick Start

### 1. Install

```bash
git clone https://github.com/Evan-Kim2028/walrus-checkpoint-indexing
cd walrus-checkpoint-indexing
```

### 2. Run the Indexer

```bash
# Index 10,000 checkpoints to Parquet
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239000000 \
  --end 239010000 \
  --output-dir ./parquet_output

# With parallel blob downloads (recommended for large ranges)
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239000000 \
  --end 239100000 \
  --output-dir ./parquet_output \
  --parallel-prefetch \
  --prefetch-concurrency 4
```

### 3. Query with DuckDB

```bash
# Install DuckDB: brew install duckdb (macOS) or download from duckdb.org

duckdb -c "
  SELECT sender, COUNT(*) as tx_count
  FROM read_parquet('./parquet_output/transactions.parquet')
  GROUP BY sender ORDER BY tx_count DESC LIMIT 10
"
```

## Features

### High-Performance Indexing
- **Parallel blob prefetching** - Download multiple blobs concurrently (2x faster downloads)
- **Streaming architecture** - Process checkpoints as they download
- **Local caching** - Reuse downloaded blobs for iterative analysis

### 10 Output Tables
| Table | Description |
|-------|-------------|
| `transactions` | Transaction metadata, gas usage, status |
| `events` | Emitted events with BCS data |
| `move_calls` | Function calls by package/module |
| `objects` | Object changes (created, mutated, deleted) |
| `checkpoints` | Checkpoint metadata and timestamps |
| `balance_changes` | Coin balance deltas per owner |
| `input_objects` | Pre-transaction object state |
| `output_objects` | Post-transaction object state |
| `packages` | Published packages |
| `execution_errors` | Failed transaction details |

### Flexible Data Access
- **HTTP Aggregator** - No CLI needed, works anywhere
- **Walrus CLI** - Direct access, faster and more reliable
- **Caching modes** - Ephemeral (auto-cleanup) or persistent

## Examples

### Export All Events to Parquet

```bash
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 \
  --end 239600500 \
  --output-dir ./parquet_output
```

### Filter by Package (e.g., DeepBook)

```bash
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 \
  --end 239600500 \
  --output-dir ./deepbook_output \
  --package 0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809
```

### With DuckDB Summary

```bash
cargo run --release --example alt_checkpoint_parquet --features duckdb -- \
  --start 239600000 \
  --end 239600500 \
  --output-dir ./parquet_output \
  --duckdb-summary
```

### Reuse Cached Checkpoints

```bash
# First run: download and cache
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 --end 239600500 \
  --cache-enabled \
  --spool-mode cache \
  --spool-dir ./checkpoint-spool

# Subsequent runs: instant (no re-download)
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 --end 239600500 \
  --spool-mode cache \
  --spool-dir ./checkpoint-spool
```

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--start` | Starting checkpoint (inclusive) | Required |
| `--end` | Ending checkpoint (exclusive) | Required |
| `--output-dir` | Parquet output directory | `./parquet_output` |
| `--parallel-prefetch` | Enable parallel blob downloads | false |
| `--prefetch-concurrency` | Number of concurrent downloads | 4 |
| `--cache-enabled` | Cache blobs locally (~3 GB each) | false |
| `--spool-mode` | `ephemeral` or `cache` | `ephemeral` |
| `--spool-dir` | Directory for checkpoint spool | temp dir |
| `--package` | Filter events by package ID | none |
| `--duckdb-summary` | Print table stats after indexing | false |

## Example Queries

**Top active addresses:**
```sql
SELECT sender, COUNT(*) as txs
FROM read_parquet('./parquet_output/transactions.parquet')
GROUP BY sender ORDER BY txs DESC LIMIT 10;
```

**Most called functions:**
```sql
SELECT module, function, COUNT(*) as calls
FROM read_parquet('./parquet_output/move_calls.parquet')
GROUP BY module, function ORDER BY calls DESC LIMIT 10;
```

**Token transfer volume:**
```sql
SELECT
  SPLIT_PART(coin_type, '::', 3) as token,
  COUNT(*) as transfers
FROM read_parquet('./parquet_output/balance_changes.parquet')
GROUP BY token ORDER BY transfers DESC LIMIT 10;
```

**Detect swaps (multi-token transactions):**
```sql
WITH swaps AS (
  SELECT tx_digest FROM read_parquet('./parquet_output/balance_changes.parquet')
  GROUP BY tx_digest HAVING COUNT(DISTINCT coin_type) >= 2
)
SELECT COUNT(*) as swap_count FROM swaps;
```

## Performance

See [INDEXER_REPORT.md](INDEXER_REPORT.md) for detailed benchmarks.

| Metric | Sequential | Parallel (4x) |
|--------|-----------|---------------|
| Total Runtime | 18m 47s | 11m 42s |
| Download Speed | 27 MB/s | 53 MB/s |
| Throughput | ~1,880 checkpoints/sec |

**Data sizes:** 24.5 GB blobs → 1.84 GB Parquet (13x compression)

## Library Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
walrus-checkpoint-indexing = { git = "https://github.com/Evan-Kim2028/walrus-checkpoint-indexing" }
```

```rust
use walrus_checkpoint_indexing::{Config, WalrusStorage};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::builder()
        .walrus_cli_path("/path/to/walrus")  // optional
        .build()?;

    let storage = WalrusStorage::new(config).await?;
    storage.initialize().await?;

    // Get a single checkpoint
    let checkpoint = storage.get_checkpoint(239000000).await?;
    println!("Transactions: {}", checkpoint.transactions.len());

    // Stream a range
    storage.stream_checkpoints(239000000..239001000, |cp| async move {
        println!("Checkpoint {}", cp.checkpoint_summary.sequence_number);
        Ok(())
    }).await?;

    Ok(())
}
```

## Architecture

```
Walrus Storage → Blob Download → Checkpoint Extraction → Sui Alt Framework → Parquet Writers
                 (parallel)      (BCS decode)            (pipeline)          (Arrow)
```

**Blob format:** Each blob is ~3 GB containing ~9,000 checkpoints with a self-indexing footer.

## Documentation

- [INDEXER_REPORT.md](INDEXER_REPORT.md) - Detailed benchmarks and test results
- [docs/ARCHIVE_NODE.md](docs/ARCHIVE_NODE.md) - Building a local archive node
- [docs/CUSTOM_INDEXING.md](docs/CUSTOM_INDEXING.md) - Custom indexing patterns

## Related Projects

- [deepbookv3-walrus-streaming](https://github.com/Evan-Kim2028/deepbookv3-walrus-streaming) - DeepBook indexer using this library
- [sui-indexer-alt](https://github.com/MystenLabs/sui/tree/main/crates/sui-indexer-alt) - Official Sui indexer reference

## License

Apache-2.0
