# Walrus Checkpoint Indexing

Index Sui blockchain checkpoints from [Walrus](https://walrus.xyz) decentralized storage into queryable Parquet files.

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Sui](https://img.shields.io/badge/Sui-mainnet--v1.64.2-4DA2FF)](https://github.com/MystenLabs/sui/releases/tag/mainnet-v1.64.2)

## Why This Project?

Sui checkpoint data is archived on Walrus decentralized storage. This project bridges that data with the official [sui-indexer-alt-framework](https://github.com/MystenLabs/sui/tree/main/crates/sui-indexer-alt-framework), enabling you to:

- **Stream checkpoints** from Walrus at high throughput
- **Export to Parquet** for analysis with DuckDB, Polars, or Spark
- **Match official schemas** compatible with [sui-indexer-alt](https://github.com/MystenLabs/sui/tree/main/crates/sui-indexer-alt)

**Performance:** 132k checkpoints in 11 minutes, 24.5 GB → 1.84 GB Parquet (13x compression)

## How It Works

```
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────────────┐     ┌─────────────┐
│  Walrus Storage │ ──▶ │  walrus-checkpoint-  │ ──▶ │  sui-indexer-alt-       │ ──▶ │   Parquet   │
│  (checkpoint    │     │  indexing            │     │  framework              │     │   Files     │
│   blobs)        │     │  (download + spool)  │     │  (process checkpoints)  │     │             │
└─────────────────┘     └──────────────────────┘     └─────────────────────────┘     └─────────────┘
```

This library:
1. Downloads checkpoint blobs from the [Walrus archival service](https://walrus-sui-archival.wal.app/)
2. Extracts and spools checkpoints in `.chk` format (same as Sui full nodes)
3. Feeds them to [sui-indexer-alt-framework](https://github.com/MystenLabs/sui/tree/main/crates/sui-indexer-alt-framework) for processing
4. Outputs 10 Parquet tables matching the official indexer schemas

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

# With parallel downloads and progress bars (recommended)
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239000000 \
  --end 239100000 \
  --output-dir ./parquet_output \
  --parallel-prefetch \
  --prefetch-concurrency 4 \
  --show-progress
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
- **Parallel blob prefetching** - Download multiple blobs concurrently (2x faster)
- **Progress bars** - Real-time download progress for each blob (`--show-progress`)
- **Streaming architecture** - Process checkpoints as they download
- **Local caching** - Reuse downloaded blobs for iterative analysis

### 10 Output Tables

Output schemas match the official [sui-indexer-alt](https://github.com/MystenLabs/sui/tree/main/crates/sui-indexer-alt) implementation:

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

### Data Access Methods
- **Walrus CLI (recommended)** - Direct access via official Walrus CLI, faster and more reliable
- **HTTP Aggregator** - Fallback when CLI is not available
- **Caching** - Downloaded blobs are cached locally for repeated access

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

### With Progress Bars and DuckDB Summary

```bash
cargo run --release --example alt_checkpoint_parquet --features duckdb -- \
  --start 239600000 \
  --end 239600500 \
  --output-dir ./parquet_output \
  --parallel-prefetch \
  --show-progress \
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
| `--show-progress` | Show download progress bars | false |
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
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           walrus-checkpoint-indexing                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐    ┌──────────┐ │
│  │   Walrus    │    │    Blob     │    │  sui-indexer-alt-   │    │  Parquet │ │
│  │  Storage    │───▶│  Download   │───▶│  framework          │───▶│  Writers │ │
│  │             │    │  (parallel) │    │  (from MystenLabs)  │    │  (Arrow) │ │
│  └─────────────┘    └─────────────┘    └─────────────────────┘    └──────────┘ │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Blob format:** Each blob is ~3 GB containing ~9,000 checkpoints with a self-indexing footer.

## Key Dependencies

| Crate | Purpose | Source |
|-------|---------|--------|
| [sui-indexer-alt-framework](https://github.com/MystenLabs/sui/tree/main/crates/sui-indexer-alt-framework) | Checkpoint processing pipeline | MystenLabs/sui |
| [sui-types](https://github.com/MystenLabs/sui/tree/main/crates/sui-types) | Sui type definitions | MystenLabs/sui |
| [sui-storage](https://github.com/MystenLabs/sui/tree/main/crates/sui-storage) | Checkpoint file format | MystenLabs/sui |

## Documentation

- [INDEXER_REPORT.md](INDEXER_REPORT.md) - Detailed benchmarks and test results
- [docs/ARCHIVE_NODE.md](docs/ARCHIVE_NODE.md) - Building a local archive node
- [docs/CUSTOM_INDEXING.md](docs/CUSTOM_INDEXING.md) - Custom indexing patterns

## Related Projects

- [Walrus](https://walrus.xyz) - Decentralized storage for Sui
- [sui-indexer-alt](https://github.com/MystenLabs/sui/tree/main/crates/sui-indexer-alt) - Official Sui indexer
- [deepbookv3-walrus-streaming](https://github.com/Evan-Kim2028/deepbookv3-walrus-streaming) - DeepBook indexer using this library

## License

Apache-2.0
