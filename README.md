# Walrus Checkpoint Streaming

Stream Sui checkpoints from Walrus decentralized storage with high performance.

## Overview

This library provides efficient streaming of Sui checkpoint data stored on Walrus. It supports multiple fetch strategies:

- **Full blob download**: Download entire blobs (2-3 GB each) for maximum throughput
- **Byte-range streaming**: Stream specific byte ranges using the forked Walrus CLI
- **Adaptive fetching**: Automatically choose strategy based on network health

## Features

- **High Performance**: 10-18 checkpoints/sec on healthy networks (8x faster than Sui bucket)
- **Node Health Tracking**: Monitor Walrus storage node health for diagnostics
- **Sliver Prediction**: Understand which byte ranges may be slow due to down nodes
- **Resumable Downloads**: State tracking for reliable backfills
- **Configurable Concurrency**: Tune parallelism for your use case

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
walrus-checkpoint-streaming = { git = "https://github.com/Evan-Kim2028/walrus-checkpoint-streaming" }
```

### Basic Usage

```rust
use walrus_checkpoint_streaming::{Config, WalrusStorage};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create storage with default config
    let config = Config::default();
    let storage = WalrusStorage::new(config).await?;
    storage.initialize().await?;

    // Get a single checkpoint
    let checkpoint = storage.get_checkpoint(239000000).await?;
    println!("Checkpoint {}: {} transactions",
        checkpoint.checkpoint_summary.sequence_number,
        checkpoint.transactions.len());

    // Stream a range of checkpoints
    storage.stream_checkpoints(239000000..239001000, |cp| async move {
        println!("Processing checkpoint {}", cp.checkpoint_summary.sequence_number);
        Ok(())
    }).await?;

    Ok(())
}
```

### CLI Usage

```bash
# Build the CLI
cargo build --release

# Show available checkpoints
./target/release/walrus-checkpoint-stream info

# Stream checkpoints in a range
./target/release/walrus-checkpoint-stream stream --start 239000000 --end 239001000

# Get a single checkpoint
./target/release/walrus-checkpoint-stream get --checkpoint 239000000

# Check node health
./target/release/walrus-checkpoint-stream health
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `WALRUS_ARCHIVAL_URL` | Archival service URL | `https://walrus-sui-archival.mainnet.walrus.space` |
| `WALRUS_AGGREGATOR_URL` | Aggregator URL | `https://aggregator.walrus-mainnet.walrus.space` |

### CLI Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--archival-url` | Archival service URL | env or default |
| `--aggregator-url` | Aggregator URL | env or default |
| `--walrus-cli-path` | Path to forked Walrus CLI | none (uses HTTP) |
| `--walrus-cli-context` | Walrus CLI context | `mainnet` |
| `--cli-timeout-secs` | CLI command timeout | 180 |
| `--range-concurrency` | Concurrent range requests | 96 |
| `--blob-concurrency` | Concurrent blob processing | 4 |
| `--cache-enabled` | Enable local blob cache | false |
| `--cache-dir` | Cache directory | `.walrus-cache` |

## Architecture

### Blob Format

Sui checkpoint data is stored on Walrus as self-indexed blobs:

```
+------------------------------------------+
|            Checkpoint Data               |
|  (BCS-encoded, ~350KB each, ~9000/blob)  |
+------------------------------------------+
|            Index Section                 |
|  (checkpoint_num -> offset, length)      |
+------------------------------------------+
|              Footer (24B)                |
|  magic | version | index_offset | count  |
+------------------------------------------+
```

Each blob is approximately 3.2 GB containing ~9,200 checkpoints.

### Walrus Erasure Coding

Walrus uses RedStuff erasure coding:

- **1000 shards** across ~100 storage nodes
- **334 primary slivers** + **667 secondary slivers**
- Only need ~1/3 of slivers to reconstruct data
- Per-blob rotation ensures fault tolerance

### Sliver Prediction

The library predicts which byte ranges may be slow based on node health:

```
Byte Range -> Primary Sliver -> Shard -> Node -> Health Status

If node is DOWN/DEGRADED:
  - Range is classified as "risky"
  - May timeout and require retries
  - Health data helps debug slow ranges
```

Risk levels:
- **Safe**: No problematic shards, use aggressive concurrency
- **Low**: 1-2 problematic shards, may timeout
- **High**: 3-5 problematic shards, high timeout chance
- **Critical**: 6+ problematic shards, likely to fail

## Forked Walrus CLI

For byte-range streaming, this library uses a forked Walrus CLI with additional features:

- `--start-byte <N>`: Starting byte position
- `--byte-length <N>`: Number of bytes to read
- `--size-only`: Get blob size without downloading
- `--stream`: Zero-copy streaming to stdout

See: [walrus-cli-streaming](https://github.com/Evan-Kim2028/walrus-cli-streaming)

## Performance

### Healthy Network (< 5% problematic shards)

| Metric | Value |
|--------|-------|
| Throughput | 10-18 checkpoints/sec |
| Bandwidth | 1-2.5 MB/s |
| Latency | Low, minimal timeouts |

### Degraded Network (7-11% problematic shards)

| Metric | Value |
|--------|-------|
| Throughput | 3-6 checkpoints/sec |
| Bandwidth | 0.3-0.7 MB/s |
| Latency | Highly variable, frequent timeouts |

## Troubleshooting

### Check Node Health

```bash
# Via CLI
./target/release/walrus-checkpoint-stream health

# Via Walrus CLI directly
walrus health --committee --context mainnet --json
```

### Common Issues

1. **Timeouts on specific ranges**: Some byte ranges hit down nodes. The library will retry and split ranges automatically.

2. **Slow downloads**: Check node health. If many nodes are down, consider:
   - Using cache mode (`--cache-enabled`) for more reliable full-blob downloads
   - Waiting for nodes to recover
   - Reducing concurrency to avoid overwhelming healthy nodes

3. **Index parsing errors**: Blob format may have changed. Check archival service for updated metadata.

## Contributing

Contributions welcome! Please open issues for bugs or feature requests.

## License

Apache-2.0

## Related Projects

- [deepbookv3-walrus-streaming](https://github.com/Evan-Kim2028/deepbookv3-walrus-streaming) - DeepBook indexer using this library
- [walrus-cli-streaming](https://github.com/Evan-Kim2028/walrus-cli-streaming) - Forked Walrus CLI with byte-range support
