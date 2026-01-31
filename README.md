# Walrus Checkpoint Indexing

[![Official CLI](https://img.shields.io/badge/Official%20CLI-mainnet--v1.40.3_(pinned)-blue)](https://github.com/MystenLabs/walrus/releases/tag/mainnet-v1.40.3) [![Forked CLI](https://img.shields.io/badge/Forked%20CLI-v1.40_(pinned)-orange)](https://github.com/Evan-Kim2028/walrus-cli-streaming)

Index Sui checkpoints from Walrus decentralized storage with high performance.

> **Note:** This library is tested against the official Walrus CLI **mainnet-v1.40.3**. The forked CLI is based on v1.40. Newer versions may work but are not guaranteed to be compatible.

## Overview

This library provides efficient streaming of Sui checkpoint data stored on Walrus. It supports multiple fetch strategies:

- **Full blob download**: Download entire blobs (2-3 GB each) for maximum throughput
- **Byte-range streaming** *(Experimental - Forked CLI)*: Stream specific byte ranges using the forked Walrus CLI
- **Adaptive fetching**: Automatically choose strategy based on network health

## Project Goals

This is a public-good repository focused on:

- **Walrus as an archive backend**: Use Walrus storage as the durable source of truth for Sui checkpoints.
- **Multiple Walrus sources**: Show how to read checkpoints via the archival service, HTTP aggregator, or Walrus CLI.
- **Custom indexing**: Provide small, composable examples for event filtering and domain-specific indexing.

If you want the mental model, start with:

- `docs/ARCHIVE_NODE.md` for building a local archive node backed by Walrus
- `docs/CUSTOM_INDEXING.md` for indexing patterns and example output shapes

## CLI Compatibility

This library works with **two versions** of the Walrus CLI:

| Feature | Official CLI | Forked CLI |
|---------|-------------|------------|
| Full blob download | ✅ Yes | ✅ Yes |
| Node health tracking | ✅ Yes | ✅ Yes |
| Sliver prediction | ✅ Yes | ✅ Yes |
| **Byte-range streaming** | ❌ No | ⚠️ Experimental |
| **Size-only queries** | ❌ No | ⚠️ Experimental |
| **Zero-copy streaming** | ❌ No | ⚠️ Experimental |

### Official Walrus CLI

The standard [MystenLabs/walrus](https://github.com/MystenLabs/walrus) binary. Use this for:
- Sequential checkpoint processing
- Full blob downloads (more reliable when network has down nodes)
- Production environments (no custom fork needed)

### Forked Walrus CLI (walrus-cli-streaming) - Experimental

> ⚠️ **Experimental**: The forked CLI features are experimental and may change or be deprecated. Use the official CLI for production workloads.

The [walrus-cli-streaming](https://github.com/Evan-Kim2028/walrus-cli-streaming) fork adds experimental features:
- `--start-byte <N>` - Starting byte position *(Experimental)*
- `--byte-length <N>` - Number of bytes to read *(Experimental)*
- `--size-only` - Get blob size without downloading *(Experimental)*
- `--stream` - Zero-copy streaming to stdout *(Experimental)*

Use this for:
- Random access to specific checkpoints
- Minimizing bandwidth when fetching sparse checkpoints
- Memory-efficient streaming

## Features

- **High Performance**: 10-18 checkpoints/sec on healthy networks
- **Node Health Tracking**: Monitor Walrus storage node health for diagnostics
- **Sliver Prediction**: Understand which byte ranges may be slow due to down nodes
- **Resumable Downloads**: State tracking for reliable backfills
- **Configurable Concurrency**: Tune parallelism for your use case

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
walrus-checkpoint-indexing = { git = "https://github.com/Evan-Kim2028/walrus-checkpoint-indexing" }
```

### Basic Usage (Official CLI)

```rust
use walrus_checkpoint_indexing::{Config, WalrusStorage, CliCapabilities};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Using official CLI (default) - full blob download mode
    let config = Config::builder()
        .walrus_cli_path("/path/to/walrus")
        .cli_capabilities(CliCapabilities::Official)  // default
        .build()?;

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

### Byte-Range Streaming (Forked CLI) - Experimental

> ⚠️ **Experimental - Forked CLI Feature**: This functionality requires the [walrus-cli-streaming](https://github.com/Evan-Kim2028/walrus-cli-streaming) fork and is experimental. The API and behavior may change.

```rust
use walrus_checkpoint_indexing::{Config, WalrusStorage, CliCapabilities, FetchStrategy};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Using forked CLI with byte-range streaming (EXPERIMENTAL)
    let config = Config::builder()
        .walrus_cli_path("/path/to/walrus-fork")
        .cli_capabilities(CliCapabilities::Forked)  // REQUIRED for byte-range (experimental)
        .fetch_strategy(FetchStrategy::ByteRangeStream)  // Experimental
        .build()?;

    let storage = WalrusStorage::new(config).await?;
    storage.initialize().await?;

    // Now uses efficient byte-range streaming instead of full blob download
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
./target/release/walrus-checkpoint-index info

# Stream checkpoints in a range
./target/release/walrus-checkpoint-index stream --start 239000000 --end 239001000

# Get a single checkpoint
./target/release/walrus-checkpoint-index get --checkpoint 239000000

# Check node health
./target/release/walrus-checkpoint-index health
```

### Documentation

- `docs/ARCHIVE_NODE.md`: Build a local archive node using Walrus as the backend
- `docs/CUSTOM_INDEXING.md`: Custom indexing patterns and output shapes

### Quick Example

```bash
# All events -> Parquet
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 --end 239600050 --output ./checkpoint_events.parquet

# Filter by package (e.g., DeepBook)
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 --end 239600050 --output ./deepbook_events.parquet \
  --package 0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809
```

## Examples

### DeepBook Event Extraction (HTTP Aggregator)

The `deepbook_events_aggregator` example demonstrates extracting events using the **HTTP Aggregator**:

```text
┌─────────────┐    HTTP Range     ┌──────────────────┐    Walrus     ┌─────────────────┐
│  This Code  │ ───────────────▶  │  HTTP Aggregator │ ────────────▶ │  Storage Nodes  │
│             │   Requests        │  (walrus.space)  │   Protocol    │  (RedStuff)     │
└─────────────┘                   └──────────────────┘               └─────────────────┘
```

**Trade-offs:**
- ✅ No CLI installation needed
- ✅ Works anywhere with HTTP access
- ❌ Slower than direct CLI access (extra network hop)
- ❌ Dependent on aggregator availability

```bash
# Run with HTTP aggregator (no CLI needed)
cargo run --example deepbook_events_aggregator

# Custom package and checkpoint range
cargo run --example deepbook_events_aggregator -- \
  --package 0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809 \
  --start 239600000 \
  --end 239600050 \
  --verbose
```

This demonstrates the API pattern:

```
Input:  blob_id + package_id
Output: Vec<Event> with type breakdown and performance metrics
```

See `examples/deepbook_events_aggregator.rs` for the full implementation.

### DeepBook Event Extraction (Walrus CLI)

The `deepbook_events_cli` example demonstrates extracting events using the **Walrus CLI** with full blob download:

```text
┌─────────────┐    walrus read    ┌─────────────────┐
│  This Code  │ ────────────────▶ │  Storage Nodes  │
│             │   (RedStuff)      │  (1000 shards)  │
└─────────────┘                   └─────────────────┘
       │
       ▼
┌─────────────┐
│ Local Blob  │  (~3.2 GB cached)
│   Cache     │
└─────────────┘
```

**Trade-offs:**
- ✅ Faster and more reliable than HTTP aggregator
- ✅ Full blob cached locally for repeated access
- ✅ No dependency on aggregator availability
- ❌ Requires Walrus CLI installation
- ❌ Initial download is large (~3.2 GB per blob)

```bash
# Requires Walrus CLI
cargo run --example deepbook_events_cli -- --walrus-cli-path /path/to/walrus

# With environment variable
export WALRUS_CLI_PATH=/path/to/walrus
cargo run --example deepbook_events_cli

# Custom package and checkpoint range
cargo run --example deepbook_events_cli -- \
  --walrus-cli-path /path/to/walrus \
  --start 238954764 \
  --end 238954864 \
  --verbose
```

See `examples/deepbook_events_cli.rs` for the full implementation.

### Alt Checkpoint Ingest -> Parquet (All Events)

The `alt_checkpoint_parquet` example shows a **lightweight end-to-end flow** that ingests **all events**:

1. Stream checkpoints from Walrus
2. Spool them locally in Sui's `.chk` format
3. Use the **Sui indexer alt framework** to process checkpoints from the local path
4. Write filtered events to Parquet (DuckDB/Polars friendly)

```bash
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 \
  --end 239600050 \
  --output ./checkpoint_events.parquet

# Filter by package (e.g., DeepBook)
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 \
  --end 239600050 \
  --package 0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809

# Reuse spooled checkpoints for faster iteration
cargo run --release --example alt_checkpoint_parquet -- \
  --start 239600000 \
  --end 239600050 \
  --spool-mode cache \
  --spool-dir ./checkpoint-spool

# Optional DuckDB summary (tables + row count)
cargo run --release --example alt_checkpoint_parquet --features duckdb -- \
  --start 239600000 \
  --end 239600050 \
  --duckdb-summary
```

See `examples/alt_checkpoint_parquet.rs` for the full implementation.

### Output Schema

The parquet output contains these columns:

| Column | Type | Description |
|--------|------|-------------|
| `checkpoint_num` | u64 | Checkpoint sequence number |
| `timestamp_ms` | u64 | Checkpoint timestamp (milliseconds) |
| `tx_digest` | string | Transaction digest |
| `event_index` | u32 | Event index within transaction |
| `package_id` | string | Move package ID |
| `module` | string | Move module name |
| `event_type` | string | Event type name |
| `sender` | string | Transaction sender |
| `event_json` | string | Event metadata as JSON |
| `bcs_data` | binary | Raw BCS-encoded event data |

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `WALRUS_CLI_CAPABILITIES` | CLI type: "official" or "forked" *(forked is experimental)* | `official` |
| `WALRUS_ARCHIVAL_URL` | Archival service URL | `https://walrus-sui-archival.mainnet.walrus.space` |
| `WALRUS_AGGREGATOR_URL` | Aggregator URL | `https://aggregator.walrus-mainnet.walrus.space` |

### CLI Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--archival-url` | Archival service URL | env or default |
| `--aggregator-url` | Aggregator URL | env or default |
| `--walrus-cli-path` | Path to Walrus CLI binary | none (uses HTTP) |
| `--walrus-cli-context` | Walrus CLI context | `mainnet` |
| `--cli-capabilities` | CLI type: "official" or "forked" *(forked is experimental)* | `official` |
| `--fetch-strategy` | Strategy: "full-blob", "byte-range" *(experimental)*, "adaptive" | `full-blob` |
| `--cli-timeout-secs` | CLI command timeout | 180 |
| `--range-concurrency` | Concurrent range requests | 32 |
| `--blob-concurrency` | Concurrent blob processing | 4 |
| `--cache-enabled` | Enable local blob cache | false |
| `--cache-dir` | Cache directory | `.walrus-cache` |

**Note**: Using `--fetch-strategy=byte-range` requires `--cli-capabilities=forked`. Both are experimental features from the forked Walrus CLI.

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

## Forked Walrus CLI - Experimental Features

> ⚠️ **Experimental**: All features from the forked Walrus CLI are experimental and may change or be deprecated without notice. For production use, prefer the official CLI with full blob download mode.

For byte-range streaming, this library uses a forked Walrus CLI with additional experimental features:

- `--start-byte <N>`: Starting byte position *(Experimental - Forked CLI)*
- `--byte-length <N>`: Number of bytes to read *(Experimental - Forked CLI)*
- `--size-only`: Get blob size without downloading *(Experimental - Forked CLI)*
- `--stream`: Zero-copy streaming to stdout *(Experimental - Forked CLI)*

See: [walrus-cli-streaming](https://github.com/Evan-Kim2028/walrus-cli-streaming) (Forked CLI)

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
./target/release/walrus-checkpoint-index health

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
- [walrus-cli-streaming](https://github.com/Evan-Kim2028/walrus-cli-streaming) - Forked Walrus CLI with byte-range support *(Experimental)*
