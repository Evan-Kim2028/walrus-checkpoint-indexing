//! Example: Extract DeepBook v3 events using the Forked Walrus CLI with byte-range streaming.
//!
//! ## Data Flow
//!
//! This example uses the **Forked Walrus CLI** with byte-range streaming to parallelize reads:
//!
//! ```text
//!                              ┌─────────────────┐
//!                         ┌───▶│  Storage Nodes  │
//!                         │    └─────────────────┘
//! ┌─────────────┐         │    ┌─────────────────┐
//! │  This Code  │ ────────┼───▶│  Storage Nodes  │  (parallel byte-range requests)
//! │  (parallel) │         │    └─────────────────┘
//! └─────────────┘         │    ┌─────────────────┐
//!                         └───▶│  Storage Nodes  │
//!                              └─────────────────┘
//! ```
//!
//! The forked Walrus CLI adds these flags for byte-range streaming:
//! - `--start-byte <N>` - Starting byte position
//! - `--byte-length <N>` - Number of bytes to read
//! - `--stream` - Stream raw bytes to stdout (zero-copy)
//!
//! **Trade-offs:**
//! - ✅ Parallel fetching of checkpoint ranges
//! - ✅ No need to download entire blob upfront
//! - ✅ Lower initial latency for sparse access patterns
//! - ❌ Requires forked CLI installation
//! - ❌ More network overhead for sequential access
//! - ❌ May be slower than cached full blob for full scans
//!
//! ## Prerequisites
//!
//! 1. Build the forked Walrus CLI from: https://github.com/Evan-Kim2028/walrus-cli-streaming
//! 2. The fork must support `--start-byte`, `--byte-length`, and `--stream` flags
//!
//! ## Usage
//!
//! ```bash
//! # Basic usage
//! cargo run --example deepbook_events_forked_cli -- --walrus-cli-path /path/to/walrus-fork
//!
//! # With custom concurrency
//! cargo run --example deepbook_events_forked_cli -- \
//!   --walrus-cli-path /path/to/walrus-fork \
//!   --concurrency 64
//!
//! # Limit checkpoints for testing
//! cargo run --example deepbook_events_forked_cli -- \
//!   --walrus-cli-path /path/to/walrus-fork \
//!   --max-checkpoints 1000
//! ```

use anyhow::{Context, Result};
use clap::Parser;
use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use sui_storage::blob::Blob;
use sui_types::base_types::ObjectID;
use sui_types::full_checkpoint_content::CheckpointData;
use tokio::process::Command;
use tokio::sync::Semaphore;

/// DeepBook v3 package address on Sui mainnet
const DEEPBOOK_PACKAGE: &str = "0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809";

/// A sample blob ID for testing (same blob as other examples for fair comparison)
const SAMPLE_BLOB_ID: &str = "lJIYRNvG_cgmwx_OT8t7gvdr3rTqG_NsSQPF1714qu0";

/// Known checkpoint range for the sample blob
const SAMPLE_BLOB_START: u64 = 238954764;
const SAMPLE_BLOB_END: u64 = 238966916;

/// Blob index footer magic bytes ("DBLW" in ASCII, little-endian)
const BLOB_FOOTER_MAGIC: u32 = 0x574c4244;

#[derive(Parser, Debug)]
#[command(name = "deepbook-events-forked-cli")]
#[command(about = "Extract DeepBook v3 events using Forked Walrus CLI (byte-range streaming)")]
struct Args {
    /// Path to forked Walrus CLI binary (required)
    /// Must support --start-byte, --byte-length, --stream flags
    #[arg(long, env = "WALRUS_CLI_PATH")]
    walrus_cli_path: PathBuf,

    /// Blob ID to process (uses sample blob if not specified)
    #[arg(long)]
    blob_id: Option<String>,

    /// Starting checkpoint number (within the blob's range)
    #[arg(long)]
    start: Option<u64>,

    /// Ending checkpoint number (exclusive)
    #[arg(long)]
    end: Option<u64>,

    /// Package ID to filter events (defaults to DeepBook v3)
    #[arg(long, default_value = DEEPBOOK_PACKAGE)]
    package: String,

    /// Walrus CLI context (mainnet, testnet, etc.)
    #[arg(long, default_value = "mainnet")]
    context: String,

    /// Number of parallel byte-range requests
    #[arg(long, default_value = "32")]
    concurrency: usize,

    /// CLI timeout in seconds per request
    #[arg(long, default_value = "60")]
    timeout_secs: u64,

    /// Maximum checkpoints to process (default: all checkpoints in blob, ~12k)
    #[arg(long)]
    max_checkpoints: Option<u64>,

    /// Show individual events (verbose)
    #[arg(long, short)]
    verbose: bool,
}

/// Parsed index entry from a blob
#[derive(Debug, Clone)]
struct BlobIndexEntry {
    pub checkpoint_number: u64,
    pub offset: u64,
    pub length: u64,
}

/// Extracted event information
#[derive(Debug, Clone)]
struct ExtractedEvent {
    checkpoint: u64,
    tx_digest: String,
    event_type: String,
    module: String,
    #[allow(dead_code)]
    data_size: usize,
}

/// Performance metrics
#[derive(Debug, Default)]
struct Metrics {
    checkpoints_processed: AtomicU64,
    transactions_scanned: AtomicU64,
    total_events: AtomicU64,
    matching_events: AtomicU64,
    bytes_downloaded: AtomicU64,
}

/// Read a byte range from a blob using the forked CLI
async fn read_byte_range(
    cli_path: &PathBuf,
    blob_id: &str,
    context: &str,
    start: u64,
    length: u64,
    timeout_secs: u64,
) -> Result<Vec<u8>> {
    let mut cmd = Command::new(cli_path);
    cmd.arg("read")
        .arg(blob_id)
        .arg("--context")
        .arg(context)
        .arg("--start-byte")
        .arg(start.to_string())
        .arg("--byte-length")
        .arg(length.to_string())
        .arg("--stream")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let output = tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), cmd.output())
        .await
        .context("CLI timeout")?
        .context("Failed to execute CLI")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("CLI failed: {}", stderr.trim());
    }

    Ok(output.stdout)
}

/// Get blob size using the forked CLI --size-only flag
async fn get_blob_size(
    cli_path: &PathBuf,
    blob_id: &str,
    context: &str,
    timeout_secs: u64,
) -> Result<u64> {
    let mut cmd = Command::new(cli_path);
    cmd.arg("read")
        .arg(blob_id)
        .arg("--context")
        .arg(context)
        .arg("--size-only")
        .arg("--json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let output = tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), cmd.output())
        .await
        .context("CLI timeout")?
        .context("Failed to execute CLI")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("CLI failed: {}", stderr.trim());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Parse JSON response for size
    let json: serde_json::Value =
        serde_json::from_str(&stdout).context("Failed to parse size response")?;

    json["blobSize"]
        .as_u64()
        .or_else(|| json["size"].as_u64())
        .or_else(|| json["blob_size"].as_u64())
        .ok_or_else(|| anyhow::anyhow!("No size field in response: {}", stdout))
}

/// Parse the blob index from footer bytes
fn parse_blob_index_from_bytes(
    footer_bytes: &[u8],
    index_bytes: &[u8],
) -> Result<Vec<BlobIndexEntry>> {
    use byteorder::{LittleEndian, ReadBytesExt};
    use std::io::Cursor;

    let mut cursor = Cursor::new(footer_bytes);
    let magic = cursor.read_u32::<LittleEndian>()?;
    if magic != BLOB_FOOTER_MAGIC {
        anyhow::bail!(
            "Invalid blob footer magic: expected {:#x}, got {:#x}",
            BLOB_FOOTER_MAGIC,
            magic
        );
    }

    let _version = cursor.read_u32::<LittleEndian>()?;
    let _index_offset = cursor.read_u64::<LittleEndian>()?;
    let entry_count = cursor.read_u32::<LittleEndian>()?;

    // Parse index entries
    let mut cursor = Cursor::new(index_bytes);
    let mut entries = Vec::with_capacity(entry_count as usize);

    for _ in 0..entry_count {
        let name_len = cursor.read_u32::<LittleEndian>()?;
        let mut name_bytes = vec![0u8; name_len as usize];
        cursor.read_exact(&mut name_bytes)?;
        let name_str = String::from_utf8(name_bytes).context("Invalid UTF-8 in checkpoint name")?;
        let checkpoint_number = name_str
            .parse::<u64>()
            .context("Failed to parse checkpoint number")?;
        let offset = cursor.read_u64::<LittleEndian>()?;
        let length = cursor.read_u64::<LittleEndian>()?;
        let _crc = cursor.read_u32::<LittleEndian>()?;

        entries.push(BlobIndexEntry {
            checkpoint_number,
            offset,
            length,
        });
    }

    Ok(entries)
}

/// Extract events from a checkpoint that match the given package ID
fn extract_package_events(
    checkpoint: &CheckpointData,
    package_id: &ObjectID,
) -> (Vec<ExtractedEvent>, u64, u64) {
    let mut events = Vec::new();
    let checkpoint_num = checkpoint.checkpoint_summary.sequence_number;
    let mut total_events = 0u64;
    let tx_count = checkpoint.transactions.len() as u64;

    for tx in &checkpoint.transactions {
        let tx_digest = format!("{:?}", tx.transaction.digest());

        if let Some(tx_events) = &tx.events {
            total_events += tx_events.data.len() as u64;

            for event in &tx_events.data {
                if &event.package_id == package_id {
                    let event_type = format!(
                        "{}::{}::{}",
                        event.package_id, event.transaction_module, event.type_.name
                    );

                    events.push(ExtractedEvent {
                        checkpoint: checkpoint_num,
                        tx_digest: tx_digest.clone(),
                        event_type,
                        module: event.transaction_module.to_string(),
                        data_size: event.contents.len(),
                    });
                }
            }
        }
    }

    (events, total_events, tx_count)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Parse package ID
    let package_id: ObjectID = args.package.parse().context("Invalid package ID format")?;

    let blob_id = args
        .blob_id
        .clone()
        .unwrap_or_else(|| SAMPLE_BLOB_ID.to_string());

    // Determine checkpoint range - default to entire blob
    let start_cp = args.start.unwrap_or(SAMPLE_BLOB_START);
    let end_cp = args.end.unwrap_or_else(|| match args.max_checkpoints {
        Some(max) => (start_cp + max).min(SAMPLE_BLOB_END + 1),
        None => SAMPLE_BLOB_END + 1,
    });

    println!("═══════════════════════════════════════════════════════════════");
    println!("  DeepBook Event Extractor - Forked CLI Mode (Byte-Range Streaming)");
    println!("═══════════════════════════════════════════════════════════════");
    println!();
    println!("Configuration:");
    println!("  Walrus CLI (forked): {:?}", args.walrus_cli_path);
    println!("  Context: {}", args.context);
    println!("  Package filter: {}", args.package);
    println!("  Blob ID: {}", blob_id);
    println!(
        "  Checkpoint range: {} to {} ({} checkpoints)",
        start_cp,
        end_cp,
        end_cp - start_cp
    );
    println!("  Concurrency: {} parallel requests", args.concurrency);
    println!("  Timeout: {}s per request", args.timeout_secs);
    println!();

    println!("Step 1: Get blob size and index");
    println!("───────────────────────────────────────────────────────────────");

    let step1_start = Instant::now();

    // Get blob size
    let blob_size = get_blob_size(
        &args.walrus_cli_path,
        &blob_id,
        &args.context,
        args.timeout_secs,
    )
    .await?;
    println!("  Blob size: {:.2} MB", blob_size as f64 / 1_000_000.0);

    // Read footer (last 24 bytes)
    let footer_bytes = read_byte_range(
        &args.walrus_cli_path,
        &blob_id,
        &args.context,
        blob_size - 24,
        24,
        args.timeout_secs,
    )
    .await?;

    // Parse footer to get index offset
    use byteorder::{LittleEndian, ReadBytesExt};
    let mut cursor = std::io::Cursor::new(&footer_bytes);
    let _magic = cursor.read_u32::<LittleEndian>()?;
    let _version = cursor.read_u32::<LittleEndian>()?;
    let index_offset = cursor.read_u64::<LittleEndian>()?;

    // Read full index
    let index_len = blob_size - index_offset;
    let index_bytes = read_byte_range(
        &args.walrus_cli_path,
        &blob_id,
        &args.context,
        index_offset,
        index_len,
        args.timeout_secs,
    )
    .await?;

    let index = parse_blob_index_from_bytes(&footer_bytes, &index_bytes)?;
    println!("  Found {} checkpoints in blob index", index.len());
    if let (Some(first), Some(last)) = (index.first(), index.last()) {
        println!(
            "  Range: {} to {}",
            first.checkpoint_number, last.checkpoint_number
        );
    }
    println!(
        "  Index fetch time: {:.2}s",
        step1_start.elapsed().as_secs_f64()
    );
    println!();

    println!("Step 2: Stream checkpoints in parallel (byte-range)");
    println!("───────────────────────────────────────────────────────────────");

    let start_time = Instant::now();
    let metrics = Arc::new(Metrics::default());
    let all_events = Arc::new(tokio::sync::Mutex::new(Vec::<ExtractedEvent>::new()));
    let event_type_counts = Arc::new(tokio::sync::Mutex::new(HashMap::<String, u64>::new()));

    // Filter entries to requested range
    let entries_to_process: Vec<_> = index
        .iter()
        .filter(|e| e.checkpoint_number >= start_cp && e.checkpoint_number < end_cp)
        .cloned()
        .collect();

    let total_to_process = entries_to_process.len();
    println!(
        "  Processing {} checkpoints with {} parallel requests...",
        total_to_process, args.concurrency
    );

    // Create semaphore for concurrency control
    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let cli_path = Arc::new(args.walrus_cli_path.clone());
    let context = Arc::new(args.context.clone());
    let blob_id_arc = Arc::new(blob_id.clone());

    // Process entries in parallel
    let tasks: Vec<_> = entries_to_process
        .into_iter()
        .map(|entry| {
            let semaphore = semaphore.clone();
            let cli_path = cli_path.clone();
            let context = context.clone();
            let blob_id = blob_id_arc.clone();
            let metrics = metrics.clone();
            let all_events = all_events.clone();
            let event_type_counts = event_type_counts.clone();
            let package_id = package_id.clone();
            let timeout_secs = args.timeout_secs;

            tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();

                // Fetch checkpoint bytes
                let bytes = match read_byte_range(
                    &cli_path,
                    &blob_id,
                    &context,
                    entry.offset,
                    entry.length,
                    timeout_secs,
                )
                .await
                {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!(
                            "  Error fetching checkpoint {}: {}",
                            entry.checkpoint_number, e
                        );
                        return;
                    }
                };

                metrics
                    .bytes_downloaded
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);

                // Parse checkpoint
                let checkpoint = match Blob::from_bytes::<CheckpointData>(&bytes) {
                    Ok(cp) => cp,
                    Err(e) => {
                        eprintln!(
                            "  Error parsing checkpoint {}: {}",
                            entry.checkpoint_number, e
                        );
                        return;
                    }
                };

                // Extract events
                let (events, total_events, tx_count) =
                    extract_package_events(&checkpoint, &package_id);

                metrics
                    .checkpoints_processed
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .transactions_scanned
                    .fetch_add(tx_count, Ordering::Relaxed);
                metrics
                    .total_events
                    .fetch_add(total_events, Ordering::Relaxed);
                metrics
                    .matching_events
                    .fetch_add(events.len() as u64, Ordering::Relaxed);

                // Update event type counts
                {
                    let mut counts = event_type_counts.lock().await;
                    for event in &events {
                        let short_type = event
                            .event_type
                            .split("::")
                            .last()
                            .unwrap_or(&event.event_type);
                        *counts.entry(short_type.to_string()).or_insert(0) += 1;
                    }
                }

                // Store events
                {
                    let mut all = all_events.lock().await;
                    all.extend(events);
                }

                // Progress logging
                let processed = metrics.checkpoints_processed.load(Ordering::Relaxed);
                if processed % 500 == 0 {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    let rate = processed as f64 / elapsed;
                    let matching = metrics.matching_events.load(Ordering::Relaxed);
                    println!(
                        "  Progress: {}/{} checkpoints ({:.1} cp/s), {} DeepBook events",
                        processed, total_to_process, rate, matching
                    );
                }
            })
        })
        .collect();

    // Wait for all tasks
    for task in tasks {
        let _ = task.await;
    }

    let elapsed_secs = start_time.elapsed().as_secs_f64();
    let checkpoints_processed = metrics.checkpoints_processed.load(Ordering::Relaxed);
    let transactions_scanned = metrics.transactions_scanned.load(Ordering::Relaxed);
    let total_events = metrics.total_events.load(Ordering::Relaxed);
    let matching_events = metrics.matching_events.load(Ordering::Relaxed);
    let bytes_downloaded = metrics.bytes_downloaded.load(Ordering::Relaxed);

    let all_events = all_events.lock().await;
    let event_type_counts = event_type_counts.lock().await;

    // Print results
    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Results");
    println!("═══════════════════════════════════════════════════════════════");
    println!();
    println!("Event Summary:");
    println!("  Total events scanned: {}", total_events);
    println!("  DeepBook events found: {}", matching_events);
    println!("  Transactions scanned: {}", transactions_scanned);
    println!();

    if !event_type_counts.is_empty() {
        println!("Event Types:");
        let mut sorted_types: Vec<_> = event_type_counts.iter().collect();
        sorted_types.sort_by(|a, b| b.1.cmp(a.1));
        for (event_type, count) in sorted_types.iter().take(10) {
            println!("  {:40} {:>6}", event_type, count);
        }
        if sorted_types.len() > 10 {
            println!("  ... and {} more event types", sorted_types.len() - 10);
        }
        println!();
    }

    if args.verbose && !all_events.is_empty() {
        println!("Sample Events (first 10):");
        for event in all_events.iter().take(10) {
            println!(
                "  Checkpoint {}: {} - {}",
                event.checkpoint,
                event.module,
                event
                    .event_type
                    .split("::")
                    .last()
                    .unwrap_or(&event.event_type)
            );
            println!("    TX: {}", event.tx_digest);
        }
        println!();
    }

    println!("Performance Metrics:");
    println!("  Checkpoints processed: {}", checkpoints_processed);
    println!("  Time elapsed: {:.2}s", elapsed_secs);
    println!(
        "  Throughput: {:.2} checkpoints/sec",
        checkpoints_processed as f64 / elapsed_secs
    );
    println!(
        "  Data downloaded: {:.2} MB",
        bytes_downloaded as f64 / 1_000_000.0
    );
    println!(
        "  Download speed: {:.2} MB/s",
        (bytes_downloaded as f64 / 1_000_000.0) / elapsed_secs
    );
    println!("  Concurrency: {} parallel requests", args.concurrency);
    println!();

    // Demonstrate the simple API pattern
    println!("═══════════════════════════════════════════════════════════════");
    println!("  API Pattern: blob_id + package -> events");
    println!("═══════════════════════════════════════════════════════════════");
    println!();
    println!("  Input:");
    println!("    blob_id: {}", blob_id);
    println!("    package: {}", args.package);
    println!();
    println!("  Output:");
    println!("    events: {} DeepBook events extracted", all_events.len());
    println!("    unique_types: {} event types", event_type_counts.len());
    println!();

    Ok(())
}
