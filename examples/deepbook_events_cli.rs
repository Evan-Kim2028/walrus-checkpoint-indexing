//! Example: Extract DeepBook v3 events using the Walrus CLI (full blob download).
//!
//! ## Data Flow
//!
//! This example uses the **Walrus CLI** to download the full blob directly:
//!
//! ```text
//! ┌─────────────┐    walrus read    ┌─────────────────┐
//! │  This Code  │ ────────────────▶ │  Storage Nodes  │
//! │             │   (RedStuff)      │  (1000 shards)  │
//! └─────────────┘                   └─────────────────┘
//!        │
//!        ▼
//! ┌─────────────┐
//! │ Local Blob  │  (~3.2 GB cached)
//! │   Cache     │
//! └─────────────┘
//! ```
//!
//! The Walrus CLI:
//! - Talks directly to storage nodes using the RedStuff erasure coding protocol
//! - Downloads the full blob (~3.2 GB) and caches it locally
//! - Subsequent runs reuse the cached blob (instant)
//!
//! **Trade-offs:**
//! - ✅ Faster and more reliable than HTTP aggregator
//! - ✅ Full blob cached locally for repeated access
//! - ✅ No dependency on aggregator availability
//! - ❌ Requires Walrus CLI installation
//! - ❌ Initial download is large (~3.2 GB per blob)
//!
//! ## Prerequisites
//!
//! 1. Install the Walrus CLI from: https://github.com/MystenLabs/walrus/releases
//! 2. Configure it for mainnet: `walrus config --context mainnet`
//!
//! ## Usage
//!
//! ```bash
//! # Basic usage (requires WALRUS_CLI_PATH env var or --walrus-cli-path)
//! cargo run --example deepbook_events_cli -- --walrus-cli-path /path/to/walrus
//!
//! # With environment variable
//! export WALRUS_CLI_PATH=/path/to/walrus
//! cargo run --example deepbook_events_cli
//!
//! # Custom package and checkpoint range
//! cargo run --example deepbook_events_cli -- \
//!   --walrus-cli-path /path/to/walrus \
//!   --package 0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809 \
//!   --start 239600000 \
//!   --end 239600100 \
//!   --verbose
//! ```
//!
//! ## Output
//!
//! The example will output:
//! - Blob download progress (first run only)
//! - Number of checkpoints processed
//! - DeepBook events found with type breakdown
//! - Performance metrics

use anyhow::{Context, Result};
use clap::Parser;
use std::collections::HashMap;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Instant;
use sui_storage::blob::Blob;
use sui_types::base_types::ObjectID;
use sui_types::full_checkpoint_content::CheckpointData;

/// DeepBook v3 package address on Sui mainnet
const DEEPBOOK_PACKAGE: &str = "0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809";

/// A sample blob ID for testing (recent mainnet blob with ~12k checkpoints)
const SAMPLE_BLOB_ID: &str = "lJIYRNvG_cgmwx_OT8t7gvdr3rTqG_NsSQPF1714qu0";

/// Known checkpoint range for the sample blob
const SAMPLE_BLOB_START: u64 = 238954764;
const SAMPLE_BLOB_END: u64 = 238966916;

/// Blob index footer magic bytes ("DBLW" in ASCII, little-endian)
const BLOB_FOOTER_MAGIC: u32 = 0x574c4244;

#[derive(Parser, Debug)]
#[command(name = "deepbook-events-cli")]
#[command(about = "Extract DeepBook v3 events using Walrus CLI (full blob download)")]
struct Args {
    /// Path to Walrus CLI binary (required)
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

    /// Cache directory for downloaded blobs
    #[arg(long, default_value = ".walrus-blobs-cache")]
    cache_dir: PathBuf,

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
    checkpoints_processed: u64,
    transactions_scanned: u64,
    total_events: u64,
    matching_events: u64,
    blob_size_bytes: u64,
    elapsed_secs: f64,
}

impl Metrics {
    fn checkpoints_per_sec(&self) -> f64 {
        if self.elapsed_secs > 0.0 {
            self.checkpoints_processed as f64 / self.elapsed_secs
        } else {
            0.0
        }
    }

    fn blob_size_mb(&self) -> f64 {
        self.blob_size_bytes as f64 / 1_000_000.0
    }
}

/// Download a blob using the Walrus CLI
fn download_blob_via_cli(
    cli_path: &PathBuf,
    blob_id: &str,
    context: &str,
    output_path: &PathBuf,
) -> Result<()> {
    println!("  Downloading blob {} via Walrus CLI...", blob_id);
    println!("  This may take a few minutes for a ~3.2 GB blob.");
    println!();

    let start = Instant::now();

    let output = std::process::Command::new(cli_path)
        .arg("read")
        .arg(blob_id)
        .arg("--context")
        .arg(context)
        .arg("--out")
        .arg(output_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("Failed to execute walrus CLI")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("walrus CLI failed: {}", stderr.trim());
    }

    let elapsed = start.elapsed();
    let size = std::fs::metadata(output_path)?.len();
    let mb = size as f64 / 1_000_000.0;
    let mbps = mb / elapsed.as_secs_f64();

    println!("  Downloaded {:.2} MB in {:.1}s ({:.2} MB/s)", mb, elapsed.as_secs_f64(), mbps);

    Ok(())
}

/// Parse the blob index from a downloaded blob file
///
/// Blob format:
/// - Checkpoint data (BCS encoded)
/// - Index entries: [name_len (u32), name_bytes, offset (u64), length (u64), crc (u32)]
/// - Footer (24 bytes): [magic (u32), version (u32), index_offset (u64), count (u32), padding (u32)]
fn parse_blob_index(blob_path: &PathBuf) -> Result<Vec<BlobIndexEntry>> {
    use byteorder::{LittleEndian, ReadBytesExt};

    let file = std::fs::File::open(blob_path)?;
    let file_size = file.metadata()?.len();
    let mut reader = BufReader::new(file);

    if file_size < 24 {
        anyhow::bail!("Blob file too small: {} bytes", file_size);
    }

    // Read footer (last 24 bytes)
    reader.seek(SeekFrom::End(-24))?;

    let magic = reader.read_u32::<LittleEndian>()?;
    if magic != BLOB_FOOTER_MAGIC {
        anyhow::bail!("Invalid blob footer magic: expected {:#x}, got {:#x}", BLOB_FOOTER_MAGIC, magic);
    }

    let _version = reader.read_u32::<LittleEndian>()?;
    let index_offset = reader.read_u64::<LittleEndian>()?;
    let entry_count = reader.read_u32::<LittleEndian>()?;
    let _padding = reader.read_u32::<LittleEndian>()?;

    // Seek to index start and read all index bytes
    reader.seek(SeekFrom::Start(index_offset))?;
    let mut index_bytes = Vec::new();
    reader.read_to_end(&mut index_bytes)?;

    // Parse index entries
    // Format: name_len (u32) | name_bytes | offset (u64) | length (u64) | crc (u32)
    let mut cursor = std::io::Cursor::new(&index_bytes);
    let mut entries = Vec::with_capacity(entry_count as usize);

    for _ in 0..entry_count {
        let name_len = cursor.read_u32::<LittleEndian>()?;
        let mut name_bytes = vec![0u8; name_len as usize];
        cursor.read_exact(&mut name_bytes)?;
        let name_str = String::from_utf8(name_bytes)
            .context("Invalid UTF-8 in checkpoint name")?;
        let checkpoint_number = name_str.parse::<u64>()
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

/// Read a checkpoint from the blob at the given offset
///
/// Uses sui_storage::blob::Blob to handle the blob format which includes
/// a header before the BCS-encoded checkpoint data.
fn read_checkpoint_from_blob(blob_path: &PathBuf, entry: &BlobIndexEntry) -> Result<CheckpointData> {
    let mut file = std::fs::File::open(blob_path)?;
    file.seek(SeekFrom::Start(entry.offset))?;

    let mut buffer = vec![0u8; entry.length as usize];
    file.read_exact(&mut buffer)?;

    // Use sui_storage::blob::Blob which handles the blob format
    let checkpoint = Blob::from_bytes::<CheckpointData>(&buffer)
        .context("Failed to deserialize checkpoint")?;

    Ok(checkpoint)
}

/// Extract events from a checkpoint that match the given package ID
fn extract_package_events(
    checkpoint: &CheckpointData,
    package_id: &ObjectID,
) -> Vec<ExtractedEvent> {
    let mut events = Vec::new();
    let checkpoint_num = checkpoint.checkpoint_summary.sequence_number;

    for tx in &checkpoint.transactions {
        let tx_digest = format!("{:?}", tx.transaction.digest());

        if let Some(tx_events) = &tx.events {
            for event in &tx_events.data {
                if &event.package_id == package_id {
                    let event_type = format!(
                        "{}::{}::{}",
                        event.package_id,
                        event.transaction_module,
                        event.type_.name
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

    events
}

/// Count total events in a checkpoint
fn count_total_events(checkpoint: &CheckpointData) -> u64 {
    checkpoint
        .transactions
        .iter()
        .filter_map(|tx| tx.events.as_ref())
        .map(|events| events.data.len() as u64)
        .sum()
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Parse package ID
    let package_id: ObjectID = args
        .package
        .parse()
        .context("Invalid package ID format")?;

    let blob_id = args.blob_id.clone().unwrap_or_else(|| SAMPLE_BLOB_ID.to_string());

    // Determine checkpoint range - default to entire blob
    let start_cp = args.start.unwrap_or(SAMPLE_BLOB_START);
    let end_cp = args.end.unwrap_or_else(|| {
        // If max_checkpoints specified, use it; otherwise use entire blob
        match args.max_checkpoints {
            Some(max) => (start_cp + max).min(SAMPLE_BLOB_END + 1),
            None => SAMPLE_BLOB_END + 1,  // Process entire blob
        }
    });

    println!("═══════════════════════════════════════════════════════════════");
    println!("  DeepBook Event Extractor - Walrus CLI Mode (Full Blob)");
    println!("═══════════════════════════════════════════════════════════════");
    println!();
    println!("Configuration:");
    println!("  Walrus CLI: {:?}", args.walrus_cli_path);
    println!("  Context: {}", args.context);
    println!("  Package filter: {}", args.package);
    println!("  Blob ID: {}", blob_id);
    println!("  Checkpoint range: {} to {} ({} checkpoints)", start_cp, end_cp, end_cp - start_cp);
    println!("  Cache dir: {:?}", args.cache_dir);
    println!();

    // Ensure cache directory exists
    std::fs::create_dir_all(&args.cache_dir)?;

    // Check if blob is already cached
    let blob_path = args.cache_dir.join(&blob_id);
    let download_needed = !blob_path.exists();

    if download_needed {
        println!("Step 1: Download blob via Walrus CLI");
        println!("───────────────────────────────────────────────────────────────");
        download_blob_via_cli(&args.walrus_cli_path, &blob_id, &args.context, &blob_path)?;
        println!();
    } else {
        println!("Step 1: Using cached blob");
        println!("───────────────────────────────────────────────────────────────");
        let size = std::fs::metadata(&blob_path)?.len();
        println!("  Blob already cached at: {:?}", blob_path);
        println!("  Size: {:.2} MB", size as f64 / 1_000_000.0);
        println!();
    }

    println!("Step 2: Parse blob index");
    println!("───────────────────────────────────────────────────────────────");
    let index = parse_blob_index(&blob_path)?;
    println!("  Found {} checkpoints in blob index", index.len());
    if let (Some(first), Some(last)) = (index.first(), index.last()) {
        println!("  Range: {} to {}", first.checkpoint_number, last.checkpoint_number);
    }
    println!();

    println!("Step 3: Extract DeepBook events");
    println!("───────────────────────────────────────────────────────────────");

    let start_time = Instant::now();
    let mut metrics = Metrics {
        blob_size_bytes: std::fs::metadata(&blob_path)?.len(),
        ..Default::default()
    };

    let mut all_events: Vec<ExtractedEvent> = Vec::new();
    let mut event_type_counts: HashMap<String, u64> = HashMap::new();

    // Filter index entries to requested range
    let entries_to_process: Vec<_> = index
        .iter()
        .filter(|e| e.checkpoint_number >= start_cp && e.checkpoint_number < end_cp)
        .collect();

    println!("  Processing {} checkpoints...", entries_to_process.len());

    for (i, entry) in entries_to_process.iter().enumerate() {
        let checkpoint = read_checkpoint_from_blob(&blob_path, entry)?;

        // Count total events
        let total_in_checkpoint = count_total_events(&checkpoint);
        metrics.total_events += total_in_checkpoint;
        metrics.transactions_scanned += checkpoint.transactions.len() as u64;

        // Extract matching events
        let matching = extract_package_events(&checkpoint, &package_id);

        for event in &matching {
            let short_type = event.event_type.split("::").last().unwrap_or(&event.event_type);
            *event_type_counts.entry(short_type.to_string()).or_insert(0) += 1;
        }

        metrics.matching_events += matching.len() as u64;
        all_events.extend(matching);
        metrics.checkpoints_processed += 1;

        // Progress logging
        if (i + 1) % 50 == 0 || i + 1 == entries_to_process.len() {
            let elapsed = start_time.elapsed().as_secs_f64();
            let rate = (i + 1) as f64 / elapsed;
            println!(
                "  Progress: {}/{} checkpoints ({:.1} cp/s), {} DeepBook events",
                i + 1, entries_to_process.len(), rate, metrics.matching_events
            );
        }
    }

    metrics.elapsed_secs = start_time.elapsed().as_secs_f64();

    // Print results
    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Results");
    println!("═══════════════════════════════════════════════════════════════");
    println!();
    println!("Event Summary:");
    println!("  Total events scanned: {}", metrics.total_events);
    println!("  DeepBook events found: {}", metrics.matching_events);
    println!("  Transactions scanned: {}", metrics.transactions_scanned);
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
            println!("  Checkpoint {}: {} - {}",
                     event.checkpoint,
                     event.module,
                     event.event_type.split("::").last().unwrap_or(&event.event_type));
            println!("    TX: {}", event.tx_digest);
        }
        println!();
    }

    println!("Performance Metrics:");
    println!("  Checkpoints processed: {}", metrics.checkpoints_processed);
    println!("  Time elapsed: {:.2}s", metrics.elapsed_secs);
    println!("  Throughput: {:.2} checkpoints/sec", metrics.checkpoints_per_sec());
    println!("  Blob size: {:.2} MB", metrics.blob_size_mb());
    println!("  Blob cached: {}", if download_needed { "No (downloaded this run)" } else { "Yes (reused)" });
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
