//! Example: Extract DeepBook v3 events using the Walrus HTTP Aggregator.
//!
//! ## Data Flow
//!
//! This example uses the **HTTP Aggregator** to fetch checkpoint data:
//!
//! ```text
//! ┌─────────────┐    HTTP Range     ┌──────────────────┐    Walrus     ┌─────────────────┐
//! │  This Code  │ ───────────────▶  │  HTTP Aggregator │ ────────────▶ │  Storage Nodes  │
//! │             │   Requests        │  (walrus.space)  │   Protocol    │  (RedStuff)     │
//! └─────────────┘                   └──────────────────┘               └─────────────────┘
//! ```
//!
//! The HTTP Aggregator is a convenience layer that:
//! - Handles the Walrus protocol (erasure coding, shard selection) internally
//! - Exposes a simple HTTP API for fetching blob data
//! - No Walrus CLI installation required
//!
//! **Trade-offs:**
//! - ✅ No CLI installation needed
//! - ✅ Works anywhere with HTTP access
//! - ❌ Slower than direct CLI access (extra network hop)
//! - ❌ Dependent on aggregator availability
//!
//! For direct Walrus protocol access (faster, more reliable), use the CLI-based example.
//!
//! ## Usage
//!
//! ```bash
//! # Run with HTTP aggregator (no CLI needed)
//! cargo run --example deepbook_events_aggregator
//!
//! # Custom package and checkpoint range
//! cargo run --example deepbook_events_aggregator -- \
//!   --package 0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809 \
//!   --start 239600000 \
//!   --end 239600050 \
//!   --verbose
//! ```
//!
//! ## Output
//!
//! The example will output:
//! - Number of checkpoints processed
//! - Number of transactions with DeepBook events
//! - Total DeepBook events found
//! - Event type breakdown
//! - Performance metrics (throughput, bytes downloaded)

use anyhow::{Context, Result};
use clap::Parser;
use std::collections::HashMap;
use std::time::Instant;
use sui_types::base_types::ObjectID;
use sui_types::full_checkpoint_content::CheckpointData;

use walrus_checkpoint_indexing::{Config, WalrusStorage};

/// DeepBook v3 package address on Sui mainnet
const DEEPBOOK_PACKAGE: &str = "0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809";

/// A sample blob ID for testing (same blob as CLI example for fair comparison)
/// This blob contains checkpoints 238954764 to 238966916 (~12k checkpoints, ~3.2 GB)
const SAMPLE_BLOB_ID: &str = "lJIYRNvG_cgmwx_OT8t7gvdr3rTqG_NsSQPF1714qu0";

/// Known checkpoint range for the sample blob (same as CLI example)
const SAMPLE_BLOB_START: u64 = 238954764;
const SAMPLE_BLOB_END: u64 = 238966916;

#[derive(Parser, Debug)]
#[command(name = "deepbook-events-aggregator")]
#[command(about = "Extract DeepBook v3 events using Walrus HTTP Aggregator")]
struct Args {
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

    /// Walrus aggregator URL
    #[arg(long, default_value = "https://aggregator.walrus-mainnet.walrus.space")]
    aggregator_url: String,

    /// Maximum checkpoints to process (default: all checkpoints in blob, ~12k)
    #[arg(long)]
    max_checkpoints: Option<u64>,

    /// Show individual events (verbose)
    #[arg(long, short)]
    verbose: bool,
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
    bytes_downloaded: u64,
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

    fn mb_downloaded(&self) -> f64 {
        self.bytes_downloaded as f64 / 1_000_000.0
    }

    fn throughput_mbps(&self) -> f64 {
        if self.elapsed_secs > 0.0 {
            self.mb_downloaded() / self.elapsed_secs
        } else {
            0.0
        }
    }
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
                // Check if the event's package matches our target
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

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,walrus_checkpoint_indexing=info".into()),
        )
        .init();

    let args = Args::parse();

    // Parse package ID
    let package_id: ObjectID = args.package.parse().context("Invalid package ID format")?;

    println!("═══════════════════════════════════════════════════════════════");
    println!("  DeepBook Event Extractor - HTTP Aggregator Mode");
    println!("═══════════════════════════════════════════════════════════════");
    println!();
    println!("Configuration:");
    println!("  Package filter: {}", args.package);
    println!(
        "  Max checkpoints: {}",
        args.max_checkpoints
            .map(|n| n.to_string())
            .unwrap_or_else(|| "all (~12k)".to_string())
    );
    println!("  Aggregator URL: {}", args.aggregator_url);
    println!("  Mode: HTTP Aggregator (no CLI)");
    println!();

    // Build configuration - HTTP aggregator only, no CLI
    let config = Config::builder()
        .aggregator_url(&args.aggregator_url)
        .build()?;

    // Create storage instance
    let storage = WalrusStorage::new(config).await?;
    storage.initialize().await?;

    // Get blob info
    let blob_id = args
        .blob_id
        .clone()
        .unwrap_or_else(|| SAMPLE_BLOB_ID.to_string());

    // Determine checkpoint range - default to entire blob (same as CLI example)
    let start_cp = args.start.unwrap_or(SAMPLE_BLOB_START);
    let end_cp = args.end.unwrap_or_else(|| {
        // If max_checkpoints specified, use it; otherwise use entire blob
        match args.max_checkpoints {
            Some(max) => (start_cp + max).min(SAMPLE_BLOB_END + 1),
            None => SAMPLE_BLOB_END + 1, // Process entire blob (~12k checkpoints)
        }
    });

    println!("Processing:");
    println!("  Blob ID: {}", blob_id);
    println!(
        "  Checkpoint range: {} to {} ({} checkpoints)",
        start_cp,
        end_cp,
        end_cp - start_cp
    );
    println!();

    // Start timing
    let start_time = Instant::now();

    // Collect all matching events
    let mut all_events: Vec<ExtractedEvent> = Vec::new();
    let mut metrics = Metrics::default();
    let mut event_type_counts: HashMap<String, u64> = HashMap::new();

    // Stream checkpoints
    println!("Streaming checkpoints...");
    storage
        .stream_checkpoints(start_cp..end_cp, |checkpoint| {
            let _checkpoint_num = checkpoint.checkpoint_summary.sequence_number;

            // Count total events
            let total_in_checkpoint = count_total_events(&checkpoint);
            metrics.total_events += total_in_checkpoint;
            metrics.transactions_scanned += checkpoint.transactions.len() as u64;

            // Extract matching events
            let matching = extract_package_events(&checkpoint, &package_id);

            for event in &matching {
                // Track event type distribution
                let short_type = event
                    .event_type
                    .split("::")
                    .last()
                    .unwrap_or(&event.event_type);
                *event_type_counts.entry(short_type.to_string()).or_insert(0) += 1;
            }

            metrics.matching_events += matching.len() as u64;
            all_events.extend(matching);
            metrics.checkpoints_processed += 1;

            // Progress logging
            if metrics.checkpoints_processed % 50 == 0 {
                let elapsed = start_time.elapsed().as_secs_f64();
                let rate = metrics.checkpoints_processed as f64 / elapsed;
                println!(
                    "  Progress: {} checkpoints ({:.1} cp/s), {} matching events so far",
                    metrics.checkpoints_processed, rate, metrics.matching_events
                );
            }

            async move { Ok::<(), anyhow::Error>(()) }
        })
        .await?;

    // Finalize metrics
    metrics.elapsed_secs = start_time.elapsed().as_secs_f64();
    metrics.bytes_downloaded = storage.bytes_downloaded();

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
    println!("  Checkpoints processed: {}", metrics.checkpoints_processed);
    println!("  Time elapsed: {:.2}s", metrics.elapsed_secs);
    println!(
        "  Throughput: {:.2} checkpoints/sec",
        metrics.checkpoints_per_sec()
    );
    println!("  Data downloaded: {:.2} MB", metrics.mb_downloaded());
    println!("  Download speed: {:.2} MB/s", metrics.throughput_mbps());
    println!();

    // Demonstrate the simple API pattern: blob_id + package -> events
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
