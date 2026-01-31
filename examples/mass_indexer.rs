//! Example: Mass checkpoint indexer using the Walrus storage backend.
//!
//! This example demonstrates how to build a mass indexer that processes all
//! checkpoints from a Walrus blob. It follows patterns compatible with the
//! Sui custom indexing framework (`sui-indexer-alt-framework`).
//!
//! ## Features Demonstrated
//!
//! - **Processor trait**: Define how to extract and transform checkpoint data
//! - **Watermark tracking**: Resume indexing from where you left off
//! - **Progress logging**: Monitor indexing progress
//! - **Cached blob support**: Use locally cached blobs for fast iteration
//!
//! ## Usage
//!
//! ```bash
//! # Run with HTTP aggregator (downloads on demand)
//! cargo run --example mass_indexer
//!
//! # Run with a cached blob (much faster for development)
//! cargo run --example mass_indexer -- --cache-dir ./walrus-cache --cache-enabled
//!
//! # Run with Walrus CLI
//! cargo run --example mass_indexer -- \
//!   --walrus-cli-path /path/to/walrus \
//!   --cache-enabled
//!
//! # Process specific checkpoint range
//! cargo run --example mass_indexer -- --start 238954764 --end 238955000
//!
//! # Resume from watermark
//! cargo run --example mass_indexer -- --watermark-file ./indexer.watermark
//! ```
//!
//! ## Architecture
//!
//! This example uses the `MassIndexer` which orchestrates:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                         MassIndexer                                      │
//! │                                                                          │
//! │  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐  │
//! │  │  WalrusStorage   │───▶│  StatsProcessor  │───▶│   Watermark      │  │
//! │  │  (cached blob)   │    │  (count events)  │    │   (progress)     │  │
//! │  └──────────────────┘    └──────────────────┘    └──────────────────┘  │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use sui_types::base_types::ObjectID;
use sui_types::full_checkpoint_content::CheckpointData;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::sync::RwLock;

use walrus_checkpoint_indexing::indexer::{IndexerConfig, MassIndexer, Processor};
use walrus_checkpoint_indexing::Config;

/// A sample blob ID for testing (contains checkpoints 238954764 to 238966916)
const SAMPLE_BLOB_START: u64 = 238954764;
#[allow(dead_code)]
const SAMPLE_BLOB_END: u64 = 238966916;

#[derive(Parser, Debug)]
#[command(name = "mass-indexer")]
#[command(about = "Mass checkpoint indexer example using Walrus storage")]
struct Args {
    /// Starting checkpoint number
    #[arg(long, default_value_t = SAMPLE_BLOB_START)]
    start: u64,

    /// Ending checkpoint number (exclusive)
    #[arg(long)]
    end: Option<u64>,

    /// Maximum checkpoints to process
    #[arg(long, default_value = "1000")]
    max_checkpoints: u64,

    /// Walrus aggregator URL
    #[arg(long, default_value = "https://aggregator.walrus-mainnet.walrus.space")]
    aggregator_url: String,

    /// Path to Walrus CLI binary (optional)
    #[arg(long, env = "WALRUS_CLI_PATH")]
    walrus_cli_path: Option<PathBuf>,

    /// Cache directory for downloaded blobs
    #[arg(long, default_value = ".walrus-cache")]
    cache_dir: PathBuf,

    /// Enable blob caching
    #[arg(long)]
    cache_enabled: bool,

    /// Watermark file for resumability
    #[arg(long)]
    watermark_file: Option<PathBuf>,

    /// Optional package ID to filter events
    #[arg(long)]
    package: Option<String>,

    /// Log progress every N checkpoints
    #[arg(long, default_value = "50")]
    log_interval: u64,

    /// Show detailed event breakdown
    #[arg(long, short)]
    verbose: bool,
}

/// Output from processing a single checkpoint.
#[derive(Debug, Clone)]
struct CheckpointOutput {
    checkpoint_num: u64,
    transaction_count: u64,
    event_count: u64,
    filtered_event_count: u64,
    event_types: HashMap<String, u64>,
}

/// Processor that collects statistics and optionally filters by package.
struct StatsProcessor {
    /// Optional package ID filter
    package_filter: Option<ObjectID>,
    /// Accumulated statistics
    stats: Arc<RwLock<AggregatedStats>>,
    /// Verbose output
    verbose: bool,
}

#[derive(Debug, Default)]
struct AggregatedStats {
    total_checkpoints: u64,
    total_transactions: u64,
    total_events: u64,
    filtered_events: u64,
    event_type_counts: HashMap<String, u64>,
    first_checkpoint: Option<u64>,
    last_checkpoint: Option<u64>,
}

impl StatsProcessor {
    fn new(package_filter: Option<ObjectID>, verbose: bool) -> Self {
        Self {
            package_filter,
            stats: Arc::new(RwLock::new(AggregatedStats::default())),
            verbose,
        }
    }

    #[allow(dead_code)]
    async fn get_stats(&self) -> AggregatedStats {
        let stats = self.stats.read().await;
        AggregatedStats {
            total_checkpoints: stats.total_checkpoints,
            total_transactions: stats.total_transactions,
            total_events: stats.total_events,
            filtered_events: stats.filtered_events,
            event_type_counts: stats.event_type_counts.clone(),
            first_checkpoint: stats.first_checkpoint,
            last_checkpoint: stats.last_checkpoint,
        }
    }
}

#[async_trait]
impl Processor for StatsProcessor {
    type Output = CheckpointOutput;

    async fn process(&self, checkpoint: &CheckpointData) -> Result<Self::Output> {
        let checkpoint_num = checkpoint.checkpoint_summary.sequence_number;
        let transaction_count = checkpoint.transactions.len() as u64;

        let mut event_count = 0u64;
        let mut filtered_event_count = 0u64;
        let mut event_types: HashMap<String, u64> = HashMap::new();

        for tx in &checkpoint.transactions {
            if let Some(events) = &tx.events {
                for event in &events.data {
                    event_count += 1;

                    // Apply package filter if configured
                    let matches_filter = match &self.package_filter {
                        Some(pkg) => &event.package_id == pkg,
                        None => true,
                    };

                    if matches_filter {
                        filtered_event_count += 1;

                        // Track event types
                        let event_type =
                            format!("{}::{}", event.transaction_module, event.type_.name);
                        *event_types.entry(event_type).or_insert(0) += 1;
                    }
                }
            }
        }

        Ok(CheckpointOutput {
            checkpoint_num,
            transaction_count,
            event_count,
            filtered_event_count,
            event_types,
        })
    }

    async fn commit(
        &mut self,
        _checkpoint_num: CheckpointSequenceNumber,
        data: Self::Output,
    ) -> Result<()> {
        let mut stats = self.stats.write().await;

        stats.total_checkpoints += 1;
        stats.total_transactions += data.transaction_count;
        stats.total_events += data.event_count;
        stats.filtered_events += data.filtered_event_count;

        // Update checkpoint range
        if stats.first_checkpoint.is_none() {
            stats.first_checkpoint = Some(data.checkpoint_num);
        }
        stats.last_checkpoint = Some(data.checkpoint_num);

        // Merge event type counts
        for (event_type, count) in data.event_types {
            *stats.event_type_counts.entry(event_type).or_insert(0) += count;
        }

        // Verbose output
        if self.verbose && data.filtered_event_count > 0 {
            println!(
                "  Checkpoint {}: {} txs, {} events ({} filtered)",
                data.checkpoint_num,
                data.transaction_count,
                data.event_count,
                data.filtered_event_count
            );
        }

        Ok(())
    }

    async fn on_start(
        &mut self,
        start: CheckpointSequenceNumber,
        end: CheckpointSequenceNumber,
    ) -> Result<()> {
        println!("Starting mass indexer: checkpoints {} to {}", start, end);
        if let Some(pkg) = &self.package_filter {
            println!("Filtering events by package: {}", pkg);
        }
        Ok(())
    }

    async fn on_complete(&mut self, total_processed: u64) -> Result<()> {
        println!(
            "\nIndexing complete: {} checkpoints processed",
            total_processed
        );
        Ok(())
    }
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

    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mass Checkpoint Indexer - Walrus Storage Backend");
    println!("═══════════════════════════════════════════════════════════════");
    println!();

    // Parse optional package filter
    let package_filter: Option<ObjectID> = args.package.as_ref().map(|p| p.parse()).transpose()?;

    // Determine checkpoint range
    let end_checkpoint = args.end.unwrap_or(args.start + args.max_checkpoints);
    let range = args.start..end_checkpoint;

    println!("Configuration:");
    println!(
        "  Checkpoint range: {} to {} ({} checkpoints)",
        range.start,
        range.end,
        range.end - range.start
    );
    if let Some(pkg) = &package_filter {
        println!("  Package filter: {}", pkg);
    } else {
        println!("  Package filter: none (all events)");
    }
    println!("  Cache enabled: {}", args.cache_enabled);
    if args.cache_enabled {
        println!("  Cache directory: {}", args.cache_dir.display());
    }
    if let Some(wm) = &args.watermark_file {
        println!("  Watermark file: {}", wm.display());
    }
    println!();

    // Build storage configuration
    let mut config_builder = Config::builder()
        .aggregator_url(&args.aggregator_url)
        .cache_enabled(args.cache_enabled)
        .cache_dir(&args.cache_dir);

    if let Some(cli_path) = &args.walrus_cli_path {
        config_builder = config_builder.walrus_cli_path(cli_path);
    }

    let storage_config = config_builder.build()?;

    // Build indexer configuration
    let mut indexer_config = IndexerConfig::new(storage_config)
        .with_log_interval(args.log_interval)
        .with_watermark_interval(args.log_interval);

    if let Some(wm_path) = &args.watermark_file {
        indexer_config = indexer_config.with_watermark_file(wm_path);
    }

    // Create processor
    let processor = StatsProcessor::new(package_filter, args.verbose);
    let stats_handle = processor.stats.clone();

    // Create and run indexer
    let mut indexer = MassIndexer::new(indexer_config, processor).await?;

    // Check for existing watermark
    if let Some(wm) = indexer.watermark().await {
        println!("Found existing watermark: checkpoint {}", wm);
        if wm >= range.start && wm < range.end {
            println!("Will resume from checkpoint {}", wm + 1);
        }
    }

    println!();
    println!("Starting indexer...");
    println!();

    let result = indexer.run(range.clone()).await?;

    // Get final stats
    let final_stats = {
        let stats = stats_handle.read().await;
        AggregatedStats {
            total_checkpoints: stats.total_checkpoints,
            total_transactions: stats.total_transactions,
            total_events: stats.total_events,
            filtered_events: stats.filtered_events,
            event_type_counts: stats.event_type_counts.clone(),
            first_checkpoint: stats.first_checkpoint,
            last_checkpoint: stats.last_checkpoint,
        }
    };

    // Print results
    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Results");
    println!("═══════════════════════════════════════════════════════════════");
    println!();

    println!("Checkpoint Summary:");
    println!(
        "  Total checkpoints processed: {}",
        final_stats.total_checkpoints
    );
    if let (Some(first), Some(last)) = (final_stats.first_checkpoint, final_stats.last_checkpoint) {
        println!("  Checkpoint range: {} to {}", first, last);
    }
    println!();

    println!("Data Summary:");
    println!("  Total transactions: {}", final_stats.total_transactions);
    println!("  Total events: {}", final_stats.total_events);
    println!("  Filtered events: {}", final_stats.filtered_events);
    println!();

    if !final_stats.event_type_counts.is_empty() {
        println!("Event Types (top 15):");
        let mut sorted: Vec<_> = final_stats.event_type_counts.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1));
        for (event_type, count) in sorted.iter().take(15) {
            println!("  {:50} {:>8}", event_type, count);
        }
        if sorted.len() > 15 {
            println!("  ... and {} more event types", sorted.len() - 15);
        }
        println!();
    }

    println!("Performance:");
    println!("  Total time: {:.2}s", result.elapsed_secs);
    println!(
        "  Throughput: {:.2} checkpoints/sec",
        result.checkpoints_per_sec()
    );
    println!("  Data downloaded: {:.2} MB", result.mb_downloaded());
    if result.elapsed_secs > 0.0 {
        println!(
            "  Download speed: {:.2} MB/s",
            result.mb_downloaded() / result.elapsed_secs
        );
    }
    println!();

    // Verify end-to-end
    println!("═══════════════════════════════════════════════════════════════");
    println!("  End-to-End Verification");
    println!("═══════════════════════════════════════════════════════════════");
    println!();

    let verification_passed = final_stats.total_checkpoints > 0
        && final_stats.total_checkpoints == result.checkpoints_processed
        && final_stats.total_transactions > 0;

    if verification_passed {
        println!(
            "  [PASS] Checkpoints processed: {} (expected > 0)",
            final_stats.total_checkpoints
        );
        println!(
            "  [PASS] Stats match: processor={}, indexer={}",
            final_stats.total_checkpoints, result.checkpoints_processed
        );
        println!(
            "  [PASS] Transactions found: {} (expected > 0)",
            final_stats.total_transactions
        );
        println!();
        println!("  Mass indexer pipeline verified successfully!");
    } else {
        println!("  [FAIL] Verification failed:");
        if final_stats.total_checkpoints == 0 {
            println!("    - No checkpoints processed");
        }
        if final_stats.total_checkpoints != result.checkpoints_processed {
            println!(
                "    - Stats mismatch: processor={}, indexer={}",
                final_stats.total_checkpoints, result.checkpoints_processed
            );
        }
        if final_stats.total_transactions == 0 {
            println!("    - No transactions found");
        }
    }

    println!();

    Ok(())
}
