//! CLI binary for indexing Sui checkpoints from Walrus.

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use walrus_checkpoint_indexing::{Config, WalrusStorage};

#[derive(Parser)]
#[command(name = "walrus-checkpoint-index")]
#[command(about = "Index Sui checkpoints from Walrus decentralized storage")]
struct Cli {
    #[command(flatten)]
    config: Config,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Stream checkpoints in a range
    Stream {
        /// Starting checkpoint number
        #[arg(long)]
        start: u64,

        /// Ending checkpoint number (exclusive)
        #[arg(long)]
        end: u64,

        /// Log progress every N checkpoints
        #[arg(long, default_value = "100")]
        log_interval: u64,
    },

    /// Get a single checkpoint
    Get {
        /// Checkpoint number to fetch
        #[arg(long)]
        checkpoint: u64,
    },

    /// Show info about available checkpoints
    Info,

    /// Check node health
    Health,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "walrus_checkpoint_indexing=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();

    // Create storage instance
    let storage = WalrusStorage::new(cli.config).await?;
    storage.initialize().await?;

    match cli.command {
        Commands::Stream {
            start,
            end,
            log_interval,
        } => {
            stream_checkpoints(&storage, start, end, log_interval).await?;
        }
        Commands::Get { checkpoint } => {
            get_checkpoint(&storage, checkpoint).await?;
        }
        Commands::Info => {
            show_info(&storage).await?;
        }
        Commands::Health => {
            check_health(&storage).await?;
        }
    }

    Ok(())
}

async fn stream_checkpoints(
    storage: &WalrusStorage,
    start: u64,
    end: u64,
    log_interval: u64,
) -> Result<()> {
    tracing::info!("streaming checkpoints {} to {}", start, end);

    let start_time = std::time::Instant::now();
    let mut count = 0u64;

    storage
        .stream_checkpoints(start..end, |checkpoint| {
            count += 1;
            async move {
                if count % log_interval == 0 {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    let rate = count as f64 / elapsed;
                    tracing::info!(
                        "processed {} checkpoints ({:.2} cp/s), latest: {}",
                        count,
                        rate,
                        checkpoint.checkpoint_summary.sequence_number
                    );
                }
                Ok(())
            }
        })
        .await?;

    let elapsed = start_time.elapsed();
    let rate = count as f64 / elapsed.as_secs_f64();
    let bytes = storage.bytes_downloaded();
    let mb = bytes as f64 / 1_000_000.0;

    tracing::info!(
        "completed: {} checkpoints in {:.2}s ({:.2} cp/s)",
        count,
        elapsed.as_secs_f64(),
        rate
    );
    tracing::info!(
        "downloaded {:.2} MB ({:.2} MB/s)",
        mb,
        mb / elapsed.as_secs_f64()
    );

    Ok(())
}

async fn get_checkpoint(storage: &WalrusStorage, checkpoint: u64) -> Result<()> {
    tracing::info!("fetching checkpoint {}", checkpoint);

    let cp = storage.get_checkpoint(checkpoint).await?;

    println!("Checkpoint {}", cp.checkpoint_summary.sequence_number);
    println!("  Digest: {:?}", cp.checkpoint_summary.content_digest);
    println!("  Epoch: {}", cp.checkpoint_summary.epoch);
    println!("  Transactions: {}", cp.transactions.len());

    let mut total_events = 0;
    for tx in &cp.transactions {
        if let Some(events) = &tx.events {
            total_events += events.data.len();
        }
    }
    println!("  Events: {}", total_events);

    Ok(())
}

async fn show_info(storage: &WalrusStorage) -> Result<()> {
    if let Some((min, max)) = storage.coverage_range().await {
        println!("Checkpoint coverage: {} to {}", min, max);
        println!("Total checkpoints: {}", max - min + 1);
    } else {
        println!("No checkpoint data available");
    }

    if let Some(latest) = storage.get_latest_checkpoint().await {
        println!("Latest checkpoint: {}", latest);
    }

    Ok(())
}

async fn check_health(storage: &WalrusStorage) -> Result<()> {
    if let Some(tracker) = storage.health_tracker() {
        let summary = tracker.poll_health().await?;

        println!("Node Health Summary");
        println!("  Total nodes: {}", summary.total_nodes);
        println!("  Healthy: {}", summary.healthy_nodes);
        println!("  Degraded: {}", summary.degraded_nodes);
        println!("  Down: {}", summary.down_nodes);
        println!();
        println!("Shard Summary");
        println!("  Total shards: {}", summary.total_shards);
        println!("  Problematic: {}", summary.problematic_shards);

        if !summary.down_node_names.is_empty() {
            println!();
            println!("Down nodes:");
            for name in &summary.down_node_names {
                println!("  - {}", name);
            }
        }

        if !summary.degraded_node_names.is_empty() {
            println!();
            println!("Degraded nodes:");
            for name in &summary.degraded_node_names {
                println!("  - {}", name);
            }
        }
    } else {
        println!("Health tracking requires --walrus-cli-path to be set");
    }

    Ok(())
}
