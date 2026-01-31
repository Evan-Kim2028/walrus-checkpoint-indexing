//! Example: Mass checkpoint indexer with Parquet output.
//!
//! This example processes checkpoints from Walrus storage and writes
//! extracted events to Parquet files for analytics.
//!
//! ## Default Behavior
//!
//! By default, this processes the **entire sample blob** (~12k checkpoints, ~3.2GB).
//! The blob is cached locally so subsequent runs are fast.
//!
//! ## Output Schema
//!
//! The output Parquet file contains these columns:
//! - `checkpoint_num`: u64 - Checkpoint sequence number
//! - `timestamp_ms`: u64 - Checkpoint timestamp in milliseconds
//! - `tx_digest`: string - Transaction digest (base58)
//! - `event_index`: u32 - Event index within transaction
//! - `package_id`: string - Package ID (hex)
//! - `module`: string - Module name
//! - `event_type`: string - Event type name
//! - `sender`: string - Transaction sender (hex)
//! - `event_json`: string - JSON payload with event metadata + base64 BCS
//! - `bcs_data`: binary - Raw BCS-encoded event data
//!
//! ## Usage
//!
//! ```bash
//! # Process full blob with caching (recommended)
//! cargo run --release --example mass_indexer_parquet --features parquet-output
//!
//! # First run downloads ~3.2GB blob, subsequent runs read from cache
//! # Expected: ~12k checkpoints, ~500k events, ~30MB parquet output
//!
//! # Filter by package (e.g., DeepBook)
//! cargo run --release --example mass_indexer_parquet --features parquet-output -- \
//!   --package 0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809
//!
//! # Query with Polars
//! python3 -c "import polars as pl; print(pl.scan_parquet('checkpoint_events.parquet').group_by('module','event_type').agg(pl.len()).sort('len',descending=True).head(20).collect())"
//! ```

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BinaryBuilder, StringBuilder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as Base64Engine;
use base64::Engine;
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde_json::json;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use sui_types::base_types::ObjectID;
use sui_types::full_checkpoint_content::CheckpointData;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use sui_types::transaction::TransactionDataAPI;

use walrus_checkpoint_indexing::indexer::{IndexerConfig, MassIndexer, Processor};
use walrus_checkpoint_indexing::Config;

/// Sample blob checkpoint range (blob ID: lJIYRNvG_cgmwx_OT8t7gvdr3rTqG_NsSQPF1714qu0)
const SAMPLE_BLOB_START: u64 = 238954764;
const SAMPLE_BLOB_END: u64 = 238966916;

#[derive(Parser, Debug)]
#[command(name = "mass-indexer-parquet")]
#[command(about = "Mass checkpoint indexer with Parquet output")]
struct Args {
    /// Starting checkpoint number (defaults to sample blob start)
    #[arg(long, default_value_t = SAMPLE_BLOB_START)]
    start: u64,

    /// Ending checkpoint number (defaults to sample blob end + 1)
    #[arg(long, default_value_t = SAMPLE_BLOB_END + 1)]
    end: u64,

    /// Output parquet file path
    #[arg(long, short, default_value = "./checkpoint_events.parquet")]
    output: PathBuf,

    /// Walrus aggregator URL
    #[arg(long, default_value = "https://aggregator.walrus-mainnet.walrus.space")]
    aggregator_url: String,

    /// Path to Walrus CLI binary (enables faster blob download)
    #[arg(long, env = "WALRUS_CLI_PATH")]
    walrus_cli_path: Option<PathBuf>,

    /// Cache directory for downloaded blobs
    #[arg(long, default_value = ".walrus-cache")]
    cache_dir: PathBuf,

    /// Disable blob caching (not recommended - will re-download 3.2GB each run)
    #[arg(long)]
    no_cache: bool,

    /// Watermark file for resumability
    #[arg(long)]
    watermark_file: Option<PathBuf>,

    /// Optional package ID to filter events
    #[arg(long)]
    package: Option<String>,

    /// Log progress every N checkpoints
    #[arg(long, default_value = "1000")]
    log_interval: u64,
}

/// Extracted event data ready for Parquet serialization.
#[derive(Debug, Clone)]
struct EventRow {
    checkpoint_num: u64,
    timestamp_ms: u64,
    tx_digest: String,
    event_index: u32,
    package_id: String,
    module: String,
    event_type: String,
    sender: String,
    event_json: String,
    bcs_data: Vec<u8>,
}

fn build_event_json(
    checkpoint_num: u64,
    timestamp_ms: u64,
    tx_digest: &str,
    event_index: u32,
    package_id: &str,
    module: &str,
    event_type: &str,
    sender: &str,
    bcs_data: &[u8],
) -> Result<String> {
    let value = json!({
        "checkpoint_num": checkpoint_num,
        "timestamp_ms": timestamp_ms,
        "tx_digest": tx_digest,
        "event_index": event_index,
        "package_id": package_id,
        "module": module,
        "event_type": event_type,
        "sender": sender,
        "bcs_data_base64": Base64Engine.encode(bcs_data),
    });
    serde_json::to_string(&value).context("failed to serialize event_json")
}

/// Processor that extracts events and collects them for Parquet output.
struct ParquetProcessor {
    package_filter: Option<ObjectID>,
    output_path: PathBuf,
    buffer: Vec<EventRow>,
    total_checkpoints: u64,
}

impl ParquetProcessor {
    fn new(output_path: PathBuf, package_filter: Option<ObjectID>) -> Self {
        Self {
            package_filter,
            output_path,
            buffer: Vec::with_capacity(500_000), // Pre-allocate for ~500k events
            total_checkpoints: 0,
        }
    }

    fn write_parquet(&self) -> Result<u64> {
        if self.buffer.is_empty() {
            return Ok(0);
        }

        let write_start = Instant::now();

        let schema = Arc::new(Schema::new(vec![
            Field::new("checkpoint_num", DataType::UInt64, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("tx_digest", DataType::Utf8, false),
            Field::new("event_index", DataType::UInt32, false),
            Field::new("package_id", DataType::Utf8, false),
            Field::new("module", DataType::Utf8, false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("sender", DataType::Utf8, false),
            Field::new("event_json", DataType::Utf8, false),
            Field::new("bcs_data", DataType::Binary, false),
        ]));

        let file = File::create(&self.output_path)
            .with_context(|| format!("failed to create output file: {:?}", self.output_path))?;

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        // Build Arrow arrays
        let mut checkpoint_num_builder = UInt64Builder::with_capacity(self.buffer.len());
        let mut timestamp_ms_builder = UInt64Builder::with_capacity(self.buffer.len());
        let mut tx_digest_builder = StringBuilder::with_capacity(self.buffer.len(), self.buffer.len() * 44);
        let mut event_index_builder = UInt32Builder::with_capacity(self.buffer.len());
        let mut package_id_builder = StringBuilder::with_capacity(self.buffer.len(), self.buffer.len() * 66);
        let mut module_builder = StringBuilder::with_capacity(self.buffer.len(), self.buffer.len() * 32);
        let mut event_type_builder = StringBuilder::with_capacity(self.buffer.len(), self.buffer.len() * 64);
        let mut sender_builder = StringBuilder::with_capacity(self.buffer.len(), self.buffer.len() * 66);
        let mut event_json_builder = StringBuilder::with_capacity(self.buffer.len(), self.buffer.len() * 256);
        let mut bcs_data_builder = BinaryBuilder::with_capacity(self.buffer.len(), self.buffer.len() * 256);

        for event in &self.buffer {
            checkpoint_num_builder.append_value(event.checkpoint_num);
            timestamp_ms_builder.append_value(event.timestamp_ms);
            tx_digest_builder.append_value(&event.tx_digest);
            event_index_builder.append_value(event.event_index);
            package_id_builder.append_value(&event.package_id);
            module_builder.append_value(&event.module);
            event_type_builder.append_value(&event.event_type);
            sender_builder.append_value(&event.sender);
            event_json_builder.append_value(&event.event_json);
            bcs_data_builder.append_value(&event.bcs_data);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(checkpoint_num_builder.finish()) as ArrayRef,
                Arc::new(timestamp_ms_builder.finish()) as ArrayRef,
                Arc::new(tx_digest_builder.finish()) as ArrayRef,
                Arc::new(event_index_builder.finish()) as ArrayRef,
                Arc::new(package_id_builder.finish()) as ArrayRef,
                Arc::new(module_builder.finish()) as ArrayRef,
                Arc::new(event_type_builder.finish()) as ArrayRef,
                Arc::new(sender_builder.finish()) as ArrayRef,
                Arc::new(event_json_builder.finish()) as ArrayRef,
                Arc::new(bcs_data_builder.finish()) as ArrayRef,
            ],
        )?;

        writer.write(&batch)?;
        let total = self.buffer.len() as u64;
        writer.close()?;

        let write_elapsed = write_start.elapsed().as_secs_f64();
        println!("  Parquet write time: {:.2}s ({:.0} events/sec)",
                 write_elapsed, total as f64 / write_elapsed);

        Ok(total)
    }
}

#[async_trait]
impl Processor for ParquetProcessor {
    type Output = Vec<EventRow>;

    async fn process(&self, checkpoint: &CheckpointData) -> Result<Self::Output> {
        let checkpoint_num = checkpoint.checkpoint_summary.sequence_number;
        let timestamp_ms = checkpoint.checkpoint_summary.timestamp_ms;

        let mut events = Vec::new();

        for tx in &checkpoint.transactions {
            let tx_digest = format!("{}", tx.transaction.digest());
            let sender = format!("{}", tx.transaction.data().intent_message().value.sender());

            if let Some(tx_events) = &tx.events {
                for (event_index, event) in tx_events.data.iter().enumerate() {
                    let matches_filter = match &self.package_filter {
                        Some(pkg) => &event.package_id == pkg,
                        None => true,
                    };

                    if matches_filter {
                        let package_id = format!("{}", event.package_id);
                        let module = event.transaction_module.to_string();
                        let event_type = event.type_.name.to_string();
                        let event_json = build_event_json(
                            checkpoint_num,
                            timestamp_ms,
                            &tx_digest,
                            event_index as u32,
                            &package_id,
                            &module,
                            &event_type,
                            &sender,
                            &event.contents,
                        )?;

                        events.push(EventRow {
                            checkpoint_num,
                            timestamp_ms,
                            tx_digest: tx_digest.clone(),
                            event_index: event_index as u32,
                            package_id,
                            module,
                            event_type,
                            sender: sender.clone(),
                            event_json,
                            bcs_data: event.contents.clone(),
                        });
                    }
                }
            }
        }

        Ok(events)
    }

    async fn commit(&mut self, _checkpoint_num: CheckpointSequenceNumber, data: Self::Output) -> Result<()> {
        self.buffer.extend(data);
        self.total_checkpoints += 1;
        Ok(())
    }

    async fn on_start(&mut self, start: CheckpointSequenceNumber, end: CheckpointSequenceNumber) -> Result<()> {
        println!("Processing checkpoints {} to {}", start, end);
        if let Some(pkg) = &self.package_filter {
            println!("Filtering by package: {}", pkg);
        }
        Ok(())
    }

    async fn on_complete(&mut self, _total_processed: u64) -> Result<()> {
        println!("\nWriting {} events to parquet...", self.buffer.len());
        let total_events = self.write_parquet()?;
        println!("  Checkpoints processed: {}", self.total_checkpoints);
        println!("  Events written: {}", total_events);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,walrus_checkpoint_indexing=info".into()),
        )
        .init();

    let args = Args::parse();
    let total_start = Instant::now();

    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mass Checkpoint Indexer - Parquet Output");
    println!("═══════════════════════════════════════════════════════════════");
    println!();

    let package_filter: Option<ObjectID> = args.package.as_ref().map(|p| p.parse()).transpose()?;
    let range = args.start..args.end;
    let cache_enabled = !args.no_cache;

    println!("Configuration:");
    println!("  Checkpoint range: {} to {} ({} checkpoints)",
             range.start, range.end, range.end - range.start);
    println!("  Output file: {:?}", args.output);
    if let Some(pkg) = &package_filter {
        println!("  Package filter: {}", pkg);
    } else {
        println!("  Package filter: none (all events)");
    }
    println!("  Cache enabled: {} (dir: {:?})", cache_enabled, args.cache_dir);
    if args.walrus_cli_path.is_some() {
        println!("  Walrus CLI: enabled (faster downloads)");
    }
    println!();

    // Check if blob is already cached
    let blob_id = "lJIYRNvG_cgmwx_OT8t7gvdr3rTqG_NsSQPF1714qu0";
    let cached_blob_path = args.cache_dir.join(blob_id);
    let blob_cached = cached_blob_path.exists();

    if blob_cached {
        let size = std::fs::metadata(&cached_blob_path)?.len();
        println!("Blob already cached: {:?} ({:.2} GB)", cached_blob_path, size as f64 / 1_000_000_000.0);
    } else if cache_enabled {
        println!("Blob not cached - will download ~3.2 GB on first run");
        println!("  (subsequent runs will be much faster)");
    }
    println!();

    // Build storage configuration
    let mut config_builder = Config::builder()
        .aggregator_url(&args.aggregator_url)
        .cache_enabled(cache_enabled)
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

    let processor = ParquetProcessor::new(args.output.clone(), package_filter);
    let mut indexer = MassIndexer::new(indexer_config, processor).await?;

    println!("Starting indexer...");
    println!();

    let stats = indexer.run(range).await?;

    let total_elapsed = total_start.elapsed().as_secs_f64();

    // Print final results
    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Results");
    println!("═══════════════════════════════════════════════════════════════");
    println!();

    println!("Timing:");
    println!("  Total time: {:.2}s", total_elapsed);
    println!("  Processing time: {:.2}s", stats.elapsed_secs);
    println!("  Throughput: {:.0} checkpoints/sec", stats.checkpoints_processed as f64 / stats.elapsed_secs);

    if stats.mb_downloaded() > 0.0 {
        println!("  Data downloaded: {:.2} MB ({:.2} MB/s)",
                 stats.mb_downloaded(),
                 stats.mb_downloaded() / stats.elapsed_secs);
    }
    println!();

    if args.output.exists() {
        let file_size = std::fs::metadata(&args.output)?.len();
        println!("Output:");
        println!("  File: {:?}", args.output);
        println!("  Size: {:.2} MB", file_size as f64 / 1_000_000.0);
        println!();
        println!("Query with Polars:");
        println!("  python3 -c \"import polars as pl; print(pl.scan_parquet('{}').group_by('module','event_type').agg(pl.len()).sort('len',descending=True).head(20).collect())\"", args.output.display());
    }

    println!();

    Ok(())
}
