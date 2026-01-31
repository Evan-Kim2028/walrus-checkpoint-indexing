//! # Walrus Checkpoint Indexer
//!
//! Index Sui blockchain checkpoints from Walrus decentralized storage into queryable Parquet files.
//!
//! ## Quick Start
//!
//! ```bash
//! # Minimal test (1,000 checkpoints, ~30 seconds)
//! cargo run --release --example alt_checkpoint_parquet --features duckdb -- \
//!   --start 239000000 --end 239001000 \
//!   --output-dir ./parquet_output \
//!   --duckdb-summary
//!
//! # Medium run with blob caching (10,000 checkpoints)
//! cargo run --release --example alt_checkpoint_parquet --features duckdb -- \
//!   --start 239000000 --end 239010000 \
//!   --output-dir ./parquet_output \
//!   --cache-enabled \
//!   --duckdb-summary
//!
//! # Large run with parallel blob prefetching (132k checkpoints, ~12 minutes)
//! cargo run --release --example alt_checkpoint_parquet --features duckdb -- \
//!   --start 238954764 --end 239086998 \
//!   --output-dir ./parquet_output \
//!   --cache-enabled \
//!   --parallel-prefetch \
//!   --prefetch-concurrency 4 \
//!   --duckdb-summary
//! ```
//!
//! ## Command Line Options
//!
//! | Option | Description | Default |
//! |--------|-------------|---------|
//! | `--start` | Starting checkpoint (inclusive) | Required |
//! | `--end` | Ending checkpoint (exclusive) | Required |
//! | `--output-dir` | Parquet output directory | `./parquet_output` |
//! | `--cache-enabled` | Cache downloaded blobs locally | false |
//! | `--parallel-prefetch` | Download blobs in parallel before processing | false |
//! | `--prefetch-concurrency` | Number of parallel blob downloads | 4 |
//! | `--spool-mode` | `ephemeral` (temp) or `cache` (persistent) | `ephemeral` |
//! | `--duckdb-summary` | Print data summary after indexing | false |
//!
//! ## Output Tables (10 total)
//!
//! | Table | Description | Key Columns |
//! |-------|-------------|-------------|
//! | `transactions.parquet` | Transaction metadata | tx_digest, sender, gas_used, status |
//! | `events.parquet` | Emitted events | package_id, module, event_type, bcs_data |
//! | `move_calls.parquet` | Function calls | package_id, module, function |
//! | `objects.parquet` | Object changes | object_id, operation, object_type |
//! | `checkpoints.parquet` | Checkpoint metadata | epoch, timestamp_ms, gas totals |
//! | `packages.parquet` | Published packages | package_id, version |
//! | `input_objects.parquet` | Pre-tx object state | object_id, version, owner |
//! | `output_objects.parquet` | Post-tx object state | object_id, version, owner |
//! | `balance_changes.parquet` | Coin transfers | owner, coin_type, amount (+/-) |
//! | `execution_errors.parquet` | Failed transactions | error_type, error_message |
//!
//! ## Querying with DuckDB
//!
//! ```bash
//! # Install DuckDB
//! brew install duckdb  # macOS
//!
//! # Query transactions
//! duckdb -c "SELECT status, COUNT(*) FROM read_parquet('./parquet_output/transactions.parquet') GROUP BY status"
//!
//! # Top move calls
//! duckdb -c "SELECT function, COUNT(*) as calls FROM read_parquet('./parquet_output/move_calls.parquet') GROUP BY function ORDER BY calls DESC LIMIT 10"
//!
//! # Token transfer volume
//! duckdb -c "SELECT SPLIT_PART(coin_type, '::', 3) as token, COUNT(*) as transfers FROM read_parquet('./parquet_output/balance_changes.parquet') GROUP BY token ORDER BY transfers DESC LIMIT 10"
//! ```
//!
//! ## Performance Tips
//!
//! - **Small tests (< 10k checkpoints):** Use default settings
//! - **Larger runs:** Enable `--cache-enabled` to avoid re-downloading blobs
//! - **Best performance:** Add `--parallel-prefetch --prefetch-concurrency 4`
//!
//! Each blob is ~2-3 GB. With 10 blobs, expect ~25 GB cache usage.
//!
//! ## Technical Notes
//!
//! This indexer follows the official `sui-indexer-alt` implementation:
//! - Uses `Coin::extract_balance_if_coin()` for balance changes
//! - Uses `tx.transaction.move_calls()` API for move call extraction
//! - Handles all owner types (Address, Object, Shared, Immutable)

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Int64Builder, StringBuilder, UInt32Builder,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use clap::{Parser, ValueEnum};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use scoped_futures::ScopedBoxFuture;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use sui_types::effects::TransactionEffectsAPI;
use sui_types::transaction::TransactionDataAPI;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use framework::ingestion::{ClientArgs, IngestionConfig};
use framework::pipeline::sequential::{Handler, SequentialConfig};
use framework::pipeline::Processor;
use framework::store::{
    CommitterWatermark, Connection, PrunerWatermark, ReaderWatermark, Store, TransactionalStore,
};
use framework::types::base_types::ObjectID;
use framework::types::full_checkpoint_content::CheckpointData;
use sui_types::full_checkpoint_content::Checkpoint;
use framework::{Indexer, IndexerArgs};
use sui_indexer_alt_framework as framework;
use sui_storage::blob::{Blob, BlobEncoding};
use walrus_checkpoint_indexing::{Config, WalrusStorage};

#[derive(Parser, Debug)]
#[command(name = "alt-checkpoint-parquet")]
#[command(about = "Comprehensive checkpoint indexer -> Multiple Parquet tables")]
struct Args {
    /// Starting checkpoint number (inclusive)
    #[arg(long)]
    start: u64,

    /// Ending checkpoint number (exclusive)
    #[arg(long)]
    end: u64,

    /// Output directory for parquet files
    #[arg(long, short, default_value = "./parquet_output")]
    output_dir: PathBuf,

    /// Optional package ID to filter events (hex string, e.g. 0xabc...)
    #[arg(long)]
    package: Option<String>,

    /// Spool mode: ephemeral (default) or cache
    #[arg(long, value_enum, default_value_t = SpoolMode::Ephemeral)]
    spool_mode: SpoolMode,

    /// Spool directory (optional). If not set, a temp dir is used for ephemeral mode.
    #[arg(long)]
    spool_dir: Option<PathBuf>,

    /// Optional watermark file for the framework (enables resumability)
    #[arg(long)]
    watermark_file: Option<PathBuf>,

    /// Reset watermark file if it exists
    #[arg(long)]
    reset_watermark: bool,

    /// Print DuckDB summary after writing parquet
    #[arg(long)]
    duckdb_summary: bool,

    /// Enable parallel blob prefetching before processing
    #[arg(long)]
    parallel_prefetch: bool,

    /// Concurrency level for parallel blob prefetching (default: 4)
    #[arg(long, default_value = "4")]
    prefetch_concurrency: usize,

    #[command(flatten)]
    walrus: Config,
}

#[derive(ValueEnum, Clone, Debug)]
enum SpoolMode {
    Ephemeral,
    Cache,
}

// =============================================================================
// Row Types for Each Table
// =============================================================================

/// Tier 1: Events table
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
    bcs_data: Vec<u8>,
}

/// Tier 1: Transactions table
#[derive(Debug, Clone)]
struct TransactionRow {
    checkpoint_num: u64,
    timestamp_ms: u64,
    tx_digest: String,
    sender: String,
    gas_used: u64,
    gas_budget: u64,
    gas_price: u64,
    status: String,
    error_message: Option<String>,
    events_count: u32,
    move_calls_count: u32,
    created_count: u32,
    mutated_count: u32,
    deleted_count: u32,
}

/// Tier 1: Move calls table
#[derive(Debug, Clone)]
struct MoveCallRow {
    checkpoint_num: u64,
    timestamp_ms: u64,
    tx_digest: String,
    call_index: u32,
    package_id: String,
    module: String,
    function: String,
}

/// Tier 1: Object changes table
#[derive(Debug, Clone)]
struct ObjectChangeRow {
    checkpoint_num: u64,
    timestamp_ms: u64,
    tx_digest: String,
    object_id: String,
    version: u64,
    operation: String, // "created", "mutated", "deleted", "wrapped", "unwrapped"
    object_type: Option<String>,
    owner: Option<String>,
}

/// Tier 2: Checkpoints table
#[derive(Debug, Clone)]
struct CheckpointRow {
    checkpoint_num: u64,
    timestamp_ms: u64,
    epoch: u64,
    previous_digest: Option<String>,
    transactions_count: u32,
    computation_cost: u64,
    storage_cost: u64,
    storage_rebate: u64,
    non_refundable_storage_fee: u64,
    end_of_epoch: bool,
}

/// Tier 2: Packages table
#[derive(Debug, Clone)]
struct PackageRow {
    checkpoint_num: u64,
    timestamp_ms: u64,
    tx_digest: String,
    package_id: String,
    package_version: u64,
}

/// Tier 3: Input objects table
#[derive(Debug, Clone)]
struct InputObjectRow {
    checkpoint_num: u64,
    timestamp_ms: u64,
    tx_digest: String,
    object_id: String,
    version: u64,
    object_type: Option<String>,
    owner: Option<String>,
}

/// Tier 3: Output objects table
#[derive(Debug, Clone)]
struct OutputObjectRow {
    checkpoint_num: u64,
    timestamp_ms: u64,
    tx_digest: String,
    object_id: String,
    version: u64,
    object_type: Option<String>,
    owner: Option<String>,
}

/// Tier 3: Balance changes table
#[derive(Debug, Clone)]
struct BalanceChangeRow {
    checkpoint_num: u64,
    timestamp_ms: u64,
    tx_digest: String,
    owner: String,
    coin_type: String,
    amount: i64, // Can be negative for decreases
}

/// Tier 3: Execution errors table
#[derive(Debug, Clone)]
struct ExecutionErrorRow {
    checkpoint_num: u64,
    timestamp_ms: u64,
    tx_digest: String,
    error_type: String,
    error_message: String,
}

/// Combined output from processing a checkpoint
#[derive(Debug, Clone, Default)]
struct CheckpointOutput {
    events: Vec<EventRow>,
    transactions: Vec<TransactionRow>,
    move_calls: Vec<MoveCallRow>,
    object_changes: Vec<ObjectChangeRow>,
    checkpoints: Vec<CheckpointRow>,
    packages: Vec<PackageRow>,
    input_objects: Vec<InputObjectRow>,
    output_objects: Vec<OutputObjectRow>,
    balance_changes: Vec<BalanceChangeRow>,
    execution_errors: Vec<ExecutionErrorRow>,
}

// =============================================================================
// Multi-Table Parquet Sink
// =============================================================================

struct MultiParquetSink {
    output_dir: PathBuf,
    events_writer: Mutex<Option<ArrowWriter<File>>>,
    transactions_writer: Mutex<Option<ArrowWriter<File>>>,
    move_calls_writer: Mutex<Option<ArrowWriter<File>>>,
    object_changes_writer: Mutex<Option<ArrowWriter<File>>>,
    checkpoints_writer: Mutex<Option<ArrowWriter<File>>>,
    packages_writer: Mutex<Option<ArrowWriter<File>>>,
    input_objects_writer: Mutex<Option<ArrowWriter<File>>>,
    output_objects_writer: Mutex<Option<ArrowWriter<File>>>,
    balance_changes_writer: Mutex<Option<ArrowWriter<File>>>,
    execution_errors_writer: Mutex<Option<ArrowWriter<File>>>,
}

impl MultiParquetSink {
    fn new(output_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&output_dir)
            .with_context(|| format!("failed to create output dir: {}", output_dir.display()))?;
        Ok(Self {
            output_dir,
            events_writer: Mutex::new(None),
            transactions_writer: Mutex::new(None),
            move_calls_writer: Mutex::new(None),
            object_changes_writer: Mutex::new(None),
            checkpoints_writer: Mutex::new(None),
            packages_writer: Mutex::new(None),
            input_objects_writer: Mutex::new(None),
            output_objects_writer: Mutex::new(None),
            balance_changes_writer: Mutex::new(None),
            execution_errors_writer: Mutex::new(None),
        })
    }

    fn writer_props() -> WriterProperties {
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build()
    }

    // Schema definitions
    fn events_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("checkpoint_num", DataType::UInt64, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("tx_digest", DataType::Utf8, false),
            Field::new("event_index", DataType::UInt32, false),
            Field::new("package_id", DataType::Utf8, false),
            Field::new("module", DataType::Utf8, false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("sender", DataType::Utf8, false),
            Field::new("bcs_data", DataType::Binary, false),
        ]))
    }

    fn transactions_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("checkpoint_num", DataType::UInt64, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("tx_digest", DataType::Utf8, false),
            Field::new("sender", DataType::Utf8, false),
            Field::new("gas_used", DataType::UInt64, false),
            Field::new("gas_budget", DataType::UInt64, false),
            Field::new("gas_price", DataType::UInt64, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("error_message", DataType::Utf8, true),
            Field::new("events_count", DataType::UInt32, false),
            Field::new("move_calls_count", DataType::UInt32, false),
            Field::new("created_count", DataType::UInt32, false),
            Field::new("mutated_count", DataType::UInt32, false),
            Field::new("deleted_count", DataType::UInt32, false),
        ]))
    }

    fn move_calls_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("checkpoint_num", DataType::UInt64, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("tx_digest", DataType::Utf8, false),
            Field::new("call_index", DataType::UInt32, false),
            Field::new("package_id", DataType::Utf8, false),
            Field::new("module", DataType::Utf8, false),
            Field::new("function", DataType::Utf8, false),
        ]))
    }

    fn object_changes_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("checkpoint_num", DataType::UInt64, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("tx_digest", DataType::Utf8, false),
            Field::new("object_id", DataType::Utf8, false),
            Field::new("version", DataType::UInt64, false),
            Field::new("operation", DataType::Utf8, false),
            Field::new("object_type", DataType::Utf8, true),
            Field::new("owner", DataType::Utf8, true),
        ]))
    }

    fn checkpoints_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("checkpoint_num", DataType::UInt64, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("epoch", DataType::UInt64, false),
            Field::new("previous_digest", DataType::Utf8, true),
            Field::new("transactions_count", DataType::UInt32, false),
            Field::new("computation_cost", DataType::UInt64, false),
            Field::new("storage_cost", DataType::UInt64, false),
            Field::new("storage_rebate", DataType::UInt64, false),
            Field::new("non_refundable_storage_fee", DataType::UInt64, false),
            Field::new("end_of_epoch", DataType::Boolean, false),
        ]))
    }

    fn packages_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("checkpoint_num", DataType::UInt64, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("tx_digest", DataType::Utf8, false),
            Field::new("package_id", DataType::Utf8, false),
            Field::new("package_version", DataType::UInt64, false),
        ]))
    }

    fn input_objects_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("checkpoint_num", DataType::UInt64, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("tx_digest", DataType::Utf8, false),
            Field::new("object_id", DataType::Utf8, false),
            Field::new("version", DataType::UInt64, false),
            Field::new("object_type", DataType::Utf8, true),
            Field::new("owner", DataType::Utf8, true),
        ]))
    }

    fn output_objects_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("checkpoint_num", DataType::UInt64, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("tx_digest", DataType::Utf8, false),
            Field::new("object_id", DataType::Utf8, false),
            Field::new("version", DataType::UInt64, false),
            Field::new("object_type", DataType::Utf8, true),
            Field::new("owner", DataType::Utf8, true),
        ]))
    }

    fn balance_changes_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("checkpoint_num", DataType::UInt64, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("tx_digest", DataType::Utf8, false),
            Field::new("owner", DataType::Utf8, false),
            Field::new("coin_type", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]))
    }

    fn execution_errors_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("checkpoint_num", DataType::UInt64, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("tx_digest", DataType::Utf8, false),
            Field::new("error_type", DataType::Utf8, false),
            Field::new("error_message", DataType::Utf8, false),
        ]))
    }

    async fn write_all(&self, output: &CheckpointOutput) -> Result<()> {
        // Write events
        if !output.events.is_empty() {
            self.write_events(&output.events).await?;
        }
        // Write transactions
        if !output.transactions.is_empty() {
            self.write_transactions(&output.transactions).await?;
        }
        // Write move_calls
        if !output.move_calls.is_empty() {
            self.write_move_calls(&output.move_calls).await?;
        }
        // Write object_changes
        if !output.object_changes.is_empty() {
            self.write_object_changes(&output.object_changes).await?;
        }
        // Write checkpoints
        if !output.checkpoints.is_empty() {
            self.write_checkpoints(&output.checkpoints).await?;
        }
        // Write packages
        if !output.packages.is_empty() {
            self.write_packages(&output.packages).await?;
        }
        // Write input_objects
        if !output.input_objects.is_empty() {
            self.write_input_objects(&output.input_objects).await?;
        }
        // Write output_objects
        if !output.output_objects.is_empty() {
            self.write_output_objects(&output.output_objects).await?;
        }
        // Write balance_changes
        if !output.balance_changes.is_empty() {
            self.write_balance_changes(&output.balance_changes).await?;
        }
        // Write execution_errors
        if !output.execution_errors.is_empty() {
            self.write_execution_errors(&output.execution_errors)
                .await?;
        }
        Ok(())
    }

    async fn write_events(&self, rows: &[EventRow]) -> Result<()> {
        let mut guard = self.events_writer.lock().await;
        if guard.is_none() {
            let file = File::create(self.output_dir.join("events.parquet"))?;
            *guard = Some(ArrowWriter::try_new(
                file,
                Self::events_schema(),
                Some(Self::writer_props()),
            )?);
        }

        let schema = Self::events_schema();
        let mut checkpoint_num = UInt64Builder::with_capacity(rows.len());
        let mut timestamp_ms = UInt64Builder::with_capacity(rows.len());
        let mut tx_digest = StringBuilder::with_capacity(rows.len(), rows.len() * 44);
        let mut event_index = UInt32Builder::with_capacity(rows.len());
        let mut package_id = StringBuilder::with_capacity(rows.len(), rows.len() * 66);
        let mut module = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
        let mut event_type = StringBuilder::with_capacity(rows.len(), rows.len() * 64);
        let mut sender = StringBuilder::with_capacity(rows.len(), rows.len() * 66);
        let mut bcs_data = BinaryBuilder::with_capacity(rows.len(), rows.len() * 256);

        for row in rows {
            checkpoint_num.append_value(row.checkpoint_num);
            timestamp_ms.append_value(row.timestamp_ms);
            tx_digest.append_value(&row.tx_digest);
            event_index.append_value(row.event_index);
            package_id.append_value(&row.package_id);
            module.append_value(&row.module);
            event_type.append_value(&row.event_type);
            sender.append_value(&row.sender);
            bcs_data.append_value(&row.bcs_data);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(checkpoint_num.finish()) as ArrayRef,
                Arc::new(timestamp_ms.finish()),
                Arc::new(tx_digest.finish()),
                Arc::new(event_index.finish()),
                Arc::new(package_id.finish()),
                Arc::new(module.finish()),
                Arc::new(event_type.finish()),
                Arc::new(sender.finish()),
                Arc::new(bcs_data.finish()),
            ],
        )?;

        if let Some(writer) = guard.as_mut() {
            writer.write(&batch)?;
        }
        Ok(())
    }

    async fn write_transactions(&self, rows: &[TransactionRow]) -> Result<()> {
        let mut guard = self.transactions_writer.lock().await;
        if guard.is_none() {
            let file = File::create(self.output_dir.join("transactions.parquet"))?;
            *guard = Some(ArrowWriter::try_new(
                file,
                Self::transactions_schema(),
                Some(Self::writer_props()),
            )?);
        }

        let schema = Self::transactions_schema();
        let mut checkpoint_num = UInt64Builder::with_capacity(rows.len());
        let mut timestamp_ms = UInt64Builder::with_capacity(rows.len());
        let mut tx_digest = StringBuilder::with_capacity(rows.len(), rows.len() * 44);
        let mut sender = StringBuilder::with_capacity(rows.len(), rows.len() * 66);
        let mut gas_used = UInt64Builder::with_capacity(rows.len());
        let mut gas_budget = UInt64Builder::with_capacity(rows.len());
        let mut gas_price = UInt64Builder::with_capacity(rows.len());
        let mut status = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
        let mut error_message = StringBuilder::with_capacity(rows.len(), rows.len() * 64);
        let mut events_count = UInt32Builder::with_capacity(rows.len());
        let mut move_calls_count = UInt32Builder::with_capacity(rows.len());
        let mut created_count = UInt32Builder::with_capacity(rows.len());
        let mut mutated_count = UInt32Builder::with_capacity(rows.len());
        let mut deleted_count = UInt32Builder::with_capacity(rows.len());

        for row in rows {
            checkpoint_num.append_value(row.checkpoint_num);
            timestamp_ms.append_value(row.timestamp_ms);
            tx_digest.append_value(&row.tx_digest);
            sender.append_value(&row.sender);
            gas_used.append_value(row.gas_used);
            gas_budget.append_value(row.gas_budget);
            gas_price.append_value(row.gas_price);
            status.append_value(&row.status);
            match &row.error_message {
                Some(msg) => error_message.append_value(msg),
                None => error_message.append_null(),
            }
            events_count.append_value(row.events_count);
            move_calls_count.append_value(row.move_calls_count);
            created_count.append_value(row.created_count);
            mutated_count.append_value(row.mutated_count);
            deleted_count.append_value(row.deleted_count);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(checkpoint_num.finish()) as ArrayRef,
                Arc::new(timestamp_ms.finish()),
                Arc::new(tx_digest.finish()),
                Arc::new(sender.finish()),
                Arc::new(gas_used.finish()),
                Arc::new(gas_budget.finish()),
                Arc::new(gas_price.finish()),
                Arc::new(status.finish()),
                Arc::new(error_message.finish()),
                Arc::new(events_count.finish()),
                Arc::new(move_calls_count.finish()),
                Arc::new(created_count.finish()),
                Arc::new(mutated_count.finish()),
                Arc::new(deleted_count.finish()),
            ],
        )?;

        if let Some(writer) = guard.as_mut() {
            writer.write(&batch)?;
        }
        Ok(())
    }

    async fn write_move_calls(&self, rows: &[MoveCallRow]) -> Result<()> {
        let mut guard = self.move_calls_writer.lock().await;
        if guard.is_none() {
            let file = File::create(self.output_dir.join("move_calls.parquet"))?;
            *guard = Some(ArrowWriter::try_new(
                file,
                Self::move_calls_schema(),
                Some(Self::writer_props()),
            )?);
        }

        let schema = Self::move_calls_schema();
        let mut checkpoint_num = UInt64Builder::with_capacity(rows.len());
        let mut timestamp_ms = UInt64Builder::with_capacity(rows.len());
        let mut tx_digest = StringBuilder::with_capacity(rows.len(), rows.len() * 44);
        let mut call_index = UInt32Builder::with_capacity(rows.len());
        let mut package_id = StringBuilder::with_capacity(rows.len(), rows.len() * 66);
        let mut module = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
        let mut function = StringBuilder::with_capacity(rows.len(), rows.len() * 32);

        for row in rows {
            checkpoint_num.append_value(row.checkpoint_num);
            timestamp_ms.append_value(row.timestamp_ms);
            tx_digest.append_value(&row.tx_digest);
            call_index.append_value(row.call_index);
            package_id.append_value(&row.package_id);
            module.append_value(&row.module);
            function.append_value(&row.function);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(checkpoint_num.finish()) as ArrayRef,
                Arc::new(timestamp_ms.finish()),
                Arc::new(tx_digest.finish()),
                Arc::new(call_index.finish()),
                Arc::new(package_id.finish()),
                Arc::new(module.finish()),
                Arc::new(function.finish()),
            ],
        )?;

        if let Some(writer) = guard.as_mut() {
            writer.write(&batch)?;
        }
        Ok(())
    }

    async fn write_object_changes(&self, rows: &[ObjectChangeRow]) -> Result<()> {
        let mut guard = self.object_changes_writer.lock().await;
        if guard.is_none() {
            let file = File::create(self.output_dir.join("objects.parquet"))?;
            *guard = Some(ArrowWriter::try_new(
                file,
                Self::object_changes_schema(),
                Some(Self::writer_props()),
            )?);
        }

        let schema = Self::object_changes_schema();
        let mut checkpoint_num = UInt64Builder::with_capacity(rows.len());
        let mut timestamp_ms = UInt64Builder::with_capacity(rows.len());
        let mut tx_digest = StringBuilder::with_capacity(rows.len(), rows.len() * 44);
        let mut object_id = StringBuilder::with_capacity(rows.len(), rows.len() * 66);
        let mut version = UInt64Builder::with_capacity(rows.len());
        let mut operation = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
        let mut object_type = StringBuilder::with_capacity(rows.len(), rows.len() * 64);
        let mut owner = StringBuilder::with_capacity(rows.len(), rows.len() * 66);

        for row in rows {
            checkpoint_num.append_value(row.checkpoint_num);
            timestamp_ms.append_value(row.timestamp_ms);
            tx_digest.append_value(&row.tx_digest);
            object_id.append_value(&row.object_id);
            version.append_value(row.version);
            operation.append_value(&row.operation);
            match &row.object_type {
                Some(t) => object_type.append_value(t),
                None => object_type.append_null(),
            }
            match &row.owner {
                Some(o) => owner.append_value(o),
                None => owner.append_null(),
            }
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(checkpoint_num.finish()) as ArrayRef,
                Arc::new(timestamp_ms.finish()),
                Arc::new(tx_digest.finish()),
                Arc::new(object_id.finish()),
                Arc::new(version.finish()),
                Arc::new(operation.finish()),
                Arc::new(object_type.finish()),
                Arc::new(owner.finish()),
            ],
        )?;

        if let Some(writer) = guard.as_mut() {
            writer.write(&batch)?;
        }
        Ok(())
    }

    async fn write_checkpoints(&self, rows: &[CheckpointRow]) -> Result<()> {
        let mut guard = self.checkpoints_writer.lock().await;
        if guard.is_none() {
            let file = File::create(self.output_dir.join("checkpoints.parquet"))?;
            *guard = Some(ArrowWriter::try_new(
                file,
                Self::checkpoints_schema(),
                Some(Self::writer_props()),
            )?);
        }

        let schema = Self::checkpoints_schema();
        let mut checkpoint_num = UInt64Builder::with_capacity(rows.len());
        let mut timestamp_ms = UInt64Builder::with_capacity(rows.len());
        let mut epoch = UInt64Builder::with_capacity(rows.len());
        let mut previous_digest = StringBuilder::with_capacity(rows.len(), rows.len() * 44);
        let mut transactions_count = UInt32Builder::with_capacity(rows.len());
        let mut computation_cost = UInt64Builder::with_capacity(rows.len());
        let mut storage_cost = UInt64Builder::with_capacity(rows.len());
        let mut storage_rebate = UInt64Builder::with_capacity(rows.len());
        let mut non_refundable_storage_fee = UInt64Builder::with_capacity(rows.len());
        let mut end_of_epoch = BooleanBuilder::with_capacity(rows.len());

        for row in rows {
            checkpoint_num.append_value(row.checkpoint_num);
            timestamp_ms.append_value(row.timestamp_ms);
            epoch.append_value(row.epoch);
            match &row.previous_digest {
                Some(d) => previous_digest.append_value(d),
                None => previous_digest.append_null(),
            }
            transactions_count.append_value(row.transactions_count);
            computation_cost.append_value(row.computation_cost);
            storage_cost.append_value(row.storage_cost);
            storage_rebate.append_value(row.storage_rebate);
            non_refundable_storage_fee.append_value(row.non_refundable_storage_fee);
            end_of_epoch.append_value(row.end_of_epoch);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(checkpoint_num.finish()) as ArrayRef,
                Arc::new(timestamp_ms.finish()),
                Arc::new(epoch.finish()),
                Arc::new(previous_digest.finish()),
                Arc::new(transactions_count.finish()),
                Arc::new(computation_cost.finish()),
                Arc::new(storage_cost.finish()),
                Arc::new(storage_rebate.finish()),
                Arc::new(non_refundable_storage_fee.finish()),
                Arc::new(end_of_epoch.finish()),
            ],
        )?;

        if let Some(writer) = guard.as_mut() {
            writer.write(&batch)?;
        }
        Ok(())
    }

    async fn write_packages(&self, rows: &[PackageRow]) -> Result<()> {
        let mut guard = self.packages_writer.lock().await;
        if guard.is_none() {
            let file = File::create(self.output_dir.join("packages.parquet"))?;
            *guard = Some(ArrowWriter::try_new(
                file,
                Self::packages_schema(),
                Some(Self::writer_props()),
            )?);
        }

        let schema = Self::packages_schema();
        let mut checkpoint_num = UInt64Builder::with_capacity(rows.len());
        let mut timestamp_ms = UInt64Builder::with_capacity(rows.len());
        let mut tx_digest = StringBuilder::with_capacity(rows.len(), rows.len() * 44);
        let mut package_id = StringBuilder::with_capacity(rows.len(), rows.len() * 66);
        let mut package_version = UInt64Builder::with_capacity(rows.len());

        for row in rows {
            checkpoint_num.append_value(row.checkpoint_num);
            timestamp_ms.append_value(row.timestamp_ms);
            tx_digest.append_value(&row.tx_digest);
            package_id.append_value(&row.package_id);
            package_version.append_value(row.package_version);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(checkpoint_num.finish()) as ArrayRef,
                Arc::new(timestamp_ms.finish()),
                Arc::new(tx_digest.finish()),
                Arc::new(package_id.finish()),
                Arc::new(package_version.finish()),
            ],
        )?;

        if let Some(writer) = guard.as_mut() {
            writer.write(&batch)?;
        }
        Ok(())
    }

    async fn write_input_objects(&self, rows: &[InputObjectRow]) -> Result<()> {
        let mut guard = self.input_objects_writer.lock().await;
        if guard.is_none() {
            let file = File::create(self.output_dir.join("input_objects.parquet"))?;
            *guard = Some(ArrowWriter::try_new(
                file,
                Self::input_objects_schema(),
                Some(Self::writer_props()),
            )?);
        }

        let schema = Self::input_objects_schema();
        let mut checkpoint_num = UInt64Builder::with_capacity(rows.len());
        let mut timestamp_ms = UInt64Builder::with_capacity(rows.len());
        let mut tx_digest = StringBuilder::with_capacity(rows.len(), rows.len() * 44);
        let mut object_id = StringBuilder::with_capacity(rows.len(), rows.len() * 66);
        let mut version = UInt64Builder::with_capacity(rows.len());
        let mut object_type = StringBuilder::with_capacity(rows.len(), rows.len() * 64);
        let mut owner = StringBuilder::with_capacity(rows.len(), rows.len() * 66);

        for row in rows {
            checkpoint_num.append_value(row.checkpoint_num);
            timestamp_ms.append_value(row.timestamp_ms);
            tx_digest.append_value(&row.tx_digest);
            object_id.append_value(&row.object_id);
            version.append_value(row.version);
            match &row.object_type {
                Some(t) => object_type.append_value(t),
                None => object_type.append_null(),
            }
            match &row.owner {
                Some(o) => owner.append_value(o),
                None => owner.append_null(),
            }
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(checkpoint_num.finish()) as ArrayRef,
                Arc::new(timestamp_ms.finish()),
                Arc::new(tx_digest.finish()),
                Arc::new(object_id.finish()),
                Arc::new(version.finish()),
                Arc::new(object_type.finish()),
                Arc::new(owner.finish()),
            ],
        )?;

        if let Some(writer) = guard.as_mut() {
            writer.write(&batch)?;
        }
        Ok(())
    }

    async fn write_output_objects(&self, rows: &[OutputObjectRow]) -> Result<()> {
        let mut guard = self.output_objects_writer.lock().await;
        if guard.is_none() {
            let file = File::create(self.output_dir.join("output_objects.parquet"))?;
            *guard = Some(ArrowWriter::try_new(
                file,
                Self::output_objects_schema(),
                Some(Self::writer_props()),
            )?);
        }

        let schema = Self::output_objects_schema();
        let mut checkpoint_num = UInt64Builder::with_capacity(rows.len());
        let mut timestamp_ms = UInt64Builder::with_capacity(rows.len());
        let mut tx_digest = StringBuilder::with_capacity(rows.len(), rows.len() * 44);
        let mut object_id = StringBuilder::with_capacity(rows.len(), rows.len() * 66);
        let mut version = UInt64Builder::with_capacity(rows.len());
        let mut object_type = StringBuilder::with_capacity(rows.len(), rows.len() * 64);
        let mut owner = StringBuilder::with_capacity(rows.len(), rows.len() * 66);

        for row in rows {
            checkpoint_num.append_value(row.checkpoint_num);
            timestamp_ms.append_value(row.timestamp_ms);
            tx_digest.append_value(&row.tx_digest);
            object_id.append_value(&row.object_id);
            version.append_value(row.version);
            match &row.object_type {
                Some(t) => object_type.append_value(t),
                None => object_type.append_null(),
            }
            match &row.owner {
                Some(o) => owner.append_value(o),
                None => owner.append_null(),
            }
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(checkpoint_num.finish()) as ArrayRef,
                Arc::new(timestamp_ms.finish()),
                Arc::new(tx_digest.finish()),
                Arc::new(object_id.finish()),
                Arc::new(version.finish()),
                Arc::new(object_type.finish()),
                Arc::new(owner.finish()),
            ],
        )?;

        if let Some(writer) = guard.as_mut() {
            writer.write(&batch)?;
        }
        Ok(())
    }

    async fn write_balance_changes(&self, rows: &[BalanceChangeRow]) -> Result<()> {
        let mut guard = self.balance_changes_writer.lock().await;
        if guard.is_none() {
            let file = File::create(self.output_dir.join("balance_changes.parquet"))?;
            *guard = Some(ArrowWriter::try_new(
                file,
                Self::balance_changes_schema(),
                Some(Self::writer_props()),
            )?);
        }

        let schema = Self::balance_changes_schema();
        let mut checkpoint_num = UInt64Builder::with_capacity(rows.len());
        let mut timestamp_ms = UInt64Builder::with_capacity(rows.len());
        let mut tx_digest = StringBuilder::with_capacity(rows.len(), rows.len() * 44);
        let mut owner = StringBuilder::with_capacity(rows.len(), rows.len() * 66);
        let mut coin_type = StringBuilder::with_capacity(rows.len(), rows.len() * 64);
        let mut amount = Int64Builder::with_capacity(rows.len());

        for row in rows {
            checkpoint_num.append_value(row.checkpoint_num);
            timestamp_ms.append_value(row.timestamp_ms);
            tx_digest.append_value(&row.tx_digest);
            owner.append_value(&row.owner);
            coin_type.append_value(&row.coin_type);
            amount.append_value(row.amount);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(checkpoint_num.finish()) as ArrayRef,
                Arc::new(timestamp_ms.finish()),
                Arc::new(tx_digest.finish()),
                Arc::new(owner.finish()),
                Arc::new(coin_type.finish()),
                Arc::new(amount.finish()),
            ],
        )?;

        if let Some(writer) = guard.as_mut() {
            writer.write(&batch)?;
        }
        Ok(())
    }

    async fn write_execution_errors(&self, rows: &[ExecutionErrorRow]) -> Result<()> {
        let mut guard = self.execution_errors_writer.lock().await;
        if guard.is_none() {
            let file = File::create(self.output_dir.join("execution_errors.parquet"))?;
            *guard = Some(ArrowWriter::try_new(
                file,
                Self::execution_errors_schema(),
                Some(Self::writer_props()),
            )?);
        }

        let schema = Self::execution_errors_schema();
        let mut checkpoint_num = UInt64Builder::with_capacity(rows.len());
        let mut timestamp_ms = UInt64Builder::with_capacity(rows.len());
        let mut tx_digest = StringBuilder::with_capacity(rows.len(), rows.len() * 44);
        let mut error_type = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
        let mut error_message = StringBuilder::with_capacity(rows.len(), rows.len() * 128);

        for row in rows {
            checkpoint_num.append_value(row.checkpoint_num);
            timestamp_ms.append_value(row.timestamp_ms);
            tx_digest.append_value(&row.tx_digest);
            error_type.append_value(&row.error_type);
            error_message.append_value(&row.error_message);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(checkpoint_num.finish()) as ArrayRef,
                Arc::new(timestamp_ms.finish()),
                Arc::new(tx_digest.finish()),
                Arc::new(error_type.finish()),
                Arc::new(error_message.finish()),
            ],
        )?;

        if let Some(writer) = guard.as_mut() {
            writer.write(&batch)?;
        }
        Ok(())
    }

    async fn close_all(&self) -> Result<()> {
        macro_rules! close_writer {
            ($writer:expr) => {
                let mut guard = $writer.lock().await;
                if let Some(writer) = guard.take() {
                    writer.close()?;
                }
            };
        }

        close_writer!(self.events_writer);
        close_writer!(self.transactions_writer);
        close_writer!(self.move_calls_writer);
        close_writer!(self.object_changes_writer);
        close_writer!(self.checkpoints_writer);
        close_writer!(self.packages_writer);
        close_writer!(self.input_objects_writer);
        close_writer!(self.output_objects_writer);
        close_writer!(self.balance_changes_writer);
        close_writer!(self.execution_errors_writer);

        Ok(())
    }
}

// =============================================================================
// Pipeline Implementation
// =============================================================================

struct ComprehensivePipeline {
    package_filter: Option<ObjectID>,
    sink: Arc<MultiParquetSink>,
}

#[async_trait::async_trait]
impl Processor for ComprehensivePipeline {
    const NAME: &'static str = "comprehensive_indexer";

    type Value = CheckpointOutput;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> Result<Vec<Self::Value>> {
        let checkpoint_num = checkpoint.summary.sequence_number;
        let timestamp_ms = checkpoint.summary.timestamp_ms;
        let epoch = checkpoint.summary.epoch;

        let mut output = CheckpointOutput::default();

        // Tier 2: Checkpoint metadata
        let gas_summary = checkpoint.summary.epoch_rolling_gas_cost_summary.clone();
        output.checkpoints.push(CheckpointRow {
            checkpoint_num,
            timestamp_ms,
            epoch,
            previous_digest: checkpoint
                .summary
                .previous_digest
                .map(|d| format!("{}", d)),
            transactions_count: checkpoint.transactions.len() as u32,
            computation_cost: gas_summary.computation_cost,
            storage_cost: gas_summary.storage_cost,
            storage_rebate: gas_summary.storage_rebate,
            non_refundable_storage_fee: gas_summary.non_refundable_storage_fee,
            end_of_epoch: checkpoint.summary.end_of_epoch_data.is_some(),
        });

        // Process each transaction
        for tx in &checkpoint.transactions {
            let tx_digest = format!("{}", tx.effects.transaction_digest());
            let tx_data = &tx.transaction;
            let sender = format!("{}", tx_data.sender());
            let effects = &tx.effects;

            // Get gas info
            let gas_summary = effects.gas_cost_summary();
            let gas_used =
                gas_summary.computation_cost + gas_summary.storage_cost - gas_summary.storage_rebate;

            // Check execution status
            let (status, error_message) = match effects.status() {
                sui_types::execution_status::ExecutionStatus::Success => {
                    ("success".to_string(), None)
                }
                sui_types::execution_status::ExecutionStatus::Failure { error, command } => {
                    let err_msg = format!("{:?} at command {:?}", error, command);
                    output.execution_errors.push(ExecutionErrorRow {
                        checkpoint_num,
                        timestamp_ms,
                        tx_digest: tx_digest.clone(),
                        error_type: format!("{:?}", error),
                        error_message: err_msg.clone(),
                    });
                    ("failure".to_string(), Some(err_msg))
                }
            };

            // Count effects
            let created_count = effects.created().len() as u32;
            let mutated_count = effects.mutated().len() as u32;
            let deleted_count = effects.deleted().len() as u32;

            // Extract Move calls using the official API method
            let calls = tx_data.move_calls();
            let move_calls_count = calls.len() as u32;
            for (idx, package, module, function) in calls {
                output.move_calls.push(MoveCallRow {
                    checkpoint_num,
                    timestamp_ms,
                    tx_digest: tx_digest.clone(),
                    call_index: idx as u32,
                    package_id: format!("{}", package),
                    module: module.to_string(),
                    function: function.to_string(),
                });
            }

            // Events count
            let events_count = tx.events.as_ref().map(|e| e.data.len()).unwrap_or(0) as u32;

            // Tier 1: Transaction row
            output.transactions.push(TransactionRow {
                checkpoint_num,
                timestamp_ms,
                tx_digest: tx_digest.clone(),
                sender: sender.clone(),
                gas_used,
                gas_budget: tx_data.gas_budget(),
                gas_price: tx_data.gas_price(),
                status,
                error_message,
                events_count,
                move_calls_count,
                created_count,
                mutated_count,
                deleted_count,
            });

            // Tier 1: Events
            if let Some(tx_events) = &tx.events {
                for (event_index, event) in tx_events.data.iter().enumerate() {
                    let matches_filter = match &self.package_filter {
                        Some(pkg) => &event.package_id == pkg,
                        None => true,
                    };

                    if matches_filter {
                        output.events.push(EventRow {
                            checkpoint_num,
                            timestamp_ms,
                            tx_digest: tx_digest.clone(),
                            event_index: event_index as u32,
                            package_id: format!("{}", event.package_id),
                            module: event.transaction_module.to_string(),
                            event_type: event.type_.name.to_string(),
                            sender: format!("{}", event.sender),
                            bcs_data: event.contents.clone(),
                        });
                    }
                }
            }

            // Tier 1: Object changes from effects
            // Look up object types from the object_set for created/mutated objects
            use sui_types::storage::ObjectKey;

            for (obj_ref, owner) in effects.created() {
                let object_type = checkpoint
                    .object_set
                    .get(&ObjectKey(obj_ref.0, obj_ref.1))
                    .and_then(|obj| obj.type_().map(|t| t.to_string()));
                output.object_changes.push(ObjectChangeRow {
                    checkpoint_num,
                    timestamp_ms,
                    tx_digest: tx_digest.clone(),
                    object_id: format!("{}", obj_ref.0),
                    version: obj_ref.1.value(),
                    operation: "created".to_string(),
                    object_type,
                    owner: Some(format!("{:?}", owner)),
                });
            }

            for (obj_ref, owner) in effects.mutated() {
                let object_type = checkpoint
                    .object_set
                    .get(&ObjectKey(obj_ref.0, obj_ref.1))
                    .and_then(|obj| obj.type_().map(|t| t.to_string()));
                output.object_changes.push(ObjectChangeRow {
                    checkpoint_num,
                    timestamp_ms,
                    tx_digest: tx_digest.clone(),
                    object_id: format!("{}", obj_ref.0),
                    version: obj_ref.1.value(),
                    operation: "mutated".to_string(),
                    object_type,
                    owner: Some(format!("{:?}", owner)),
                });
            }

            // For deleted objects, look up the pre-deletion version from input objects
            for obj_ref in effects.deleted() {
                // Deleted objects won't be in output, try to find type from input objects
                let object_type = tx
                    .input_objects(&checkpoint.object_set)
                    .find(|o| o.id() == obj_ref.0)
                    .and_then(|obj| obj.type_().map(|t| t.to_string()));
                output.object_changes.push(ObjectChangeRow {
                    checkpoint_num,
                    timestamp_ms,
                    tx_digest: tx_digest.clone(),
                    object_id: format!("{}", obj_ref.0),
                    version: obj_ref.1.value(),
                    operation: "deleted".to_string(),
                    object_type,
                    owner: None,
                });
            }

            // For wrapped objects, look up from input objects (pre-wrap state)
            for obj_ref in effects.wrapped() {
                let object_type = tx
                    .input_objects(&checkpoint.object_set)
                    .find(|o| o.id() == obj_ref.0)
                    .and_then(|obj| obj.type_().map(|t| t.to_string()));
                output.object_changes.push(ObjectChangeRow {
                    checkpoint_num,
                    timestamp_ms,
                    tx_digest: tx_digest.clone(),
                    object_id: format!("{}", obj_ref.0),
                    version: obj_ref.1.value(),
                    operation: "wrapped".to_string(),
                    object_type,
                    owner: None,
                });
            }

            for (obj_ref, owner) in effects.unwrapped() {
                let object_type = checkpoint
                    .object_set
                    .get(&ObjectKey(obj_ref.0, obj_ref.1))
                    .and_then(|obj| obj.type_().map(|t| t.to_string()));
                output.object_changes.push(ObjectChangeRow {
                    checkpoint_num,
                    timestamp_ms,
                    tx_digest: tx_digest.clone(),
                    object_id: format!("{}", obj_ref.0),
                    version: obj_ref.1.value(),
                    operation: "unwrapped".to_string(),
                    object_type,
                    owner: Some(format!("{:?}", owner)),
                });
            }

            // Tier 2: Packages (check output objects for packages)
            for obj in tx.output_objects(&checkpoint.object_set) {
                if obj.is_package() {
                    output.packages.push(PackageRow {
                        checkpoint_num,
                        timestamp_ms,
                        tx_digest: tx_digest.clone(),
                        package_id: format!("{}", obj.id()),
                        package_version: obj.version().value(),
                    });
                }
            }

            // Tier 3: Input objects
            for obj in tx.input_objects(&checkpoint.object_set) {
                output.input_objects.push(InputObjectRow {
                    checkpoint_num,
                    timestamp_ms,
                    tx_digest: tx_digest.clone(),
                    object_id: format!("{}", obj.id()),
                    version: obj.version().value(),
                    object_type: obj.type_().map(|t| t.to_string()),
                    owner: Some(format!("{:?}", obj.owner())),
                });
            }

            // Tier 3: Output objects
            for obj in tx.output_objects(&checkpoint.object_set) {
                output.output_objects.push(OutputObjectRow {
                    checkpoint_num,
                    timestamp_ms,
                    tx_digest: tx_digest.clone(),
                    object_id: format!("{}", obj.id()),
                    version: obj.version().value(),
                    object_type: obj.type_().map(|t| t.to_string()),
                    owner: Some(format!("{:?}", obj.owner())),
                });
            }

            // Tier 3: Balance changes - using the same algorithm as sui-indexer-alt
            // See: sui-indexer-alt/src/handlers/tx_balance_changes.rs
            let balance_changes = compute_balance_changes(tx, checkpoint)?;
            for (owner, coin_type, amount) in balance_changes {
                output.balance_changes.push(BalanceChangeRow {
                    checkpoint_num,
                    timestamp_ms,
                    tx_digest: tx_digest.clone(),
                    owner,
                    coin_type,
                    amount,
                });
            }
        }

        Ok(vec![output])
    }
}

#[async_trait::async_trait]
impl Handler for ComprehensivePipeline {
    type Store = LocalStore;
    type Batch = Vec<CheckpointOutput>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        batch.extend(values);
    }

    async fn commit<'a>(
        &self,
        batch: &Self::Batch,
        _conn: &mut <Self::Store as Store>::Connection<'a>,
    ) -> anyhow::Result<usize> {
        let mut total = 0;
        for output in batch {
            self.sink.write_all(output).await?;
            total += output.events.len()
                + output.transactions.len()
                + output.move_calls.len()
                + output.object_changes.len();
        }
        Ok(total)
    }
}

// =============================================================================
// Local Store (for watermark tracking)
// =============================================================================

struct SpoolHandle {
    dir: PathBuf,
    _temp: Option<TempDir>,
    cache_mode: bool,
}

impl SpoolHandle {
    fn new(mode: SpoolMode, dir: Option<PathBuf>) -> Result<Self> {
        match mode {
            SpoolMode::Cache => {
                let path = dir.unwrap_or_else(|| PathBuf::from("./checkpoint-spool"));
                std::fs::create_dir_all(&path)
                    .with_context(|| format!("failed to create spool dir: {}", path.display()))?;
                Ok(Self {
                    dir: path,
                    _temp: None,
                    cache_mode: true,
                })
            }
            SpoolMode::Ephemeral => {
                if let Some(path) = dir {
                    std::fs::create_dir_all(&path).with_context(|| {
                        format!("failed to create spool dir: {}", path.display())
                    })?;
                    Ok(Self {
                        dir: path,
                        _temp: None,
                        cache_mode: false,
                    })
                } else {
                    let temp = TempDir::new().context("failed to create temp spool dir")?;
                    Ok(Self {
                        dir: temp.path().to_path_buf(),
                        _temp: Some(temp),
                        cache_mode: false,
                    })
                }
            }
        }
    }
}

#[derive(Clone)]
struct LocalStore {
    state: Arc<Mutex<WatermarkState>>,
    path: Option<PathBuf>,
}

#[derive(Default, Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WatermarkState {
    entries: HashMap<String, WatermarkEntry>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WatermarkEntry {
    epoch_hi_inclusive: u64,
    checkpoint_hi_inclusive: u64,
    tx_hi: u64,
    timestamp_ms_hi_inclusive: u64,
    reader_lo: u64,
    pruner_hi: u64,
    pruner_timestamp_ms: u64,
}

impl WatermarkEntry {
    fn committer(&self) -> CommitterWatermark {
        CommitterWatermark {
            epoch_hi_inclusive: self.epoch_hi_inclusive,
            checkpoint_hi_inclusive: self.checkpoint_hi_inclusive,
            tx_hi: self.tx_hi,
            timestamp_ms_hi_inclusive: self.timestamp_ms_hi_inclusive,
        }
    }
}

impl LocalStore {
    fn new(path: Option<PathBuf>) -> Result<Self> {
        let state = if let Some(path) = &path {
            if path.exists() {
                let data = std::fs::read_to_string(path).with_context(|| {
                    format!("failed to read watermark file: {}", path.display())
                })?;
                serde_json::from_str::<WatermarkState>(&data).with_context(|| {
                    format!("failed to parse watermark file: {}", path.display())
                })?
            } else {
                WatermarkState::default()
            }
        } else {
            WatermarkState::default()
        };

        Ok(Self {
            state: Arc::new(Mutex::new(state)),
            path,
        })
    }
}

struct LocalConnection {
    state: Arc<Mutex<WatermarkState>>,
    path: Option<PathBuf>,
}

#[async_trait::async_trait]
impl Connection for LocalConnection {
    async fn init_watermark(
        &mut self,
        pipeline_task: &str,
        default_next_checkpoint: u64,
    ) -> anyhow::Result<Option<u64>> {
        let mut state = self.state.lock().await;

        if let Some(entry) = state.entries.get(pipeline_task) {
            return Ok(Some(entry.checkpoint_hi_inclusive));
        }

        let Some(checkpoint_hi_inclusive) = default_next_checkpoint.checked_sub(1) else {
            return Ok(None);
        };

        let now_ms = now_ms();
        state.entries.insert(
            pipeline_task.to_string(),
            WatermarkEntry {
                epoch_hi_inclusive: 0,
                checkpoint_hi_inclusive,
                tx_hi: 0,
                timestamp_ms_hi_inclusive: 0,
                reader_lo: default_next_checkpoint,
                pruner_hi: default_next_checkpoint,
                pruner_timestamp_ms: now_ms,
            },
        );

        persist_state(&self.path, &state).await?;
        Ok(Some(checkpoint_hi_inclusive))
    }

    async fn committer_watermark(
        &mut self,
        pipeline_task: &str,
    ) -> anyhow::Result<Option<CommitterWatermark>> {
        let state = self.state.lock().await;
        Ok(state
            .entries
            .get(pipeline_task)
            .map(|entry| entry.committer()))
    }

    async fn reader_watermark(
        &mut self,
        pipeline: &'static str,
    ) -> anyhow::Result<Option<ReaderWatermark>> {
        let state = self.state.lock().await;
        Ok(state.entries.get(pipeline).map(|entry| ReaderWatermark {
            checkpoint_hi_inclusive: entry.checkpoint_hi_inclusive,
            reader_lo: entry.reader_lo,
        }))
    }

    async fn pruner_watermark(
        &mut self,
        pipeline: &'static str,
        delay: Duration,
    ) -> anyhow::Result<Option<PrunerWatermark>> {
        let state = self.state.lock().await;
        let Some(entry) = state.entries.get(pipeline) else {
            return Ok(None);
        };
        let now = now_ms() as i64;
        let target = entry.pruner_timestamp_ms as i64 + delay.as_millis() as i64;
        let wait_for_ms = target - now;

        Ok(Some(PrunerWatermark {
            wait_for_ms,
            pruner_hi: entry.pruner_hi,
            reader_lo: entry.reader_lo,
        }))
    }

    async fn set_committer_watermark(
        &mut self,
        pipeline_task: &str,
        watermark: CommitterWatermark,
    ) -> anyhow::Result<bool> {
        let mut state = self.state.lock().await;
        let entry = state
            .entries
            .entry(pipeline_task.to_string())
            .or_insert_with(|| WatermarkEntry {
                epoch_hi_inclusive: 0,
                checkpoint_hi_inclusive: 0,
                tx_hi: 0,
                timestamp_ms_hi_inclusive: 0,
                reader_lo: 0,
                pruner_hi: 0,
                pruner_timestamp_ms: now_ms(),
            });

        if watermark.checkpoint_hi_inclusive <= entry.checkpoint_hi_inclusive {
            return Ok(false);
        }

        entry.epoch_hi_inclusive = watermark.epoch_hi_inclusive;
        entry.checkpoint_hi_inclusive = watermark.checkpoint_hi_inclusive;
        entry.tx_hi = watermark.tx_hi;
        entry.timestamp_ms_hi_inclusive = watermark.timestamp_ms_hi_inclusive;

        persist_state(&self.path, &state).await?;
        Ok(true)
    }

    async fn set_reader_watermark(
        &mut self,
        pipeline: &'static str,
        reader_lo: u64,
    ) -> anyhow::Result<bool> {
        let mut state = self.state.lock().await;
        let entry = state
            .entries
            .entry(pipeline.to_string())
            .or_insert_with(|| WatermarkEntry {
                epoch_hi_inclusive: 0,
                checkpoint_hi_inclusive: 0,
                tx_hi: 0,
                timestamp_ms_hi_inclusive: 0,
                reader_lo: 0,
                pruner_hi: 0,
                pruner_timestamp_ms: now_ms(),
            });

        if reader_lo <= entry.reader_lo {
            return Ok(false);
        }

        entry.reader_lo = reader_lo;
        entry.pruner_timestamp_ms = now_ms();

        persist_state(&self.path, &state).await?;
        Ok(true)
    }

    async fn set_pruner_watermark(
        &mut self,
        pipeline: &'static str,
        pruner_hi: u64,
    ) -> anyhow::Result<bool> {
        let mut state = self.state.lock().await;
        let entry = state
            .entries
            .entry(pipeline.to_string())
            .or_insert_with(|| WatermarkEntry {
                epoch_hi_inclusive: 0,
                checkpoint_hi_inclusive: 0,
                tx_hi: 0,
                timestamp_ms_hi_inclusive: 0,
                reader_lo: 0,
                pruner_hi: 0,
                pruner_timestamp_ms: now_ms(),
            });

        if pruner_hi <= entry.pruner_hi {
            return Ok(false);
        }

        entry.pruner_hi = pruner_hi;
        persist_state(&self.path, &state).await?;
        Ok(true)
    }
}

async fn persist_state(path: &Option<PathBuf>, state: &WatermarkState) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };

    let tmp_path = path.with_extension("tmp");
    let data = serde_json::to_string_pretty(state)?;
    tokio::fs::write(&tmp_path, data).await.with_context(|| {
        format!(
            "failed to write watermark temp file: {}",
            tmp_path.display()
        )
    })?;
    tokio::fs::rename(&tmp_path, path).await.with_context(|| {
        format!(
            "failed to move watermark file into place: {}",
            path.display()
        )
    })?;
    Ok(())
}

#[async_trait::async_trait]
impl Store for LocalStore {
    type Connection<'c>
        = LocalConnection
    where
        Self: 'c;

    async fn connect<'c>(&'c self) -> anyhow::Result<Self::Connection<'c>> {
        Ok(LocalConnection {
            state: self.state.clone(),
            path: self.path.clone(),
        })
    }
}

#[async_trait::async_trait]
impl TransactionalStore for LocalStore {
    async fn transaction<'a, R, F>(&self, f: F) -> anyhow::Result<R>
    where
        R: Send + 'a,
        F: Send + 'a,
        F: for<'r> FnOnce(
            &'r mut Self::Connection<'_>,
        ) -> ScopedBoxFuture<'a, 'r, anyhow::Result<R>>,
    {
        let mut conn = self.connect().await?;
        f(&mut conn).await
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn checkpoint_to_chk_bytes(checkpoint_data: CheckpointData) -> Result<Vec<u8>> {
    let blob = Blob::encode(&checkpoint_data, BlobEncoding::Bcs)
        .context("failed to encode checkpoint blob")?;
    Ok(blob.to_bytes())
}

fn validate_spool_file(path: &PathBuf) -> Result<()> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read spool file: {}", path.display()))?;
    let _checkpoint: CheckpointData = Blob::from_bytes(&bytes)
        .with_context(|| format!("failed to decode checkpoint blob: {}", path.display()))?;
    Ok(())
}

async fn spool_checkpoints(
    storage: &WalrusStorage,
    range: std::ops::Range<u64>,
    spool: &SpoolHandle,
) -> Result<()> {
    std::fs::create_dir_all(&spool.dir)
        .with_context(|| format!("failed to create spool dir: {}", spool.dir.display()))?;

    let written = Arc::new(AtomicU64::new(0));
    let skipped = Arc::new(AtomicU64::new(0));

    let spool_dir = spool.dir.clone();
    let cache_mode = spool.cache_mode;

    storage
        .stream_checkpoints(range, |checkpoint| {
            let spool_dir = spool_dir.clone();
            let written = written.clone();
            let skipped = skipped.clone();
            async move {
                let seq = checkpoint.checkpoint_summary.sequence_number;
                let path = spool_dir.join(format!("{}.chk", seq));

                if cache_mode && path.exists() {
                    skipped.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }

                let bytes = checkpoint_to_chk_bytes(checkpoint)?;
                tokio::fs::write(&path, bytes).await.with_context(|| {
                    format!("failed to write checkpoint file: {}", path.display())
                })?;

                written.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        })
        .await?;

    println!(
        "Spool complete: {} written, {} skipped (dir: {})",
        written.load(Ordering::Relaxed),
        skipped.load(Ordering::Relaxed),
        spool.dir.display()
    );

    Ok(())
}

/// Compute balance changes for a transaction using the same algorithm as sui-indexer-alt.
/// Returns Vec<(owner_string, coin_type_string, amount_i64)>
///
/// This properly handles:
/// - Failed transactions (only gas is charged)
/// - Coin balance extraction via BCS deserialization
/// - Owner formatting for different owner types
fn compute_balance_changes(
    tx: &sui_types::full_checkpoint_content::ExecutedTransaction,
    checkpoint: &Checkpoint,
) -> Result<Vec<(String, String, i64)>> {
    use std::collections::BTreeMap;
    use sui_types::coin::Coin;
    use sui_types::gas_coin::GAS;

    // For failed transactions, only gas is charged
    if tx.effects.status().is_err() {
        let net_gas_usage = tx.effects.gas_cost_summary().net_gas_usage();
        if net_gas_usage > 0 {
            let gas_owner = format_owner(&tx.effects.gas_object().1);
            let coin_type = GAS::type_tag().to_canonical_string(true);
            return Ok(vec![(gas_owner, coin_type, -net_gas_usage)]);
        }
        return Ok(vec![]);
    }

    let mut changes: BTreeMap<(String, String), i128> = BTreeMap::new();

    // Subtract input coin balances
    for object in tx.input_objects(&checkpoint.object_set) {
        if let Some((type_tag, balance)) = Coin::extract_balance_if_coin(object)
            .context("Failed to extract coin balance from input object")?
        {
            let owner = format_owner(object.owner());
            let coin_type = type_tag.to_canonical_string(true);
            *changes.entry((owner, coin_type)).or_insert(0i128) -= balance as i128;
        }
    }

    // Add output coin balances
    for object in tx.output_objects(&checkpoint.object_set) {
        if let Some((type_tag, balance)) = Coin::extract_balance_if_coin(object)
            .context("Failed to extract coin balance from output object")?
        {
            let owner = format_owner(object.owner());
            let coin_type = type_tag.to_canonical_string(true);
            *changes.entry((owner, coin_type)).or_insert(0i128) += balance as i128;
        }
    }

    // Convert to output format (matching official: keep all entries including zero changes)
    // Note: amount as i64 may overflow for extremely large values
    // The official indexer uses i128 in the database, but for parquet
    // we use i64 which handles values up to ~9.2 quintillion
    Ok(changes
        .into_iter()
        .map(|((owner, coin_type), amount)| (owner, coin_type, amount as i64))
        .collect())
}

/// Format owner for display, matching the official indexer's format
fn format_owner(owner: &sui_types::object::Owner) -> String {
    match owner {
        sui_types::object::Owner::AddressOwner(addr) => format!("{}", addr),
        sui_types::object::Owner::ObjectOwner(id) => format!("object:{}", id),
        sui_types::object::Owner::Shared { .. } => "shared".to_string(),
        sui_types::object::Owner::Immutable => "immutable".to_string(),
        sui_types::object::Owner::ConsensusAddressOwner { owner, .. } => {
            format!("consensus:{}", owner)
        }
    }
}

fn parse_package(package: &Option<String>) -> Result<Option<ObjectID>> {
    match package {
        Some(pkg) => {
            let id = ObjectID::from_hex_literal(pkg)
                .with_context(|| format!("invalid package id: {}", pkg))?;
            Ok(Some(id))
        }
        None => Ok(None),
    }
}

#[cfg(feature = "duckdb")]
fn duckdb_summary(output_dir: &Path) -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()
        .context("failed to open in-memory DuckDB connection")?;

    let tables = [
        "events",
        "transactions",
        "move_calls",
        "objects",
        "checkpoints",
        "packages",
        "input_objects",
        "output_objects",
        "balance_changes",
        "execution_errors",
    ];

    println!("\n=== DuckDB Summary ===\n");

    for table in tables {
        let path = output_dir.join(format!("{}.parquet", table));
        if path.exists() {
            let path_str = path.to_str().unwrap().replace('\'', "''");
            let count_query = format!("SELECT COUNT(*) FROM read_parquet('{}')", path_str);
            match conn.query_row(&count_query, [], |row| row.get::<_, i64>(0)) {
                Ok(count) => println!("{}: {} rows", table, count),
                Err(e) => println!("{}: error - {}", table, e),
            }
        } else {
            println!("{}: (no data)", table);
        }
    }

    Ok(())
}

#[cfg(not(feature = "duckdb"))]
fn duckdb_summary(_output_dir: &Path) -> Result<()> {
    anyhow::bail!("duckdb feature not enabled; rebuild with --features duckdb")
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    if args.start >= args.end {
        anyhow::bail!("start must be < end");
    }

    args.walrus.validate()?;

    if args.reset_watermark {
        if let Some(path) = &args.watermark_file {
            if path.exists() {
                std::fs::remove_file(path).with_context(|| {
                    format!("failed to remove watermark file: {}", path.display())
                })?;
            }
        }
    }

    let spool = SpoolHandle::new(args.spool_mode, args.spool_dir.clone())?;
    println!("Spooling checkpoints to: {}", spool.dir.display());

    let storage = WalrusStorage::new(args.walrus.clone()).await?;
    storage.initialize().await?;

    // Parallel blob prefetching if enabled
    if args.parallel_prefetch {
        let blobs = storage.get_blobs_for_range(args.start..args.end).await;
        println!(
            "Prefetching {} blobs in parallel (concurrency: {})...",
            blobs.len(),
            args.prefetch_concurrency
        );
        let blob_ids: Vec<String> = blobs.iter().map(|b| b.blob_id.clone()).collect();
        let prefetch_start = std::time::Instant::now();
        let prefetched = storage
            .prefetch_blobs_parallel(&blob_ids, args.prefetch_concurrency)
            .await?;
        let prefetch_elapsed = prefetch_start.elapsed();
        println!(
            "Prefetched {}/{} blobs in {:.2}s",
            prefetched,
            blobs.len(),
            prefetch_elapsed.as_secs_f64()
        );
    }

    spool_checkpoints(&storage, args.start..args.end, &spool).await?;

    let first_path = spool.dir.join(format!("{}.chk", args.start));
    let last_path = spool.dir.join(format!("{}.chk", args.end - 1));
    validate_spool_file(&first_path)?;
    validate_spool_file(&last_path)?;

    let package_filter = parse_package(&args.package)?;
    let sink = Arc::new(MultiParquetSink::new(args.output_dir.clone())?);
    let pipeline = ComprehensivePipeline {
        package_filter,
        sink: sink.clone(),
    };

    let store = LocalStore::new(args.watermark_file.clone())?;

    let mut indexer = Indexer::new(
        store,
        IndexerArgs {
            first_checkpoint: Some(args.start),
            last_checkpoint: Some(args.end - 1),
            pipeline: Vec::new(),
            task: framework::TaskArgs::default(),
        },
        ClientArgs {
            ingestion: framework::ingestion::ingestion_client::IngestionClientArgs {
                local_ingestion_path: Some(spool.dir.clone()),
                ..Default::default()
            },
            streaming: framework::ingestion::streaming_client::StreamingClientArgs::default(),
        },
        IngestionConfig::default(),
        None,
        &prometheus::Registry::new(),
    )
    .await?;

    indexer
        .sequential_pipeline(pipeline, SequentialConfig::default())
        .await?;

    let mut service = indexer.run().await?;
    service.join().await?;

    sink.close_all().await?;

    println!("\n=== Output Files ===");
    println!("Directory: {}", args.output_dir.display());
    for entry in std::fs::read_dir(&args.output_dir)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        let size_kb = metadata.len() as f64 / 1024.0;
        println!("  {} ({:.1} KB)", entry.file_name().to_string_lossy(), size_kb);
    }

    if args.duckdb_summary {
        duckdb_summary(&args.output_dir)?;
    }

    Ok(())
}
