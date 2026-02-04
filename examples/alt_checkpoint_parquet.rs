//! # Walrus Checkpoint Indexer
//!
//! Index Sui blockchain checkpoints from Walrus decentralized storage into queryable Parquet files.
//!
//! ## Quick Start
//!
//! ```bash
//! # Minimal test (1,000 checkpoints)
//! cargo run --release --example alt_checkpoint_parquet --features duckdb -- \
//!   --start 239000000 --end 239001000 \
//!   --output-dir ./parquet_output
//!
//! # Medium run with blob caching (10,000 checkpoints)
//! cargo run --release --example alt_checkpoint_parquet --features duckdb -- \
//!   --start 239000000 --end 239010000 \
//!   --output-dir ./parquet_output \
//!   --cache-enabled
//!
//! # Large run with parallel blob prefetching and progress bars
//! cargo run --release --example alt_checkpoint_parquet --features duckdb -- \
//!   --start 238954764 --end 239086998 \
//!   --output-dir ./parquet_output \
//!   --cache-enabled \
//!   --parallel-prefetch \
//!   --prefetch-concurrency 4 \
//!   --show-progress
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
//! | `--show-progress` | Show download progress bars | false |
//! | `--spool-mode` | `ephemeral` (temp) or `cache` (persistent) | `ephemeral` |
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
use move_binary_format::file_format::{Ability, AbilitySet, Visibility};
use move_binary_format::CompiledModule;
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
use framework::{Indexer, IndexerArgs};
use sui_indexer_alt_framework as framework;
use sui_storage::blob::{Blob, BlobEncoding};
use sui_types::full_checkpoint_content::Checkpoint;
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

    /// Optional package ID to filter output to transactions touching this package
    /// (events/move_calls filtered to package; tx/object/balance rows limited to matching txs)
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

    /// Enable parallel blob prefetching before processing
    #[arg(long)]
    parallel_prefetch: bool,

    /// Concurrency level for parallel blob prefetching (default: 4)
    #[arg(long, default_value = "4")]
    prefetch_concurrency: usize,

    /// Show download progress bars (requires --parallel-prefetch)
    #[arg(long)]
    show_progress: bool,

    /// Emit JSON structured logs to stderr
    #[arg(long, default_value_t = false)]
    json_logs: bool,

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

/// Tier 1: Transactions table (with full BCS for offline replay)
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
    /// Full TransactionData BCS bytes for offline replay
    tx_bcs: Vec<u8>,
    /// TransactionEffects BCS bytes for validation
    effects_bcs: Vec<u8>,
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

/// Tier 3: Input objects table (with full BCS for offline replay)
#[derive(Debug, Clone)]
struct InputObjectRow {
    checkpoint_num: u64,
    timestamp_ms: u64,
    tx_digest: String,
    object_id: String,
    version: u64,
    object_type: Option<String>,
    owner: Option<String>,
    /// Full Object BCS bytes for offline replay
    bcs_data: Vec<u8>,
}

/// Tier 3: Output objects table (with full BCS for offline replay)
#[derive(Debug, Clone)]
struct OutputObjectRow {
    checkpoint_num: u64,
    timestamp_ms: u64,
    tx_digest: String,
    object_id: String,
    version: u64,
    object_type: Option<String>,
    owner: Option<String>,
    /// Full Object BCS bytes for offline replay
    bcs_data: Vec<u8>,
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

/// Package module interface - extracted from bytecode
#[derive(Debug, Clone)]
struct PackageModuleRow {
    checkpoint_num: u64,
    timestamp_ms: u64,
    package_id: String,
    package_version: u64,
    module_name: String,
    // Struct info (NULL if this row is for a function)
    struct_name: Option<String>,
    struct_abilities: Option<String>, // comma-separated: "copy,drop,store,key"
    struct_type_params: Option<u32>,  // count of type parameters
    struct_fields_count: Option<u32>,
    // Function info (NULL if this row is for a struct)
    function_name: Option<String>,
    function_visibility: Option<String>, // "public", "friend", "private"
    function_is_entry: Option<bool>,
    function_type_params: Option<u32>,
    function_params_count: Option<u32>,
    function_returns_count: Option<u32>,
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
    package_modules: Vec<PackageModuleRow>,
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
    package_modules_writer: Mutex<Option<ArrowWriter<File>>>,
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
            package_modules_writer: Mutex::new(None),
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
            Field::new("tx_bcs", DataType::Binary, false),
            Field::new("effects_bcs", DataType::Binary, false),
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

    fn package_modules_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("checkpoint_num", DataType::UInt64, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("package_id", DataType::Utf8, false),
            Field::new("package_version", DataType::UInt64, false),
            Field::new("module_name", DataType::Utf8, false),
            // Struct fields (nullable - only set for struct rows)
            Field::new("struct_name", DataType::Utf8, true),
            Field::new("struct_abilities", DataType::Utf8, true),
            Field::new("struct_type_params", DataType::UInt32, true),
            Field::new("struct_fields_count", DataType::UInt32, true),
            // Function fields (nullable - only set for function rows)
            Field::new("function_name", DataType::Utf8, true),
            Field::new("function_visibility", DataType::Utf8, true),
            Field::new("function_is_entry", DataType::Boolean, true),
            Field::new("function_type_params", DataType::UInt32, true),
            Field::new("function_params_count", DataType::UInt32, true),
            Field::new("function_returns_count", DataType::UInt32, true),
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
            Field::new("bcs_data", DataType::Binary, false),
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
            Field::new("bcs_data", DataType::Binary, false),
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
        // Write package_modules
        if !output.package_modules.is_empty() {
            self.write_package_modules(&output.package_modules).await?;
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
        let mut tx_bcs = BinaryBuilder::with_capacity(rows.len(), rows.len() * 512);
        let mut effects_bcs = BinaryBuilder::with_capacity(rows.len(), rows.len() * 256);

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
            tx_bcs.append_value(&row.tx_bcs);
            effects_bcs.append_value(&row.effects_bcs);
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
                Arc::new(tx_bcs.finish()),
                Arc::new(effects_bcs.finish()),
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

    async fn write_package_modules(&self, rows: &[PackageModuleRow]) -> Result<()> {
        let mut guard = self.package_modules_writer.lock().await;
        if guard.is_none() {
            let file = File::create(self.output_dir.join("package_modules.parquet"))?;
            *guard = Some(ArrowWriter::try_new(
                file,
                Self::package_modules_schema(),
                Some(Self::writer_props()),
            )?);
        }

        let schema = Self::package_modules_schema();
        let mut checkpoint_num = UInt64Builder::with_capacity(rows.len());
        let mut timestamp_ms = UInt64Builder::with_capacity(rows.len());
        let mut package_id = StringBuilder::with_capacity(rows.len(), rows.len() * 66);
        let mut package_version = UInt64Builder::with_capacity(rows.len());
        let mut module_name = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
        let mut struct_name = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
        let mut struct_abilities = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
        let mut struct_type_params = UInt32Builder::with_capacity(rows.len());
        let mut struct_fields_count = UInt32Builder::with_capacity(rows.len());
        let mut function_name = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
        let mut function_visibility = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
        let mut function_is_entry = BooleanBuilder::with_capacity(rows.len());
        let mut function_type_params = UInt32Builder::with_capacity(rows.len());
        let mut function_params_count = UInt32Builder::with_capacity(rows.len());
        let mut function_returns_count = UInt32Builder::with_capacity(rows.len());

        for row in rows {
            checkpoint_num.append_value(row.checkpoint_num);
            timestamp_ms.append_value(row.timestamp_ms);
            package_id.append_value(&row.package_id);
            package_version.append_value(row.package_version);
            module_name.append_value(&row.module_name);

            // Struct fields
            match &row.struct_name {
                Some(v) => struct_name.append_value(v),
                None => struct_name.append_null(),
            }
            match &row.struct_abilities {
                Some(v) => struct_abilities.append_value(v),
                None => struct_abilities.append_null(),
            }
            match row.struct_type_params {
                Some(v) => struct_type_params.append_value(v),
                None => struct_type_params.append_null(),
            }
            match row.struct_fields_count {
                Some(v) => struct_fields_count.append_value(v),
                None => struct_fields_count.append_null(),
            }

            // Function fields
            match &row.function_name {
                Some(v) => function_name.append_value(v),
                None => function_name.append_null(),
            }
            match &row.function_visibility {
                Some(v) => function_visibility.append_value(v),
                None => function_visibility.append_null(),
            }
            match row.function_is_entry {
                Some(v) => function_is_entry.append_value(v),
                None => function_is_entry.append_null(),
            }
            match row.function_type_params {
                Some(v) => function_type_params.append_value(v),
                None => function_type_params.append_null(),
            }
            match row.function_params_count {
                Some(v) => function_params_count.append_value(v),
                None => function_params_count.append_null(),
            }
            match row.function_returns_count {
                Some(v) => function_returns_count.append_value(v),
                None => function_returns_count.append_null(),
            }
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(checkpoint_num.finish()) as ArrayRef,
                Arc::new(timestamp_ms.finish()),
                Arc::new(package_id.finish()),
                Arc::new(package_version.finish()),
                Arc::new(module_name.finish()),
                Arc::new(struct_name.finish()),
                Arc::new(struct_abilities.finish()),
                Arc::new(struct_type_params.finish()),
                Arc::new(struct_fields_count.finish()),
                Arc::new(function_name.finish()),
                Arc::new(function_visibility.finish()),
                Arc::new(function_is_entry.finish()),
                Arc::new(function_type_params.finish()),
                Arc::new(function_params_count.finish()),
                Arc::new(function_returns_count.finish()),
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
        let mut bcs_data = BinaryBuilder::with_capacity(rows.len(), rows.len() * 1024);

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
            bcs_data.append_value(&row.bcs_data);
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
                Arc::new(bcs_data.finish()),
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
        let mut bcs_data = BinaryBuilder::with_capacity(rows.len(), rows.len() * 1024);

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
            bcs_data.append_value(&row.bcs_data);
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
                Arc::new(bcs_data.finish()),
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
        close_writer!(self.package_modules_writer);
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

        // Tier 2: Checkpoint metadata (emitted only if any matching txs when filtered)
        let gas_summary = checkpoint.summary.epoch_rolling_gas_cost_summary.clone();
        let checkpoint_row = CheckpointRow {
            checkpoint_num,
            timestamp_ms,
            epoch,
            previous_digest: checkpoint.summary.previous_digest.map(|d| format!("{}", d)),
            transactions_count: checkpoint.transactions.len() as u32,
            computation_cost: gas_summary.computation_cost,
            storage_cost: gas_summary.storage_cost,
            storage_rebate: gas_summary.storage_rebate,
            non_refundable_storage_fee: gas_summary.non_refundable_storage_fee,
            end_of_epoch: checkpoint.summary.end_of_epoch_data.is_some(),
        };

        let mut checkpoint_has_matches = self.package_filter.is_none();

        // Process each transaction
        for tx in &checkpoint.transactions {
            let tx_digest = format!("{}", tx.effects.transaction_digest());
            let tx_data = &tx.transaction;
            let sender = format!("{}", tx_data.sender());
            let effects = &tx.effects;

            // Get gas info
            let gas_summary = effects.gas_cost_summary();
            let gas_used = gas_summary.computation_cost + gas_summary.storage_cost
                - gas_summary.storage_rebate;

            // Check execution status
            let mut execution_error_row: Option<ExecutionErrorRow> = None;
            let (status, error_message) = match effects.status() {
                sui_types::execution_status::ExecutionStatus::Success => {
                    ("success".to_string(), None)
                }
                sui_types::execution_status::ExecutionStatus::Failure { error, command } => {
                    let err_msg = format!("{:?} at command {:?}", error, command);
                    execution_error_row = Some(ExecutionErrorRow {
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

            // Package filter: determine if this tx should be included at all
            let package_filter = self.package_filter.as_ref();
            let package_prefix = package_filter.map(|pkg| format!("{}::", pkg));

            let mut tx_matches = package_filter.is_none();
            if let Some(pkg) = package_filter {
                if calls.iter().any(|(_, package, _, _)| **package == *pkg) {
                    tx_matches = true;
                }

                if !tx_matches {
                    if let Some(tx_events) = &tx.events {
                        if tx_events.data.iter().any(|e| &e.package_id == pkg) {
                            tx_matches = true;
                        }
                    }
                }

                if !tx_matches {
                    for obj in tx.output_objects(&checkpoint.object_set) {
                        if obj.is_package() && obj.id() == *pkg {
                            tx_matches = true;
                            break;
                        }
                    }
                }

                if !tx_matches {
                    // Include dynamic field mutations even when package filtering is enabled.
                    // This ensures downstream state reconstruction (e.g. DeepBook orderbooks)
                    // has access to dynamic field objects.
                    let has_dynamic_field = tx
                        .input_objects(&checkpoint.object_set)
                        .chain(tx.output_objects(&checkpoint.object_set))
                        .any(|obj| {
                            obj.type_()
                                .map(|t| t.to_string().starts_with("0x2::dynamic_field"))
                                .unwrap_or(false)
                        });
                    if has_dynamic_field {
                        tx_matches = true;
                    }
                }

                if !tx_matches {
                    if let Some(prefix) = package_prefix.as_ref() {
                        for obj in tx.input_objects(&checkpoint.object_set) {
                            if obj
                                .type_()
                                .map(|t| t.to_string().starts_with(prefix))
                                .unwrap_or(false)
                            {
                                tx_matches = true;
                                break;
                            }
                        }
                        if !tx_matches {
                            for obj in tx.output_objects(&checkpoint.object_set) {
                                if obj
                                    .type_()
                                    .map(|t| t.to_string().starts_with(prefix))
                                    .unwrap_or(false)
                                {
                                    tx_matches = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            if package_filter.is_some() && !tx_matches {
                continue;
            }

            checkpoint_has_matches = true;

            if let Some(row) = execution_error_row.take() {
                output.execution_errors.push(row);
            }

            let mut move_calls_count = 0u32;
            for (idx, package, module, function) in calls {
                let matches_filter = match package_filter {
                    Some(pkg) => *package == *pkg,
                    None => true,
                };

                if matches_filter {
                    output.move_calls.push(MoveCallRow {
                        checkpoint_num,
                        timestamp_ms,
                        tx_digest: tx_digest.clone(),
                        call_index: idx as u32,
                        package_id: format!("{}", package),
                        module: module.to_string(),
                        function: function.to_string(),
                    });
                    move_calls_count += 1;
                }
            }

            let mut events_count = 0u32;
            // Serialize transaction and effects for offline replay
            let tx_bcs = bcs::to_bytes(&tx.transaction).unwrap_or_else(|_| Vec::new());
            let effects_bcs = bcs::to_bytes(&tx.effects).unwrap_or_else(|_| Vec::new());

            // Tier 1: Events
            if let Some(tx_events) = &tx.events {
                for (event_index, event) in tx_events.data.iter().enumerate() {
                    let matches_filter = match package_filter {
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
                        events_count += 1;
                    }
                }
            }

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
                tx_bcs,
                effects_bcs,
            });

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
                    let matches_filter = match package_filter {
                        Some(pkg) => obj.id() == *pkg,
                        None => true,
                    };

                    if matches_filter {
                        let pkg_id = format!("{}", obj.id());
                        let pkg_version = obj.version().value();

                        output.packages.push(PackageRow {
                            checkpoint_num,
                            timestamp_ms,
                            tx_digest: tx_digest.clone(),
                            package_id: pkg_id.clone(),
                            package_version: pkg_version,
                        });

                        // Extract module bytecode information
                        if let Some(package) = obj.data.try_as_package() {
                            for (module_name, bytecode) in package.serialized_module_map() {
                                // Try to deserialize the module bytecode
                                if let Ok(compiled) =
                                    CompiledModule::deserialize_with_defaults(bytecode)
                                {
                                    // Extract struct definitions
                                    for struct_def in compiled.struct_defs() {
                                        let handle =
                                            compiled.datatype_handle_at(struct_def.struct_handle);
                                        let name = compiled.identifier_at(handle.name).to_string();
                                        let abilities = ability_set_to_string(&handle.abilities);
                                        let type_params_count = handle.type_parameters.len() as u32;
                                        let fields_count = match &struct_def.field_information {
                                            move_binary_format::file_format::StructFieldInformation::Declared(fields) => {
                                                fields.len() as u32
                                            }
                                            move_binary_format::file_format::StructFieldInformation::Native => 0,
                                        };

                                        output.package_modules.push(PackageModuleRow {
                                            checkpoint_num,
                                            timestamp_ms,
                                            package_id: pkg_id.clone(),
                                            package_version: pkg_version,
                                            module_name: module_name.clone(),
                                            struct_name: Some(name),
                                            struct_abilities: Some(abilities),
                                            struct_type_params: Some(type_params_count),
                                            struct_fields_count: Some(fields_count),
                                            function_name: None,
                                            function_visibility: None,
                                            function_is_entry: None,
                                            function_type_params: None,
                                            function_params_count: None,
                                            function_returns_count: None,
                                        });
                                    }

                                    // Extract function definitions
                                    for func_def in compiled.function_defs() {
                                        let handle = compiled.function_handle_at(func_def.function);
                                        let name = compiled.identifier_at(handle.name).to_string();
                                        let visibility = visibility_to_string(func_def.visibility);
                                        let is_entry = func_def.is_entry;
                                        let type_params_count = handle.type_parameters.len() as u32;
                                        let params_count =
                                            compiled.signature_at(handle.parameters).0.len() as u32;
                                        let returns_count =
                                            compiled.signature_at(handle.return_).0.len() as u32;

                                        output.package_modules.push(PackageModuleRow {
                                            checkpoint_num,
                                            timestamp_ms,
                                            package_id: pkg_id.clone(),
                                            package_version: pkg_version,
                                            module_name: module_name.clone(),
                                            struct_name: None,
                                            struct_abilities: None,
                                            struct_type_params: None,
                                            struct_fields_count: None,
                                            function_name: Some(name),
                                            function_visibility: Some(visibility.to_string()),
                                            function_is_entry: Some(is_entry),
                                            function_type_params: Some(type_params_count),
                                            function_params_count: Some(params_count),
                                            function_returns_count: Some(returns_count),
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Tier 3: Input objects (with full BCS for offline replay)
            for obj in tx.input_objects(&checkpoint.object_set) {
                // Serialize the full Object for offline replay
                let bcs_data = bcs::to_bytes(obj).unwrap_or_else(|_| Vec::new());
                output.input_objects.push(InputObjectRow {
                    checkpoint_num,
                    timestamp_ms,
                    tx_digest: tx_digest.clone(),
                    object_id: format!("{}", obj.id()),
                    version: obj.version().value(),
                    object_type: obj.type_().map(|t| t.to_string()),
                    owner: Some(format!("{:?}", obj.owner())),
                    bcs_data,
                });
            }

            // Tier 3: Output objects (with full BCS for offline replay)
            for obj in tx.output_objects(&checkpoint.object_set) {
                // Serialize the full Object for offline replay
                let bcs_data = bcs::to_bytes(obj).unwrap_or_else(|_| Vec::new());
                output.output_objects.push(OutputObjectRow {
                    checkpoint_num,
                    timestamp_ms,
                    tx_digest: tx_digest.clone(),
                    object_id: format!("{}", obj.id()),
                    version: obj.version().value(),
                    object_type: obj.type_().map(|t| t.to_string()),
                    owner: Some(format!("{:?}", obj.owner())),
                    bcs_data,
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

        if checkpoint_has_matches {
            output.checkpoints.push(checkpoint_row);
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

    eprintln!(
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

/// Convert ability set to comma-separated string
fn ability_set_to_string(abilities: &AbilitySet) -> String {
    let mut out = Vec::new();
    if abilities.has_ability(Ability::Copy) {
        out.push("copy");
    }
    if abilities.has_ability(Ability::Drop) {
        out.push("drop");
    }
    if abilities.has_ability(Ability::Store) {
        out.push("store");
    }
    if abilities.has_ability(Ability::Key) {
        out.push("key");
    }
    out.join(",")
}

/// Convert visibility to string
fn visibility_to_string(v: Visibility) -> &'static str {
    match v {
        Visibility::Public => "public",
        Visibility::Friend => "friend",
        Visibility::Private => "private",
    }
}

/// Table metadata for output summary
struct TableInfo {
    name: &'static str,
    description: &'static str,
}

const OUTPUT_TABLES: &[TableInfo] = &[
    TableInfo {
        name: "checkpoints",
        description: "Checkpoint metadata (epoch, gas totals)",
    },
    TableInfo {
        name: "transactions",
        description: "Transaction metadata (sender, gas, status)",
    },
    TableInfo {
        name: "events",
        description: "Emitted events (package, module, type, bcs)",
    },
    TableInfo {
        name: "move_calls",
        description: "Function calls (package, module, function)",
    },
    TableInfo {
        name: "objects",
        description: "Object changes (created/mutated/deleted)",
    },
    TableInfo {
        name: "input_objects",
        description: "Pre-transaction object state",
    },
    TableInfo {
        name: "output_objects",
        description: "Post-transaction object state",
    },
    TableInfo {
        name: "balance_changes",
        description: "Coin balance changes (+/- amounts)",
    },
    TableInfo {
        name: "packages",
        description: "Published packages",
    },
    TableInfo {
        name: "package_modules",
        description: "Package bytecode (structs/functions)",
    },
    TableInfo {
        name: "execution_errors",
        description: "Failed transaction errors",
    },
];

#[cfg(feature = "duckdb")]
fn output_summary(output_dir: &Path) -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()
        .context("failed to open in-memory DuckDB connection")?;

    eprintln!("\n=== Output Summary ===");
    eprintln!("Directory: {}", output_dir.display());
    eprintln!();
    eprintln!(
        "{:<20} {:>12} {:>10}  {}",
        "Table", "Rows", "Size", "Description"
    );
    eprintln!("{}", "-".repeat(80));

    for table in OUTPUT_TABLES {
        let path = output_dir.join(format!("{}.parquet", table.name));
        if path.exists() {
            let size_kb = path
                .metadata()
                .map(|m| m.len() as f64 / 1024.0)
                .unwrap_or(0.0);
            let size_str = if size_kb >= 1024.0 {
                format!("{:.1} MB", size_kb / 1024.0)
            } else {
                format!("{:.1} KB", size_kb)
            };

            let path_str = path.to_str().unwrap().replace('\'', "''");
            let count_query = format!("SELECT COUNT(*) FROM read_parquet('{}')", path_str);
            match conn.query_row(&count_query, [], |row| row.get::<_, i64>(0)) {
                Ok(count) => {
                    eprintln!(
                        "{:<20} {:>12} {:>10}  {}",
                        table.name,
                        format_count(count),
                        size_str,
                        table.description
                    );
                }
                Err(_) => {
                    eprintln!(
                        "{:<20} {:>12} {:>10}  {}",
                        table.name, "error", size_str, table.description
                    );
                }
            }
        } else {
            eprintln!(
                "{:<20} {:>12} {:>10}  {}",
                table.name, "-", "-", table.description
            );
        }
    }

    Ok(())
}

#[cfg(not(feature = "duckdb"))]
fn output_summary(output_dir: &Path) -> Result<()> {
    eprintln!("\n=== Output Summary ===");
    eprintln!("Directory: {}", output_dir.display());
    eprintln!();
    eprintln!("{:<20} {:>10}  {}", "Table", "Size", "Description");
    eprintln!("{}", "-".repeat(70));

    for table in OUTPUT_TABLES {
        let path = output_dir.join(format!("{}.parquet", table.name));
        if path.exists() {
            let size_kb = path
                .metadata()
                .map(|m| m.len() as f64 / 1024.0)
                .unwrap_or(0.0);
            let size_str = if size_kb >= 1024.0 {
                format!("{:.1} MB", size_kb / 1024.0)
            } else {
                format!("{:.1} KB", size_kb)
            };
            eprintln!("{:<20} {:>10}  {}", table.name, size_str, table.description);
        } else {
            eprintln!("{:<20} {:>10}  {}", table.name, "-", table.description);
        }
    }

    eprintln!();
    eprintln!("(Row counts available with --features duckdb)");
    Ok(())
}

fn format_count(count: i64) -> String {
    if count >= 1_000_000 {
        format!("{:.1}M", count as f64 / 1_000_000.0)
    } else if count >= 1_000 {
        format!("{:.1}K", count as f64 / 1_000.0)
    } else {
        count.to_string()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Configure tracing to write to stderr (not stdout) so piping to DuckDB works
    let env_filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into());
    let registry = tracing_subscriber::registry().with(env_filter);
    if args.json_logs {
        registry
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .json(),
            )
            .init();
    } else {
        registry
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .init();
    }

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
    eprintln!("Spooling checkpoints to: {}", spool.dir.display());

    let storage = WalrusStorage::new(args.walrus.clone()).await?;
    storage.initialize().await?;

    // Display archive coverage (metadata already fetched in initialize())
    eprintln!();
    eprintln!("=== Archive Available ===");
    if let Some((min_start, max_end)) = storage.coverage_range().await {
        eprintln!(
            "Checkpoint range: {}..{} ({} checkpoints)",
            min_start,
            max_end,
            max_end.saturating_sub(min_start)
        );
    } else {
        eprintln!("Checkpoint range: unknown (no metadata)");
    }

    // Parallel blob prefetching if enabled
    if args.parallel_prefetch {
        let blobs = storage.get_blobs_for_range(args.start..args.end).await;
        let checkpoints_requested = args.end.saturating_sub(args.start);
        let num_blobs = blobs.len();

        // Calculate total size from blob metadata
        let total_bytes: u64 = blobs.iter().map(|b| b.total_size).sum();
        let total_gb = total_bytes as f64 / 1_000_000_000.0;

        eprintln!();
        eprintln!("=== Blob Download ===");
        eprintln!(
            "Checkpoints: {} (range {}..{})",
            checkpoints_requested, args.start, args.end
        );
        eprintln!("Blobs to download: {}", num_blobs);
        eprintln!("Total download size: {:.2} GB", total_gb);
        eprintln!();
        eprintln!("Individual blobs:");
        for (i, blob) in blobs.iter().enumerate() {
            let size_gb = blob.total_size as f64 / 1_000_000_000.0;
            eprintln!(
                "  [{}] {} ({:.2} GB, {} checkpoints: {}..{})",
                i + 1,
                blob.blob_id,
                size_gb,
                blob.entries_count,
                blob.start_checkpoint,
                blob.end_checkpoint
            );
        }
        eprintln!();
        eprintln!(
            "Concurrency: {} parallel downloads",
            args.prefetch_concurrency
        );
        eprintln!("Timeout: {} seconds per blob", args.walrus.cli_timeout_secs);
        eprintln!();

        let blob_ids: Vec<String> = blobs.iter().map(|b| b.blob_id.clone()).collect();
        let prefetch_start = std::time::Instant::now();
        let prefetched = storage
            .prefetch_blobs_parallel_with_progress(
                &blob_ids,
                args.prefetch_concurrency,
                args.show_progress,
            )
            .await?;
        let prefetch_elapsed = prefetch_start.elapsed();

        let downloaded_bytes = storage.bytes_downloaded();
        let downloaded_gb = downloaded_bytes as f64 / 1_000_000_000.0;
        let speed_mbps = (downloaded_bytes as f64 / 1_000_000.0) / prefetch_elapsed.as_secs_f64();

        eprintln!();
        eprintln!("=== Download Complete ===");
        eprintln!(
            "Downloaded: {}/{} blobs ({:.2} GB)",
            prefetched, num_blobs, downloaded_gb
        );
        eprintln!(
            "Time: {:.1}s ({:.1} MB/s average)",
            prefetch_elapsed.as_secs_f64(),
            speed_mbps
        );
        eprintln!();
    }

    spool_checkpoints(&storage, args.start..args.end, &spool).await?;

    let first_path = spool.dir.join(format!("{}.chk", args.start));
    let last_path = spool.dir.join(format!("{}.chk", args.end - 1));
    validate_spool_file(&first_path)?;
    validate_spool_file(&last_path)?;

    let num_checkpoints = args.end.saturating_sub(args.start);
    eprintln!();
    eprintln!("=== Processing Checkpoints ===");
    eprintln!(
        "Checkpoints to process: {} ({}..{})",
        num_checkpoints, args.start, args.end
    );
    eprintln!("Output directory: {}", args.output_dir.display());
    eprintln!();
    eprintln!("Processing with sui-indexer-alt-framework...");
    eprintln!("(this may take a while for large ranges)");

    let process_start = std::time::Instant::now();

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

    let process_elapsed = process_start.elapsed();
    let checkpoints_per_sec = num_checkpoints as f64 / process_elapsed.as_secs_f64();
    eprintln!();
    eprintln!(
        "Processing complete: {} checkpoints in {:.1}s ({:.0} checkpoints/sec)",
        num_checkpoints,
        process_elapsed.as_secs_f64(),
        checkpoints_per_sec
    );

    output_summary(&args.output_dir)?;

    Ok(())
}
