//! Example: Walrus -> local checkpoint spool -> Sui indexer alt framework -> Parquet (all events).
//!
//! This example keeps the integration light and replicable:
//! - Streams checkpoints from Walrus
//! - Writes local checkpoint files in the Sui `.chk` format
//! - Uses the Sui indexer alt framework to process checkpoints from the local path
//! - Writes filtered events to a Parquet file (DuckDB/Polars friendly)
//!
//! Default behavior is ephemeral spooling (temp dir). Use `--spool-mode cache` to reuse
//! checkpoint files for faster iteration.

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, BinaryBuilder, StringBuilder, UInt32Builder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use base64::engine::general_purpose::STANDARD as Base64Engine;
use base64::Engine;
use clap::{Parser, ValueEnum};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use scoped_futures::ScopedBoxFuture;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use serde_json::json;

use sui_indexer_alt_framework as framework;
use framework::ingestion::{ClientArgs, IngestionConfig};
use framework::pipeline::Processor;
use framework::pipeline::sequential::{Handler, SequentialConfig};
use framework::store::{CommitterWatermark, Connection, PrunerWatermark, ReaderWatermark, Store, TransactionalStore};
use framework::{Indexer, IndexerArgs};
use framework::types::base_types::ObjectID;
use framework::types::full_checkpoint_content::{Checkpoint, CheckpointData};
use sui_storage::blob::{Blob, BlobEncoding};
use walrus_checkpoint_indexing::{Config, WalrusStorage};

#[derive(Parser, Debug)]
#[command(name = "alt-checkpoint-parquet")]
#[command(about = "Walrus -> Sui indexer alt framework -> Parquet (all events)")]
struct Args {
    /// Starting checkpoint number (inclusive)
    #[arg(long)]
    start: u64,

    /// Ending checkpoint number (exclusive)
    #[arg(long)]
    end: u64,

    /// Output parquet file path
    #[arg(long, short, default_value = "./checkpoint_events.parquet")]
    output: PathBuf,

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

    /// Print DuckDB summary (tables + row count) after writing parquet
    #[arg(long)]
    duckdb_summary: bool,

    #[command(flatten)]
    walrus: Config,
}

#[derive(ValueEnum, Clone, Debug)]
enum SpoolMode {
    Ephemeral,
    Cache,
}

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
                Ok(Self { dir: path, _temp: None, cache_mode: true })
            }
            SpoolMode::Ephemeral => {
                if let Some(path) = dir {
                    std::fs::create_dir_all(&path)
                        .with_context(|| format!("failed to create spool dir: {}", path.display()))?;
                    Ok(Self { dir: path, _temp: None, cache_mode: false })
                } else {
                    let temp = TempDir::new().context("failed to create temp spool dir")?;
                    Ok(Self { dir: temp.path().to_path_buf(), _temp: Some(temp), cache_mode: false })
                }
            }
        }
    }
}

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

struct ParquetSink {
    output_path: PathBuf,
    writer: Mutex<Option<ArrowWriter<File>>>,
}

impl ParquetSink {
    fn new(output_path: PathBuf) -> Self {
        Self {
            output_path,
            writer: Mutex::new(None),
        }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
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
        ]))
    }

    async fn ensure_writer(&self) -> Result<()> {
        let mut guard = self.writer.lock().await;
        if guard.is_some() {
            return Ok(());
        }

        let file = File::create(&self.output_path)
            .with_context(|| format!("failed to create output file: {}", self.output_path.display()))?;

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let writer = ArrowWriter::try_new(file, Self::schema(), Some(props))?;
        *guard = Some(writer);
        Ok(())
    }

    async fn write_rows(&self, rows: &[EventRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        self.ensure_writer().await?;

        let schema = Self::schema();
        let mut checkpoint_num_builder = UInt64Builder::with_capacity(rows.len());
        let mut timestamp_ms_builder = UInt64Builder::with_capacity(rows.len());
        let mut tx_digest_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 44);
        let mut event_index_builder = UInt32Builder::with_capacity(rows.len());
        let mut package_id_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 66);
        let mut module_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
        let mut event_type_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 64);
        let mut sender_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 66);
        let mut event_json_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 256);
        let mut bcs_data_builder = BinaryBuilder::with_capacity(rows.len(), rows.len() * 256);

        for event in rows {
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

        let mut guard = self.writer.lock().await;
        if let Some(writer) = guard.as_mut() {
            writer.write(&batch)?;
        }

        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let mut guard = self.writer.lock().await;
        if let Some(writer) = guard.take() {
            writer.close()?;
        }
        Ok(())
    }
}

struct ParquetPipeline {
    package_filter: Option<ObjectID>,
    sink: Arc<ParquetSink>,
}

#[async_trait::async_trait]
impl Processor for ParquetPipeline {
    const NAME: &'static str = "walrus_events";

    type Value = EventRow;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> Result<Vec<Self::Value>> {
        let checkpoint_num = checkpoint.summary.sequence_number;
        let timestamp_ms = checkpoint.summary.timestamp_ms;

        let mut rows = Vec::new();

        for tx in &checkpoint.transactions {
            let tx_digest = format!("{}", tx.transaction.digest());

            if let Some(tx_events) = &tx.events {
                for (event_index, event) in tx_events.data.iter().enumerate() {
                    let matches_filter = match &self.package_filter {
                        Some(pkg) => &event.package_id == pkg,
                        None => true,
                    };

                    if !matches_filter {
                        continue;
                    }

                    let package_id = format!("{}", event.package_id);
                    let module = event.transaction_module.to_string();
                    let event_type = event.type_.name.to_string();
                    let sender = format!("{}", event.sender);
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

                    rows.push(EventRow {
                        checkpoint_num,
                        timestamp_ms,
                        tx_digest: tx_digest.clone(),
                        event_index: event_index as u32,
                        package_id,
                        module,
                        event_type,
                        sender,
                        event_json,
                        bcs_data: event.contents.clone(),
                    });
                }
            }
        }

        Ok(rows)
    }
}

#[async_trait::async_trait]
impl Handler for ParquetPipeline {
    type Store = LocalStore;
    type Batch = Vec<EventRow>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        batch.extend(values);
    }

    async fn commit<'a>(
        &self,
        batch: &Self::Batch,
        _conn: &mut <Self::Store as Store>::Connection<'a>,
    ) -> anyhow::Result<usize> {
        self.sink.write_rows(batch).await?;
        Ok(batch.len())
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
                let data = std::fs::read_to_string(path)
                    .with_context(|| format!("failed to read watermark file: {}", path.display()))?;
                serde_json::from_str::<WatermarkState>(&data)
                    .with_context(|| format!("failed to parse watermark file: {}", path.display()))?
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
        Ok(state.entries.get(pipeline_task).map(|entry| entry.committer()))
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
        let entry = state.entries.entry(pipeline_task.to_string()).or_insert_with(|| WatermarkEntry {
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
        let entry = state.entries.entry(pipeline.to_string()).or_insert_with(|| WatermarkEntry {
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
        let entry = state.entries.entry(pipeline.to_string()).or_insert_with(|| WatermarkEntry {
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
    tokio::fs::write(&tmp_path, data)
        .await
        .with_context(|| format!("failed to write watermark temp file: {}", tmp_path.display()))?;
    tokio::fs::rename(&tmp_path, path)
        .await
        .with_context(|| format!("failed to move watermark file into place: {}", path.display()))?;
    Ok(())
}

#[async_trait::async_trait]
impl Store for LocalStore {
    type Connection<'c> = LocalConnection where Self: 'c;

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
        F: for<'r> FnOnce(&'r mut Self::Connection<'_>) -> ScopedBoxFuture<'a, 'r, anyhow::Result<R>>,
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
                tokio::fs::write(&path, bytes)
                    .await
                    .with_context(|| format!("failed to write checkpoint file: {}", path.display()))?;

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

#[cfg(feature = "duckdb")]
fn duckdb_summary(path: &Path) -> Result<()> {
    let path_str = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("parquet path is not valid UTF-8"))?;
    let conn = duckdb::Connection::open_in_memory()
        .context("failed to open in-memory DuckDB connection")?;

    let escaped = path_str.replace('\'', "''");
    let create_view = format!(
        "CREATE VIEW events AS SELECT * FROM read_parquet('{}');",
        escaped
    );
    conn.execute(&create_view, [])
        .context("failed to create DuckDB view")?;

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .context("failed to query row count")?;
    println!("DuckDB row count: {}", count);

    let mut tables = conn
        .prepare("PRAGMA show_tables")
        .context("failed to prepare show_tables")?;
    let mut rows = tables.query([])?;
    println!("DuckDB tables:");
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        println!("  - {}", name);
    }

    let mut schema = conn
        .prepare("DESCRIBE events")
        .context("failed to prepare describe")?;
    let mut rows = schema.query([])?;
    println!("DuckDB schema:");
    while let Some(row) = rows.next()? {
        let col: String = row.get(0)?;
        let col_type: String = row.get(1)?;
        println!("  - {} {}", col, col_type);
    }

    Ok(())
}

#[cfg(not(feature = "duckdb"))]
fn duckdb_summary(_path: &Path) -> Result<()> {
    anyhow::bail!("duckdb feature not enabled; rebuild with --features duckdb")
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
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
                std::fs::remove_file(path)
                    .with_context(|| format!("failed to remove watermark file: {}", path.display()))?;
            }
        }
    }

    let spool = SpoolHandle::new(args.spool_mode, args.spool_dir.clone())?;
    println!("Spooling checkpoints to: {}", spool.dir.display());

    let storage = WalrusStorage::new(args.walrus.clone()).await?;
    storage.initialize().await?;

    spool_checkpoints(&storage, args.start..args.end, &spool).await?;

    let first_path = spool.dir.join(format!("{}.chk", args.start));
    let last_path = spool.dir.join(format!("{}.chk", args.end - 1));
    validate_spool_file(&first_path)?;
    validate_spool_file(&last_path)?;

    let package_filter = parse_package(&args.package)?;
    let sink = Arc::new(ParquetSink::new(args.output.clone()));
    let pipeline = ParquetPipeline {
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

    sink.close().await?;

    println!("Parquet written to: {}", args.output.display());
    if args.duckdb_summary {
        duckdb_summary(&args.output)?;
    }
    Ok(())
}
