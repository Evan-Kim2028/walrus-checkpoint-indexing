//! Mass checkpoint indexer infrastructure.
//!
//! This module provides a framework for building mass checkpoint indexers that process
//! all checkpoints from Walrus storage. It follows patterns similar to the Sui custom
//! indexing framework (`sui-indexer-alt-framework`) and can be used as a foundation
//! for full framework integration.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                         MassIndexer                                      │
//! │                                                                          │
//! │  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐  │
//! │  │  WalrusStorage   │───▶│    Processor     │───▶│     Committer    │  │
//! │  │  (data source)   │    │  (transform)     │    │  (persistence)   │  │
//! │  └──────────────────┘    └──────────────────┘    └──────────────────┘  │
//! │           │                                               │             │
//! │           │              ┌──────────────────┐              │             │
//! │           └─────────────▶│    Watermark     │◀─────────────┘             │
//! │                          │    (progress)    │                            │
//! │                          └──────────────────┘                            │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Concepts
//!
//! - **Processor**: Transforms checkpoint data into your desired output format
//! - **Watermark**: Tracks the last successfully processed checkpoint for resumability
//! - **Committer**: Handles persistence of processed data (implement your own storage)
//!
//! ## Example
//!
//! ```rust,ignore
//! use walrus_checkpoint_indexing::indexer::{MassIndexer, Processor, IndexerConfig};
//!
//! struct MyProcessor;
//!
//! #[async_trait::async_trait]
//! impl Processor for MyProcessor {
//!     type Output = Vec<MyEvent>;
//!
//!     async fn process(&self, checkpoint: &CheckpointData) -> Result<Self::Output> {
//!         // Extract events, filter by package, transform data
//!         Ok(extract_my_events(checkpoint))
//!     }
//!
//!     async fn commit(&mut self, checkpoint_num: u64, data: Self::Output) -> Result<()> {
//!         // Write to database, file, etc.
//!         Ok(())
//!     }
//! }
//!
//! let indexer = MassIndexer::new(config, MyProcessor).await?;
//! indexer.run(start..end).await?;
//! ```

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use sui_types::full_checkpoint_content::CheckpointData;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::sync::{Mutex, RwLock};

use crate::config::Config;
use crate::storage::WalrusStorage;

/// Trait for processing checkpoint data.
///
/// This trait is designed to be compatible with the `sui-indexer-alt-framework`'s
/// `Processor` pattern. Implement this to define how checkpoints are transformed
/// and persisted.
///
/// ## Type Parameter
///
/// - `Output`: The intermediate data type produced by `process()` and consumed by `commit()`.
///   This is typically a collection of extracted events, transformed records, etc.
#[async_trait]
pub trait Processor: Send + Sync {
    /// The output type produced by processing a checkpoint.
    type Output: Send;

    /// Process a single checkpoint and produce output data.
    ///
    /// This method should:
    /// - Extract relevant data from the checkpoint (events, transactions, objects)
    /// - Filter by package ID, module, event type, etc.
    /// - Transform into your desired output format
    ///
    /// This is called for each checkpoint in order. The framework handles
    /// parallelization at the blob level.
    async fn process(&self, checkpoint: &CheckpointData) -> Result<Self::Output>;

    /// Commit processed data to storage.
    ///
    /// This method should:
    /// - Persist the output data to your storage backend (database, file, etc.)
    /// - Handle any batching or buffering internally if needed
    ///
    /// Called after `process()` for each checkpoint. The checkpoint number is
    /// provided for watermark tracking.
    async fn commit(
        &mut self,
        checkpoint_num: CheckpointSequenceNumber,
        data: Self::Output,
    ) -> Result<()>;

    /// Called before processing begins.
    ///
    /// Override to perform setup (open connections, create tables, etc.).
    async fn on_start(
        &mut self,
        _start: CheckpointSequenceNumber,
        _end: CheckpointSequenceNumber,
    ) -> Result<()> {
        Ok(())
    }

    /// Called after all checkpoints are processed.
    ///
    /// Override to perform cleanup (flush buffers, close connections, etc.).
    async fn on_complete(&mut self, _total_processed: u64) -> Result<()> {
        Ok(())
    }

    /// Called when an error occurs.
    ///
    /// Override to handle errors (logging, metrics, etc.).
    async fn on_error(&mut self, _checkpoint: CheckpointSequenceNumber, _error: &anyhow::Error) {}
}

/// Watermark manager for tracking indexing progress.
///
/// The watermark represents the highest checkpoint that has been fully processed
/// and committed. This enables resumable indexing - if the indexer crashes, it
/// can restart from the watermark.
#[derive(Debug, Clone)]
pub struct Watermark {
    /// Path to the watermark file (if file-based persistence is used)
    file_path: Option<PathBuf>,
    /// Current watermark value (last successfully processed checkpoint)
    current: Arc<RwLock<Option<CheckpointSequenceNumber>>>,
}

impl Watermark {
    /// Create a new watermark manager with file-based persistence.
    pub fn with_file(path: impl Into<PathBuf>) -> Self {
        Self {
            file_path: Some(path.into()),
            current: Arc::new(RwLock::new(None)),
        }
    }

    /// Create an in-memory watermark (no persistence).
    pub fn in_memory() -> Self {
        Self {
            file_path: None,
            current: Arc::new(RwLock::new(None)),
        }
    }

    /// Load watermark from file (if configured).
    pub async fn load(&self) -> Result<Option<CheckpointSequenceNumber>> {
        if let Some(path) = &self.file_path {
            if path.exists() {
                let content = tokio::fs::read_to_string(path).await?;
                let checkpoint: CheckpointSequenceNumber = content.trim().parse()?;
                let mut current = self.current.write().await;
                *current = Some(checkpoint);
                return Ok(Some(checkpoint));
            }
        }
        Ok(None)
    }

    /// Save watermark to file (if configured).
    pub async fn save(&self, checkpoint: CheckpointSequenceNumber) -> Result<()> {
        {
            let mut current = self.current.write().await;
            *current = Some(checkpoint);
        }

        if let Some(path) = &self.file_path {
            // Write to temp file first, then rename for atomicity
            let temp_path = path.with_extension("tmp");
            tokio::fs::write(&temp_path, checkpoint.to_string()).await?;
            tokio::fs::rename(&temp_path, path).await?;
        }
        Ok(())
    }

    /// Get the current watermark value.
    pub async fn get(&self) -> Option<CheckpointSequenceNumber> {
        let current = self.current.read().await;
        *current
    }
}

/// Configuration for the mass indexer.
#[derive(Debug, Clone)]
pub struct IndexerConfig {
    /// Walrus storage configuration
    pub storage_config: Config,
    /// Path to watermark file (optional, for resumability)
    pub watermark_path: Option<PathBuf>,
    /// Log progress every N checkpoints
    pub log_interval: u64,
    /// Commit watermark every N checkpoints
    pub watermark_interval: u64,
}

impl IndexerConfig {
    /// Create a new indexer config with defaults.
    pub fn new(storage_config: Config) -> Self {
        Self {
            storage_config,
            watermark_path: None,
            log_interval: 100,
            watermark_interval: 100,
        }
    }

    /// Set the watermark file path for resumability.
    pub fn with_watermark_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.watermark_path = Some(path.into());
        self
    }

    /// Set the logging interval.
    pub fn with_log_interval(mut self, interval: u64) -> Self {
        self.log_interval = interval;
        self
    }

    /// Set the watermark commit interval.
    pub fn with_watermark_interval(mut self, interval: u64) -> Self {
        self.watermark_interval = interval;
        self
    }
}

/// Statistics about indexer progress.
#[derive(Debug, Clone, Default)]
pub struct IndexerStats {
    pub checkpoints_processed: u64,
    pub checkpoints_total: u64,
    pub bytes_downloaded: u64,
    pub elapsed_secs: f64,
    pub current_checkpoint: Option<CheckpointSequenceNumber>,
}

impl IndexerStats {
    pub fn checkpoints_per_sec(&self) -> f64 {
        if self.elapsed_secs > 0.0 {
            self.checkpoints_processed as f64 / self.elapsed_secs
        } else {
            0.0
        }
    }

    pub fn progress_percent(&self) -> f64 {
        if self.checkpoints_total > 0 {
            (self.checkpoints_processed as f64 / self.checkpoints_total as f64) * 100.0
        } else {
            0.0
        }
    }

    pub fn mb_downloaded(&self) -> f64 {
        self.bytes_downloaded as f64 / 1_000_000.0
    }
}

/// Mass checkpoint indexer.
///
/// This struct orchestrates the indexing pipeline:
/// 1. Fetches checkpoints from Walrus storage
/// 2. Processes each checkpoint through the provided `Processor`
/// 3. Tracks progress via watermarks for resumability
pub struct MassIndexer<P: Processor> {
    storage: WalrusStorage,
    processor: Arc<Mutex<P>>,
    watermark: Watermark,
    config: IndexerConfig,
}

impl<P: Processor> MassIndexer<P> {
    /// Create a new mass indexer.
    pub async fn new(config: IndexerConfig, processor: P) -> Result<Self> {
        let storage = WalrusStorage::new(config.storage_config.clone()).await?;
        storage.initialize().await?;

        let watermark = match &config.watermark_path {
            Some(path) => Watermark::with_file(path),
            None => Watermark::in_memory(),
        };

        // Load existing watermark if available
        if let Some(checkpoint) = watermark.load().await? {
            tracing::info!("loaded watermark: checkpoint {}", checkpoint);
        }

        Ok(Self {
            storage,
            processor: Arc::new(Mutex::new(processor)),
            watermark,
            config,
        })
    }

    /// Get the underlying storage instance.
    pub fn storage(&self) -> &WalrusStorage {
        &self.storage
    }

    /// Get the current watermark.
    pub async fn watermark(&self) -> Option<CheckpointSequenceNumber> {
        self.watermark.get().await
    }

    /// Run the indexer over a checkpoint range.
    ///
    /// If a watermark exists and is within the range, indexing will resume
    /// from the watermark + 1.
    pub async fn run(
        &mut self,
        range: std::ops::Range<CheckpointSequenceNumber>,
    ) -> Result<IndexerStats> {
        let mut stats = IndexerStats {
            checkpoints_total: range.end.saturating_sub(range.start),
            ..Default::default()
        };

        // Determine starting point (resume from watermark if available)
        let start = match self.watermark.get().await {
            Some(wm) if wm >= range.start && wm < range.end => {
                tracing::info!("resuming from watermark: checkpoint {}", wm + 1);
                wm + 1
            }
            _ => range.start,
        };

        if start >= range.end {
            tracing::info!("nothing to process: start {} >= end {}", start, range.end);
            return Ok(stats);
        }

        let effective_range = start..range.end;
        let total_checkpoints = effective_range.end - effective_range.start;

        tracing::info!(
            "starting mass indexer: checkpoints {} to {} ({} total)",
            effective_range.start,
            effective_range.end,
            total_checkpoints
        );

        // Notify processor
        {
            let mut processor = self.processor.lock().await;
            processor
                .on_start(effective_range.start, effective_range.end)
                .await?;
        }

        let start_time = Instant::now();
        let log_interval = self.config.log_interval;
        let watermark_interval = self.config.watermark_interval;

        let prefetch_blobs = self.config.storage_config.prefetch_blobs;
        let processor = Arc::clone(&self.processor);
        let watermark = self.watermark.clone();
        let stats = Arc::new(Mutex::new(stats));
        let stats_for_closure = Arc::clone(&stats);
        self.storage
            .stream_checkpoints_prefetch(effective_range.clone(), prefetch_blobs, move |checkpoint| {
                let processor = Arc::clone(&processor);
                let watermark = watermark.clone();
                let stats = Arc::clone(&stats_for_closure);
                async move {
                    let checkpoint_num = checkpoint.checkpoint_summary.sequence_number;

                    let output = {
                        let processor = processor.lock().await;
                        processor
                            .process(&checkpoint)
                            .await
                            .with_context(|| {
                                format!("failed to process checkpoint {}", checkpoint_num)
                            })?
                    };

                    {
                        let mut processor = processor.lock().await;
                        processor
                            .commit(checkpoint_num, output)
                            .await
                            .with_context(|| {
                                format!("failed to commit checkpoint {}", checkpoint_num)
                            })?;
                    }

                    let mut stats_guard = stats.lock().await;
                    stats_guard.checkpoints_processed += 1;
                    stats_guard.current_checkpoint = Some(checkpoint_num);

                    if stats_guard
                        .checkpoints_processed
                        .is_multiple_of(watermark_interval)
                    {
                        watermark.save(checkpoint_num).await?;
                    }

                    if stats_guard.checkpoints_processed.is_multiple_of(log_interval) {
                        let elapsed = start_time.elapsed().as_secs_f64();
                        let rate = stats_guard.checkpoints_processed as f64 / elapsed;
                        let progress = (stats_guard.checkpoints_processed as f64
                            / total_checkpoints as f64)
                            * 100.0;
                        tracing::info!(
                            "progress: {} / {} ({:.1}%), {:.1} cp/s, checkpoint {}",
                            stats_guard.checkpoints_processed,
                            total_checkpoints,
                            progress,
                            rate,
                            checkpoint_num
                        );
                    }
                    Ok(())
                }
            })
            .await
            .context("failed to stream checkpoints from storage")?;

        // Finalize stats
        let mut stats = stats.lock().await.clone();
        stats.elapsed_secs = start_time.elapsed().as_secs_f64();
        stats.bytes_downloaded = self.storage.bytes_downloaded();

        // Save final watermark
        if let Some(last_cp) = stats.current_checkpoint {
            self.watermark.save(last_cp).await?;
        }

        // Notify processor
        {
            let mut processor = self.processor.lock().await;
            processor
                .on_complete(stats.checkpoints_processed)
                .await?;
        }

        tracing::info!(
            "mass indexer complete: {} checkpoints in {:.2}s ({:.1} cp/s)",
            stats.checkpoints_processed,
            stats.elapsed_secs,
            stats.checkpoints_per_sec()
        );

        Ok(stats)
    }

    /// Run the indexer for all available checkpoints.
    pub async fn run_all(&mut self) -> Result<IndexerStats> {
        let coverage = self
            .storage
            .coverage_range()
            .await
            .ok_or_else(|| anyhow::anyhow!("no checkpoint coverage available"))?;

        self.run(coverage.0..coverage.1 + 1).await
    }
}

/// A simple processor that counts checkpoints and collects basic stats.
///
/// Useful for testing and verification.
pub struct CountingProcessor {
    pub checkpoints_seen: u64,
    pub total_transactions: u64,
    pub total_events: u64,
}

impl CountingProcessor {
    pub fn new() -> Self {
        Self {
            checkpoints_seen: 0,
            total_transactions: 0,
            total_events: 0,
        }
    }
}

impl Default for CountingProcessor {
    fn default() -> Self {
        Self::new()
    }
}

/// Output from the counting processor.
pub struct CountingOutput {
    pub transactions: u64,
    pub events: u64,
}

#[async_trait]
impl Processor for CountingProcessor {
    type Output = CountingOutput;

    async fn process(&self, checkpoint: &CheckpointData) -> Result<Self::Output> {
        let transactions = checkpoint.transactions.len() as u64;
        let events: u64 = checkpoint
            .transactions
            .iter()
            .filter_map(|tx| tx.events.as_ref())
            .map(|events| events.data.len() as u64)
            .sum();

        Ok(CountingOutput {
            transactions,
            events,
        })
    }

    async fn commit(
        &mut self,
        _checkpoint_num: CheckpointSequenceNumber,
        data: Self::Output,
    ) -> Result<()> {
        self.checkpoints_seen += 1;
        self.total_transactions += data.transactions;
        self.total_events += data.events;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_watermark_in_memory() {
        let watermark = Watermark::in_memory();
        assert!(watermark.get().await.is_none());

        watermark.save(100).await.unwrap();
        assert_eq!(watermark.get().await, Some(100));

        watermark.save(200).await.unwrap();
        assert_eq!(watermark.get().await, Some(200));
    }

    #[tokio::test]
    async fn test_counting_processor() {
        let processor = CountingProcessor::new();
        assert_eq!(processor.checkpoints_seen, 0);
        assert_eq!(processor.total_transactions, 0);
        assert_eq!(processor.total_events, 0);
    }
}
