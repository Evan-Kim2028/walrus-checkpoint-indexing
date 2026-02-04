//! Core Walrus checkpoint storage and retrieval.
//!
//! This module provides the main `WalrusStorage` type for streaming Sui checkpoints
//! from Walrus decentralized storage. It supports multiple fetch strategies:
//!
//! - **Full blob download**: Download entire blobs (2-3 GB each) for maximum throughput
//! - **Byte-range streaming**: Stream specific byte ranges using forked CLI
//! - **Adaptive fetching**: Automatically choose strategy based on network health
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    WalrusStorage                             │
//! │                                                              │
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
//! │  │   Metadata   │  │   Index      │  │   Range      │       │
//! │  │   Service    │  │   Cache      │  │   Fetcher    │       │
//! │  └──────────────┘  └──────────────┘  └──────────────┘       │
//! │         │                 │                 │                │
//! │         └─────────────────┼─────────────────┘                │
//! │                           │                                  │
//! │                  ┌────────▼────────┐                         │
//! │                  │  Checkpoint     │                         │
//! │                  │  Deserializer   │                         │
//! │                  └─────────────────┘                         │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use anyhow::{Context, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use futures::stream::{self, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use sui_types::full_checkpoint_content::CheckpointData;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::RwLock;

use crate::blob::BlobMetadata;
use crate::config::Config;
use crate::node_health::NodeHealthTracker;
use crate::sliver::{BlobAnalysis, RangeRisk, SliverPredictor};

/// Parsed index entry from a blob
#[derive(Debug, Clone)]
struct BlobIndexEntry {
    #[allow(dead_code)]
    pub checkpoint_number: u64,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone)]
struct CoalescedRange<T> {
    pub start: u64,
    pub end: u64,
    pub entries: Vec<(CheckpointSequenceNumber, BlobIndexEntry, T)>,
}

impl<T> CoalescedRange<T> {
    fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }
}

fn coalesce_entries<T: Clone>(
    entries: &[(CheckpointSequenceNumber, BlobIndexEntry, T)],
    max_gap_bytes: u64,
    max_range_bytes: u64,
) -> Vec<CoalescedRange<T>> {
    if entries.is_empty() {
        return Vec::new();
    }

    let mut sorted = entries.to_vec();
    sorted.sort_by_key(|(_, entry, _)| entry.offset);

    let mut ranges = Vec::new();
    let mut current: Option<CoalescedRange<T>> = None;

    for (checkpoint, entry, payload) in sorted {
        let entry_start = entry.offset;
        let entry_end = entry.offset.saturating_add(entry.length);

        match current.take() {
            Some(mut range) => {
                let gap_ok = entry_start <= range.end.saturating_add(max_gap_bytes);
                let new_end = range.end.max(entry_end);
                let size_ok = new_end.saturating_sub(range.start) <= max_range_bytes;

                if gap_ok && size_ok {
                    range.end = new_end;
                    range.entries.push((checkpoint, entry, payload));
                    current = Some(range);
                } else {
                    ranges.push(range);
                    current = Some(CoalescedRange {
                        start: entry_start,
                        end: entry_end,
                        entries: vec![(checkpoint, entry, payload)],
                    });
                }
            }
            None => {
                current = Some(CoalescedRange {
                    start: entry_start,
                    end: entry_end,
                    entries: vec![(checkpoint, entry, payload)],
                });
            }
        }
    }

    if let Some(range) = current {
        ranges.push(range);
    }

    ranges
}

/// Statistics about download performance
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DownloadStats {
    pub bytes_downloaded: u64,
    pub timeout_count: usize,
    pub ranges_fetched: u64,
    pub checkpoints_processed: u64,
}

/// Walrus checkpoint storage
///
/// Downloads checkpoints from Walrus aggregator using blob-based storage:
/// 1. Fetch blob metadata from walrus-sui-archival service
/// 2. Download blobs (2-3 GB each) or use local cache
/// 3. Extract checkpoints from blobs using internal index
#[derive(Clone)]
pub struct WalrusStorage {
    inner: Arc<Inner>,
}

struct Inner {
    archival_url: String,
    aggregator_url: String,
    cache_dir: PathBuf,
    cache_enabled: bool,
    walrus_cli_path: Option<PathBuf>,
    walrus_cli_context: String,
    walrus_cli_timeout_secs: u64,
    coalesce_gap_bytes: u64,
    coalesce_max_range_bytes: u64,
    walrus_cli_blob_concurrency: usize,
    walrus_cli_range_concurrency: usize,
    walrus_cli_range_max_retries: usize,
    walrus_cli_range_retry_delay_secs: u64,
    client: Client,
    index_cache: RwLock<HashMap<String, HashMap<u64, BlobIndexEntry>>>,
    blob_size_cache: RwLock<HashMap<String, u64>>,
    bad_blobs: RwLock<HashSet<String>>,
    metadata: RwLock<Vec<BlobMetadata>>,
    bytes_downloaded: AtomicU64,
    // Node health tracking
    health_tracker: Option<NodeHealthTracker>,
    timeout_count: AtomicUsize,
    last_health_poll_timeout_count: AtomicUsize,
    // Sliver prediction for adaptive fetching
    sliver_predictor: RwLock<SliverPredictor>,
}

impl WalrusStorage {
    /// Create a new Walrus storage instance from configuration
    pub async fn new(config: Config) -> Result<Self> {
        let cache_dir = config
            .cache_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(".walrus-cache"));

        // Create cache directory if caching is enabled (for CLI or HTTP)
        if config.cache_enabled {
            std::fs::create_dir_all(&cache_dir).context("failed to create cache dir")?;
        }

        let walrus_cli_path = config.resolved_cli_path();

        // Create health tracker if CLI path is available
        let health_tracker = walrus_cli_path.as_ref().map(|cli_path| {
            NodeHealthTracker::new(
                cli_path.clone(),
                config.walrus_cli_context.clone(),
                config.cli_timeout_secs,
                config.health_poll_interval_secs,
            )
        });

        Ok(Self {
            inner: Arc::new(Inner {
                archival_url: config.archival_url,
                aggregator_url: config.aggregator_url,
                cache_dir,
                cache_enabled: config.cache_enabled,
                walrus_cli_path,
                walrus_cli_context: config.walrus_cli_context,
                walrus_cli_timeout_secs: config.cli_timeout_secs,
                coalesce_gap_bytes: config.coalesce_gap_bytes,
                coalesce_max_range_bytes: config.coalesce_max_range_bytes,
                walrus_cli_blob_concurrency: config.blob_concurrency.max(1),
                walrus_cli_range_concurrency: config.range_concurrency.max(1),
                walrus_cli_range_max_retries: config.max_retries.max(1),
                walrus_cli_range_retry_delay_secs: config.retry_delay_secs,
                client: Client::builder()
                    .timeout(Duration::from_secs(config.http_timeout_secs))
                    .build()
                    .context("failed to create HTTP client")?,
                index_cache: RwLock::new(HashMap::new()),
                blob_size_cache: RwLock::new(HashMap::new()),
                bad_blobs: RwLock::new(HashSet::new()),
                metadata: RwLock::new(Vec::new()),
                bytes_downloaded: AtomicU64::new(0),
                health_tracker,
                timeout_count: AtomicUsize::new(0),
                last_health_poll_timeout_count: AtomicUsize::new(0),
                sliver_predictor: RwLock::new(SliverPredictor::new(HashSet::new())),
            }),
        })
    }

    /// Get total bytes downloaded
    pub fn bytes_downloaded(&self) -> u64 {
        self.inner.bytes_downloaded.load(Ordering::Relaxed)
    }

    /// Get the node health tracker (if available)
    pub fn health_tracker(&self) -> Option<&NodeHealthTracker> {
        self.inner.health_tracker.as_ref()
    }

    /// Get current timeout count
    pub fn timeout_count(&self) -> usize {
        self.inner.timeout_count.load(Ordering::Relaxed)
    }

    /// Get download statistics
    pub fn stats(&self) -> DownloadStats {
        DownloadStats {
            bytes_downloaded: self.bytes_downloaded(),
            timeout_count: self.timeout_count(),
            ranges_fetched: 0,        // TODO: track this
            checkpoints_processed: 0, // TODO: track this
        }
    }

    /// Poll node health and log summary
    pub async fn poll_node_health(&self) -> Result<()> {
        if let Some(tracker) = &self.inner.health_tracker {
            let summary = tracker.poll_health().await?;
            tracing::info!(
                "node health: {} healthy, {} degraded, {} down (of {} total); {} problematic shards of {}",
                summary.healthy_nodes,
                summary.degraded_nodes,
                summary.down_nodes,
                summary.total_nodes,
                summary.problematic_shards,
                summary.total_shards
            );
            if !summary.down_node_names.is_empty() {
                tracing::warn!("DOWN nodes: {}", summary.down_node_names.join(", "));
            }
            if !summary.degraded_node_names.is_empty() {
                tracing::info!("DEGRADED nodes: {}", summary.degraded_node_names.join(", "));
            }
            // Update sliver predictor with new problematic shards
            self.update_sliver_predictor().await;
        }
        Ok(())
    }

    /// Check if health poll is needed based on timeout count
    async fn maybe_poll_health_on_timeout(&self) {
        let current_timeouts = self.inner.timeout_count.load(Ordering::Relaxed);
        let last_poll_timeouts = self
            .inner
            .last_health_poll_timeout_count
            .load(Ordering::Relaxed);

        if current_timeouts >= last_poll_timeouts + 3 {
            if let Some(tracker) = &self.inner.health_tracker {
                if tracker.needs_poll().await {
                    tracing::info!(
                        "triggering health poll due to {} new timeouts",
                        current_timeouts - last_poll_timeouts
                    );
                    if let Err(e) = self.poll_node_health().await {
                        tracing::warn!("failed to poll node health: {}", e);
                    } else {
                        self.update_sliver_predictor().await;
                    }
                    self.inner
                        .last_health_poll_timeout_count
                        .store(current_timeouts, Ordering::Relaxed);
                }
            }
        }
    }

    fn record_timeout(&self) {
        self.inner.timeout_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Update the sliver predictor with current problematic shards
    pub async fn update_sliver_predictor(&self) {
        if let Some(tracker) = &self.inner.health_tracker {
            let problematic_shards = tracker.get_problematic_shards().await;
            let mut predictor = self.inner.sliver_predictor.write().await;
            predictor.update_shards(problematic_shards);
        }
    }

    /// Analyze a blob and get its safe/risky ranges
    pub async fn analyze_blob(&self, blob_id: &str, blob_size: u64) -> Option<BlobAnalysis> {
        let predictor = self.inner.sliver_predictor.read().await;
        predictor.analyze_blob(blob_id, blob_size)
    }

    /// Get the risk classification for a byte range
    pub async fn classify_range(&self, blob_id: &str, range: &std::ops::Range<u64>) -> RangeRisk {
        let predictor = self.inner.sliver_predictor.read().await;
        predictor.classify_range(blob_id, range)
    }

    async fn log_blob_analysis(&self, blob_id: &str, blob_size: u64) {
        if let Some(analysis) = self.analyze_blob(blob_id, blob_size).await {
            let safe_count = analysis.safe_ranges.len();
            let risky_count = analysis.risky_ranges.len();
            let problematic_primary = analysis.problematic_primary_slivers.len();

            if problematic_primary > 0 {
                tracing::info!(
                    "blob {} analysis: {:.1}% safe, {} safe ranges, {} risky ranges, {} problematic primary slivers (rotation={})",
                    blob_id,
                    analysis.safe_percentage,
                    safe_count,
                    risky_count,
                    problematic_primary,
                    analysis.rotation_offset
                );
            } else {
                tracing::debug!(
                    "blob {} analysis: 100% safe (rotation={})",
                    blob_id,
                    analysis.rotation_offset
                );
            }
        }
    }

    /// Initialize by fetching blob metadata from archival service
    pub async fn initialize(&self) -> Result<()> {
        tracing::info!(
            "fetching Walrus blob metadata from: {}",
            self.inner.archival_url
        );

        let base_url = format!("{}/v1/app_blobs", self.inner.archival_url);
        let mut all_blobs: Vec<BlobMetadata> = Vec::new();
        let mut seen_ids: HashSet<String> = HashSet::new();
        let mut next_cursor: Option<u64> = None;

        loop {
            let url = match next_cursor {
                Some(cursor) => format!("{}?cursor={}", base_url, cursor),
                None => base_url.clone(),
            };

            let response = self
                .inner
                .client
                .get(&url)
                .send()
                .await
                .with_context(|| format!("failed to fetch blobs from: {}", url))?;

            if !response.status().is_success() {
                return Err(anyhow::anyhow!(
                    "Walrus archival service returned error status {}",
                    response.status()
                ));
            }

            let blobs: crate::blob::BlobsResponse = response
                .json()
                .await
                .context("failed to parse blobs response")?;

            let mut added = 0usize;
            for blob in blobs.blobs {
                if seen_ids.insert(blob.blob_id.clone()) {
                    all_blobs.push(blob);
                    added += 1;
                }
            }

            next_cursor = blobs.next_cursor;

            // Stop if no new blobs or no further cursor
            if added == 0 || next_cursor.is_none() {
                break;
            }
        }

        let mut metadata = self.inner.metadata.write().await;
        *metadata = all_blobs;

        let min_start = metadata
            .iter()
            .map(|b| b.start_checkpoint)
            .min()
            .unwrap_or(0);
        let max_end = metadata.iter().map(|b| b.end_checkpoint).max().unwrap_or(0);

        tracing::info!(
            "fetched {} Walrus blobs covering checkpoints {}..{}",
            metadata.len(),
            min_start,
            max_end
        );

        // Poll node health at startup
        if let Some(tracker) = &self.inner.health_tracker {
            match tracker.poll_health().await {
                Ok(summary) => {
                    tracing::info!(
                        "initial node health: {} healthy, {} degraded, {} down (of {} total); {} problematic shards",
                        summary.healthy_nodes,
                        summary.degraded_nodes,
                        summary.down_nodes,
                        summary.total_nodes,
                        summary.problematic_shards
                    );
                    if !summary.down_node_names.is_empty() {
                        tracing::warn!(
                            "DOWN nodes at startup: {}",
                            summary.down_node_names.join(", ")
                        );
                    }
                    self.update_sliver_predictor().await;
                    tracing::info!(
                        "sliver predictor initialized with {} problematic shards",
                        summary.problematic_shards
                    );
                }
                Err(e) => {
                    tracing::warn!("failed to poll initial node health: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Get min/max checkpoint coverage from blob metadata
    pub async fn coverage_range(&self) -> Option<(u64, u64)> {
        let metadata = self.inner.metadata.read().await;
        let min_start = metadata.iter().map(|b| b.start_checkpoint).min()?;
        let max_end = metadata.iter().map(|b| b.end_checkpoint).max()?;
        Some((min_start, max_end))
    }

    /// Find blob containing a specific checkpoint
    async fn find_blob_for_checkpoint(&self, checkpoint: u64) -> Option<BlobMetadata> {
        let metadata = self.inner.metadata.read().await;
        metadata
            .iter()
            .find(|blob| checkpoint >= blob.start_checkpoint && checkpoint <= blob.end_checkpoint)
            .cloned()
    }

    /// Stream checkpoints in a range, calling the handler for each
    pub async fn stream_checkpoints<F, Fut>(
        &self,
        range: std::ops::Range<CheckpointSequenceNumber>,
        mut on_checkpoint: F,
    ) -> Result<u64>
    where
        F: FnMut(CheckpointData) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let metadata = self.inner.metadata.read().await;
        let mut blobs: Vec<BlobMetadata> = metadata
            .iter()
            .filter(|b| b.end_checkpoint >= range.start && b.start_checkpoint < range.end)
            .cloned()
            .collect();
        drop(metadata);

        if blobs.is_empty() {
            return Ok(0);
        }

        blobs.sort_by_key(|b| b.start_checkpoint);
        let mut total_processed = 0u64;

        for blob in blobs {
            // Ensure blob is cached (works for both CLI and HTTP)
            self.ensure_blob_cached(&blob.blob_id).await?;

            let index = match self.load_blob_index(&blob.blob_id).await {
                Ok(index) => index,
                Err(e) if self.inner.walrus_cli_path.is_some() => {
                    self.mark_bad_blob(&blob.blob_id).await;
                    tracing::warn!(
                        "skipping blob {} due to index load error: {}",
                        blob.blob_id,
                        e
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };

            let start_cp = range.start.max(blob.start_checkpoint);
            let end_cp_exclusive = range.end.min(blob.end_checkpoint.saturating_add(1));
            if start_cp >= end_cp_exclusive {
                continue;
            }

            let mut tasks = Vec::new();
            for cp_num in start_cp..end_cp_exclusive {
                if let Some(entry) = index.get(&cp_num) {
                    tasks.push((cp_num, entry.clone()));
                }
            }

            if tasks.is_empty() {
                continue;
            }

            let max_gap_bytes = self.inner.coalesce_gap_bytes;
            let max_range_bytes = self.inner.coalesce_max_range_bytes;

            // Log blob analysis for streaming mode
            if self.inner.walrus_cli_path.is_some() && !self.inner.cache_enabled {
                match self.blob_size_via_cli(&blob.blob_id).await {
                    Ok(actual_size) => {
                        self.log_blob_analysis(&blob.blob_id, actual_size).await;
                    }
                    Err(e) => {
                        tracing::warn!("failed to get blob size for analysis: {}", e);
                    }
                }
            }

            let pending: Vec<(CheckpointSequenceNumber, BlobIndexEntry, ())> = tasks
                .into_iter()
                .map(|(cp_num, entry)| (cp_num, entry, ()))
                .collect();
            let coalesced = coalesce_entries(&pending, max_gap_bytes, max_range_bytes);

            for coalesced_range in coalesced {
                if coalesced_range.len() == 0 {
                    continue;
                }

                let bytes = self
                    .download_range(&blob.blob_id, coalesced_range.start, coalesced_range.len())
                    .await?;

                let mut entries = coalesced_range.entries;
                entries.sort_by_key(|(cp_num, _, _)| *cp_num);

                for (cp_num, entry, _) in entries {
                    let start = entry.offset.saturating_sub(coalesced_range.start) as usize;
                    let end = start + entry.length as usize;
                    let checkpoint = sui_storage::blob::Blob::from_bytes::<CheckpointData>(
                        &bytes[start..end],
                    )
                    .with_context(|| format!("failed to deserialize checkpoint {}", cp_num))?;
                    on_checkpoint(checkpoint).await?;
                    total_processed += 1;
                }
            }
        }

        Ok(total_processed)
    }

    /// Get a single checkpoint
    pub async fn get_checkpoint(
        &self,
        checkpoint: CheckpointSequenceNumber,
    ) -> Result<CheckpointData> {
        let blob = self
            .find_blob_for_checkpoint(checkpoint)
            .await
            .ok_or_else(|| anyhow::anyhow!("no blob found for checkpoint {}", checkpoint))?;

        let index = self.load_blob_index(&blob.blob_id).await?;

        let entry = index
            .get(&checkpoint)
            .ok_or_else(|| anyhow::anyhow!("checkpoint {} not found in blob index", checkpoint))?;

        let cp_bytes = self
            .download_range(&blob.blob_id, entry.offset, entry.length)
            .await?;

        let checkpoint_data = sui_storage::blob::Blob::from_bytes::<CheckpointData>(&cp_bytes)
            .with_context(|| format!("failed to deserialize checkpoint {}", checkpoint))?;

        Ok(checkpoint_data)
    }

    /// Get multiple checkpoints in a range
    pub async fn get_checkpoints(
        &self,
        range: std::ops::Range<CheckpointSequenceNumber>,
    ) -> Result<Vec<CheckpointData>> {
        let metadata = self.inner.metadata.read().await;
        let blobs: Vec<BlobMetadata> = metadata
            .iter()
            .filter(|b| b.end_checkpoint >= range.start && b.start_checkpoint < range.end)
            .cloned()
            .collect();
        drop(metadata);

        if blobs.is_empty() {
            return Ok(Vec::new());
        }

        let checkpoints = Arc::new(RwLock::new(Vec::new()));
        let blob_concurrency = if self.inner.walrus_cli_path.is_some() {
            self.inner.walrus_cli_blob_concurrency
        } else {
            1
        };

        let mut blob_stream = stream::iter(blobs)
            .map(|blob| {
                let storage = self.clone();
                let range = range.clone();
                let checkpoints = checkpoints.clone();

                async move {
                    // Refresh node health when starting a new blob (only when streaming, not caching)
                    if !storage.inner.cache_enabled {
                        if let Err(e) = storage.poll_node_health().await {
                            tracing::debug!(
                                "failed to refresh node health for blob {}: {}",
                                blob.blob_id,
                                e
                            );
                        } else {
                            storage.update_sliver_predictor().await;
                        }
                    }

                    // Ensure blob is cached (works for both CLI and HTTP)
                    storage.ensure_blob_cached(&blob.blob_id).await?;

                    let index = match storage.load_blob_index(&blob.blob_id).await {
                        Ok(index) => index,
                        Err(e) if storage.inner.walrus_cli_path.is_some() => {
                            storage.mark_bad_blob(&blob.blob_id).await;
                            tracing::warn!(
                                "skipping blob {} due to index load error: {}",
                                blob.blob_id,
                                e
                            );
                            return Ok::<(), anyhow::Error>(());
                        }
                        Err(e) => return Err(e),
                    };

                    let mut tasks = Vec::new();
                    for cp_num in range.start..range.end {
                        if let Some(entry) = index.get(&cp_num) {
                            tasks.push((cp_num, entry.clone()));
                        }
                    }

                    if tasks.is_empty() {
                        return Ok::<(), anyhow::Error>(());
                    }

                    // Log blob analysis
                    if storage.inner.walrus_cli_path.is_some() && !storage.inner.cache_enabled {
                        match storage.blob_size_via_cli(&blob.blob_id).await {
                            Ok(actual_size) => {
                                storage.log_blob_analysis(&blob.blob_id, actual_size).await;
                            }
                            Err(e) => {
                                tracing::warn!("failed to get blob size for analysis: {}", e);
                            }
                        }
                    }

                    let chunk_size = if storage.inner.walrus_cli_path.is_some() {
                        tasks.len()
                    } else {
                        200
                    };
                    let mut results = Vec::new();

                    let max_gap_bytes = storage.inner.coalesce_gap_bytes;
                    let max_range_bytes = storage.inner.coalesce_max_range_bytes;

                    for chunk in tasks.chunks(chunk_size) {
                        let pending: Vec<(CheckpointSequenceNumber, BlobIndexEntry, ())> = chunk
                            .iter()
                            .cloned()
                            .map(|(cp_num, entry)| (cp_num, entry, ()))
                            .collect();

                        let coalesced = coalesce_entries(&pending, max_gap_bytes, max_range_bytes);

                        let range_concurrency = if storage.inner.walrus_cli_path.is_some() {
                            storage.inner.walrus_cli_range_concurrency
                        } else {
                            1
                        };

                        let mut range_stream = stream::iter(coalesced)
                            .map(|range| {
                                let storage = storage.clone();
                                let blob_id = blob.blob_id.clone();
                                async move {
                                    if range.len() == 0 {
                                        return Ok::<Vec<CheckpointData>, anyhow::Error>(Vec::new());
                                    }

                                    let bytes = storage
                                        .download_range(&blob_id, range.start, range.len())
                                        .await?;

                                    let mut decoded = Vec::with_capacity(range.entries.len());
                                    for (cp_num, entry, _) in range.entries {
                                        let start =
                                            entry.offset.saturating_sub(range.start) as usize;
                                        let end = start + entry.length as usize;
                                        let checkpoint = sui_storage::blob::Blob::from_bytes::<
                                            CheckpointData,
                                        >(
                                            &bytes[start..end]
                                        )
                                        .with_context(|| {
                                            format!("failed to deserialize checkpoint {}", cp_num)
                                        })?;
                                        decoded.push(checkpoint);
                                    }
                                    Ok(decoded)
                                }
                            })
                            .buffer_unordered(range_concurrency);

                        while let Some(range_result) = range_stream.next().await {
                            let mut decoded = range_result?;
                            results.append(&mut decoded);
                        }
                    }

                    let mut lock = checkpoints.write().await;
                    lock.extend(results);
                    Ok(())
                }
            })
            .buffer_unordered(blob_concurrency);

        while let Some(result) = blob_stream.next().await {
            result?;
        }

        drop(blob_stream);

        let mut final_checkpoints = Arc::try_unwrap(checkpoints).unwrap().into_inner();
        final_checkpoints.sort_by_key(|cp| cp.checkpoint_summary.sequence_number);

        Ok(final_checkpoints)
    }

    /// Check if a checkpoint is available
    pub async fn has_checkpoint(&self, checkpoint: CheckpointSequenceNumber) -> bool {
        self.find_blob_for_checkpoint(checkpoint).await.is_some()
    }

    /// Get the latest available checkpoint
    pub async fn get_latest_checkpoint(&self) -> Option<CheckpointSequenceNumber> {
        let metadata = self.inner.metadata.read().await;
        metadata.iter().map(|blob| blob.end_checkpoint).max()
    }

    // === Internal methods ===

    async fn mark_bad_blob(&self, blob_id: &str) {
        let mut bad = self.inner.bad_blobs.write().await;
        bad.insert(blob_id.to_string());
    }

    fn get_cached_blob_path(&self, blob_id: &str) -> PathBuf {
        self.inner.cache_dir.join(blob_id)
    }

    async fn run_cli_with_timeout(&self, mut cmd: Command) -> Result<std::process::Output> {
        cmd.kill_on_drop(true);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let child = cmd.spawn().context("failed to spawn walrus cli")?;
        let timeout = Duration::from_secs(self.inner.walrus_cli_timeout_secs);
        match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(res) => res.context("failed to wait on walrus cli"),
            Err(_) => Err(anyhow::anyhow!(
                "walrus cli timed out after {}s",
                self.inner.walrus_cli_timeout_secs
            )),
        }
    }

    async fn download_blob_via_cli(&self, blob_id: &str) -> Result<PathBuf> {
        let cli_path = self
            .inner
            .walrus_cli_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Walrus CLI path not configured"))?;

        let output_path = self.get_cached_blob_path(blob_id);

        if output_path.exists() {
            return Ok(output_path);
        }

        tracing::info!(
            "Starting download of blob {} via CLI to {}",
            blob_id,
            output_path.display()
        );
        let start_time = Instant::now();

        let mut cmd = Command::new(cli_path);
        cmd.arg("read")
            .arg(blob_id)
            .arg("--context")
            .arg(&self.inner.walrus_cli_context)
            .arg("--out")
            .arg(&output_path);

        let output = self.run_cli_with_timeout(cmd).await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "walrus cli failed with status {}: {}",
                output.status,
                stderr.trim()
            ));
        }

        let elapsed = start_time.elapsed();
        let size_bytes = output_path.metadata()?.len();
        self.inner
            .bytes_downloaded
            .fetch_add(size_bytes, Ordering::Relaxed);
        let size_mb = size_bytes as f64 / 1_000_000.0;
        tracing::info!(
            "Finished download of blob {} in {:.2}s ({:.2} MB/s, total size: {:.2} MB)",
            blob_id,
            elapsed.as_secs_f64(),
            size_mb / elapsed.as_secs_f64(),
            size_mb
        );

        Ok(output_path)
    }

    /// Ensure a blob is cached locally (unified across CLI and HTTP methods)
    /// Returns the path to the cached blob, or None if caching is disabled
    async fn ensure_blob_cached(&self, blob_id: &str) -> Result<Option<PathBuf>> {
        if !self.inner.cache_enabled {
            return Ok(None);
        }

        let cached_path = self.get_cached_blob_path(blob_id);
        if cached_path.exists() {
            return Ok(Some(cached_path));
        }

        // Download via CLI if available, otherwise via HTTP
        if self.inner.walrus_cli_path.is_some() {
            self.download_blob_via_cli(blob_id).await?;
        } else {
            self.download_blob_via_http(blob_id).await?;
        }

        Ok(Some(cached_path))
    }

    /// Check if a blob is cached
    pub fn is_blob_cached(&self, blob_id: &str) -> bool {
        self.get_cached_blob_path(blob_id).exists()
    }

    /// Get blobs that cover a checkpoint range
    pub async fn get_blobs_for_range(
        &self,
        range: std::ops::Range<CheckpointSequenceNumber>,
    ) -> Vec<BlobMetadata> {
        let metadata = self.inner.metadata.read().await;
        let mut blobs: Vec<BlobMetadata> = metadata
            .iter()
            .filter(|b| b.end_checkpoint >= range.start && b.start_checkpoint < range.end)
            .cloned()
            .collect();
        blobs.sort_by_key(|b| b.start_checkpoint);
        blobs
    }

    /// Prefetch multiple blobs in parallel (no progress bars)
    /// Returns the number of blobs successfully prefetched
    pub async fn prefetch_blobs_parallel(
        &self,
        blob_ids: &[String],
        concurrency: usize,
    ) -> Result<usize> {
        self.prefetch_blobs_parallel_with_progress(blob_ids, concurrency, false)
            .await
    }

    /// Prefetch multiple blobs in parallel with optional progress bars
    /// Returns the number of blobs successfully prefetched
    pub async fn prefetch_blobs_parallel_with_progress(
        &self,
        blob_ids: &[String],
        concurrency: usize,
        show_progress: bool,
    ) -> Result<usize> {
        if blob_ids.is_empty() {
            return Ok(0);
        }

        let concurrency = concurrency.max(1);
        tracing::info!(
            "Starting parallel prefetch of {} blobs with concurrency {}",
            blob_ids.len(),
            concurrency
        );
        let start_time = Instant::now();

        // Set up multi-progress for concurrent progress bars
        let multi_progress = if show_progress {
            Some(Arc::new(MultiProgress::new()))
        } else {
            None
        };

        let results: Vec<Result<PathBuf>> = stream::iter(blob_ids.iter().cloned().enumerate())
            .map(|(idx, blob_id)| {
                let storage = self.clone();
                let mp = multi_progress.clone();
                async move {
                    if storage.inner.walrus_cli_path.is_some() {
                        storage
                            .download_blob_via_cli_with_progress(&blob_id, idx, mp)
                            .await
                    } else {
                        storage
                            .download_blob_via_http_with_progress(&blob_id, idx, mp)
                            .await
                    }
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;

        let mut success_count = 0;
        let mut fail_count = 0;
        for result in results {
            match result {
                Ok(_) => success_count += 1,
                Err(e) => {
                    fail_count += 1;
                    tracing::warn!("Failed to prefetch blob: {}", e);
                }
            }
        }

        let elapsed = start_time.elapsed();
        let total_bytes = self.bytes_downloaded();
        let mb = total_bytes as f64 / 1_000_000.0;

        if show_progress {
            eprintln!(
                "\nPrefetch complete: {}/{} blobs in {:.1}s ({:.1} MB, {:.1} MB/s)",
                success_count,
                blob_ids.len(),
                elapsed.as_secs_f64(),
                mb,
                mb / elapsed.as_secs_f64()
            );
        } else {
            tracing::info!(
                "Parallel prefetch complete: {}/{} blobs in {:.2}s ({:.2} MB total, {:.2} MB/s avg)",
                success_count,
                blob_ids.len(),
                elapsed.as_secs_f64(),
                mb,
                mb / elapsed.as_secs_f64()
            );
        }

        if fail_count > 0 {
            tracing::warn!("{} blobs failed to prefetch", fail_count);
        }

        Ok(success_count)
    }

    async fn download_blob_via_http(&self, blob_id: &str) -> Result<PathBuf> {
        let output_path = self.get_cached_blob_path(blob_id);

        if output_path.exists() {
            return Ok(output_path);
        }

        tracing::info!(
            "Starting download of blob {} via HTTP to {}",
            blob_id,
            output_path.display()
        );
        let start_time = Instant::now();

        let url = format!("{}/v1/blobs/{}", self.inner.aggregator_url, blob_id);

        let bytes = self
            .with_retry(|| {
                let client = self.inner.client.clone();
                let url = url.clone();
                async move {
                    let response = client
                        .get(&url)
                        .timeout(Duration::from_secs(600)) // 10 min timeout for large blobs
                        .send()
                        .await?;

                    if !response.status().is_success() {
                        return Err(anyhow::anyhow!("status {}", response.status()));
                    }
                    Ok(response.bytes().await?.to_vec())
                }
            })
            .await
            .with_context(|| format!("failed to download blob {}", blob_id))?;

        // Write to temp file then rename for atomicity
        let temp_path = output_path.with_extension("tmp");
        tokio::fs::write(&temp_path, &bytes)
            .await
            .with_context(|| format!("failed to write blob to {:?}", temp_path))?;
        tokio::fs::rename(&temp_path, &output_path)
            .await
            .with_context(|| format!("failed to rename {:?} to {:?}", temp_path, output_path))?;

        let elapsed = start_time.elapsed();
        let size_bytes = bytes.len() as u64;
        self.inner
            .bytes_downloaded
            .fetch_add(size_bytes, Ordering::Relaxed);
        let size_mb = size_bytes as f64 / 1_000_000.0;
        tracing::info!(
            "Finished download of blob {} in {:.2}s ({:.2} MB/s, total size: {:.2} MB)",
            blob_id,
            elapsed.as_secs_f64(),
            size_mb / elapsed.as_secs_f64(),
            size_mb
        );

        Ok(output_path)
    }

    /// Download blob via HTTP with progress bar
    async fn download_blob_via_http_with_progress(
        &self,
        blob_id: &str,
        blob_index: usize,
        multi_progress: Option<Arc<MultiProgress>>,
    ) -> Result<PathBuf> {
        let output_path = self.get_cached_blob_path(blob_id);

        if output_path.exists() {
            // Show cached status briefly
            if let Some(mp) = &multi_progress {
                let pb = mp.add(ProgressBar::new(100));
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template("{prefix:.bold} {msg}")
                        .unwrap(),
                );
                pb.set_prefix(format!("[Blob {}]", blob_index + 1));
                pb.set_message("cached ✓");
                pb.finish();
            }
            return Ok(output_path);
        }

        let start_time = Instant::now();
        let url = format!("{}/v1/blobs/{}", self.inner.aggregator_url, blob_id);
        let short_id = &blob_id[..12.min(blob_id.len())];

        // First, get the content length with a HEAD request
        let total_size = match self
            .inner
            .client
            .head(&url)
            .timeout(Duration::from_secs(30))
            .send()
            .await
        {
            Ok(resp) => resp
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(3_200_000_000),
            Err(_) => 3_200_000_000, // Default ~3.2 GB estimate
        };

        // Create progress bar
        let pb = if let Some(mp) = &multi_progress {
            let pb = mp.add(ProgressBar::new(total_size));
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{prefix:.bold} [{bar:30.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) {msg}")
                    .unwrap()
                    .progress_chars("█▓░"),
            );
            pb.set_prefix(format!("[Blob {}]", blob_index + 1));
            pb.set_message(format!("{}...", short_id));
            Some(pb)
        } else {
            None
        };

        // Stream the download with progress updates
        let response = self
            .inner
            .client
            .get(&url)
            .timeout(Duration::from_secs(600))
            .send()
            .await
            .with_context(|| format!("failed to start download for blob {}", blob_id))?;

        if !response.status().is_success() {
            if let Some(pb) = &pb {
                pb.abandon_with_message("failed");
            }
            return Err(anyhow::anyhow!(
                "HTTP {} for blob {}",
                response.status(),
                blob_id
            ));
        }

        // Stream to file with progress
        let temp_path = output_path.with_extension("tmp");
        let mut file = tokio::fs::File::create(&temp_path)
            .await
            .with_context(|| format!("failed to create temp file {:?}", temp_path))?;

        let mut stream = response.bytes_stream();
        let mut downloaded: u64 = 0;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.with_context(|| "error reading response chunk")?;
            file.write_all(&chunk)
                .await
                .with_context(|| "error writing chunk to file")?;
            downloaded += chunk.len() as u64;
            if let Some(pb) = &pb {
                pb.set_position(downloaded);
            }
        }

        file.flush().await?;
        drop(file);

        // Rename for atomicity
        tokio::fs::rename(&temp_path, &output_path)
            .await
            .with_context(|| format!("failed to rename {:?} to {:?}", temp_path, output_path))?;

        let elapsed = start_time.elapsed();
        self.inner
            .bytes_downloaded
            .fetch_add(downloaded, Ordering::Relaxed);

        if let Some(pb) = &pb {
            let mb = downloaded as f64 / 1_000_000.0;
            pb.finish_with_message(format!(
                "done ({:.1} MB, {:.1} MB/s)",
                mb,
                mb / elapsed.as_secs_f64()
            ));
        }

        Ok(output_path)
    }

    /// Download blob via CLI with progress bar
    async fn download_blob_via_cli_with_progress(
        &self,
        blob_id: &str,
        blob_index: usize,
        multi_progress: Option<Arc<MultiProgress>>,
    ) -> Result<PathBuf> {
        let cli_path = self
            .inner
            .walrus_cli_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Walrus CLI path not configured"))?;

        let output_path = self.get_cached_blob_path(blob_id);

        if output_path.exists() {
            if let Some(mp) = &multi_progress {
                let pb = mp.add(ProgressBar::new(100));
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template("{prefix:.bold} {msg}")
                        .unwrap(),
                );
                pb.set_prefix(format!("[Blob {}]", blob_index + 1));
                pb.set_message("cached ✓");
                pb.finish();
            }
            return Ok(output_path);
        }

        let start_time = Instant::now();
        let short_id = &blob_id[..12.min(blob_id.len())];

        // Try to get expected blob size for progress bar
        let expected_size = self.blob_size_via_cli(blob_id).await.unwrap_or(3_200_000_000);

        // Create progress bar that tracks actual file size
        let pb = if let Some(mp) = &multi_progress {
            let pb = mp.add(ProgressBar::new(expected_size));
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{prefix:.bold} [{bar:30.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) {msg}")
                    .unwrap()
                    .progress_chars("█▓░"),
            );
            pb.set_prefix(format!("[Blob {}]", blob_index + 1));
            pb.set_message(format!("{}...", short_id));
            Some(pb)
        } else {
            None
        };

        // The walrus CLI writes to a .tmp file first, then renames
        let tmp_path = output_path.with_extension("tmp");

        let mut cmd = Command::new(cli_path);
        cmd.arg("read")
            .arg(blob_id)
            .arg("--context")
            .arg(&self.inner.walrus_cli_context)
            .arg("--out")
            .arg(&output_path);

        // Spawn the CLI process
        cmd.kill_on_drop(true);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd.spawn().context("failed to spawn walrus cli")?;

        // Poll the .tmp file size while the CLI runs
        let timeout = Duration::from_secs(self.inner.walrus_cli_timeout_secs);
        let poll_interval = Duration::from_millis(250);
        let deadline = Instant::now() + timeout;

        loop {
            // Check if process finished
            match tokio::time::timeout(poll_interval, child.wait()).await {
                Ok(Ok(status)) => {
                    // Process finished
                    let elapsed = start_time.elapsed();
                    let size = tokio::fs::metadata(&output_path)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(0);

                    if status.success() {
                        self.inner
                            .bytes_downloaded
                            .fetch_add(size, Ordering::Relaxed);

                        if let Some(pb) = &pb {
                            pb.set_position(size);
                            let mb = size as f64 / 1_000_000.0;
                            pb.finish_with_message(format!(
                                "done ({:.1} MB, {:.1} MB/s)",
                                mb,
                                mb / elapsed.as_secs_f64()
                            ));
                        }
                        return Ok(output_path);
                    } else {
                        if let Some(pb) = &pb {
                            pb.abandon_with_message("failed");
                        }
                        // Try to get stderr
                        let output = child.wait_with_output().await.ok();
                        let stderr = output
                            .map(|o| String::from_utf8_lossy(&o.stderr).to_string())
                            .unwrap_or_default();
                        return Err(anyhow::anyhow!("CLI failed with status {}: {}", status, stderr));
                    }
                }
                Ok(Err(e)) => {
                    if let Some(pb) = &pb {
                        pb.abandon_with_message("failed");
                    }
                    return Err(anyhow::anyhow!("CLI process error: {}", e));
                }
                Err(_) => {
                    // Timeout on wait - process still running, update progress
                    if let Some(pb) = &pb {
                        // Check .tmp file size
                        if let Ok(metadata) = tokio::fs::metadata(&tmp_path).await {
                            pb.set_position(metadata.len());
                        }
                    }

                    // Check overall timeout
                    if Instant::now() > deadline {
                        child.kill().await.ok();
                        if let Some(pb) = &pb {
                            pb.abandon_with_message("timed out");
                        }
                        return Err(anyhow::anyhow!(
                            "walrus cli timed out after {}s",
                            self.inner.walrus_cli_timeout_secs
                        ));
                    }
                }
            }
        }
    }

    async fn read_range_via_cli(&self, blob_id: &str, start: u64, length: u64) -> Result<Vec<u8>> {
        let cli_path = self
            .inner
            .walrus_cli_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Walrus CLI path not configured"))?;

        let t0 = Instant::now();
        let mut cmd = Command::new(cli_path);
        cmd.arg("read")
            .arg(blob_id)
            .arg("--context")
            .arg(&self.inner.walrus_cli_context)
            .arg("--stream")
            .arg("--start-byte")
            .arg(start.to_string())
            .arg("--byte-length")
            .arg(length.to_string());

        let output = self.run_cli_with_timeout(cmd).await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("walrus cli failed: {}", stderr.trim()));
        }

        let elapsed = t0.elapsed().as_secs_f64().max(1e-6);
        self.inner
            .bytes_downloaded
            .fetch_add(output.stdout.len() as u64, Ordering::Relaxed);
        let len = output.stdout.len() as u64;
        if len >= 4 * 1024 * 1024 {
            let mb = len as f64 / 1_000_000.0;
            tracing::info!(
                "walrus cli read {:.2} MB from {} (offset {}, len {}) in {:.2}s ({:.2} MB/s)",
                mb,
                blob_id,
                start,
                len,
                elapsed,
                mb / elapsed
            );
        }
        Ok(output.stdout)
    }

    async fn read_range_via_cli_resilient(
        &self,
        blob_id: &str,
        start: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let max_retries = self.inner.walrus_cli_range_max_retries;
        let retry_delay_secs = self.inner.walrus_cli_range_retry_delay_secs;

        let mut last_err: Option<anyhow::Error> = None;

        for attempt in 1..=max_retries {
            match self.read_range_via_cli(blob_id, start, length).await {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    let is_timeout = e.to_string().contains("timed out");
                    if is_timeout {
                        self.record_timeout();
                        self.maybe_poll_health_on_timeout().await;
                    }

                    tracing::warn!(
                        "walrus cli range read failed (attempt {}/{}): {}",
                        attempt,
                        max_retries,
                        e
                    );
                    last_err = Some(e);
                    if attempt < max_retries {
                        let delay = retry_delay_secs.saturating_mul(attempt as u64).max(1);
                        tokio::time::sleep(Duration::from_secs(delay)).await;
                    }
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("walrus cli range read failed")))
    }

    async fn blob_size_via_cli(&self, blob_id: &str) -> Result<u64> {
        {
            let cache = self.inner.blob_size_cache.read().await;
            if let Some(size) = cache.get(blob_id) {
                return Ok(*size);
            }
        }

        let cli_path = self
            .inner
            .walrus_cli_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Walrus CLI path not configured"))?;

        let mut stdout = String::new();
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 1..=3 {
            let mut cmd = Command::new(cli_path);
            cmd.arg("read")
                .arg(blob_id)
                .arg("--context")
                .arg(&self.inner.walrus_cli_context)
                .arg("--size-only")
                .arg("--json");

            let output = self.run_cli_with_timeout(cmd).await?;

            if output.status.success() {
                stdout = String::from_utf8_lossy(&output.stdout).to_string();
                last_err = None;
                break;
            }

            let stderr = String::from_utf8_lossy(&output.stderr);
            last_err = Some(anyhow::anyhow!(
                "walrus cli size-only failed with status {}: {}",
                output.status,
                stderr.trim()
            ));
            tracing::warn!("walrus cli size-only failed (attempt {}/3)", attempt);
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        if let Some(err) = last_err {
            return Err(err);
        }

        let trimmed = stdout.trim();
        let mut size: Option<u64> = None;
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) {
            if let Some(s) = val.get("size").and_then(|v| v.as_u64()) {
                size = Some(s);
            } else if let Some(s) = val.get("blobSize").and_then(|v| v.as_u64()) {
                size = Some(s);
            } else if let Some(s) = val.get("blob_size").and_then(|v| v.as_u64()) {
                size = Some(s);
            } else if let Some(s) = val.get("unencoded_size").and_then(|v| v.as_u64()) {
                size = Some(s);
            } else if let Some(s) = val.as_u64() {
                size = Some(s);
            }
        } else if let Ok(s) = trimmed.parse::<u64>() {
            size = Some(s);
        }

        let size = size.ok_or_else(|| {
            anyhow::anyhow!(
                "unable to parse blob size from walrus cli output: {}",
                trimmed
            )
        })?;

        let mut cache = self.inner.blob_size_cache.write().await;
        cache.insert(blob_id.to_string(), size);
        Ok(size)
    }

    async fn load_blob_index(&self, blob_id: &str) -> Result<HashMap<u64, BlobIndexEntry>> {
        // Check memory cache
        {
            let cache = self.inner.index_cache.read().await;
            if let Some(index) = cache.get(blob_id) {
                return Ok(index.clone());
            }
        }

        // Try reading from local file
        let cached_path = self.get_cached_blob_path(blob_id);
        if cached_path.exists() {
            tracing::info!("loading index from cached blob {}", cached_path.display());
            let mut file = std::fs::File::open(&cached_path)?;

            let file_len = file.metadata()?.len();
            if file_len < 24 {
                return Err(anyhow::anyhow!("cached blob too small"));
            }
            file.seek(SeekFrom::Start(file_len - 24))?;
            let magic = file.read_u32::<LittleEndian>()?;
            if magic != 0x574c4244 {
                return Err(anyhow::anyhow!("invalid blob footer magic"));
            }
            let _version = file.read_u32::<LittleEndian>()?;
            let index_start_offset = file.read_u64::<LittleEndian>()?;
            let count = file.read_u32::<LittleEndian>()?;

            file.seek(SeekFrom::Start(index_start_offset))?;
            let mut index_bytes = Vec::new();
            file.read_to_end(&mut index_bytes)?;

            let mut cursor = Cursor::new(&index_bytes);
            let mut index = HashMap::with_capacity(count as usize);

            for _ in 0..count {
                let name_len = cursor.read_u32::<LittleEndian>()?;
                let mut name_bytes = vec![0u8; name_len as usize];
                cursor.read_exact(&mut name_bytes)?;
                let name_str = String::from_utf8(name_bytes)?;
                let checkpoint_number = name_str.parse::<u64>()?;
                let offset = cursor.read_u64::<LittleEndian>()?;
                let length = cursor.read_u64::<LittleEndian>()?;
                let _entry_crc = cursor.read_u32::<LittleEndian>()?;

                index.insert(
                    checkpoint_number,
                    BlobIndexEntry {
                        checkpoint_number,
                        offset,
                        length,
                    },
                );
            }

            let mut cache = self.inner.index_cache.write().await;
            cache.insert(blob_id.to_string(), index.clone());
            return Ok(index);
        }

        // If CLI is configured, read footer + index via range reads
        if self.inner.walrus_cli_path.is_some() {
            let total_size = self.blob_size_via_cli(blob_id).await?;

            if total_size < 24 {
                return Err(anyhow::anyhow!("blob {} is too small", blob_id));
            }

            let footer_start = total_size - 24;
            let footer_bytes = self
                .read_range_via_cli_resilient(blob_id, footer_start, 24)
                .await?;

            if footer_bytes.len() != 24 {
                return Err(anyhow::anyhow!(
                    "invalid footer length for blob {}: {}",
                    blob_id,
                    footer_bytes.len()
                ));
            }

            let mut cursor = Cursor::new(&footer_bytes);
            let magic = cursor.read_u32::<LittleEndian>()?;
            if magic != 0x574c4244 {
                return Err(anyhow::anyhow!("invalid blob footer magic: {:x}", magic));
            }

            let _version = cursor.read_u32::<LittleEndian>()?;
            let index_start_offset = cursor.read_u64::<LittleEndian>()?;
            let count = cursor.read_u32::<LittleEndian>()?;

            if index_start_offset >= total_size {
                return Err(anyhow::anyhow!(
                    "invalid index start offset {} for blob size {}",
                    index_start_offset,
                    total_size
                ));
            }

            let index_len = total_size - index_start_offset;
            let index_bytes = self
                .read_range_via_cli_resilient(blob_id, index_start_offset, index_len)
                .await?;

            let mut cursor = Cursor::new(&index_bytes);
            let mut index = HashMap::with_capacity(count as usize);

            for _ in 0..count {
                let name_len = cursor.read_u32::<LittleEndian>()?;
                let mut name_bytes = vec![0u8; name_len as usize];
                cursor.read_exact(&mut name_bytes)?;
                let name_str = String::from_utf8(name_bytes)?;
                let checkpoint_number = name_str.parse::<u64>()?;
                let offset = cursor.read_u64::<LittleEndian>()?;
                let length = cursor.read_u64::<LittleEndian>()?;
                let _entry_crc = cursor.read_u32::<LittleEndian>()?;

                index.insert(
                    checkpoint_number,
                    BlobIndexEntry {
                        checkpoint_number,
                        offset,
                        length,
                    },
                );
            }

            let mut cache = self.inner.index_cache.write().await;
            cache.insert(blob_id.to_string(), index.clone());
            return Ok(index);
        }

        // Fallback to HTTP aggregator
        tracing::info!("fetching index for blob {} from aggregator", blob_id);

        let url = format!("{}/v1/blobs/{}", self.inner.aggregator_url, blob_id);
        let footer_bytes = self
            .with_retry(|| {
                let client = self.inner.client.clone();
                let url = url.clone();
                async move {
                    let response = client.get(&url).header("Range", "bytes=-24").send().await?;

                    if !response.status().is_success() {
                        return Err(anyhow::anyhow!("status {}", response.status()));
                    }
                    Ok(response.bytes().await?)
                }
            })
            .await
            .with_context(|| format!("failed to fetch footer for blob {}", blob_id))?;

        if footer_bytes.len() != 24 {
            return Err(anyhow::anyhow!(
                "invalid footer length: {}",
                footer_bytes.len()
            ));
        }

        let mut cursor = Cursor::new(&footer_bytes);
        let magic = cursor.read_u32::<LittleEndian>()?;
        if magic != 0x574c4244 {
            return Err(anyhow::anyhow!("invalid blob footer magic: {:x}", magic));
        }

        let _version = cursor.read_u32::<LittleEndian>()?;
        let index_start_offset = cursor.read_u64::<LittleEndian>()?;
        let count = cursor.read_u32::<LittleEndian>()?;

        let index_bytes = self
            .with_retry(|| {
                let client = self.inner.client.clone();
                let url = url.clone();
                async move {
                    let response = client
                        .get(&url)
                        .header("Range", format!("bytes={}-", index_start_offset))
                        .send()
                        .await?;

                    if !response.status().is_success() {
                        return Err(anyhow::anyhow!("status {}", response.status()));
                    }
                    Ok(response.bytes().await?)
                }
            })
            .await
            .with_context(|| format!("failed to fetch index for blob {}", blob_id))?;

        let mut cursor = Cursor::new(&index_bytes);
        let mut index = HashMap::with_capacity(count as usize);

        for _ in 0..count {
            let name_len = cursor.read_u32::<LittleEndian>()?;
            let mut name_bytes = vec![0u8; name_len as usize];
            cursor.read_exact(&mut name_bytes)?;

            let name_str =
                String::from_utf8(name_bytes).context("invalid utf8 in checkpoint name")?;
            let checkpoint_number = name_str
                .parse::<u64>()
                .context("invalid checkpoint number string")?;

            let offset = cursor.read_u64::<LittleEndian>()?;
            let length = cursor.read_u64::<LittleEndian>()?;
            let _entry_crc = cursor.read_u32::<LittleEndian>()?;

            index.insert(
                checkpoint_number,
                BlobIndexEntry {
                    checkpoint_number,
                    offset,
                    length,
                },
            );
        }

        {
            let mut cache = self.inner.index_cache.write().await;
            cache.insert(blob_id.to_string(), index.clone());
        }

        Ok(index)
    }

    async fn with_retry<F, Fut, T>(&self, mut f: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempts = 0;
        let max_attempts = 5;
        loop {
            match f().await {
                Ok(res) => return Ok(res),
                Err(e) if attempts < max_attempts => {
                    attempts += 1;
                    tracing::warn!(
                        "aggregator request failed (attempt {}): {}. Retrying...",
                        attempts,
                        e
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn download_range(&self, blob_id: &str, start: u64, length: u64) -> Result<Vec<u8>> {
        let cached_path = self.get_cached_blob_path(blob_id);

        // Check if blob is already cached (most common path after ensure_blob_cached is called)
        if cached_path.exists() {
            let mut file = std::fs::File::open(&cached_path)
                .with_context(|| format!("failed to open cached blob {}", cached_path.display()))?;

            file.seek(SeekFrom::Start(start))?;
            let mut buffer = vec![0u8; length as usize];
            file.read_exact(&mut buffer)?;

            return Ok(buffer);
        }

        // If caching is enabled, ensure blob is cached first (unified method)
        if self.inner.cache_enabled {
            self.ensure_blob_cached(blob_id).await?;
            // Read from cached file
            let mut file = std::fs::File::open(&cached_path)
                .with_context(|| format!("failed to open cached blob {}", cached_path.display()))?;
            file.seek(SeekFrom::Start(start))?;
            let mut buffer = vec![0u8; length as usize];
            file.read_exact(&mut buffer)?;
            return Ok(buffer);
        }

        // Streaming mode (no caching) - use CLI range reads if available
        if self.inner.walrus_cli_path.is_some() {
            return self
                .read_range_via_cli_resilient(blob_id, start, length)
                .await;
        }

        // Fallback to HTTP range requests (no caching, no CLI)
        let url = format!("{}/v1/blobs/{}", self.inner.aggregator_url, blob_id);
        let end = start + length - 1;

        tracing::debug!(
            "downloading range {}-{} (len {}) from {}",
            start,
            end,
            length,
            blob_id
        );

        let bytes = self
            .with_retry(|| {
                let client = self.inner.client.clone();
                let url = url.clone();
                async move {
                    let response = client
                        .get(&url)
                        .header("Range", format!("bytes={}-{}", start, end))
                        .send()
                        .await?;

                    if !response.status().is_success() {
                        return Err(anyhow::anyhow!("status {}", response.status()));
                    }

                    Ok(response.bytes().await?.to_vec())
                }
            })
            .await?;

        if bytes.len() as u64 != length {
            tracing::warn!("expected {} bytes, got {}", length, bytes.len());
        }

        self.inner
            .bytes_downloaded
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalesce_entries_merges_adjacent() {
        let entries = vec![
            (
                1u64,
                BlobIndexEntry {
                    checkpoint_number: 1,
                    offset: 0,
                    length: 100,
                },
                (),
            ),
            (
                2u64,
                BlobIndexEntry {
                    checkpoint_number: 2,
                    offset: 100,
                    length: 50,
                },
                (),
            ),
            (
                3u64,
                BlobIndexEntry {
                    checkpoint_number: 3,
                    offset: 200,
                    length: 25,
                },
                (),
            ),
        ];

        let ranges = coalesce_entries(&entries, 0, 1024);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 150);
        assert_eq!(ranges[0].entries.len(), 2);
        assert_eq!(ranges[1].start, 200);
        assert_eq!(ranges[1].end, 225);
    }

    #[test]
    fn coalesce_entries_respects_max_range() {
        let entries = vec![
            (
                1u64,
                BlobIndexEntry {
                    checkpoint_number: 1,
                    offset: 0,
                    length: 100,
                },
                (),
            ),
            (
                2u64,
                BlobIndexEntry {
                    checkpoint_number: 2,
                    offset: 100,
                    length: 100,
                },
                (),
            ),
            (
                3u64,
                BlobIndexEntry {
                    checkpoint_number: 3,
                    offset: 200,
                    length: 100,
                },
                (),
            ),
        ];

        let ranges = coalesce_entries(&entries, 0, 150);
        assert_eq!(ranges.len(), 3);
    }
}
