//! Configuration for Walrus checkpoint streaming.
//!
//! This module provides configuration options for connecting to Walrus storage
//! and controlling fetch behavior.

use clap::Parser;
use std::path::PathBuf;

/// Fetch strategy for retrieving checkpoint data from Walrus.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FetchStrategy {
    /// Download full blobs, then extract checkpoints locally.
    /// Best for sequential access or when network has many down nodes.
    #[default]
    FullBlob,

    /// Stream specific byte ranges using forked CLI.
    /// Best for random access or sparse checkpoint ranges.
    ByteRangeStream,

    /// Automatically choose strategy based on network health and access patterns.
    Adaptive,
}

impl std::str::FromStr for FetchStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "full" | "full-blob" | "fullblob" => Ok(FetchStrategy::FullBlob),
            "stream" | "byte-range" | "byterange" => Ok(FetchStrategy::ByteRangeStream),
            "adaptive" | "auto" => Ok(FetchStrategy::Adaptive),
            _ => Err(format!("Unknown fetch strategy: {}", s)),
        }
    }
}

/// Configuration for Walrus checkpoint storage.
#[derive(Parser, Debug, Clone)]
#[command(name = "walrus-checkpoint-stream")]
#[command(about = "Stream Sui checkpoints from Walrus decentralized storage")]
pub struct Config {
    // === Walrus Connection ===

    /// URL of the Walrus archival metadata service
    #[arg(long, env = "WALRUS_ARCHIVAL_URL", default_value = "https://walrus-sui-archival.mainnet.walrus.space")]
    pub archival_url: String,

    /// URL of the Walrus aggregator for HTTP range requests
    #[arg(long, env = "WALRUS_AGGREGATOR_URL", default_value = "https://aggregator.walrus-mainnet.walrus.space")]
    pub aggregator_url: String,

    /// Path to Walrus CLI binary (required for streaming mode)
    #[arg(long, env = "WALRUS_CLI_PATH")]
    pub walrus_cli_path: Option<PathBuf>,

    /// Walrus CLI context (e.g., "mainnet", "testnet")
    #[arg(long, env = "WALRUS_CLI_CONTEXT", default_value = "mainnet")]
    pub walrus_cli_context: String,

    // === Fetch Strategy ===

    /// Fetch strategy: full-blob, byte-range, or adaptive
    #[arg(long, env = "WALRUS_FETCH_STRATEGY", default_value = "full-blob")]
    pub fetch_strategy: String,

    // === Caching ===

    /// Directory for caching downloaded blobs
    #[arg(long, env = "WALRUS_CACHE_DIR")]
    pub cache_dir: Option<PathBuf>,

    /// Enable blob caching (downloads full blob once, then reads locally)
    #[arg(long, env = "WALRUS_CACHE_ENABLED", default_value = "false")]
    pub cache_enabled: bool,

    // === Concurrency ===

    /// Number of blobs to process in parallel
    #[arg(long, env = "WALRUS_BLOB_CONCURRENCY", default_value = "4")]
    pub blob_concurrency: usize,

    /// Number of byte ranges to fetch in parallel per blob
    #[arg(long, env = "WALRUS_RANGE_CONCURRENCY", default_value = "32")]
    pub range_concurrency: usize,

    // === Timeouts & Retries ===

    /// Timeout for CLI operations in seconds
    #[arg(long, env = "WALRUS_CLI_TIMEOUT_SECS", default_value = "180")]
    pub cli_timeout_secs: u64,

    /// HTTP timeout in seconds
    #[arg(long, env = "WALRUS_HTTP_TIMEOUT_SECS", default_value = "60")]
    pub http_timeout_secs: u64,

    /// Maximum retries for failed range fetches
    #[arg(long, env = "WALRUS_MAX_RETRIES", default_value = "6")]
    pub max_retries: usize,

    /// Delay between retries in seconds
    #[arg(long, env = "WALRUS_RETRY_DELAY_SECS", default_value = "5")]
    pub retry_delay_secs: u64,

    // === Range Coalescing ===

    /// Maximum gap between checkpoints to coalesce into single range
    #[arg(long, env = "WALRUS_COALESCE_GAP_BYTES", default_value = "1048576")]
    pub coalesce_gap_bytes: u64,

    /// Maximum size of a coalesced range in bytes
    #[arg(long, env = "WALRUS_COALESCE_MAX_RANGE_BYTES", default_value = "16777216")]
    pub coalesce_max_range_bytes: u64,

    // === Health Monitoring ===

    /// Enable node health monitoring for adaptive fetching
    #[arg(long, env = "WALRUS_HEALTH_MONITORING", default_value = "true")]
    pub health_monitoring: bool,

    /// Interval for health polling in seconds
    #[arg(long, env = "WALRUS_HEALTH_POLL_INTERVAL_SECS", default_value = "300")]
    pub health_poll_interval_secs: u64,
}

impl Config {
    /// Create a new Config builder.
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    /// Get the parsed fetch strategy.
    pub fn fetch_strategy(&self) -> FetchStrategy {
        self.fetch_strategy.parse().unwrap_or_default()
    }

    /// Check if CLI streaming is available.
    pub fn cli_streaming_available(&self) -> bool {
        self.walrus_cli_path.is_some()
    }
}

/// Builder for Config.
#[derive(Default)]
pub struct ConfigBuilder {
    archival_url: Option<String>,
    aggregator_url: Option<String>,
    walrus_cli_path: Option<PathBuf>,
    walrus_cli_context: Option<String>,
    fetch_strategy: Option<String>,
    cache_dir: Option<PathBuf>,
    cache_enabled: Option<bool>,
    blob_concurrency: Option<usize>,
    range_concurrency: Option<usize>,
    cli_timeout_secs: Option<u64>,
    http_timeout_secs: Option<u64>,
    max_retries: Option<usize>,
    retry_delay_secs: Option<u64>,
    coalesce_gap_bytes: Option<u64>,
    coalesce_max_range_bytes: Option<u64>,
    health_monitoring: Option<bool>,
    health_poll_interval_secs: Option<u64>,
}

impl ConfigBuilder {
    pub fn archival_url(mut self, url: impl Into<String>) -> Self {
        self.archival_url = Some(url.into());
        self
    }

    pub fn aggregator_url(mut self, url: impl Into<String>) -> Self {
        self.aggregator_url = Some(url.into());
        self
    }

    pub fn walrus_cli_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.walrus_cli_path = Some(path.into());
        self
    }

    pub fn walrus_cli_context(mut self, context: impl Into<String>) -> Self {
        self.walrus_cli_context = Some(context.into());
        self
    }

    pub fn fetch_strategy(mut self, strategy: FetchStrategy) -> Self {
        self.fetch_strategy = Some(match strategy {
            FetchStrategy::FullBlob => "full-blob".to_string(),
            FetchStrategy::ByteRangeStream => "byte-range".to_string(),
            FetchStrategy::Adaptive => "adaptive".to_string(),
        });
        self
    }

    pub fn cache_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.cache_dir = Some(dir.into());
        self
    }

    pub fn cache_enabled(mut self, enabled: bool) -> Self {
        self.cache_enabled = Some(enabled);
        self
    }

    pub fn blob_concurrency(mut self, concurrency: usize) -> Self {
        self.blob_concurrency = Some(concurrency);
        self
    }

    pub fn range_concurrency(mut self, concurrency: usize) -> Self {
        self.range_concurrency = Some(concurrency);
        self
    }

    pub fn build(self) -> anyhow::Result<Config> {
        Ok(Config {
            archival_url: self.archival_url.unwrap_or_else(|| "https://walrus-sui-archival.mainnet.walrus.space".to_string()),
            aggregator_url: self.aggregator_url.unwrap_or_else(|| "https://aggregator.walrus-mainnet.walrus.space".to_string()),
            walrus_cli_path: self.walrus_cli_path,
            walrus_cli_context: self.walrus_cli_context.unwrap_or_else(|| "mainnet".to_string()),
            fetch_strategy: self.fetch_strategy.unwrap_or_else(|| "full-blob".to_string()),
            cache_dir: self.cache_dir,
            cache_enabled: self.cache_enabled.unwrap_or(false),
            blob_concurrency: self.blob_concurrency.unwrap_or(4),
            range_concurrency: self.range_concurrency.unwrap_or(32),
            cli_timeout_secs: self.cli_timeout_secs.unwrap_or(180),
            http_timeout_secs: self.http_timeout_secs.unwrap_or(60),
            max_retries: self.max_retries.unwrap_or(6),
            retry_delay_secs: self.retry_delay_secs.unwrap_or(5),
            coalesce_gap_bytes: self.coalesce_gap_bytes.unwrap_or(1048576),
            coalesce_max_range_bytes: self.coalesce_max_range_bytes.unwrap_or(16777216),
            health_monitoring: self.health_monitoring.unwrap_or(true),
            health_poll_interval_secs: self.health_poll_interval_secs.unwrap_or(300),
        })
    }
}
