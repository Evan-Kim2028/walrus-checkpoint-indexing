//! Configuration for Walrus checkpoint indexing.
//!
//! This module provides configuration options for connecting to Walrus storage.
//!
//! ## Data Access
//!
//! - **Walrus CLI (recommended)**: Uses the official Walrus CLI to download blobs.
//!   The CLI is auto-detected from PATH or can be specified explicitly.
//! - **HTTP Aggregator**: Fallback when CLI is not available.

use clap::Parser;
use std::path::PathBuf;
use std::process::Command;

/// Try to find the walrus CLI binary in PATH.
/// Returns the path if found and executable.
pub fn find_walrus_cli() -> Option<PathBuf> {
    // Try 'which walrus' on Unix or 'where walrus' on Windows
    #[cfg(unix)]
    let output = Command::new("which").arg("walrus").output().ok()?;
    #[cfg(windows)]
    let output = Command::new("where").arg("walrus").output().ok()?;

    if output.status.success() {
        let path_str = String::from_utf8_lossy(&output.stdout);
        let path = PathBuf::from(path_str.trim());
        if path.exists() {
            return Some(path);
        }
    }
    None
}

/// CLI capabilities.
///
/// **DEPRECATED**: This enum is deprecated. The library now only supports the
/// official Walrus CLI. The `Forked` variant is kept for backwards compatibility
/// but has no effect.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[deprecated(since = "0.2.0", note = "Forked CLI is no longer supported. Use official Walrus CLI.")]
#[allow(deprecated)]
pub enum CliCapabilities {
    /// Official Walrus CLI - the only supported option.
    #[default]
    Official,

    /// Deprecated: Forked CLI is no longer supported.
    #[deprecated(since = "0.2.0", note = "Forked CLI is no longer supported")]
    Forked,
}

#[allow(deprecated)]
impl std::str::FromStr for CliCapabilities {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "official" | "standard" | "upstream" => Ok(CliCapabilities::Official),
            "forked" | "streaming" | "custom" => Ok(CliCapabilities::Forked),
            _ => Err(format!(
                "Unknown CLI capability: {}. Use 'official' or 'forked'.",
                s
            )),
        }
    }
}

/// Fetch strategy for retrieving checkpoint data from Walrus.
///
/// **DEPRECATED**: This enum is deprecated. The library now only uses full blob
/// downloads. Other variants are kept for backwards compatibility but have no effect.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[deprecated(since = "0.2.0", note = "Only FullBlob strategy is supported. Other strategies are ignored.")]
#[allow(deprecated)]
pub enum FetchStrategy {
    /// Download full blobs, then extract checkpoints locally.
    /// This is the only supported strategy.
    #[default]
    FullBlob,

    /// Deprecated: Byte-range streaming is no longer supported.
    #[deprecated(since = "0.2.0", note = "ByteRangeStream is no longer supported")]
    ByteRangeStream,

    /// Deprecated: Adaptive strategy is no longer supported.
    #[deprecated(since = "0.2.0", note = "Adaptive is no longer supported, FullBlob is always used")]
    Adaptive,
}

#[allow(deprecated)]
impl std::str::FromStr for FetchStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // All strategies now map to FullBlob
        match s.to_lowercase().as_str() {
            "full" | "full-blob" | "fullblob" => Ok(FetchStrategy::FullBlob),
            "stream" | "byte-range" | "byterange" => {
                eprintln!("Warning: ByteRangeStream is deprecated, using FullBlob");
                Ok(FetchStrategy::FullBlob)
            }
            "adaptive" | "auto" => {
                eprintln!("Warning: Adaptive is deprecated, using FullBlob");
                Ok(FetchStrategy::FullBlob)
            }
            _ => Err(format!("Unknown fetch strategy: {}", s)),
        }
    }
}

#[allow(deprecated)]
impl FetchStrategy {
    /// Check if this strategy requires the forked CLI.
    #[deprecated(since = "0.2.0", note = "Forked CLI is no longer supported")]
    pub fn requires_forked_cli(&self) -> bool {
        false // No strategy requires forked CLI anymore
    }
}

/// Configuration for Walrus checkpoint storage.
#[derive(Parser, Debug, Clone)]
#[command(name = "walrus-checkpoint-index")]
#[command(about = "Index Sui checkpoints from Walrus decentralized storage")]
pub struct Config {
    // === Walrus Connection ===
    /// URL of the Walrus archival metadata service
    #[arg(
        long,
        env = "WALRUS_ARCHIVAL_URL",
        default_value = "https://walrus-sui-archival.mainnet.walrus.space"
    )]
    pub archival_url: String,

    /// URL of the Walrus aggregator for HTTP range requests
    #[arg(
        long,
        env = "WALRUS_AGGREGATOR_URL",
        default_value = "https://aggregator.walrus-mainnet.walrus.space"
    )]
    pub aggregator_url: String,

    /// Path to Walrus CLI binary
    ///
    /// If not specified, the CLI is auto-detected from PATH.
    #[arg(long, env = "WALRUS_CLI_PATH")]
    pub walrus_cli_path: Option<PathBuf>,

    /// Walrus CLI context (e.g., "mainnet", "testnet")
    #[arg(long, env = "WALRUS_CLI_CONTEXT", default_value = "mainnet")]
    pub walrus_cli_context: String,

    /// DEPRECATED: CLI capabilities setting (ignored)
    #[arg(long, env = "WALRUS_CLI_CAPABILITIES", default_value = "official", hide = true)]
    pub cli_capabilities: String,

    /// DEPRECATED: Fetch strategy setting (ignored, always uses full-blob)
    #[arg(long, env = "WALRUS_FETCH_STRATEGY", default_value = "full-blob", hide = true)]
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
    #[arg(
        long,
        env = "WALRUS_COALESCE_MAX_RANGE_BYTES",
        default_value = "16777216"
    )]
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
    #[allow(deprecated)]
    pub fn fetch_strategy(&self) -> FetchStrategy {
        self.fetch_strategy.parse().unwrap_or_default()
    }

    /// Get the CLI capabilities.
    #[allow(deprecated)]
    pub fn cli_capabilities(&self) -> CliCapabilities {
        self.cli_capabilities.parse().unwrap_or_default()
    }

    /// Check if CLI is available (any version).
    /// Checks explicit path first, then falls back to PATH auto-detection.
    pub fn cli_available(&self) -> bool {
        self.resolved_cli_path().is_some()
    }

    /// Get the resolved CLI path (explicit or auto-detected from PATH).
    pub fn resolved_cli_path(&self) -> Option<PathBuf> {
        self.walrus_cli_path.clone().or_else(find_walrus_cli)
    }

    /// Check if byte-range streaming is available.
    #[deprecated(since = "0.2.0", note = "Byte-range streaming is no longer supported")]
    pub fn byte_range_streaming_available(&self) -> bool {
        false
    }

    /// Validate that config is consistent.
    pub fn validate(&self) -> anyhow::Result<()> {
        // All configurations are now valid - we only use full blob downloads
        Ok(())
    }
}

/// Builder for Config.
#[derive(Default)]
#[allow(deprecated)]
pub struct ConfigBuilder {
    archival_url: Option<String>,
    aggregator_url: Option<String>,
    walrus_cli_path: Option<PathBuf>,
    walrus_cli_context: Option<String>,
    cli_capabilities: Option<CliCapabilities>,
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

    /// Set CLI capabilities.
    #[deprecated(since = "0.2.0", note = "Forked CLI is no longer supported. This setting is ignored.")]
    #[allow(deprecated)]
    pub fn cli_capabilities(mut self, capabilities: CliCapabilities) -> Self {
        self.cli_capabilities = Some(capabilities);
        self
    }

    /// Set fetch strategy.
    #[deprecated(since = "0.2.0", note = "Only FullBlob strategy is supported. This setting is ignored.")]
    #[allow(deprecated)]
    pub fn fetch_strategy(mut self, strategy: FetchStrategy) -> Self {
        self.fetch_strategy = Some(match strategy {
            FetchStrategy::FullBlob => "full-blob".to_string(),
            FetchStrategy::ByteRangeStream => "full-blob".to_string(), // Ignore, use FullBlob
            FetchStrategy::Adaptive => "full-blob".to_string(), // Ignore, use FullBlob
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

    #[allow(deprecated)]
    pub fn build(self) -> anyhow::Result<Config> {
        let cli_capabilities_str = match self.cli_capabilities.unwrap_or_default() {
            CliCapabilities::Official => "official".to_string(),
            CliCapabilities::Forked => "forked".to_string(),
        };

        let config = Config {
            archival_url: self
                .archival_url
                .unwrap_or_else(|| "https://walrus-sui-archival.mainnet.walrus.space".to_string()),
            aggregator_url: self
                .aggregator_url
                .unwrap_or_else(|| "https://aggregator.walrus-mainnet.walrus.space".to_string()),
            walrus_cli_path: self.walrus_cli_path,
            walrus_cli_context: self
                .walrus_cli_context
                .unwrap_or_else(|| "mainnet".to_string()),
            cli_capabilities: cli_capabilities_str,
            fetch_strategy: self
                .fetch_strategy
                .unwrap_or_else(|| "full-blob".to_string()),
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
        };

        // Validate configuration consistency
        config.validate()?;

        Ok(config)
    }
}
