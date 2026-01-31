//! Configuration for Walrus checkpoint streaming.
//!
//! This module provides configuration options for connecting to Walrus storage
//! and controlling fetch behavior.
//!
//! ## CLI Compatibility
//!
//! This library can work with either:
//! - **Official Walrus CLI**: Full blob download and HTTP aggregator only
//! - **Forked Walrus CLI**: Adds byte-range streaming with `--start-byte`, `--byte-length`, `--size-only`, `--stream`
//!
//! See [`CliCapabilities`] for details on which features require the forked CLI.

use clap::Parser;
use std::path::PathBuf;

/// CLI capabilities - distinguishes between official and forked Walrus CLI.
///
/// ## Official Walrus CLI
///
/// The official Walrus CLI supports:
/// - `walrus read <blob_id> --out <file>` - Download full blob to file
/// - `walrus health --committee --json` - Node health information
/// - `walrus info committee --json` - Committee/shard mapping
///
/// ## Forked Walrus CLI (walrus-cli-streaming)
///
/// The forked CLI adds these flags for byte-range streaming:
/// - `--start-byte <N>` - Starting byte position
/// - `--byte-length <N>` - Number of bytes to read
/// - `--size-only` - Get blob size without downloading
/// - `--stream` - Stream raw bytes to stdout (zero-copy)
///
/// ## Compatibility Matrix
///
/// | Feature | Official CLI | Forked CLI |
/// |---------|-------------|------------|
/// | Full blob download | Yes | Yes |
/// | Node health tracking | Yes | Yes |
/// | Sliver prediction | Yes | Yes |
/// | Byte-range streaming | **No** | Yes |
/// | Size-only queries | **No** | Yes |
/// | Zero-copy streaming | **No** | Yes |
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CliCapabilities {
    /// Official Walrus CLI - full blob download only.
    ///
    /// Use this when:
    /// - Using the official MystenLabs/walrus binary
    /// - You want reliable full-blob downloads
    /// - Network has many down nodes (full download avoids problematic slivers better)
    #[default]
    Official,

    /// Forked Walrus CLI with byte-range streaming support.
    ///
    /// Use this when:
    /// - Using the walrus-cli-streaming fork
    /// - You need random access to specific checkpoints
    /// - You want to minimize bandwidth by fetching only needed bytes
    ///
    /// Repository: https://github.com/Evan-Kim2028/walrus-cli-streaming
    Forked,
}

impl std::str::FromStr for CliCapabilities {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "official" | "standard" | "upstream" => Ok(CliCapabilities::Official),
            "forked" | "streaming" | "custom" => Ok(CliCapabilities::Forked),
            _ => Err(format!("Unknown CLI capability: {}. Use 'official' or 'forked'.", s)),
        }
    }
}

/// Fetch strategy for retrieving checkpoint data from Walrus.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FetchStrategy {
    /// Download full blobs, then extract checkpoints locally.
    ///
    /// **Works with both official and forked CLI.**
    ///
    /// Best for:
    /// - Sequential access to many checkpoints
    /// - When network has many down nodes (avoids problematic slivers)
    /// - When you'll process most checkpoints in a blob
    #[default]
    FullBlob,

    /// Stream specific byte ranges on-demand.
    ///
    /// **Requires forked CLI with byte-range support.**
    ///
    /// Best for:
    /// - Random access to specific checkpoints
    /// - Sparse checkpoint ranges
    /// - Minimizing bandwidth when only fetching few checkpoints
    ByteRangeStream,

    /// Automatically choose strategy based on context.
    ///
    /// Falls back to FullBlob if forked CLI not available.
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

impl FetchStrategy {
    /// Check if this strategy requires the forked CLI.
    pub fn requires_forked_cli(&self) -> bool {
        matches!(self, FetchStrategy::ByteRangeStream)
    }
}

/// Configuration for Walrus checkpoint storage.
#[derive(Parser, Debug, Clone)]
#[command(name = "walrus-checkpoint-index")]
#[command(about = "Index Sui checkpoints from Walrus decentralized storage")]
pub struct Config {
    // === Walrus Connection ===

    /// URL of the Walrus archival metadata service
    #[arg(long, env = "WALRUS_ARCHIVAL_URL", default_value = "https://walrus-sui-archival.mainnet.walrus.space")]
    pub archival_url: String,

    /// URL of the Walrus aggregator for HTTP range requests
    #[arg(long, env = "WALRUS_AGGREGATOR_URL", default_value = "https://aggregator.walrus-mainnet.walrus.space")]
    pub aggregator_url: String,

    /// Path to Walrus CLI binary
    ///
    /// Can be either:
    /// - Official Walrus CLI (full blob download only)
    /// - Forked CLI with byte-range streaming support
    #[arg(long, env = "WALRUS_CLI_PATH")]
    pub walrus_cli_path: Option<PathBuf>,

    /// Walrus CLI context (e.g., "mainnet", "testnet")
    #[arg(long, env = "WALRUS_CLI_CONTEXT", default_value = "mainnet")]
    pub walrus_cli_context: String,

    /// CLI capabilities: "official" or "forked"
    ///
    /// - "official": Standard Walrus CLI (full blob download, health checks)
    /// - "forked": walrus-cli-streaming fork (adds byte-range streaming)
    ///
    /// If using byte-range streaming, you MUST set this to "forked".
    #[arg(long, env = "WALRUS_CLI_CAPABILITIES", default_value = "official")]
    pub cli_capabilities: String,

    // === Fetch Strategy ===

    /// Fetch strategy: full-blob, byte-range, or adaptive
    ///
    /// Note: "byte-range" requires cli_capabilities="forked"
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

    /// Get the CLI capabilities.
    pub fn cli_capabilities(&self) -> CliCapabilities {
        self.cli_capabilities.parse().unwrap_or_default()
    }

    /// Check if CLI is available (any version).
    pub fn cli_available(&self) -> bool {
        self.walrus_cli_path.is_some()
    }

    /// Check if byte-range streaming is available.
    ///
    /// Requires both:
    /// - CLI path configured
    /// - CLI capabilities set to "forked"
    pub fn byte_range_streaming_available(&self) -> bool {
        self.walrus_cli_path.is_some() && self.cli_capabilities() == CliCapabilities::Forked
    }

    /// Validate that config is consistent.
    ///
    /// Returns error if byte-range strategy is selected but forked CLI not configured.
    pub fn validate(&self) -> anyhow::Result<()> {
        let strategy = self.fetch_strategy();
        let capabilities = self.cli_capabilities();

        if strategy.requires_forked_cli() && capabilities != CliCapabilities::Forked {
            return Err(anyhow::anyhow!(
                "Fetch strategy '{:?}' requires forked CLI. Set --cli-capabilities=forked or use --fetch-strategy=full-blob",
                strategy
            ));
        }

        if strategy.requires_forked_cli() && self.walrus_cli_path.is_none() {
            return Err(anyhow::anyhow!(
                "Fetch strategy '{:?}' requires --walrus-cli-path to be set",
                strategy
            ));
        }

        Ok(())
    }
}

/// Builder for Config.
#[derive(Default)]
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

    /// Set CLI capabilities (official or forked).
    ///
    /// Use `CliCapabilities::Forked` if using the walrus-cli-streaming fork
    /// with byte-range support.
    pub fn cli_capabilities(mut self, capabilities: CliCapabilities) -> Self {
        self.cli_capabilities = Some(capabilities);
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
        let cli_capabilities_str = match self.cli_capabilities.unwrap_or_default() {
            CliCapabilities::Official => "official".to_string(),
            CliCapabilities::Forked => "forked".to_string(),
        };

        let config = Config {
            archival_url: self.archival_url.unwrap_or_else(|| "https://walrus-sui-archival.mainnet.walrus.space".to_string()),
            aggregator_url: self.aggregator_url.unwrap_or_else(|| "https://aggregator.walrus-mainnet.walrus.space".to_string()),
            walrus_cli_path: self.walrus_cli_path,
            walrus_cli_context: self.walrus_cli_context.unwrap_or_else(|| "mainnet".to_string()),
            cli_capabilities: cli_capabilities_str,
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
        };

        // Validate configuration consistency
        config.validate()?;

        Ok(config)
    }
}
