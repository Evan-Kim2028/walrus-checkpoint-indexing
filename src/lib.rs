//! # Walrus Checkpoint Streaming
//!
//! Stream Sui checkpoints from Walrus decentralized storage.
//!
//! This library provides efficient access to historical Sui checkpoint data stored
//! on the Walrus network. It supports multiple fetch strategies:
//!
//! - **Full blob download**: Download entire blobs for maximum throughput
//! - **Byte-range streaming**: Stream specific byte ranges (requires forked CLI)
//! - **Adaptive fetching**: Automatically choose strategy based on network health
//!
//! ## CLI Compatibility
//!
//! This library works with two versions of the Walrus CLI:
//!
//! ### Official Walrus CLI (default)
//!
//! Works with the standard MystenLabs/walrus binary. Supports:
//! - Full blob downloads
//! - Node health tracking
//! - Sliver prediction (for diagnostics)
//!
//! ### Forked Walrus CLI (walrus-cli-streaming)
//!
//! Adds byte-range streaming with flags: `--start-byte`, `--byte-length`, `--size-only`, `--stream`
//!
//! Required for:
//! - Byte-range streaming (random checkpoint access)
//! - Size-only queries
//! - Zero-copy streaming
//!
//! See [`config::CliCapabilities`] for details.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use walrus_checkpoint_streaming::{WalrusStorage, Config, CliCapabilities};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Using official CLI (full blob download)
//!     let config = Config::builder()
//!         .walrus_cli_path("/path/to/walrus")
//!         .cli_capabilities(CliCapabilities::Official)  // default
//!         .build()?;
//!
//!     let storage = WalrusStorage::new(config).await?;
//!
//!     // Stream checkpoints
//!     storage.stream_checkpoints(1000..2000, |checkpoint| async {
//!         println!("Checkpoint {}", checkpoint.checkpoint_summary.sequence_number);
//!         Ok(())
//!     }).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ### Using Forked CLI for Byte-Range Streaming
//!
//! ```rust,no_run
//! use walrus_checkpoint_streaming::{WalrusStorage, Config, CliCapabilities, FetchStrategy};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = Config::builder()
//!         .walrus_cli_path("/path/to/walrus-fork")
//!         .cli_capabilities(CliCapabilities::Forked)  // Required!
//!         .fetch_strategy(FetchStrategy::ByteRangeStream)
//!         .build()?;
//!
//!     let storage = WalrusStorage::new(config).await?;
//!     // Now uses byte-range streaming instead of full blob download
//!     Ok(())
//! }
//! ```
//!
//! ## Architecture
//!
//! The library is organized into several modules:
//!
//! - [`storage`]: Core checkpoint storage and retrieval
//! - [`node_health`]: Walrus storage node health tracking
//! - [`sliver`]: Sliver prediction for identifying problematic byte ranges
//! - [`blob`]: Blob metadata and index parsing
//! - [`config`]: Configuration and CLI argument handling
//! - [`handlers`]: Checkpoint event handling traits and examples

pub mod storage;
pub mod node_health;
pub mod sliver;
pub mod blob;
pub mod config;
pub mod handlers;

// Re-exports for convenience
pub use storage::WalrusStorage;
pub use config::{Config, CliCapabilities, FetchStrategy};
pub use node_health::NodeHealthTracker;
pub use sliver::SliverPredictor;
pub use handlers::CheckpointHandler;

/// Prelude module for common imports
pub mod prelude {
    pub use crate::storage::WalrusStorage;
    pub use crate::config::{Config, CliCapabilities, FetchStrategy};
    pub use crate::handlers::CheckpointHandler;
    pub use sui_types::full_checkpoint_content::CheckpointData;
    pub use sui_types::messages_checkpoint::CheckpointSequenceNumber;
}
