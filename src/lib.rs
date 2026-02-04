//! # Walrus Checkpoint Indexing
//!
//! Index Sui checkpoints from Walrus decentralized storage.
//!
//! This library provides efficient access to historical Sui checkpoint data stored
//! on the Walrus network.
//!
//! ## Data Access
//!
//! - **Walrus CLI (recommended)**: Uses the official Walrus CLI to download blobs
//! - **HTTP Aggregator**: Fallback when CLI is not available
//!
//! When using the CLI, blobs are downloaded in full and cached locally. The library
//! automatically detects the CLI from your PATH or you can specify a path explicitly.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use walrus_checkpoint_indexing::{WalrusStorage, Config};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // CLI is auto-detected from PATH
//!     let config = Config::builder().build()?;
//!
//!     let storage = WalrusStorage::new(config).await?;
//!     storage.initialize().await?;
//!
//!     // Stream checkpoints
//!     storage.stream_checkpoints(239000000..239001000, |checkpoint| async move {
//!         println!("Checkpoint {}", checkpoint.checkpoint_summary.sequence_number);
//!         Ok(())
//!     }).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Architecture
//!
//! The library is organized into several modules:
//!
//! - [`storage`]: Core checkpoint storage and retrieval
//! - [`blob`]: Blob metadata and index parsing
//! - [`config`]: Configuration and CLI argument handling
//! - [`handlers`]: Checkpoint event handling traits

pub mod blob;
pub mod config;
pub mod deepbook;
pub mod handlers;
pub mod indexer;
pub mod node_health;
pub mod sliver;
pub mod storage;

// Re-exports for convenience
#[allow(deprecated)]
pub use config::{CliCapabilities, Config, FetchStrategy};
pub use handlers::CheckpointHandler;
pub use indexer::{IndexerConfig, IndexerStats, MassIndexer, Processor, Watermark};
pub use node_health::NodeHealthTracker;
pub use sliver::SliverPredictor;
pub use storage::WalrusStorage;

/// Prelude module for common imports
pub mod prelude {
    #[allow(deprecated)]
    pub use crate::config::{CliCapabilities, Config, FetchStrategy};
    pub use crate::handlers::CheckpointHandler;
    pub use crate::indexer::{IndexerConfig, MassIndexer, Processor, Watermark};
    pub use crate::storage::WalrusStorage;
    pub use sui_types::full_checkpoint_content::CheckpointData;
    pub use sui_types::messages_checkpoint::CheckpointSequenceNumber;
}
