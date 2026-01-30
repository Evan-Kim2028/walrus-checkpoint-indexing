//! Checkpoint event handling traits and examples.
//!
//! This module provides traits for handling checkpoint data as it streams
//! from Walrus storage. Implementations can filter, transform, and process
//! checkpoint data for various use cases.

use anyhow::Result;
use async_trait::async_trait;
use sui_types::full_checkpoint_content::CheckpointData;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;

/// Trait for handling checkpoint data as it streams from storage.
///
/// Implement this trait to process checkpoints in a streaming fashion.
/// The handler is called for each checkpoint in sequence order.
///
/// # Example
///
/// ```rust,ignore
/// use walrus_checkpoint_streaming::{CheckpointHandler, WalrusStorage};
/// use sui_types::full_checkpoint_content::CheckpointData;
///
/// struct MyHandler;
///
/// #[async_trait::async_trait]
/// impl CheckpointHandler for MyHandler {
///     async fn handle_checkpoint(&mut self, checkpoint: &CheckpointData) -> anyhow::Result<()> {
///         println!("Checkpoint {}", checkpoint.checkpoint_summary.sequence_number);
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait CheckpointHandler: Send {
    /// Handle a single checkpoint.
    ///
    /// Called for each checkpoint in sequence order. If this returns an error,
    /// streaming will stop and the error will be propagated.
    async fn handle_checkpoint(&mut self, checkpoint: &CheckpointData) -> Result<()>;

    /// Called before streaming begins.
    ///
    /// Override this to perform any setup before checkpoints start flowing.
    async fn on_start(&mut self, _start: CheckpointSequenceNumber, _end: CheckpointSequenceNumber) -> Result<()> {
        Ok(())
    }

    /// Called after streaming completes successfully.
    ///
    /// Override this to perform any cleanup or finalization.
    async fn on_complete(&mut self, _processed: u64) -> Result<()> {
        Ok(())
    }

    /// Called when an error occurs during streaming.
    ///
    /// Override this to handle errors (logging, metrics, etc.).
    /// The error will still be propagated after this is called.
    async fn on_error(&mut self, _error: &anyhow::Error) {}
}

/// A simple handler that counts checkpoints
pub struct CountingHandler {
    count: u64,
}

impl CountingHandler {
    pub fn new() -> Self {
        Self { count: 0 }
    }

    pub fn count(&self) -> u64 {
        self.count
    }
}

impl Default for CountingHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CheckpointHandler for CountingHandler {
    async fn handle_checkpoint(&mut self, _checkpoint: &CheckpointData) -> Result<()> {
        self.count += 1;
        Ok(())
    }
}

/// A handler that logs checkpoint progress
pub struct LoggingHandler {
    log_interval: u64,
    count: u64,
    start_time: std::time::Instant,
}

impl LoggingHandler {
    pub fn new(log_interval: u64) -> Self {
        Self {
            log_interval,
            count: 0,
            start_time: std::time::Instant::now(),
        }
    }
}

impl Default for LoggingHandler {
    fn default() -> Self {
        Self::new(100)
    }
}

#[async_trait]
impl CheckpointHandler for LoggingHandler {
    async fn handle_checkpoint(&mut self, checkpoint: &CheckpointData) -> Result<()> {
        self.count += 1;

        if self.count % self.log_interval == 0 {
            let elapsed = self.start_time.elapsed().as_secs_f64();
            let rate = self.count as f64 / elapsed;
            tracing::info!(
                "processed {} checkpoints ({:.2} cp/s), latest: {}",
                self.count,
                rate,
                checkpoint.checkpoint_summary.sequence_number
            );
        }

        Ok(())
    }

    async fn on_start(&mut self, start: CheckpointSequenceNumber, end: CheckpointSequenceNumber) -> Result<()> {
        self.start_time = std::time::Instant::now();
        tracing::info!("starting checkpoint stream: {} to {}", start, end);
        Ok(())
    }

    async fn on_complete(&mut self, processed: u64) -> Result<()> {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let rate = processed as f64 / elapsed;
        tracing::info!(
            "completed: {} checkpoints in {:.2}s ({:.2} cp/s)",
            processed,
            elapsed,
            rate
        );
        Ok(())
    }
}

/// A handler that collects checkpoints into a vector
pub struct CollectingHandler {
    checkpoints: Vec<CheckpointData>,
    max_capacity: Option<usize>,
}

impl CollectingHandler {
    pub fn new() -> Self {
        Self {
            checkpoints: Vec::new(),
            max_capacity: None,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            checkpoints: Vec::with_capacity(capacity),
            max_capacity: Some(capacity),
        }
    }

    pub fn into_checkpoints(self) -> Vec<CheckpointData> {
        self.checkpoints
    }

    pub fn checkpoints(&self) -> &[CheckpointData] {
        &self.checkpoints
    }
}

impl Default for CollectingHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CheckpointHandler for CollectingHandler {
    async fn handle_checkpoint(&mut self, checkpoint: &CheckpointData) -> Result<()> {
        if let Some(max) = self.max_capacity {
            if self.checkpoints.len() >= max {
                return Err(anyhow::anyhow!("exceeded max capacity of {} checkpoints", max));
            }
        }
        self.checkpoints.push(checkpoint.clone());
        Ok(())
    }
}

/// A handler that filters checkpoints based on a predicate
pub struct FilteringHandler<F, H> {
    predicate: F,
    inner: H,
}

impl<F, H> FilteringHandler<F, H>
where
    F: Fn(&CheckpointData) -> bool + Send,
    H: CheckpointHandler,
{
    pub fn new(predicate: F, inner: H) -> Self {
        Self { predicate, inner }
    }

    pub fn into_inner(self) -> H {
        self.inner
    }
}

#[async_trait]
impl<F, H> CheckpointHandler for FilteringHandler<F, H>
where
    F: Fn(&CheckpointData) -> bool + Send + Sync,
    H: CheckpointHandler,
{
    async fn handle_checkpoint(&mut self, checkpoint: &CheckpointData) -> Result<()> {
        if (self.predicate)(checkpoint) {
            self.inner.handle_checkpoint(checkpoint).await?;
        }
        Ok(())
    }

    async fn on_start(&mut self, start: CheckpointSequenceNumber, end: CheckpointSequenceNumber) -> Result<()> {
        self.inner.on_start(start, end).await
    }

    async fn on_complete(&mut self, processed: u64) -> Result<()> {
        self.inner.on_complete(processed).await
    }

    async fn on_error(&mut self, error: &anyhow::Error) {
        self.inner.on_error(error).await
    }
}

/// A handler that chains multiple handlers together
pub struct ChainHandler<A, B> {
    first: A,
    second: B,
}

impl<A, B> ChainHandler<A, B>
where
    A: CheckpointHandler,
    B: CheckpointHandler,
{
    pub fn new(first: A, second: B) -> Self {
        Self { first, second }
    }
}

#[async_trait]
impl<A, B> CheckpointHandler for ChainHandler<A, B>
where
    A: CheckpointHandler,
    B: CheckpointHandler,
{
    async fn handle_checkpoint(&mut self, checkpoint: &CheckpointData) -> Result<()> {
        self.first.handle_checkpoint(checkpoint).await?;
        self.second.handle_checkpoint(checkpoint).await?;
        Ok(())
    }

    async fn on_start(&mut self, start: CheckpointSequenceNumber, end: CheckpointSequenceNumber) -> Result<()> {
        self.first.on_start(start, end).await?;
        self.second.on_start(start, end).await?;
        Ok(())
    }

    async fn on_complete(&mut self, processed: u64) -> Result<()> {
        self.first.on_complete(processed).await?;
        self.second.on_complete(processed).await?;
        Ok(())
    }

    async fn on_error(&mut self, error: &anyhow::Error) {
        self.first.on_error(error).await;
        self.second.on_error(error).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_counting_handler() {
        let mut handler = CountingHandler::new();
        assert_eq!(handler.count(), 0);
        // Can't easily create CheckpointData in tests, but the structure is correct
    }
}
