//! Sliver Prediction for Walrus Blob Fetching
//!
//! This module predicts which byte ranges in a Walrus blob will be problematic
//! based on which storage nodes/shards are down. This enables:
//!
//! 1. Diagnostics on why certain ranges are slow
//! 2. Understanding which shards are causing problems
//! 3. Better retry strategies for risky ranges
//!
//! ## Walrus Encoding Background
//!
//! Walrus uses RedStuff erasure coding:
//! - 1000 shards across ~100 storage nodes (~10 shards each)
//! - Data arranged into a 334×667 matrix of symbols
//! - Primary slivers: 334 slivers (each contains a row of 667 symbols)
//! - Secondary slivers: 667 slivers (each contains a column of 334 symbols)
//! - Only need ~1/3 of slivers to reconstruct any byte
//!
//! ## Sliver-to-Shard Mapping
//!
//! The mapping from sliver index to shard is rotated per-blob:
//! ```text
//! shard = (sliver_index + blob_id % 1000) % 1000
//! ```
//!
//! This rotation is deterministic given the blob_id, so we can predict
//! which byte ranges will require slivers from down shards.
//!
//! ## Byte-to-Sliver Mapping
//!
//! For a given byte offset:
//! ```text
//! symbol_index = byte_offset / symbol_size
//! primary_sliver = symbol_index / 667   (which row)
//! secondary_sliver = symbol_index % 667 (which column)
//! ```

use std::collections::HashSet;
use std::ops::Range;

/// Walrus encoding parameters (mainnet values)
pub const N_SHARDS: u64 = 1000;
pub const SOURCE_SYMBOLS_PRIMARY: u64 = 334;
pub const SOURCE_SYMBOLS_SECONDARY: u64 = 667;
pub const SYMBOL_SIZE: u64 = 256; // bytes per symbol (typical)

/// Analysis result for a blob
#[derive(Debug, Clone)]
pub struct BlobAnalysis {
    pub blob_id: String,
    pub blob_size: u64,
    pub rotation_offset: u64,
    pub total_primary_slivers: u64,
    pub total_secondary_slivers: u64,
    pub problematic_primary_slivers: Vec<u64>,
    pub problematic_secondary_slivers: Vec<u64>,
    /// Byte ranges that are "safe" (don't require slivers from down nodes)
    pub safe_ranges: Vec<Range<u64>>,
    /// Byte ranges that are "risky" (require slivers from at least one down node)
    pub risky_ranges: Vec<Range<u64>>,
    /// Percentage of blob bytes that are safe
    pub safe_percentage: f64,
}

/// Range classification for adaptive fetching
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeRisk {
    /// Range doesn't depend on any down shards - use aggressive fetching
    Safe,
    /// Range depends on 1-2 down shards - may timeout, but likely recoverable
    Low,
    /// Range depends on 3+ down shards - high chance of timeout
    High,
    /// Range depends on many down shards - skip and defer
    Critical,
}

impl RangeRisk {
    /// Get recommended concurrency multiplier for this risk level
    /// Note: These are for diagnostics only - actual fetching uses single aggressive concurrency
    pub fn concurrency_multiplier(&self) -> f64 {
        match self {
            RangeRisk::Safe => 1.5,     // Very aggressive
            RangeRisk::Low => 1.0,      // Normal
            RangeRisk::High => 0.5,     // Conservative
            RangeRisk::Critical => 0.0, // Skip
        }
    }

    /// Get recommended timeout multiplier for this risk level
    pub fn timeout_multiplier(&self) -> f64 {
        match self {
            RangeRisk::Safe => 0.5,     // Short timeout
            RangeRisk::Low => 1.0,      // Normal timeout
            RangeRisk::High => 2.0,     // Longer timeout to allow retries
            RangeRisk::Critical => 0.0, // Don't even try
        }
    }
}

/// Predicts problematic byte ranges in a Walrus blob
pub struct SliverPredictor {
    /// Set of problematic shard IDs (from NodeHealthTracker)
    problematic_shards: HashSet<u32>,
    /// Symbol size in bytes (typically 256)
    symbol_size: u64,
}

impl SliverPredictor {
    /// Create a new predictor with the given problematic shards
    pub fn new(problematic_shards: HashSet<u32>) -> Self {
        Self {
            problematic_shards,
            symbol_size: SYMBOL_SIZE,
        }
    }

    /// Create a predictor with custom symbol size
    pub fn with_symbol_size(problematic_shards: HashSet<u32>, symbol_size: u64) -> Self {
        Self {
            problematic_shards,
            symbol_size,
        }
    }

    /// Update the set of problematic shards
    pub fn update_shards(&mut self, problematic_shards: HashSet<u32>) {
        self.problematic_shards = problematic_shards;
    }

    /// Compute the rotation offset for a blob
    ///
    /// The blob_id is a base64-encoded string. We need to decode it and
    /// compute blob_id_bytes % 1000 to get the rotation offset.
    pub fn compute_rotation_offset(&self, blob_id: &str) -> Option<u64> {
        // Try to decode the blob_id (it's base64url encoded)
        let decoded = base64_url_decode(blob_id)?;

        // The rotation is: blob_id (as big integer) % 1000
        // Since we only need mod 1000, we can compute this incrementally
        // from the bytes: (sum of byte[i] * 256^i) mod 1000
        let mut result: u64 = 0;
        let mut power: u64 = 1; // 256^0

        for byte in decoded.iter() {
            result = (result + (*byte as u64) * power) % N_SHARDS;
            power = (power * 256) % N_SHARDS;
        }

        Some(result)
    }

    /// Given a sliver index and rotation offset, compute which shard it maps to
    pub fn sliver_to_shard(&self, sliver_index: u64, rotation_offset: u64) -> u32 {
        ((sliver_index + rotation_offset) % N_SHARDS) as u32
    }

    /// Given a shard and rotation offset, compute which sliver index maps to it
    pub fn shard_to_sliver(&self, shard: u32, rotation_offset: u64) -> u64 {
        // sliver = (shard - offset + 1000) % 1000
        (shard as u64 + N_SHARDS - rotation_offset) % N_SHARDS
    }

    /// Check if a primary sliver is problematic (maps to a down shard)
    pub fn is_primary_sliver_problematic(&self, sliver_index: u64, rotation_offset: u64) -> bool {
        let shard = self.sliver_to_shard(sliver_index, rotation_offset);
        self.problematic_shards.contains(&shard)
    }

    /// Check if a secondary sliver is problematic
    /// Note: Secondary slivers use indices 334-1000 in the shard space
    pub fn is_secondary_sliver_problematic(&self, sliver_index: u64, rotation_offset: u64) -> bool {
        // Secondary slivers start at index SOURCE_SYMBOLS_PRIMARY (334)
        let adjusted_index = sliver_index + SOURCE_SYMBOLS_PRIMARY;
        let shard = self.sliver_to_shard(adjusted_index, rotation_offset);
        self.problematic_shards.contains(&shard)
    }

    /// Compute which byte range a primary sliver covers
    ///
    /// Primary sliver i covers symbols [i * 667, (i+1) * 667)
    /// Which translates to bytes [i * 667 * symbol_size, (i+1) * 667 * symbol_size)
    pub fn primary_sliver_byte_range(&self, sliver_index: u64) -> Range<u64> {
        let start = sliver_index * SOURCE_SYMBOLS_SECONDARY * self.symbol_size;
        let end = (sliver_index + 1) * SOURCE_SYMBOLS_SECONDARY * self.symbol_size;
        start..end
    }

    /// Compute which primary sliver covers a given byte offset
    pub fn byte_to_primary_sliver(&self, byte_offset: u64) -> u64 {
        let symbol_index = byte_offset / self.symbol_size;
        symbol_index / SOURCE_SYMBOLS_SECONDARY
    }

    /// Compute which secondary sliver covers a given byte offset
    pub fn byte_to_secondary_sliver(&self, byte_offset: u64) -> u64 {
        let symbol_index = byte_offset / self.symbol_size;
        symbol_index % SOURCE_SYMBOLS_SECONDARY
    }

    /// Analyze a blob and classify byte ranges
    pub fn analyze_blob(&self, blob_id: &str, blob_size: u64) -> Option<BlobAnalysis> {
        let rotation_offset = self.compute_rotation_offset(blob_id)?;

        // Calculate how many symbols/slivers this blob actually uses
        let total_symbols = blob_size.div_ceil(self.symbol_size);
        let actual_primary_slivers = total_symbols.div_ceil(SOURCE_SYMBOLS_SECONDARY);
        let actual_primary_slivers = actual_primary_slivers.min(SOURCE_SYMBOLS_PRIMARY);

        // Find which primary slivers are problematic
        let mut problematic_primary: Vec<u64> = Vec::new();
        for i in 0..actual_primary_slivers {
            if self.is_primary_sliver_problematic(i, rotation_offset) {
                problematic_primary.push(i);
            }
        }

        // Find which secondary slivers are problematic
        let actual_secondary_slivers = SOURCE_SYMBOLS_SECONDARY.min(total_symbols);
        let mut problematic_secondary: Vec<u64> = Vec::new();
        for i in 0..actual_secondary_slivers {
            if self.is_secondary_sliver_problematic(i, rotation_offset) {
                problematic_secondary.push(i);
            }
        }

        // Build safe and risky ranges based on primary slivers
        // (primary slivers give us contiguous byte ranges)
        let mut safe_ranges: Vec<Range<u64>> = Vec::new();
        let mut risky_ranges: Vec<Range<u64>> = Vec::new();

        let problematic_set: HashSet<u64> = problematic_primary.iter().copied().collect();

        let mut current_safe_start: Option<u64> = None;
        let mut current_risky_start: Option<u64> = None;

        for sliver_idx in 0..actual_primary_slivers {
            let range = self.primary_sliver_byte_range(sliver_idx);
            let clamped_end = range.end.min(blob_size);

            if problematic_set.contains(&sliver_idx) {
                // This sliver is problematic
                if let Some(start) = current_safe_start.take() {
                    safe_ranges.push(start..range.start.min(blob_size));
                }
                if current_risky_start.is_none() {
                    current_risky_start = Some(range.start);
                }
            } else {
                // This sliver is safe
                if let Some(start) = current_risky_start.take() {
                    risky_ranges.push(start..range.start.min(blob_size));
                }
                if current_safe_start.is_none() {
                    current_safe_start = Some(range.start);
                }
            }

            // Handle the last sliver
            if sliver_idx == actual_primary_slivers - 1 {
                if let Some(start) = current_safe_start.take() {
                    safe_ranges.push(start..clamped_end);
                }
                if let Some(start) = current_risky_start.take() {
                    risky_ranges.push(start..clamped_end);
                }
            }
        }

        // Calculate safe percentage
        let safe_bytes: u64 = safe_ranges.iter().map(|r| r.end - r.start).sum();
        let safe_percentage = if blob_size > 0 {
            (safe_bytes as f64 / blob_size as f64) * 100.0
        } else {
            100.0
        };

        Some(BlobAnalysis {
            blob_id: blob_id.to_string(),
            blob_size,
            rotation_offset,
            total_primary_slivers: actual_primary_slivers,
            total_secondary_slivers: actual_secondary_slivers,
            problematic_primary_slivers: problematic_primary,
            problematic_secondary_slivers: problematic_secondary,
            safe_ranges,
            risky_ranges,
            safe_percentage,
        })
    }

    /// Classify a byte range by its risk level
    pub fn classify_range(&self, blob_id: &str, range: &Range<u64>) -> RangeRisk {
        let rotation_offset = match self.compute_rotation_offset(blob_id) {
            Some(offset) => offset,
            None => return RangeRisk::Low, // Can't predict, assume low risk
        };

        // Check which primary slivers this range spans
        let start_sliver = self.byte_to_primary_sliver(range.start);
        let end_sliver = self.byte_to_primary_sliver(range.end.saturating_sub(1));

        let mut problematic_count = 0;
        for sliver_idx in start_sliver..=end_sliver {
            if self.is_primary_sliver_problematic(sliver_idx, rotation_offset) {
                problematic_count += 1;
            }
        }

        let total_slivers = (end_sliver - start_sliver + 1) as usize;

        match problematic_count {
            0 => RangeRisk::Safe,
            1 if total_slivers >= 3 => RangeRisk::Low, // Only 1 bad sliver in a large range
            1..=2 => RangeRisk::Low,
            3..=5 => RangeRisk::High,
            _ => RangeRisk::Critical,
        }
    }

    /// Get recommended fetching parameters for a range
    pub fn get_fetch_params(
        &self,
        blob_id: &str,
        range: &Range<u64>,
        base_concurrency: usize,
        base_timeout_secs: u64,
    ) -> FetchParams {
        let risk = self.classify_range(blob_id, range);

        let concurrency =
            ((base_concurrency as f64) * risk.concurrency_multiplier()).ceil() as usize;
        let timeout_secs = ((base_timeout_secs as f64) * risk.timeout_multiplier()).ceil() as u64;

        FetchParams {
            risk,
            concurrency: concurrency.max(1),
            timeout_secs: timeout_secs.max(10),
            should_skip: matches!(risk, RangeRisk::Critical),
        }
    }

    /// Split a range into safe and risky sub-ranges for optimized fetching
    pub fn split_range_by_risk(
        &self,
        blob_id: &str,
        range: Range<u64>,
        blob_size: u64,
    ) -> RangeSplit {
        let analysis = match self.analyze_blob(blob_id, blob_size) {
            Some(a) => a,
            None => {
                // Can't analyze, treat entire range as unknown
                return RangeSplit {
                    safe_ranges: vec![],
                    risky_ranges: vec![range],
                    skipped_ranges: vec![],
                };
            }
        };

        let mut safe_ranges = Vec::new();
        let mut risky_ranges = Vec::new();

        // Find overlapping safe ranges
        for safe_range in &analysis.safe_ranges {
            if let Some(overlap) = range_overlap(&range, safe_range) {
                safe_ranges.push(overlap);
            }
        }

        // Find overlapping risky ranges
        for risky_range in &analysis.risky_ranges {
            if let Some(overlap) = range_overlap(&range, risky_range) {
                risky_ranges.push(overlap);
            }
        }

        RangeSplit {
            safe_ranges,
            risky_ranges,
            skipped_ranges: vec![], // We don't skip by default, let caller decide
        }
    }
}

/// Parameters for fetching a range
#[derive(Debug, Clone)]
pub struct FetchParams {
    pub risk: RangeRisk,
    pub concurrency: usize,
    pub timeout_secs: u64,
    pub should_skip: bool,
}

/// Result of splitting a range by risk
#[derive(Debug, Clone)]
pub struct RangeSplit {
    pub safe_ranges: Vec<Range<u64>>,
    pub risky_ranges: Vec<Range<u64>>,
    pub skipped_ranges: Vec<Range<u64>>,
}

/// Compute the overlap between two ranges
fn range_overlap(a: &Range<u64>, b: &Range<u64>) -> Option<Range<u64>> {
    let start = a.start.max(b.start);
    let end = a.end.min(b.end);
    if start < end {
        Some(start..end)
    } else {
        None
    }
}

/// Decode a base64url-encoded string
fn base64_url_decode(input: &str) -> Option<Vec<u8>> {
    // base64url uses - and _ instead of + and /
    // and may omit padding
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    URL_SAFE_NO_PAD.decode(input).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rotation_offset_computation() {
        let predictor = SliverPredictor::new(HashSet::new());

        // Test with a known blob_id
        // The actual rotation will depend on the blob_id bytes
        let blob_id = "lJIYRNvG_cgmwx_OT8t7gvdr3rTqG_NsSQPF1714qu0";
        let offset = predictor.compute_rotation_offset(blob_id);
        assert!(offset.is_some());
        let offset = offset.unwrap();
        assert!(offset < N_SHARDS);
    }

    #[test]
    fn test_sliver_to_shard_mapping() {
        let predictor = SliverPredictor::new(HashSet::new());

        // With rotation offset 0, sliver i maps to shard i
        assert_eq!(predictor.sliver_to_shard(0, 0), 0);
        assert_eq!(predictor.sliver_to_shard(100, 0), 100);

        // With rotation offset 500, sliver 0 maps to shard 500
        assert_eq!(predictor.sliver_to_shard(0, 500), 500);
        assert_eq!(predictor.sliver_to_shard(600, 500), 100); // (600 + 500) % 1000 = 100
    }

    #[test]
    fn test_shard_to_sliver_inverse() {
        let predictor = SliverPredictor::new(HashSet::new());

        for rotation in [0, 123, 500, 999] {
            for sliver in [0, 50, 333, 500, 999] {
                let shard = predictor.sliver_to_shard(sliver, rotation);
                let recovered_sliver = predictor.shard_to_sliver(shard, rotation);
                assert_eq!(
                    sliver, recovered_sliver,
                    "sliver={}, rotation={}, shard={}",
                    sliver, rotation, shard
                );
            }
        }
    }

    #[test]
    fn test_byte_to_sliver() {
        let predictor = SliverPredictor::new(HashSet::new());

        // With default symbol_size=256, secondary_symbols=667
        // Primary sliver 0 covers bytes [0, 256*667) = [0, 170752)
        assert_eq!(predictor.byte_to_primary_sliver(0), 0);
        assert_eq!(predictor.byte_to_primary_sliver(170751), 0);
        assert_eq!(predictor.byte_to_primary_sliver(170752), 1);
    }

    #[test]
    fn test_problematic_sliver_detection() {
        // Shards 100, 200, 300 are down
        let problematic: HashSet<u32> = [100, 200, 300].into_iter().collect();
        let predictor = SliverPredictor::new(problematic);

        // With rotation offset 0:
        // Sliver 100 maps to shard 100 (down)
        assert!(predictor.is_primary_sliver_problematic(100, 0));
        // Sliver 50 maps to shard 50 (healthy)
        assert!(!predictor.is_primary_sliver_problematic(50, 0));

        // With rotation offset 50:
        // Sliver 50 maps to shard 100 (down)
        assert!(predictor.is_primary_sliver_problematic(50, 50));
    }

    #[test]
    fn test_range_classification() {
        // Only shard 100 is down
        let problematic: HashSet<u32> = [100].into_iter().collect();
        let predictor = SliverPredictor::new(problematic);

        // Use a simple blob_id that gives a predictable rotation
        // For testing, we'll check that classification works in principle
        let blob_id = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"; // All zeros -> rotation 0

        // Range covering sliver 0 (bytes 0..170752) should be safe if shard 0 is up
        let range = 0..170752;
        let risk = predictor.classify_range(blob_id, &range);
        assert_eq!(risk, RangeRisk::Safe);
    }

    #[test]
    fn test_blob_analysis() {
        // Shards 0, 1, 2 are down
        let problematic: HashSet<u32> = [0, 1, 2].into_iter().collect();
        let predictor = SliverPredictor::new(problematic);

        let blob_id = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"; // rotation 0
        let blob_size = 1_000_000_000; // 1GB blob

        let analysis = predictor.analyze_blob(blob_id, blob_size);
        assert!(analysis.is_some());

        let analysis = analysis.unwrap();
        assert_eq!(analysis.rotation_offset, 0);
        assert!(!analysis.problematic_primary_slivers.is_empty());
        // Slivers 0, 1, 2 should be problematic
        assert!(analysis.problematic_primary_slivers.contains(&0));
        assert!(analysis.problematic_primary_slivers.contains(&1));
        assert!(analysis.problematic_primary_slivers.contains(&2));
    }
}
