//! Node Health Tracking for Walrus Storage
//!
//! This module provides proactive node health monitoring to enable smarter
//! routing around slow or failing storage nodes. It polls node health via
//! the Walrus CLI and maintains a mapping of shards to nodes with their
//! current health status.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    NodeHealthTracker                         │
//! │  ┌─────────────────┐  ┌─────────────────┐                   │
//! │  │  Health Poller  │  │  Shard Mapper   │                   │
//! │  │  (every 5 min)  │  │  (shard→node)   │                   │
//! │  └────────┬────────┘  └────────┬────────┘                   │
//! │           │                    │                            │
//! │           └────────┬───────────┘                            │
//! │                    │                                        │
//! │           ┌────────▼────────┐                               │
//! │           │  Problematic    │                               │
//! │           │  Shard Registry │                               │
//! │           │  (DOWN/DEGRADED)│                               │
//! │           └─────────────────┘                               │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Usage
//!
//! The tracker can be queried before making range requests to check if
//! certain byte ranges are likely to hit problematic shards. If so, the
//! caller can proactively split the range or adjust retry strategies.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::process::Command;
use tokio::sync::RwLock;

/// Health status of a storage node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is healthy and responsive
    Healthy,
    /// Node is responding but has shards in transfer/recovery
    Degraded,
    /// Node is not responding
    Down,
    /// Status unknown (not yet polled)
    Unknown,
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeStatus::Healthy => write!(f, "HEALTHY"),
            NodeStatus::Degraded => write!(f, "DEGRADED"),
            NodeStatus::Down => write!(f, "DOWN"),
            NodeStatus::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

/// Information about a storage node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub name: String,
    pub network_address: String,
    pub status: NodeStatus,
    pub shards: Vec<u32>,
    pub shards_in_transfer: u32,
    pub shards_in_recovery: u32,
    pub last_updated: std::time::SystemTime,
}

/// Information about a shard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub shard_id: u32,
    pub node_name: String,
    pub status: NodeStatus,
}

/// Summary of network health
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkHealthSummary {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub degraded_nodes: usize,
    pub down_nodes: usize,
    pub total_shards: usize,
    pub problematic_shards: usize,
    pub down_node_names: Vec<String>,
    pub degraded_node_names: Vec<String>,
    pub last_updated: Option<std::time::SystemTime>,
}

/// Node health tracker for Walrus storage nodes
pub struct NodeHealthTracker {
    inner: Arc<Inner>,
}

struct Inner {
    cli_path: PathBuf,
    cli_context: String,
    cli_timeout_secs: u64,
    poll_interval: Duration,
    nodes: RwLock<HashMap<String, NodeInfo>>,
    shard_to_node: RwLock<HashMap<u32, String>>,
    problematic_shards: RwLock<HashSet<u32>>,
    last_poll: RwLock<Option<Instant>>,
}

impl Clone for NodeHealthTracker {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl NodeHealthTracker {
    /// Create a new node health tracker
    pub fn new(
        cli_path: PathBuf,
        cli_context: String,
        cli_timeout_secs: u64,
        poll_interval_secs: u64,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                cli_path,
                cli_context,
                cli_timeout_secs,
                poll_interval: Duration::from_secs(poll_interval_secs),
                nodes: RwLock::new(HashMap::new()),
                shard_to_node: RwLock::new(HashMap::new()),
                problematic_shards: RwLock::new(HashSet::new()),
                last_poll: RwLock::new(None),
            }),
        }
    }

    /// Check if a poll is needed based on the interval
    pub async fn needs_poll(&self) -> bool {
        let last_poll = self.inner.last_poll.read().await;
        match *last_poll {
            Some(instant) => instant.elapsed() >= self.inner.poll_interval,
            None => true,
        }
    }

    /// Force a health poll regardless of interval
    pub async fn poll_health(&self) -> Result<NetworkHealthSummary> {
        tracing::info!("polling Walrus node health...");

        // Get node health
        let health_output = self.run_health_command().await?;
        let health_json = self.parse_json_from_output(&health_output)?;

        // Get committee info for shard mapping
        let committee_output = self.run_committee_command().await?;
        let committee_json = self.parse_json_from_output(&committee_output)?;

        // Build shard-to-node mapping
        let mut shard_to_node = HashMap::new();
        let mut node_shards: HashMap<String, Vec<u32>> = HashMap::new();

        if let Some(nodes) = committee_json
            .get("currentStorageNodes")
            .and_then(|v| v.as_array())
        {
            for node in nodes {
                let name = node
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                if let Some(shard_ids) = node.get("shardIds").and_then(|v| v.as_array()) {
                    let shards: Vec<u32> = shard_ids
                        .iter()
                        .filter_map(|v| v.as_u64().map(|n| n as u32))
                        .collect();
                    for &shard_id in &shards {
                        shard_to_node.insert(shard_id, name.to_string());
                    }
                    node_shards.insert(name.to_string(), shards);
                }
            }
        }

        // Parse health info and build node registry
        let mut nodes = HashMap::new();
        let mut down_names = Vec::new();
        let mut degraded_names = Vec::new();

        if let Some(health_info) = health_json.get("healthInfo").and_then(|v| v.as_array()) {
            for info in health_info {
                let name = info
                    .get("nodeName")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let network_address = info
                    .get("nodeUrl")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");

                let (status, in_transfer, in_recovery) =
                    if let Some(err) = info.get("healthInfo").and_then(|v| v.get("Err")) {
                        tracing::debug!("node {} is DOWN: {:?}", name, err);
                        down_names.push(name.to_string());
                        (NodeStatus::Down, 0, 0)
                    } else if let Some(ok) = info.get("healthInfo").and_then(|v| v.get("Ok")) {
                        let in_transfer = ok
                            .get("shardSummary")
                            .and_then(|s| s.get("ownedShardStatus"))
                            .and_then(|o| o.get("inTransfer"))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0) as u32;
                        let in_recovery = ok
                            .get("shardSummary")
                            .and_then(|s| s.get("ownedShardStatus"))
                            .and_then(|o| o.get("inRecovery"))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0) as u32;

                        if in_transfer > 0 || in_recovery > 0 {
                            tracing::debug!(
                                "node {} is DEGRADED: inTransfer={}, inRecovery={}",
                                name,
                                in_transfer,
                                in_recovery
                            );
                            degraded_names.push(name.to_string());
                            (NodeStatus::Degraded, in_transfer, in_recovery)
                        } else {
                            (NodeStatus::Healthy, 0, 0)
                        }
                    } else {
                        (NodeStatus::Unknown, 0, 0)
                    };

                let shards = node_shards.get(name).cloned().unwrap_or_default();

                nodes.insert(
                    name.to_string(),
                    NodeInfo {
                        name: name.to_string(),
                        network_address: network_address.to_string(),
                        status,
                        shards,
                        shards_in_transfer: in_transfer,
                        shards_in_recovery: in_recovery,
                        last_updated: std::time::SystemTime::now(),
                    },
                );
            }
        }

        // Build problematic shards set
        let mut problematic = HashSet::new();
        for (shard_id, node_name) in &shard_to_node {
            if let Some(node) = nodes.get(node_name) {
                if node.status == NodeStatus::Down || node.status == NodeStatus::Degraded {
                    problematic.insert(*shard_id);
                }
            }
        }

        // Update internal state
        {
            let mut nodes_lock = self.inner.nodes.write().await;
            *nodes_lock = nodes.clone();
        }
        {
            let mut shard_lock = self.inner.shard_to_node.write().await;
            *shard_lock = shard_to_node.clone();
        }
        {
            let mut prob_lock = self.inner.problematic_shards.write().await;
            *prob_lock = problematic.clone();
        }
        {
            let mut last_poll = self.inner.last_poll.write().await;
            *last_poll = Some(Instant::now());
        }

        let summary = NetworkHealthSummary {
            total_nodes: nodes.len(),
            healthy_nodes: nodes
                .values()
                .filter(|n| n.status == NodeStatus::Healthy)
                .count(),
            degraded_nodes: degraded_names.len(),
            down_nodes: down_names.len(),
            total_shards: shard_to_node.len(),
            problematic_shards: problematic.len(),
            down_node_names: down_names,
            degraded_node_names: degraded_names,
            last_updated: Some(std::time::SystemTime::now()),
        };

        tracing::info!(
            "node health poll complete: {} healthy, {} degraded, {} down (of {} total); {} problematic shards",
            summary.healthy_nodes,
            summary.degraded_nodes,
            summary.down_nodes,
            summary.total_nodes,
            summary.problematic_shards
        );

        Ok(summary)
    }

    /// Poll health if needed (respects interval)
    pub async fn poll_if_needed(&self) -> Result<Option<NetworkHealthSummary>> {
        if self.needs_poll().await {
            Ok(Some(self.poll_health().await?))
        } else {
            Ok(None)
        }
    }

    /// Get the current network health summary (from cache, no new poll)
    pub async fn get_summary(&self) -> NetworkHealthSummary {
        let nodes = self.inner.nodes.read().await;
        let shard_to_node = self.inner.shard_to_node.read().await;
        let problematic = self.inner.problematic_shards.read().await;

        let down_names: Vec<String> = nodes
            .values()
            .filter(|n| n.status == NodeStatus::Down)
            .map(|n| n.name.clone())
            .collect();

        let degraded_names: Vec<String> = nodes
            .values()
            .filter(|n| n.status == NodeStatus::Degraded)
            .map(|n| n.name.clone())
            .collect();

        let last_poll = self.inner.last_poll.read().await;

        NetworkHealthSummary {
            total_nodes: nodes.len(),
            healthy_nodes: nodes
                .values()
                .filter(|n| n.status == NodeStatus::Healthy)
                .count(),
            degraded_nodes: degraded_names.len(),
            down_nodes: down_names.len(),
            total_shards: shard_to_node.len(),
            problematic_shards: problematic.len(),
            down_node_names: down_names,
            degraded_node_names: degraded_names,
            last_updated: last_poll.map(|_| std::time::SystemTime::now()),
        }
    }

    /// Check if a shard is problematic (down or degraded)
    pub async fn is_shard_problematic(&self, shard_id: u32) -> bool {
        let problematic = self.inner.problematic_shards.read().await;
        problematic.contains(&shard_id)
    }

    /// Get all problematic shard IDs
    pub async fn get_problematic_shards(&self) -> HashSet<u32> {
        self.inner.problematic_shards.read().await.clone()
    }

    /// Get node info by name
    pub async fn get_node(&self, name: &str) -> Option<NodeInfo> {
        let nodes = self.inner.nodes.read().await;
        nodes.get(name).cloned()
    }

    /// Get shard info
    pub async fn get_shard_info(&self, shard_id: u32) -> Option<ShardInfo> {
        let shard_to_node = self.inner.shard_to_node.read().await;
        let nodes = self.inner.nodes.read().await;

        let node_name = shard_to_node.get(&shard_id)?;
        let status = nodes
            .get(node_name)
            .map(|n| n.status)
            .unwrap_or(NodeStatus::Unknown);

        Some(ShardInfo {
            shard_id,
            node_name: node_name.clone(),
            status,
        })
    }

    /// Run the health command via CLI
    async fn run_health_command(&self) -> Result<String> {
        let mut cmd = Command::new(&self.inner.cli_path);
        cmd.arg("health")
            .arg("--committee")
            .arg("--context")
            .arg(&self.inner.cli_context)
            .arg("--json");

        cmd.kill_on_drop(true);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

        let child = cmd
            .spawn()
            .context("failed to spawn walrus health command")?;
        let timeout = Duration::from_secs(self.inner.cli_timeout_secs);

        match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(res) => {
                let output = res.context("failed to wait on walrus health command")?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    return Err(anyhow::anyhow!(
                        "walrus health command failed: {}",
                        stderr.trim()
                    ));
                }
                Ok(String::from_utf8_lossy(&output.stdout).to_string())
            }
            Err(_) => Err(anyhow::anyhow!(
                "walrus health command timed out after {}s",
                self.inner.cli_timeout_secs
            )),
        }
    }

    /// Run the committee info command via CLI
    async fn run_committee_command(&self) -> Result<String> {
        let mut cmd = Command::new(&self.inner.cli_path);
        cmd.arg("info")
            .arg("committee")
            .arg("--context")
            .arg(&self.inner.cli_context)
            .arg("--json");

        cmd.kill_on_drop(true);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

        let child = cmd
            .spawn()
            .context("failed to spawn walrus info committee command")?;
        let timeout = Duration::from_secs(self.inner.cli_timeout_secs);

        match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(res) => {
                let output = res.context("failed to wait on walrus info committee command")?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    return Err(anyhow::anyhow!(
                        "walrus info committee command failed: {}",
                        stderr.trim()
                    ));
                }
                Ok(String::from_utf8_lossy(&output.stdout).to_string())
            }
            Err(_) => Err(anyhow::anyhow!(
                "walrus info committee command timed out after {}s",
                self.inner.cli_timeout_secs
            )),
        }
    }

    /// Parse JSON from CLI output (handles log lines before JSON)
    fn parse_json_from_output(&self, output: &str) -> Result<serde_json::Value> {
        // Find the start of JSON (first '{' character)
        let json_start = output
            .find('{')
            .ok_or_else(|| anyhow::anyhow!("no JSON found in CLI output"))?;
        let json_str = &output[json_start..];

        serde_json::from_str(json_str).context("failed to parse JSON from CLI output")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_status_display() {
        assert_eq!(format!("{}", NodeStatus::Healthy), "HEALTHY");
        assert_eq!(format!("{}", NodeStatus::Degraded), "DEGRADED");
        assert_eq!(format!("{}", NodeStatus::Down), "DOWN");
        assert_eq!(format!("{}", NodeStatus::Unknown), "UNKNOWN");
    }
}
