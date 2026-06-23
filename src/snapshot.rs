// xaeroflux/src/snapshot.rs
//
// Snapshot Protocol for XaeroFlux
//
// Provides initial sync for new peers joining a group:
// 1. New peer broadcasts RequestSnapshot on group topic
// 2. Bootstrap (or any peer with data) responds with SnapshotAvailable
// 3. Requester connects via direct QUIC to download snapshot
// 4. After snapshot, peer receives only delta events via gossip
//
// Snapshot contains: groups, workspaces, boards, objects (files), chats
// Does NOT contain: whiteboard elements, notebook cells (too granular, sync via events)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use iroh::{endpoint::SendStream, Endpoint, PublicKey};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// ALPN protocol identifier for snapshot transfers
pub const SNAPSHOT_ALPN: &[u8] = b"cyan-snapshot-v1";

// ============================================================================
// Snapshot Protocol Messages (sent via gossip)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SnapshotMessage {
    /// Peer requests a snapshot for a group
    RequestSnapshot {
        group_id: String,
        requester_node_id: String,
        /// Timestamp of last known event (0 for full snapshot)
        since_ts: u64,
    },
    
    /// Peer announces it has snapshot data available
    SnapshotAvailable {
        group_id: String,
        provider_node_id: String,
        /// Number of items in snapshot
        item_count: u32,
        /// Timestamp of most recent item
        latest_ts: u64,
    },
}

// ============================================================================
// Snapshot Data Structures
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupSnapshot {
    pub group: GroupData,
    pub workspaces: Vec<WorkspaceData>,
    pub boards: Vec<BoardData>,
    pub files: Vec<FileData>,
    pub recent_chats: Vec<ChatData>,
    pub snapshot_ts: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupData {
    pub id: String,
    pub name: String,
    pub icon: String,
    pub color: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceData {
    pub id: String,
    pub group_id: String,
    pub name: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoardData {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub board_mode: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileData {
    pub id: String,
    pub group_id: String,
    pub workspace_id: Option<String>,
    pub board_id: Option<String>,
    pub name: String,
    pub hash: String,
    pub size: u64,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatData {
    pub id: String,
    pub workspace_id: String,
    pub message: String,
    pub author: String,
    pub parent_id: Option<String>,
    pub timestamp: i64,
}

// ============================================================================
// Snapshot Provider (Bootstrap side)
// ============================================================================

pub struct SnapshotProvider {
    /// group_id -> GroupSnapshot
    snapshots: Arc<RwLock<HashMap<String, GroupSnapshot>>>,
    endpoint: Endpoint,
    node_id: String,
}

impl SnapshotProvider {
    pub fn new(endpoint: Endpoint, node_id: String) -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            endpoint,
            node_id,
        }
    }

    /// Update snapshot from incoming events (called by bootstrap)
    pub async fn update_from_event(&self, group_id: &str, event: &super::Event) {
        let mut snapshots = self.snapshots.write().await;
        
        // Get or create snapshot for this group
        let snapshot = snapshots.entry(group_id.to_string()).or_insert_with(|| {
            GroupSnapshot {
                group: GroupData {
                    id: group_id.to_string(),
                    name: String::new(),
                    icon: String::new(),
                    color: String::new(),
                    created_at: 0,
                },
                workspaces: Vec::new(),
                boards: Vec::new(),
                files: Vec::new(),
                recent_chats: Vec::new(),
                snapshot_ts: 0,
            }
        });

        // Try to parse and update snapshot based on event type
        // This is a simplified version - in production, parse NetworkEvent properly
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&event.payload) {
            if let Some(event_type) = parsed.get("type").and_then(|v| v.as_str()) {
                match event_type {
                    "GroupCreated" => {
                        if let Ok(group) = serde_json::from_value::<GroupData>(parsed.clone()) {
                            snapshot.group = group;
                        }
                    }
                    "WorkspaceCreated" => {
                        if let Ok(ws) = serde_json::from_value::<WorkspaceData>(parsed.clone()) {
                            if !snapshot.workspaces.iter().any(|w| w.id == ws.id) {
                                snapshot.workspaces.push(ws);
                            }
                        }
                    }
                    "BoardCreated" => {
                        if let (Some(id), Some(ws_id), Some(name)) = (
                            parsed.get("id").and_then(|v| v.as_str()),
                            parsed.get("workspace_id").and_then(|v| v.as_str()),
                            parsed.get("name").and_then(|v| v.as_str()),
                        ) {
                            if !snapshot.boards.iter().any(|b| b.id == id) {
                                snapshot.boards.push(BoardData {
                                    id: id.to_string(),
                                    workspace_id: ws_id.to_string(),
                                    name: name.to_string(),
                                    board_mode: "freeform".to_string(),
                                    created_at: parsed.get("created_at")
                                        .and_then(|v| v.as_i64())
                                        .unwrap_or(0),
                                });
                            }
                        }
                    }
                    "FileAvailable" => {
                        if let Ok(file) = serde_json::from_value::<FileData>(parsed.clone()) {
                            if !snapshot.files.iter().any(|f| f.id == file.id) {
                                snapshot.files.push(file);
                            }
                        }
                    }
                    "ChatSent" => {
                        if let Ok(chat) = serde_json::from_value::<ChatData>(parsed.clone()) {
                            // Keep only recent chats (last 100)
                            if snapshot.recent_chats.len() >= 100 {
                                snapshot.recent_chats.remove(0);
                            }
                            snapshot.recent_chats.push(chat);
                        }
                    }
                    _ => {}
                }
            }
        }

        snapshot.snapshot_ts = event.ts;
    }

    /// Get snapshot for a group
    pub async fn get_snapshot(&self, group_id: &str) -> Option<GroupSnapshot> {
        let snapshots = self.snapshots.read().await;
        snapshots.get(group_id).cloned()
    }

    /// Handle incoming snapshot request (called when we receive RequestSnapshot)
    pub async fn handle_request(&self, group_id: &str) -> Option<SnapshotMessage> {
        let snapshots = self.snapshots.read().await;
        
        if let Some(snapshot) = snapshots.get(group_id) {
            let item_count = 1  // group
                + snapshot.workspaces.len() as u32
                + snapshot.boards.len() as u32
                + snapshot.files.len() as u32
                + snapshot.recent_chats.len() as u32;

            Some(SnapshotMessage::SnapshotAvailable {
                group_id: group_id.to_string(),
                provider_node_id: self.node_id.clone(),
                item_count,
                latest_ts: snapshot.snapshot_ts,
            })
        } else {
            None
        }
    }

    /// Serve snapshot over a direct QUIC connection.
    ///
    /// `send` is the send half of the bi-stream the requester opened — obtained by the
    /// accept loop via `conn.accept_bi()` on the incoming request. We reply on *that*
    /// accepted stream so the requester's matching `recv` half receives the bytes. (An
    /// earlier version replied on a fresh `conn.open_bi()`; the requester never read that
    /// stream, so the two halves never rendezvoused and the transfer failed "connection
    /// lost".)
    pub async fn serve_snapshot(&self, mut send: SendStream, group_id: &str) -> Result<()> {
        let snapshot = match self.get_snapshot(group_id).await {
            Some(s) => s,
            None => return Err(anyhow::anyhow!("No snapshot for group {}", group_id)),
        };

        // Serialize and send snapshot
        let data = serde_json::to_vec(&snapshot)?;
        let len = (data.len() as u32).to_be_bytes();
        
        send.write_all(&len).await?;
        send.write_all(&data).await?;
        send.finish()?;

        tracing::info!(
            "Served snapshot for group {} ({} bytes, {} items)",
            &group_id[..16.min(group_id.len())],
            data.len(),
            1 + snapshot.workspaces.len() + snapshot.boards.len() + 
            snapshot.files.len() + snapshot.recent_chats.len()
        );

        Ok(())
    }
}

// ============================================================================
// Snapshot Requester (Peer side)
// ============================================================================

pub struct SnapshotRequester {
    endpoint: Endpoint,
    node_id: String,
    /// group_id -> last sync timestamp
    sync_state: Arc<RwLock<HashMap<String, u64>>>,
}

impl SnapshotRequester {
    pub fn new(endpoint: Endpoint, node_id: String) -> Self {
        Self {
            endpoint,
            node_id,
            sync_state: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if we need a snapshot for this group
    pub async fn needs_snapshot(&self, group_id: &str) -> bool {
        let sync_state = self.sync_state.read().await;
        
        match sync_state.get(group_id) {
            None => true,  // Never synced
            Some(&last_ts) => {
                // Request new snapshot if last sync was > 6 hours ago
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                now - last_ts > 6 * 3600
            }
        }
    }

    /// Create a snapshot request message
    pub fn create_request(&self, group_id: &str) -> SnapshotMessage {
        SnapshotMessage::RequestSnapshot {
            group_id: group_id.to_string(),
            requester_node_id: self.node_id.clone(),
            since_ts: 0,  // Full snapshot
        }
    }

    /// Download snapshot from provider
    pub async fn download_snapshot(
        &self,
        provider_node_id: &str,
        group_id: &str,
    ) -> Result<GroupSnapshot> {
        let node_id: PublicKey = provider_node_id.parse()?;
        
        // Connect to provider
        let conn = self.endpoint
            .connect(node_id, SNAPSHOT_ALPN)
            .await?;

        let (mut send, mut recv) = conn.open_bi().await?;

        // Send group_id request
        let req = group_id.as_bytes();
        let len = (req.len() as u32).to_be_bytes();
        send.write_all(&len).await?;
        send.write_all(req).await?;
        send.finish()?;

        // Read response length
        let mut len_buf = [0u8; 4];
        recv.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        // Read snapshot data
        let mut data = vec![0u8; len];
        recv.read_exact(&mut data).await?;

        let snapshot: GroupSnapshot = serde_json::from_slice(&data)?;

        // Update sync state
        let mut sync_state = self.sync_state.write().await;
        sync_state.insert(group_id.to_string(), snapshot.snapshot_ts);

        tracing::info!(
            "Downloaded snapshot for group {} ({} workspaces, {} boards, {} files)",
            &group_id[..16.min(group_id.len())],
            snapshot.workspaces.len(),
            snapshot.boards.len(),
            snapshot.files.len()
        );

        Ok(snapshot)
    }

    /// Mark group as synced (after receiving snapshot or catching up via events)
    pub async fn mark_synced(&self, group_id: &str, ts: u64) {
        let mut sync_state = self.sync_state.write().await;
        sync_state.insert(group_id.to_string(), ts);
    }
}

// ============================================================================
// Integration with XaeroFlux main loop
// ============================================================================

/// Helper to determine if a peer should request snapshot
pub fn should_request_snapshot(
    local_item_count: usize,
    remote_item_count: u32,
    last_sync_ts: Option<u64>,
) -> bool {
    // Request if we have significantly fewer items
    if local_item_count == 0 {
        return true;
    }
    
    // Request if remote has 20%+ more items
    if remote_item_count as usize > local_item_count * 12 / 10 {
        return true;
    }

    // Request if we haven't synced in 6 hours
    if let Some(ts) = last_sync_ts {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now - ts > 6 * 3600 {
            return true;
        }
    }

    false
}
