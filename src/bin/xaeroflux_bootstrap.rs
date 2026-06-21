// src/bin/xaeroflux_bootstrap.rs
//
// Bootstrap server for XaeroFlux peer discovery + Iggy event forwarding.
//
// This server:
// 1. Participates in XaeroFlux gossip network (receives events from cyan-backend peers)
// 2. Parses NetworkEvents from group gossip
// 3. Converts to RawEvent format for cyan-lens
// 4. Forwards to Iggy for enrichment pipeline
// 5. Handles snapshot requests for new peers (Step 3)
//
// Build: cargo build --release --bin xaeroflux_bootstrap
// Run:   IGGY_ADDR=10.0.x.x:8090 ./xaeroflux_bootstrap

use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use xaeroflux::rendezvous::{FileSink, publish_signed};
use xaeroflux::{Event, XaeroFlux};

use iggy::client::{Client, ConsumerGroupClient, MessageClient, StreamClient, TopicClient};
use iggy::clients::client::IggyClient;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message, Partitioning};
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;

use serde::{Deserialize, Serialize};

const STREAM_NAME: &str = "cyan-lens";
const TOPIC_NAME: &str = "events.raw";
const PARTITIONS: u32 = 1;

// ============================================================================
// NetworkEvent - Must match cyan-backend/src/models/events.rs
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Group {
    pub id: String,
    pub name: String,
    pub icon: String,
    pub color: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub id: String,
    pub group_id: String,
    pub name: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum NetworkEvent {
    GroupSnapshotAvailable { source: String, group_id: String },
    GroupCreated(Group),
    GroupRenamed { id: String, name: String },
    GroupDeleted { id: String },
    GroupDissolved { id: String },
    WorkspaceCreated(Workspace),
    WorkspaceRenamed { id: String, name: String },
    WorkspaceDeleted { id: String },
    WorkspaceDissolved { id: String },
    BoardCreated { id: String, workspace_id: String, name: String, created_at: i64 },
    BoardRenamed { id: String, name: String },
    BoardDeleted { id: String },
    BoardDissolved { id: String },
    FileAvailable {
        id: String,
        group_id: Option<String>,
        workspace_id: Option<String>,
        board_id: Option<String>,
        name: String,
        hash: String,
        size: u64,
        source_peer: String,
        created_at: i64,
    },
    ChatSent {
        id: String,
        workspace_id: String,
        message: String,
        author: String,
        parent_id: Option<String>,
        timestamp: i64,
    },
    ChatDeleted { id: String },
    WhiteboardElementAdded {
        id: String,
        board_id: String,
        element_type: String,
        x: f64, y: f64, width: f64, height: f64,
        z_index: i32,
        style_json: Option<String>,
        content_json: Option<String>,
        created_at: i64,
        updated_at: i64,
    },
    WhiteboardElementUpdated {
        id: String,
        board_id: String,
        element_type: String,
        x: f64, y: f64, width: f64, height: f64,
        z_index: i32,
        style_json: Option<String>,
        content_json: Option<String>,
        updated_at: i64,
    },
    WhiteboardElementDeleted { id: String, board_id: String },
    WhiteboardCleared { board_id: String },
    NotebookCellAdded {
        id: String,
        board_id: String,
        cell_type: String,
        cell_order: i32,
        content: Option<String>,
    },
    NotebookCellUpdated {
        id: String,
        board_id: String,
        cell_type: String,
        cell_order: i32,
        content: Option<String>,
        output: Option<String>,
        collapsed: bool,
        height: Option<f64>,
        metadata_json: Option<String>,
    },
    NotebookCellDeleted { id: String, board_id: String },
    NotebookCellsReordered { board_id: String, cell_ids: Vec<String> },
    BoardModeChanged { board_id: String, mode: String },
    BoardMetadataUpdated {
        board_id: String,
        labels: Vec<String>,
        rating: i32,
        contains_model: Option<String>,
        contains_skills: Vec<String>,
    },
    BoardLabelsUpdated { board_id: String, labels: Vec<String> },
    BoardRated { board_id: String, rating: i32 },
    ProfileUpdated { node_id: String, display_name: String, avatar_hash: Option<String> },
}

// ============================================================================
// RawEvent for cyan-lens (extended with cyan-native sources)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawEvent {
    pub id: String,
    pub group_id: String,
    pub workspace_id: String,
    pub source: String,        // "cyan", "cyan_chat", "cyan_file", etc.
    pub content_kind: String,  // "cyan_group", "cyan_chat", "cyan_file", etc.
    pub external_id: String,
    pub content: String,
    pub author_id: String,
    pub author_name: String,
    pub url: String,
    pub title: Option<String>,
    pub thread_id: Option<String>,
    pub parent_id: Option<String>,
    pub ts: u64,
    pub captured_at: u64,
}

// ============================================================================
// Scope Tracker - Track workspace<->group and board<->workspace mappings
// ============================================================================

struct ScopeTracker {
    workspace_to_group: HashMap<String, String>,
    board_to_workspace: HashMap<String, String>,
}

impl ScopeTracker {
    fn new() -> Self {
        Self {
            workspace_to_group: HashMap::new(),
            board_to_workspace: HashMap::new(),
        }
    }

    fn track_workspace(&mut self, workspace_id: &str, group_id: &str) {
        self.workspace_to_group.insert(workspace_id.to_string(), group_id.to_string());
    }

    fn track_board(&mut self, board_id: &str, workspace_id: &str) {
        self.board_to_workspace.insert(board_id.to_string(), workspace_id.to_string());
    }

    fn get_group_for_workspace(&self, workspace_id: &str) -> Option<&String> {
        self.workspace_to_group.get(workspace_id)
    }

    fn get_workspace_for_board(&self, board_id: &str) -> Option<&String> {
        self.board_to_workspace.get(board_id)
    }

    fn get_scope_for_board(&self, board_id: &str) -> (String, String) {
        let workspace_id = self.board_to_workspace.get(board_id)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let group_id = self.workspace_to_group.get(&workspace_id)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        (group_id, workspace_id)
    }
}

// ============================================================================
// NetworkEvent -> RawEvent Converter
// ============================================================================

fn current_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn gen_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn convert_network_event(
    event: &NetworkEvent,
    group_id_from_topic: &str,
    tracker: &mut ScopeTracker,
) -> Option<RawEvent> {
    let now = current_ts();

    match event {
        NetworkEvent::GroupCreated(g) => Some(RawEvent {
            id: gen_id(),
            group_id: g.id.clone(),
            workspace_id: String::new(),
            source: "cyan".to_string(),
            content_kind: "cyan_group".to_string(),
            external_id: g.id.clone(),
            content: serde_json::json!({
                "action": "created",
                "name": g.name,
                "icon": g.icon,
                "color": g.color,
            }).to_string(),
            author_id: String::new(),
            author_name: String::new(),
            url: String::new(),
            title: Some(format!("Group created: {}", g.name)),
            thread_id: None,
            parent_id: None,
            ts: g.created_at as u64,
            captured_at: now,
        }),

        NetworkEvent::GroupRenamed { id, name } => Some(RawEvent {
            id: gen_id(),
            group_id: id.clone(),
            workspace_id: String::new(),
            source: "cyan".to_string(),
            content_kind: "cyan_group".to_string(),
            external_id: id.clone(),
            content: serde_json::json!({
                "action": "renamed",
                "name": name,
            }).to_string(),
            author_id: String::new(),
            author_name: String::new(),
            url: String::new(),
            title: Some(format!("Group renamed: {}", name)),
            thread_id: None,
            parent_id: None,
            ts: now,
            captured_at: now,
        }),

        NetworkEvent::WorkspaceCreated(ws) => {
            tracker.track_workspace(&ws.id, &ws.group_id);
            Some(RawEvent {
                id: gen_id(),
                group_id: ws.group_id.clone(),
                workspace_id: ws.id.clone(),
                source: "cyan".to_string(),
                content_kind: "cyan_workspace".to_string(),
                external_id: ws.id.clone(),
                content: serde_json::json!({
                    "action": "created",
                    "name": ws.name,
                }).to_string(),
                author_id: String::new(),
                author_name: String::new(),
                url: String::new(),
                title: Some(format!("Workspace created: {}", ws.name)),
                thread_id: None,
                parent_id: None,
                ts: ws.created_at as u64,
                captured_at: now,
            })
        }

        NetworkEvent::BoardCreated { id, workspace_id, name, created_at } => {
            tracker.track_board(id, workspace_id);
            let group_id = tracker.get_group_for_workspace(workspace_id)
                .cloned()
                .unwrap_or_else(|| group_id_from_topic.to_string());
            
            Some(RawEvent {
                id: gen_id(),
                group_id,
                workspace_id: workspace_id.clone(),
                source: "cyan_board".to_string(),
                content_kind: "cyan_board".to_string(),
                external_id: id.clone(),
                content: serde_json::json!({
                    "action": "created",
                    "name": name,
                    "board_id": id,
                }).to_string(),
                author_id: String::new(),
                author_name: String::new(),
                url: String::new(),
                title: Some(format!("Board created: {}", name)),
                thread_id: None,
                parent_id: None,
                ts: *created_at as u64,
                captured_at: now,
            })
        }

        NetworkEvent::ChatSent { id, workspace_id, message, author, parent_id, timestamp } => {
            let group_id = tracker.get_group_for_workspace(workspace_id)
                .cloned()
                .unwrap_or_else(|| group_id_from_topic.to_string());
            
            Some(RawEvent {
                id: gen_id(),
                group_id,
                workspace_id: workspace_id.clone(),
                source: "cyan_chat".to_string(),
                content_kind: "cyan_chat".to_string(),
                external_id: id.clone(),
                content: message.clone(),
                author_id: author.clone(),
                author_name: author.clone(),
                url: String::new(),
                title: None,
                thread_id: None,
                parent_id: parent_id.clone(),
                ts: *timestamp as u64,
                captured_at: now,
            })
        }

        NetworkEvent::FileAvailable { id, group_id, workspace_id, board_id, name, hash, size, source_peer, created_at } => {
            let gid = group_id.clone().unwrap_or_else(|| group_id_from_topic.to_string());
            let wid = workspace_id.clone().unwrap_or_default();
            
            Some(RawEvent {
                id: gen_id(),
                group_id: gid,
                workspace_id: wid,
                source: "cyan_file".to_string(),
                content_kind: "cyan_file".to_string(),
                external_id: id.clone(),
                content: serde_json::json!({
                    "filename": name,
                    "hash": hash,
                    "size": size,
                    "board_id": board_id,
                    "source_peer": source_peer,
                }).to_string(),
                author_id: source_peer.clone(),
                author_name: source_peer.clone(),
                url: String::new(),
                title: Some(name.clone()),
                thread_id: None,
                parent_id: None,
                ts: *created_at as u64,
                captured_at: now,
            })
        }

        NetworkEvent::WhiteboardElementAdded { id, board_id, element_type, content_json, created_at, .. } => {
            let (group_id, workspace_id) = tracker.get_scope_for_board(board_id);
            
            Some(RawEvent {
                id: gen_id(),
                group_id,
                workspace_id,
                source: "cyan_whiteboard".to_string(),
                content_kind: "cyan_whiteboard_element".to_string(),
                external_id: id.clone(),
                content: serde_json::json!({
                    "action": "added",
                    "board_id": board_id,
                    "element_type": element_type,
                    "content": content_json,
                }).to_string(),
                author_id: String::new(),
                author_name: String::new(),
                url: String::new(),
                title: None,
                thread_id: Some(board_id.clone()),
                parent_id: None,
                ts: *created_at as u64,
                captured_at: now,
            })
        }

        NetworkEvent::NotebookCellAdded { id, board_id, cell_type, content, .. } |
        NetworkEvent::NotebookCellUpdated { id, board_id, cell_type, content, .. } => {
            let (group_id, workspace_id) = tracker.get_scope_for_board(board_id);
            let text_content = content.clone().unwrap_or_default();
            
            // Only forward cells with meaningful content
            if text_content.trim().is_empty() {
                return None;
            }
            
            Some(RawEvent {
                id: gen_id(),
                group_id,
                workspace_id,
                source: "cyan_notebook".to_string(),
                content_kind: "cyan_notebook_cell".to_string(),
                external_id: id.clone(),
                content: text_content,
                author_id: String::new(),
                author_name: String::new(),
                url: String::new(),
                title: None,
                thread_id: Some(board_id.clone()),
                parent_id: None,
                ts: now,
                captured_at: now,
            })
        }

        // Events we don't need to forward to lens (deletes, metadata updates, etc.)
        NetworkEvent::GroupDeleted { .. } |
        NetworkEvent::GroupDissolved { .. } |
        NetworkEvent::WorkspaceDeleted { .. } |
        NetworkEvent::WorkspaceDissolved { .. } |
        NetworkEvent::WorkspaceRenamed { .. } |
        NetworkEvent::BoardDeleted { .. } |
        NetworkEvent::BoardDissolved { .. } |
        NetworkEvent::BoardRenamed { .. } |
        NetworkEvent::ChatDeleted { .. } |
        NetworkEvent::WhiteboardElementUpdated { .. } |
        NetworkEvent::WhiteboardElementDeleted { .. } |
        NetworkEvent::WhiteboardCleared { .. } |
        NetworkEvent::NotebookCellDeleted { .. } |
        NetworkEvent::NotebookCellsReordered { .. } |
        NetworkEvent::BoardModeChanged { .. } |
        NetworkEvent::BoardMetadataUpdated { .. } |
        NetworkEvent::BoardLabelsUpdated { .. } |
        NetworkEvent::BoardRated { .. } |
        NetworkEvent::ProfileUpdated { .. } |
        NetworkEvent::GroupSnapshotAvailable { .. } => None,
    }
}

// ============================================================================
// Iggy Connection with Reconnect Logic
// ============================================================================

struct IggyConnection {
    client: Option<IggyClient>,
    addr: String,
    enabled: bool,
    last_error: Option<String>,
    messages_sent: AtomicU64,
}

impl IggyConnection {
    fn new(addr: String, enabled: bool) -> Self {
        Self {
            client: None,
            addr,
            enabled,
            last_error: None,
            messages_sent: AtomicU64::new(0),
        }
    }

    async fn connect(&mut self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }

        tracing::info!("Connecting to Iggy at {}...", self.addr);

        let client = IggyClient::builder()
            .with_tcp()
            .with_server_address(self.addr.clone())
            .build()
            .map_err(|e| format!("Iggy build error: {}", e))?;

        client.connect().await
            .map_err(|e| format!("Iggy connect error: {}", e))?;

        let stream_id = Identifier::named(STREAM_NAME)
            .map_err(|e| format!("Invalid stream identifier: {}", e))?;

        match client.create_stream(STREAM_NAME, None).await {
            Ok(_) => tracing::info!("Created Iggy stream: {}", STREAM_NAME),
            Err(e) => tracing::debug!("Stream may already exist: {}", e),
        }

        let topic_id = Identifier::named(TOPIC_NAME)
            .map_err(|e| format!("Invalid topic identifier: {}", e))?;

        match client.create_topic(
            &stream_id,
            TOPIC_NAME,
            PARTITIONS,
            CompressionAlgorithm::None,
            None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::Unlimited,
        ).await {
            Ok(_) => tracing::info!("Created Iggy topic: {}/{}", STREAM_NAME, TOPIC_NAME),
            Err(e) => tracing::debug!("Topic may already exist: {}", e),
        }

        match client.create_consumer_group(&stream_id, &topic_id, "enricher-workers", None).await {
            Ok(_) => tracing::info!("Created Iggy consumer group: enricher-workers"),
            Err(e) => tracing::debug!("Consumer group may already exist: {}", e),
        }

        self.client = Some(client);
        self.last_error = None;
        tracing::info!("✅ Connected to Iggy at {}", self.addr);
        Ok(())
    }

    async fn send_raw_event(&mut self, raw_event: &RawEvent) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }

        let client = match &self.client {
            Some(c) => c,
            None => {
                self.last_error = Some("Not connected".to_string());
                return Err("Not connected to Iggy".to_string());
            }
        };

        let payload = serde_json::to_vec(raw_event)
            .map_err(|e| format!("Serialization error: {}", e))?;

        let stream_id = Identifier::named(STREAM_NAME)
            .map_err(|e| format!("Invalid stream: {}", e))?;
        let topic_id = Identifier::named(TOPIC_NAME)
            .map_err(|e| format!("Invalid topic: {}", e))?;

        let partitioning = Partitioning::balanced();
        let mut messages = vec![Message::new(None, payload.into(), None)];

        match client.send_messages(&stream_id, &topic_id, &partitioning, &mut messages).await {
            Ok(_) => {
                self.messages_sent.fetch_add(1, Ordering::Relaxed);
                self.last_error = None;
                Ok(())
            }
            Err(e) => {
                let err = format!("Iggy send error: {}", e);
                self.last_error = Some(err.clone());
                self.client = None;
                Err(err)
            }
        }
    }

    fn is_connected(&self) -> bool {
        self.client.is_some()
    }

    fn messages_sent(&self) -> u64 {
        self.messages_sent.load(Ordering::Relaxed)
    }
}

// ============================================================================
// Rendezvous self-publish (SUPER_PEER_COMPLETION_SPEC §5)
// ============================================================================

/// Sign and publish this node's rendezvous config to the file sink at `path`.
///
/// Behavior-preserving: callers treat a failure as non-fatal — the bootstrap keeps
/// serving discovery/gossip/snapshots even if the config can't be written.
fn publish_rendezvous(
    xf: &XaeroFlux,
    env_label: &str,
    relay_url: Option<String>,
    path: &str,
) -> anyhow::Result<()> {
    let signed = xf.signed_rendezvous_config(env_label, relay_url, current_ts())?;
    let sink = FileSink::new(path);
    publish_signed(&sink, &signed)
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("xaeroflux=info".parse()?)
                .add_directive("xaeroflux_bootstrap=info".parse()?)
                .add_directive("iroh=warn".parse()?)
                .add_directive("iroh_gossip=info".parse()?)
                .add_directive("iggy=warn".parse()?),
        )
        .init();

    let relay_url = env::var("RELAY_URL").ok();
    let discovery_key = env::var("DISCOVERY_KEY").unwrap_or_else(|_| "cyan-dev".to_string());
    let db_path = env::var("DB_PATH").unwrap_or_else(|_| "/opt/cyan/data/bootstrap.db".to_string());
    let no_n0 = env::var("NO_N0").map(|v| v == "1").unwrap_or(false);
    let iggy_addr = env::var("IGGY_ADDR").unwrap_or_else(|_| "127.0.0.1:8090".to_string());
    let iggy_enabled = env::var("IGGY_ENABLED").map(|v| v != "0").unwrap_or(true);
    // Rendezvous self-publish (SUPER_PEER_COMPLETION_SPEC §5): on start, write a signed config
    // advertising this node so apps discover it instead of hardcoding its node_id. The deploy
    // uploads/serves RENDEZVOUS_PATH at the well-known URL. Defaults to <db parent>/rendezvous.json.
    let rendezvous_env = env::var("XAEROFLUX_ENV").unwrap_or_else(|_| "dev".to_string());
    let rendezvous_path = env::var("RENDEZVOUS_PATH").unwrap_or_else(|_| {
        std::path::Path::new(&db_path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("rendezvous.json")
            .to_string_lossy()
            .to_string()
    });

    println!();
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  🚀 XaeroFlux Bootstrap Server + Iggy Forwarder (v2 - RawEvent)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Discovery Key:  {}", discovery_key);
    println!("  DB Path:        {}", db_path);
    println!("  Relay URL:      {}", relay_url.as_deref().unwrap_or("(default iroh relays)"));
    println!("  N0 Discovery:   {}", if no_n0 { "disabled" } else { "enabled" });
    println!("  ───────────────────────────────────────────────────────────────────");
    println!("  Iggy Enabled:   {}", if iggy_enabled { "yes" } else { "no" });
    println!("  Iggy Address:   {}", iggy_addr);
    println!("  Iggy Topic:     {}/{}", STREAM_NAME, TOPIC_NAME);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();

    if let Some(parent) = std::path::Path::new(&db_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut builder = XaeroFlux::builder()
        .discovery_key(&discovery_key)
        .db_path(&db_path);

    if let Some(ref url) = relay_url {
        builder = builder.relay_url(url.clone());
    }

    if no_n0 {
        builder = builder.no_n0_discovery();
    }

    let xf = builder.build().await?;

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  ✅ BOOTSTRAP NODE RUNNING");
    println!("  NODE ID: {}", xf.node_id);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();

    // Self-publish the signed rendezvous config (additive; publish-on-start only). Re-running on
    // every (re)start means a fresh node.key / redeploy is reflected automatically — no per-deploy
    // retune. A failure here must NOT take the bootstrap down; log and keep serving discovery.
    match publish_rendezvous(&xf, &rendezvous_env, relay_url.clone(), &rendezvous_path) {
        Ok(()) => {
            println!("📡 Published signed rendezvous config → {}", rendezvous_path);
            println!("   env={} discovery_key={} node_id={}", rendezvous_env, discovery_key, xf.node_id);
            println!();
        }
        Err(e) => {
            tracing::warn!("rendezvous publish failed (continuing): {}", e);
            println!("⚠️  Rendezvous publish failed (continuing): {}", e);
            println!();
        }
    }

    let iggy = Arc::new(RwLock::new(IggyConnection::new(iggy_addr.clone(), iggy_enabled)));
    let tracker = Arc::new(RwLock::new(ScopeTracker::new()));

    if iggy_enabled {
        let mut iggy_guard = iggy.write().await;
        match iggy_guard.connect().await {
            Ok(_) => println!("✅ Connected to Iggy at {}", iggy_addr),
            Err(e) => println!("⚠️  Iggy connection failed: {} (will retry)", e),
        }
    }

    let events_received = Arc::new(AtomicU64::new(0));
    let events_converted = Arc::new(AtomicU64::new(0));
    let events_forwarded = Arc::new(AtomicU64::new(0));

    let mut event_rx = xf.event_rx;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(60));
    let mut iggy_retry = tokio::time::interval(Duration::from_secs(30));
    let mut stats_interval = tokio::time::interval(Duration::from_secs(300));

    loop {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                events_received.fetch_add(1, Ordering::Relaxed);

                // Extract group_id from event source (format: "group/{group_id}")
                let group_id_from_topic = event.source
                    .strip_prefix("group/")
                    .unwrap_or(&event.source)
                    .to_string();

                // Try to parse as NetworkEvent
                match serde_json::from_str::<NetworkEvent>(&event.payload) {
                    Ok(net_event) => {
                        let debug_str = format!("{:?}", net_event);
                        let event_type = debug_str.split('(').next().unwrap_or("Unknown").trim();

                        println!("📨 {} from group/{}", event_type, &group_id_from_topic[..16.min(group_id_from_topic.len())]);

                        // Convert to RawEvent
                        let mut tracker_guard = tracker.write().await;
                        if let Some(raw_event) = convert_network_event(&net_event, &group_id_from_topic, &mut tracker_guard) {
                            events_converted.fetch_add(1, Ordering::Relaxed);
                            drop(tracker_guard);

                            // Forward to Iggy
                            let mut iggy_guard = iggy.write().await;
                            match iggy_guard.send_raw_event(&raw_event).await {
                                Ok(_) => {
                                    events_forwarded.fetch_add(1, Ordering::Relaxed);
                                    println!("   └─ 📤 → Iggy ({}: {})", raw_event.content_kind, &raw_event.external_id[..16.min(raw_event.external_id.len())]);
                                }
                                Err(e) => {
                                    println!("   └─ ❌ Iggy error: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // Not a NetworkEvent - might be discovery/control message
                        tracing::debug!("Non-NetworkEvent payload: {} ({})", &event.payload[..50.min(event.payload.len())], e);
                    }
                }
            }

            _ = heartbeat.tick() => {
                let iggy_guard = iggy.read().await;
                let iggy_status = if !iggy_guard.enabled {
                    "disabled".to_string()
                } else if iggy_guard.is_connected() {
                    format!("connected ({} sent)", iggy_guard.messages_sent())
                } else {
                    format!("disconnected: {}", iggy_guard.last_error.as_deref().unwrap_or("unknown"))
                };

                let recv = events_received.load(Ordering::Relaxed);
                let conv = events_converted.load(Ordering::Relaxed);
                let fwd = events_forwarded.load(Ordering::Relaxed);

                println!(
                    "💓 Heartbeat | recv={} conv={} fwd={} | Iggy: {}",
                    recv, conv, fwd, iggy_status
                );
            }

            _ = iggy_retry.tick() => {
                let mut iggy_guard = iggy.write().await;
                if iggy_guard.enabled && !iggy_guard.is_connected() {
                    match iggy_guard.connect().await {
                        Ok(_) => println!("✅ Reconnected to Iggy"),
                        Err(_) => {}
                    }
                }
            }

            _ = stats_interval.tick() => {
                let recv = events_received.load(Ordering::Relaxed);
                let conv = events_converted.load(Ordering::Relaxed);
                let fwd = events_forwarded.load(Ordering::Relaxed);
                let tracker_guard = tracker.read().await;

                println!();
                println!("📊 Stats:");
                println!("   Events: {} received, {} converted, {} forwarded", recv, conv, fwd);
                println!("   Tracked: {} workspaces, {} boards", 
                    tracker_guard.workspace_to_group.len(),
                    tracker_guard.board_to_workspace.len());
                println!();
            }
        }
    }
}
