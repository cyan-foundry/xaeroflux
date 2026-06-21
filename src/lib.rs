// XaeroFlux - Simple Event Sync Engine with Peer Introduction
// Protocol: xsp-1.0 (XaeroFlux Sync Protocol v1.0)
//
// Features:
// - Event sync via gossip (events topic)
// - Peer discovery and introduction (discovery topic)
// - SQLite persistence for events AND peer tracking
// - peer_introduction broadcast for mesh formation
//
// Use as bootstrap server:
//   let xf = XaeroFlux::builder()
//       .discovery_key("cyan-dev")
//       .db_path("/opt/cyan/data/bootstrap.db")
//       .relay_url("https://quic.dev.cyan.blockxaero.io")
//       .build().await?;

use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use iroh::{
    discovery::{
        dns::DnsDiscovery, mdns::MdnsDiscovery, pkarr::PkarrPublisher,
        static_provider::StaticProvider,
    },
    protocol::Router,
    Endpoint, PublicKey, RelayMap, RelayMode, RelayUrl, SecretKey,
};
use iroh_gossip::{
    api::{Event as GossipEvent, GossipReceiver, GossipSender},
    proto::TopicId,
    Gossip,
};
use rand_chacha::rand_core::SeedableRng;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex, RwLock};

// ---------- Core Event Type ----------
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: String,      // Unique event ID (blake3 hash)
    pub payload: String, // Application-specific data
    pub source: String,  // Node ID that created this event
    pub ts: u64,         // Unix timestamp
}

// ---------- Configuration ----------
#[derive(Clone)]
pub struct XaeroFluxConfig {
    pub discovery_key: String,
    pub db_path: String,
    pub relay_url: Option<String>,
    pub bootstrap_peers: Vec<String>,
    pub use_n0_discovery: bool,
    pub use_mdns: bool,
    /// Test/offline seam: when true, bind with `RelayMode::Disabled` (no relay contact at all).
    /// Default `false` preserves production behavior (n0 / custom relay).
    pub relay_disabled: bool,
    /// Test/offline seam: optional `StaticProvider` for manual, out-of-band address resolution
    /// (loopback meshes with no n0 DNS, no mDNS multicast, no relay). Default `None`.
    pub static_provider: Option<StaticProvider>,
}

impl Default for XaeroFluxConfig {
    fn default() -> Self {
        Self {
            discovery_key: "xaeroflux".to_string(),
            db_path: "xaeroflux.db".to_string(),
            relay_url: None,
            bootstrap_peers: vec![],
            use_n0_discovery: true,
            use_mdns: true,
            relay_disabled: false,
            static_provider: None,
        }
    }
}

// ---------- Builder Pattern ----------
pub struct XaeroFluxBuilder {
    config: XaeroFluxConfig,
}

impl XaeroFluxBuilder {
    pub fn new() -> Self {
        Self {
            config: XaeroFluxConfig::default(),
        }
    }

    pub fn discovery_key(mut self, key: impl Into<String>) -> Self {
        self.config.discovery_key = key.into();
        self
    }

    pub fn db_path(mut self, path: impl Into<String>) -> Self {
        self.config.db_path = path.into();
        self
    }

    pub fn relay_url(mut self, url: impl Into<String>) -> Self {
        self.config.relay_url = Some(url.into());
        self
    }

    pub fn bootstrap_peers(mut self, peers: Vec<String>) -> Self {
        self.config.bootstrap_peers = peers;
        self
    }

    pub fn bootstrap_peer(mut self, peer: impl Into<String>) -> Self {
        self.config.bootstrap_peers.push(peer.into());
        self
    }

    pub fn no_n0_discovery(mut self) -> Self {
        self.config.use_n0_discovery = false;
        self
    }

    pub fn no_mdns(mut self) -> Self {
        self.config.use_mdns = false;
        self
    }

    /// Test/offline seam: bind with `RelayMode::Disabled` so the node never contacts any relay.
    /// Additive and opt-in; the bootstrap binary does not call this (keeps `RelayMode::Default`).
    pub fn disable_relay(mut self) -> Self {
        self.config.relay_disabled = true;
        self
    }

    /// Test/offline seam: install a shared `StaticProvider` as an extra discovery service so peers
    /// can resolve each other's loopback addresses out-of-band. Additive and opt-in; behavior is
    /// unchanged unless a provider is supplied.
    pub fn static_provider(mut self, provider: StaticProvider) -> Self {
        self.config.static_provider = Some(provider);
        self
    }

    pub async fn build(self) -> Result<XaeroFlux> {
        XaeroFlux::from_config(self.config).await
    }
}

impl Default for XaeroFluxBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------- Public API ----------
pub struct XaeroFlux {
    pub event_tx: mpsc::UnboundedSender<Event>,
    pub event_rx: mpsc::UnboundedReceiver<Event>,
    pub discovery_key: String,
    pub node_id: String,
    /// The bound iroh endpoint (clone of the one driven by the `NetworkActor`). Exposed so test
    /// harnesses can read this node's `EndpointAddr` and feed it to a shared `StaticProvider` for
    /// offline loopback addressing. Additive; the bootstrap binary ignores it.
    pub endpoint: Endpoint,
    /// This node's secret key, retained so it can sign a self-published rendezvous config
    /// (see [`XaeroFlux::signed_rendezvous_config`]). Private — never exposed to callers; the
    /// only thing it can do from outside is sign the rendezvous config the node already advertises.
    secret_key: SecretKey,
}

impl XaeroFlux {
    pub fn builder() -> XaeroFluxBuilder {
        XaeroFluxBuilder::new()
    }

    pub async fn new(discovery_key: String, db_path: String) -> Result<Self> {
        Self::builder()
            .discovery_key(discovery_key)
            .db_path(db_path)
            .build()
            .await
    }

    pub async fn new_with_bootstrap(
        discovery_key: String,
        db_path: String,
        bootstrap_peers: Vec<String>,
    ) -> Result<Self> {
        Self::builder()
            .discovery_key(discovery_key)
            .db_path(db_path)
            .bootstrap_peers(bootstrap_peers)
            .build()
            .await
    }

    async fn from_config(config: XaeroFluxConfig) -> Result<Self> {
        // Load or generate persistent node identity
        let key_path = std::path::Path::new(&config.db_path)
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .join("node.key");

        let secret_key = if key_path.exists() {
            let bytes = std::fs::read(&key_path)?;
            let bytes: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid key file"))?;
            tracing::info!("🔑 Loaded existing secret key from {:?}", key_path);
            SecretKey::from_bytes(&bytes)
        } else {
            let mut seed = [0u8; 32];
            getrandom::fill(&mut seed)
                .map_err(|e| anyhow::anyhow!("Failed to get random seed: {}", e))?;
            let mut rng = rand_chacha::ChaCha8Rng::from_seed(seed);
            let key = SecretKey::generate(&mut rng);

            if let Some(parent) = key_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&key_path, key.to_bytes())?;
            tracing::info!("🆕 Generated new secret key at {:?}", key_path);
            key
        };

        let node_id = secret_key.public().to_string();

        // Open database
        let db = Connection::open(&config.db_path)?;
        ensure_schema(&db)?;
        let db = Arc::new(Mutex::new(db));

        // Create channels
        let (app_event_tx, app_event_rx) = mpsc::unbounded_channel::<Event>();
        let (network_event_tx, network_event_rx) = mpsc::unbounded_channel::<Event>();
        let (sync_event_tx, sync_event_rx) = mpsc::unbounded_channel::<Event>();

        // Start storage actor
        let storage_actor = StorageActor::new(db.clone(), app_event_rx, network_event_tx);
        tokio::spawn(storage_actor.run());

        // Start network actor. Clone the secret key first so the node can later sign its own
        // self-published rendezvous config without exposing the key to the network actor's owner.
        let signing_key = secret_key.clone();
        let network_actor = NetworkActor::new(
            secret_key,
            config.clone(),
            db.clone(),
            node_id.clone(),
            network_event_rx,
            sync_event_tx,
        )
            .await?;
        let endpoint = network_actor.endpoint.clone();
        tokio::spawn(network_actor.run());

        Ok(Self {
            event_tx: app_event_tx,
            event_rx: sync_event_rx,
            discovery_key: config.discovery_key,
            node_id,
            endpoint,
            secret_key: signing_key,
        })
    }

    /// Build a **signed rendezvous config** advertising this node as a bootstrap peer
    /// (SUPER_PEER_COMPLETION_SPEC §5). Apps fetch it, verify the signature against the
    /// embedded `signer` (== this node's `node_id`), and pin the `node_id` — so they
    /// discover the bootstrap dynamically instead of hardcoding it.
    ///
    /// Pulls `node_id` + `discovery_key` from this node, its dialable direct addresses
    /// from the bound endpoint, and signs with this node's own key. `relay_url` is the
    /// configured relay (falls back to a relay observed on the endpoint address); `ts`
    /// is the publish timestamp (caller-provided so this stays pure/testable).
    pub fn signed_rendezvous_config(
        &self,
        env: &str,
        relay_url: Option<String>,
        ts: u64,
    ) -> Result<rendezvous::SignedRendezvousConfig> {
        let addr = self.endpoint.addr();
        let direct: Vec<String> = addr.ip_addrs().map(|a| a.to_string()).collect();
        let relay = relay_url.or_else(|| addr.relay_urls().next().map(|u| u.to_string()));

        let config = rendezvous::RendezvousConfig {
            env: env.to_string(),
            discovery_key: self.discovery_key.clone(),
            bootstrap: rendezvous::BootstrapInfo {
                node_id: self.node_id.clone(),
                addr: direct,
            },
            relay_url: relay,
            ts,
        };
        rendezvous::sign_config(config, &self.secret_key)
    }
}

// ---------- Database Schema ----------
fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA journal_mode=WAL;

        -- Events table (for event sync to Iggy later)
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            source TEXT NOT NULL,
            ts INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
        CREATE INDEX IF NOT EXISTS idx_events_source ON events(source);

        -- Peer tracking table (for peer introduction)
        CREATE TABLE IF NOT EXISTS group_peers (
            group_id TEXT NOT NULL,
            peer_id TEXT NOT NULL,
            first_seen INTEGER NOT NULL,
            last_seen INTEGER NOT NULL,
            is_online INTEGER DEFAULT 1,
            PRIMARY KEY (group_id, peer_id)
        );
        CREATE INDEX IF NOT EXISTS idx_group_peers_group ON group_peers(group_id);
        CREATE INDEX IF NOT EXISTS idx_group_peers_online ON group_peers(is_online);
        "#,
    )?;
    Ok(())
}

// ---------- Storage Actor ----------
struct StorageActor {
    db: Arc<Mutex<Connection>>,
    app_rx: mpsc::UnboundedReceiver<Event>,
    network_tx: mpsc::UnboundedSender<Event>,
}

impl StorageActor {
    fn new(
        db: Arc<Mutex<Connection>>,
        app_rx: mpsc::UnboundedReceiver<Event>,
        network_tx: mpsc::UnboundedSender<Event>,
    ) -> Self {
        Self {
            db,
            app_rx,
            network_tx,
        }
    }

    async fn run(mut self) {
        tracing::info!("StorageActor started");

        while let Some(event) = self.app_rx.recv().await {
            tracing::debug!("Storing event: {}", event.id);

            let db = self.db.lock().await;
            match db.execute(
                "INSERT OR IGNORE INTO events (id, payload, source, ts) VALUES (?1, ?2, ?3, ?4)",
                params![event.id, event.payload, event.source, event.ts],
            ) {
                Ok(rows) => {
                    if rows > 0 {
                        tracing::info!("Event {} stored", event.id);
                        drop(db);

                        if let Err(e) = self.network_tx.send(event) {
                            tracing::error!("Failed to send event to network: {}", e);
                        }
                    } else {
                        tracing::debug!("Event {} already exists (duplicate)", event.id);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to store event {}: {}", event.id, e);
                }
            }
        }

        tracing::warn!("StorageActor stopped");
    }
}

// ---------- Peer Tracker ----------
struct PeerTracker {
    db: Arc<Mutex<Connection>>,
    cache: RwLock<HashMap<String, Vec<String>>>,
}

impl PeerTracker {
    fn new(db: Arc<Mutex<Connection>>) -> Self {
        Self {
            db,
            cache: RwLock::new(HashMap::new()),
        }
    }

    async fn load_cache(&self) -> Result<()> {
        // Collect from DB first (don't hold stmt across await)
        let rows: Vec<(String, String)> = {
            let db = self.db.lock().await;
            let mut stmt = db.prepare("SELECT group_id, peer_id FROM group_peers WHERE is_online = 1")?;
            let mapped = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            mapped.collect::<Result<Vec<_>, _>>()?
        };

        // Now update cache
        let mut cache = self.cache.write().await;
        cache.clear();

        for (group_id, peer_id) in rows {
            cache
                .entry(group_id)
                .or_insert_with(Vec::new)
                .push(peer_id);
        }

        Ok(())
    }

    async fn upsert_peer(&self, group_id: &str, peer_id: &str) -> Result<bool> {
        let now = chrono::Utc::now().timestamp();
        let is_new: bool;

        {
            let db = self.db.lock().await;

            let exists: bool = db
                .query_row(
                    "SELECT 1 FROM group_peers WHERE group_id = ?1 AND peer_id = ?2",
                    params![group_id, peer_id],
                    |_| Ok(true),
                )
                .unwrap_or(false);

            if exists {
                db.execute(
                    "UPDATE group_peers SET last_seen = ?1, is_online = 1
                     WHERE group_id = ?2 AND peer_id = ?3",
                    params![now, group_id, peer_id],
                )?;
                is_new = false;
            } else {
                db.execute(
                    "INSERT INTO group_peers (group_id, peer_id, first_seen, last_seen, is_online)
                     VALUES (?1, ?2, ?3, ?3, 1)",
                    params![group_id, peer_id, now],
                )?;
                is_new = true;
            }
        }

        // Update cache
        {
            let mut cache = self.cache.write().await;
            let peers = cache.entry(group_id.to_string()).or_insert_with(Vec::new);
            if !peers.contains(&peer_id.to_string()) {
                peers.push(peer_id.to_string());
            }
        }

        Ok(is_new)
    }

    async fn mark_offline(&self, peer_id: &str) -> Result<Vec<String>> {
        let now = chrono::Utc::now().timestamp();
        let affected_groups: Vec<String>;

        {
            let db = self.db.lock().await;

            // Collect affected groups first
            let mut stmt =
                db.prepare("SELECT group_id FROM group_peers WHERE peer_id = ?1 AND is_online = 1")?;
            let rows = stmt.query_map(params![peer_id], |row| row.get::<_, String>(0))?;
            affected_groups = rows.collect::<Result<Vec<_>, _>>()?;

            db.execute(
                "UPDATE group_peers SET is_online = 0, last_seen = ?1 WHERE peer_id = ?2",
                params![now, peer_id],
            )?;
        }

        // Update cache (after releasing db lock)
        {
            let mut cache = self.cache.write().await;
            for peers in cache.values_mut() {
                peers.retain(|p| p != peer_id);
            }
        }

        Ok(affected_groups)
    }

    async fn get_peers(&self, group_id: &str) -> Vec<String> {
        let cache = self.cache.read().await;
        cache.get(group_id).cloned().unwrap_or_default()
    }

    async fn get_active_groups(&self) -> Vec<String> {
        let cache = self.cache.read().await;
        cache
            .iter()
            .filter(|(_, peers)| !peers.is_empty())
            .map(|(k, _)| k.clone())
            .collect()
    }

    async fn prune_stale(&self, max_age: Duration) -> Result<usize> {
        let cutoff = chrono::Utc::now().timestamp() - max_age.as_secs() as i64;

        let count = {
            let db = self.db.lock().await;
            db.execute(
                "UPDATE group_peers SET is_online = 0 WHERE last_seen < ?1 AND is_online = 1",
                params![cutoff],
            )?
        };

        self.load_cache().await?;
        Ok(count)
    }

    async fn stats(&self) -> (usize, usize) {
        let cache = self.cache.read().await;
        let groups = cache.len();
        let peers: usize = cache.values().map(|v| v.len()).sum();
        (groups, peers)
    }
}

// ---------- Network Actor ----------
struct NetworkActor {
    node_id: String,
    db: Arc<Mutex<Connection>>,
    #[allow(dead_code)]
    endpoint: Endpoint,
    gossip: Arc<Gossip>,
    #[allow(dead_code)]
    router: Router,
    gossip_sender: GossipSender,
    gossip_receiver: GossipReceiver,
    outbound_rx: mpsc::UnboundedReceiver<Event>,
    inbound_tx: mpsc::UnboundedSender<Event>,
    peer_tracker: Arc<PeerTracker>,
    discovery_key: String,
}

impl NetworkActor {
    async fn new(
        secret_key: SecretKey,
        config: XaeroFluxConfig,
        db: Arc<Mutex<Connection>>,
        node_id: String,
        outbound_rx: mpsc::UnboundedReceiver<Event>,
        inbound_tx: mpsc::UnboundedSender<Event>,
    ) -> Result<Self> {
        // Configure relay mode
        let relay_mode = if config.relay_disabled {
            tracing::info!("🚫 Relay disabled (offline mode)");
            RelayMode::Disabled
        } else if let Some(ref url_str) = config.relay_url {
            match RelayUrl::from_str(url_str) {
                Ok(url) => {
                    tracing::info!("🌐 Using custom relay: {}", url);
                    RelayMode::Custom(RelayMap::from(url))
                }
                Err(e) => {
                    tracing::warn!("⚠️ Invalid relay URL '{}': {}, using default", url_str, e);
                    RelayMode::Default
                }
            }
        } else {
            tracing::info!("🌐 Using default Iroh relays");
            RelayMode::Default
        };

        // Build endpoint
        let mut builder = Endpoint::builder()
            .secret_key(secret_key)
            .alpns(vec![iroh_gossip::ALPN.to_vec(), b"xsp-1.0".to_vec()])
            .relay_mode(relay_mode);

        if config.use_n0_discovery {
            builder = builder
                .discovery(PkarrPublisher::n0_dns())
                .discovery(DnsDiscovery::n0_dns());
        }

        if config.use_mdns {
            builder = builder.discovery(MdnsDiscovery::builder());
        }

        // Test/offline seam: extra static discovery for out-of-band loopback addressing.
        if let Some(ref provider) = config.static_provider {
            builder = builder.discovery(provider.clone());
        }

        let endpoint = builder.bind().await?;
        let endpoint_id = endpoint.id();
        tracing::info!("Node ID: {}", endpoint_id);

        // Setup gossip
        let gossip = Arc::new(Gossip::builder().spawn(endpoint.clone()));

        let router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();

        // Create topic IDs
        let discovery_topic_id = TopicId::from_bytes(
            blake3::hash(format!("cyan/discovery/{}", config.discovery_key).as_bytes()).as_bytes()
                [..32]
                .try_into()?,
        );

        let events_topic_id = TopicId::from_bytes(
            blake3::hash(format!("cyan/events/{}", config.discovery_key).as_bytes()).as_bytes()
                [..32]
                .try_into()?,
        );

        // Parse bootstrap peers
        let bootstrap_ids: Vec<PublicKey> = config
            .bootstrap_peers
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();

        if !bootstrap_ids.is_empty() {
            tracing::info!("📡 Bootstrapping with {} peers", bootstrap_ids.len());
        }

        // Join events topic
        let mut events_topic = gossip
            .subscribe(events_topic_id, bootstrap_ids.clone())
            .await?;

        // Join discovery topic
        let mut discovery_topic = gossip
            .subscribe(discovery_topic_id, bootstrap_ids)
            .await?;

        tokio::time::sleep(Duration::from_millis(500)).await;
        tokio::time::timeout(Duration::from_secs(2), events_topic.joined())
            .await
            .ok();

        tracing::info!(
            "Subscribed to topics for discovery key: {}",
            config.discovery_key
        );

        let (gossip_sender, gossip_receiver) = events_topic.split();

        // Initialize peer tracker
        let peer_tracker = Arc::new(PeerTracker::new(db.clone()));
        peer_tracker.load_cache().await?;

        let (groups, peers) = peer_tracker.stats().await;
        tracing::info!("📊 Loaded {} groups with {} online peers", groups, peers);

        // Spawn discovery listener task
        let peer_tracker_clone = peer_tracker.clone();
        let discovery_key_clone = config.discovery_key.clone();
        let endpoint_id_clone = endpoint_id;
        let gossip_clone = gossip.clone();  // For dynamic group topic subscription
        let inbound_tx_clone = inbound_tx.clone();  // For forwarding group events

        tokio::spawn(async move {
            tracing::info!("Peer discovery task started");

            let mut announce_interval = tokio::time::interval(Duration::from_secs(30));
            let mut prune_interval = tokio::time::interval(Duration::from_secs(300));

            // Track which group topics we've subscribed to (for relaying)
            // Store the senders so we can add peers via join_peers
            let mut group_topic_senders: std::collections::HashMap<String, iroh_gossip::api::GossipSender> =
                std::collections::HashMap::new();

            loop {
                tokio::select! {
                    _ = announce_interval.tick() => {
                        // Announce presence
                        let announce = endpoint_id_clone.to_string();
                        if let Err(e) = discovery_topic.broadcast(Bytes::from(announce)).await {
                            tracing::warn!("Failed to announce presence: {}", e);
                        }

                        // Re-broadcast peer introductions for all active groups
                        let groups = peer_tracker_clone.get_active_groups().await;
                        for group_id in groups {
                            let peers = peer_tracker_clone.get_peers(&group_id).await;
                            if peers.len() > 1 {
                                let intro_msg = serde_json::json!({
                                    "msg_type": "peer_introduction",
                                    "group_id": group_id,
                                    "peers": peers,
                                });

                                let _ = discovery_topic.broadcast(
                                    Bytes::from(intro_msg.to_string())
                                ).await;

                                tracing::debug!(
                                    "🔄 Re-broadcast peer_introduction for {} ({} peers)",
                                    &group_id[..16.min(group_id.len())],
                                    peers.len()
                                );
                            }
                        }
                    }

                    _ = prune_interval.tick() => {
                        match peer_tracker_clone.prune_stale(Duration::from_secs(300)).await {
                            Ok(count) if count > 0 => {
                                tracing::info!("🗑️ Pruned {} stale peers", count);
                            }
                            Err(e) => tracing::warn!("Failed to prune: {}", e),
                            _ => {}
                        }
                    }

                    Some(event_result) = discovery_topic.next() => {
                        match event_result {
                            Ok(GossipEvent::Received(msg)) => {
                                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&msg.content) {
                                    if let Some(msg_type) = json.get("msg_type").and_then(|v| v.as_str()) {
                                        if msg_type == "groups_exchange" {
                                            // Use node_id from JSON payload (not delivered_from which may be relay)
                                            let from_node = match json.get("node_id").and_then(|v| v.as_str()) {
                                                Some(id) => id,
                                                None => {
                                                    tracing::warn!("groups_exchange missing node_id field!");
                                                    continue;
                                                }
                                            };

                                            tracing::info!(
                                                "📩 groups_exchange from {} (full: {})",
                                                &from_node[..16.min(from_node.len())],
                                                from_node
                                            );

                                            let groups = match json.get("local_groups").and_then(|v| v.as_array()) {
                                                Some(g) => g,
                                                None => {
                                                    tracing::warn!("groups_exchange missing local_groups!");
                                                    continue;
                                                }
                                            };

                                            tracing::info!(
                                                "📋 Processing {} groups for peer {}",
                                                groups.len(),
                                                &from_node[..16.min(from_node.len())]
                                            );

                                            let mut introductions: Vec<(String, Vec<String>)> = Vec::new();

                                            for group in groups {
                                                if let Some(gid) = group.as_str() {
                                                    let peer_pk = from_node.parse::<iroh::PublicKey>().ok();

                                                    if let Some(sender) = group_topic_senders.get(gid) {
                                                        // Already subscribed - add new peer via join_peers
                                                        if let Some(pk) = peer_pk {
                                                            if let Err(e) = sender.join_peers(vec![pk]).await {
                                                                tracing::debug!(
                                                                    "join_peers for group {} peer {}: {}",
                                                                    &gid[..16.min(gid.len())],
                                                                    &from_node[..16.min(from_node.len())],
                                                                    e
                                                                );
                                                            } else {
                                                                tracing::info!(
                                                                    "📡 Added peer {} to group topic {}",
                                                                    &from_node[..16.min(from_node.len())],
                                                                    &gid[..16.min(gid.len())]
                                                                );
                                                            }
                                                        }
                                                    } else {
                                                        // New group - subscribe with announcing peer
                                                        let group_topic_id = TopicId::from_bytes(
                                                            blake3::hash(format!("cyan/group/{}", gid).as_bytes()).as_bytes()
                                                                [..32]
                                                                .try_into()
                                                                .unwrap_or([0u8; 32]),
                                                        );

                                                        let peers = peer_pk.map(|pk| vec![pk]).unwrap_or_default();

                                                        match gossip_clone.subscribe(group_topic_id, peers).await {
                                                            Ok(topic) => {
                                                                let (sender, mut receiver) = topic.split();
                                                                group_topic_senders.insert(gid.to_string(), sender);

                                                                // Forward group events to main event channel
                                                                let gid_clone = gid.to_string();
                                                                let inbound_tx_for_group = inbound_tx_clone.clone();
                                                                tokio::spawn(async move {
                                                                    while let Some(event) = receiver.next().await {
                                                                        match event {
                                                                            Ok(iroh_gossip::api::Event::Received(msg)) => {
                                                                                // Create Event from group message
                                                                                let content = msg.content.to_vec();
                                                                                let ts = std::time::SystemTime::now()
                                                                                    .duration_since(std::time::UNIX_EPOCH)
                                                                                    .map(|d| d.as_secs())
                                                                                    .unwrap_or(0);
                                                                                
                                                                                let evt = Event {
                                                                                    id: blake3::hash(&content).to_hex().to_string(),
                                                                                    payload: String::from_utf8_lossy(&content).to_string(),
                                                                                    source: format!("group/{}", gid_clone),
                                                                                    ts,
                                                                                };
                                                                                
                                                                                tracing::info!(
                                                                                    "📨 [GROUP {}] Forwarding event: {}",
                                                                                    &gid_clone[..16.min(gid_clone.len())],
                                                                                    &evt.id[..16]
                                                                                );
                                                                                
                                                                                if let Err(e) = inbound_tx_for_group.send(evt) {
                                                                                    tracing::error!(
                                                                                        "Failed to forward group event: {}",
                                                                                        e
                                                                                    );
                                                                                }
                                                                            }
                                                                            Ok(iroh_gossip::api::Event::NeighborUp(peer)) => {
                                                                                tracing::info!(
                                                                                    "🟢 [GROUP {}] Neighbor up: {}",
                                                                                    &gid_clone[..16.min(gid_clone.len())],
                                                                                    &peer.to_string()[..16]
                                                                                );
                                                                            }
                                                                            Ok(iroh_gossip::api::Event::NeighborDown(peer)) => {
                                                                                tracing::info!(
                                                                                    "🔴 [GROUP {}] Neighbor down: {}",
                                                                                    &gid_clone[..16.min(gid_clone.len())],
                                                                                    &peer.to_string()[..16]
                                                                                );
                                                                            }
                                                                            Ok(iroh_gossip::api::Event::Lagged) => {
                                                                                tracing::warn!(
                                                                                    "⚠️ [GROUP {}] Lagged",
                                                                                    &gid_clone[..16.min(gid_clone.len())]
                                                                                );
                                                                            }
                                                                            Err(e) => {
                                                                                tracing::error!(
                                                                                    "🔴 [GROUP {}] Receiver error: {}",
                                                                                    &gid_clone[..16.min(gid_clone.len())],
                                                                                    e
                                                                                );
                                                                            }
                                                                        }
                                                                    }
                                                                });

                                                                tracing::info!(
                                                                    "📡 Subscribed to group topic: {}... (with peer {})",
                                                                    &gid[..16.min(gid.len())],
                                                                    &from_node[..16.min(from_node.len())]
                                                                );
                                                            }
                                                            Err(e) => {
                                                                tracing::warn!(
                                                                    "Failed to subscribe to group topic {}: {}",
                                                                    &gid[..16.min(gid.len())],
                                                                    e
                                                                );
                                                            }
                                                        }
                                                    }

                                                    // Use from_node (the actual peer) not delivered_from
                                                    match peer_tracker_clone.upsert_peer(gid, from_node).await {
                                                        Ok(is_new) => {
                                                            let peers = peer_tracker_clone.get_peers(gid).await;

                                                            if is_new {
                                                                tracing::info!(
                                                                    "📝 New peer {} for group {} ({} total)",
                                                                    &from_node[..16.min(from_node.len())],
                                                                    &gid[..16.min(gid.len())],
                                                                    peers.len()
                                                                );
                                                            }

                                                            if peers.len() > 1 {
                                                                introductions.push((gid.to_string(), peers));
                                                            }
                                                        }
                                                        Err(e) => tracing::warn!("Failed to track peer: {}", e),
                                                    }
                                                }
                                            }

                                            // Broadcast peer introductions
                                            for (group_id, peers) in introductions {
                                                let intro_msg = serde_json::json!({
                                                    "msg_type": "peer_introduction",
                                                    "group_id": group_id,
                                                    "peers": peers,
                                                });

                                                tracing::info!(
                                                    "📢 Broadcasting peer_introduction for {} ({} peers)",
                                                    &group_id[..16.min(group_id.len())],
                                                    peers.len()
                                                );

                                                let _ = discovery_topic.broadcast(
                                                    Bytes::from(intro_msg.to_string())
                                                ).await;
                                            }
                                        }
                                    }
                                }
                            }

                            Ok(GossipEvent::NeighborUp(peer)) => {
                                tracing::info!("🟢 Discovery neighbor up: {}", &peer.to_string()[..16]);
                            }

                            Ok(GossipEvent::NeighborDown(peer)) => {
                                let peer_str = peer.to_string();
                                tracing::info!("🔴 Discovery neighbor down: {}", &peer_str[..16]);

                                match peer_tracker_clone.mark_offline(&peer_str).await {
                                    Ok(affected_groups) => {
                                        for group_id in affected_groups {
                                            let peers = peer_tracker_clone.get_peers(&group_id).await;
                                            if peers.len() > 1 {
                                                let intro_msg = serde_json::json!({
                                                    "msg_type": "peer_introduction",
                                                    "group_id": group_id,
                                                    "peers": peers,
                                                });

                                                tracing::info!(
                                                    "📢 Re-broadcast peer_introduction for {} (peer left)",
                                                    &group_id[..16.min(group_id.len())]
                                                );

                                                let _ = discovery_topic.broadcast(
                                                    Bytes::from(intro_msg.to_string())
                                                ).await;
                                            }
                                        }
                                    }
                                    Err(e) => tracing::warn!("Failed to mark offline: {}", e),
                                }
                            }

                            _ => {}
                        }
                    }

                    else => break,
                }
            }

            tracing::info!("Peer discovery task for '{}' terminated", discovery_key_clone);
        });

        Ok(Self {
            node_id,
            db,
            endpoint,
            gossip,
            router,
            gossip_sender,
            gossip_receiver,
            outbound_rx,
            inbound_tx,
            peer_tracker,
            discovery_key: config.discovery_key,
        })
    }

    async fn run(mut self) {
        tracing::info!("NetworkActor started");

        // Heartbeat task
        let peer_tracker = self.peer_tracker.clone();
        let node_id = self.node_id.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let (groups, peers) = peer_tracker.stats().await;
                println!(
                    "💓 Heartbeat - Node ID: {} | {} groups, {} peers",
                    &node_id[..16],
                    groups,
                    peers
                );
            }
        });

        loop {
            tokio::select! {
                Some(event_result) = self.gossip_receiver.next() => {
                    match event_result {
                        Ok(GossipEvent::Received(msg)) => {
                            match serde_json::from_slice::<Event>(&msg.content) {
                                Ok(event) => {
                                    if event.source != self.node_id {
                                        tracing::info!("Received event from network: {}", event.id);

                                        let db = self.db.lock().await;
                                        match db.execute(
                                            "INSERT OR IGNORE INTO events (id, payload, source, ts) VALUES (?1, ?2, ?3, ?4)",
                                            params![event.id, event.payload, event.source, event.ts],
                                        ) {
                                            Ok(rows) => {
                                                if rows > 0 {
                                                    tracing::info!("Synced event {} from {}", event.id, event.source);
                                                    drop(db);

                                                    if let Err(e) = self.inbound_tx.send(event) {
                                                        tracing::error!("Failed to forward synced event: {}", e);
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                tracing::error!("Failed to store synced event: {}", e);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::debug!("Non-event gossip message: {}", e);
                                }
                            }
                        }
                        Ok(GossipEvent::NeighborUp(peer)) => {
                            tracing::info!("Events neighbor up: {}", peer);
                        }
                        Ok(GossipEvent::NeighborDown(peer)) => {
                            tracing::info!("Events neighbor down: {}", peer);
                        }
                        Ok(GossipEvent::Lagged) => {
                            tracing::warn!("Gossip receiver lagged");
                        }
                        Err(e) => {
                            tracing::warn!("Error in gossip receiver: {}", e);
                        }
                    }
                }

                Some(event) = self.outbound_rx.recv() => {
                    match serde_json::to_vec(&event) {
                        Ok(bytes) => {
                            if let Err(e) = self.gossip_sender.broadcast(Bytes::from(bytes)).await {
                                tracing::error!("Failed to broadcast event {}: {}", event.id, e);
                            } else {
                                tracing::info!("Broadcasted event {} to gossip network", event.id);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to serialize event {}: {}", event.id, e);
                        }
                    }
                }

                else => {
                    tracing::warn!("NetworkActor channel closed, exiting");
                    break;
                }
            }
        }

        tracing::warn!("NetworkActor stopped");
    }
}

// ---------- Helper ----------
pub fn generate_event_id(payload: &str, source: &str, ts: u64) -> String {
    let input = format!("{}{}{}", payload, source, ts);
    blake3::hash(input.as_bytes()).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basic_integration_sanity() {
        let xf = XaeroFlux::new("test".to_string(), ":memory:".to_string())
            .await
            .expect("failed to create XaeroFlux");
        assert!(!xf.node_id.is_empty());
    }

    #[tokio::test]
    async fn builder_with_custom_relay() {
        let xf = XaeroFlux::builder()
            .discovery_key("test")
            .db_path(":memory:")
            .relay_url("https://quic.dev.cyan.blockxaero.io")
            .no_n0_discovery()
            .build()
            .await
            .expect("failed to create XaeroFlux with custom relay");
        assert!(!xf.node_id.is_empty());
    }
}pub mod rendezvous;
pub mod snapshot;
pub mod swarm;
