// Substrate test harness — spin local xaeroflux nodes in one process, fully offline.
//
// Discipline (see XAEROFLUX_TEST_SPEC.md / CLAUDE.md):
// - Offline only: every node uses `no_n0_discovery()` + `no_mdns()` + `disable_relay()` and a
//   shared in-process `StaticProvider` for out-of-band loopback addressing. No public relay, no
//   n0 DNS, no mDNS multicast — a test that needs the internet is a bug.
// - Per-node identity + storage: each node gets its own unique temp dir, so each gets its own
//   `node.key` (identity is persisted next to `db_path`) and its own SQLite DB.
// - Bounded waits only: `wait_for_event` is always a `tokio::time::timeout` with a clear failure.
//
// The `StaticProvider` + `disable_relay()` + exposed `endpoint` are the minimal, additive,
// behavior-preserving engine seam (documented in STATUS_OVERNIGHT.md). Production code paths
// (the `xaeroflux_bootstrap` binary) do not use them.

#![allow(dead_code)] // harness helpers are used across multiple test files; not all in every file

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use iroh::discovery::static_provider::StaticProvider;
use iroh::protocol::Router;
use iroh::{Endpoint, PublicKey};
use iroh_gossip::api::{GossipReceiver, GossipSender};
use iroh_gossip::proto::TopicId;
use iroh_gossip::Gossip;
use tokio::sync::mpsc::UnboundedReceiver;
use xaeroflux::{generate_event_id, Event, XaeroFlux};

/// Bounded wait budget for convergence assertions.
pub const T: Duration = Duration::from_secs(15);

/// Monotonic counter to make node temp dirs and keys unique within a process.
fn next_seq() -> u64 {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

/// A discovery key unique to this call — isolates concurrent meshes from each other.
pub fn unique_key() -> String {
    format!("xf-test-key-{}", next_seq())
}

/// Process-wide registry of one shared `StaticProvider` per discovery key. All nodes in a mesh
/// share the same discovery key (and thus the same provider), so each node can resolve every other
/// node in its own mesh — and nodes in a different mesh (different key) use a different provider, so
/// concurrent meshes do not cross-wire.
fn provider_for(key: &str) -> StaticProvider {
    static REG: OnceLock<Mutex<HashMap<String, StaticProvider>>> = OnceLock::new();
    let reg = REG.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = reg.lock().expect("static provider registry poisoned");
    map.entry(key.to_string()).or_default().clone()
}

/// Resolve this node's dialable `EndpointAddr` (id + direct loopback/LAN addresses), waiting briefly
/// for the local-interface addresses to populate after bind. Bounded; never blocks forever.
async fn dialable_addr(endpoint: &Endpoint) -> Result<iroh::EndpointAddr> {
    for _ in 0..100 {
        let addr = endpoint.addr();
        if !addr.is_empty() {
            return Ok(addr);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(anyhow!(
        "endpoint never reported a direct address (offline bind failed?)"
    ))
}

/// Spawn one local node, fully offline, wired into its mesh's shared `StaticProvider`.
///
/// - `name`     — label used for the node's temp dir (debugging only).
/// - `key`      — discovery key; nodes sharing a key form one mesh.
/// - `bootstrap`— node_ids this node should dial on startup (e.g. the bootstrap peer).
pub async fn spawn_local_node(name: &str, key: &str, bootstrap: &[String]) -> XaeroFlux {
    let provider = provider_for(key);

    // Unique temp dir → unique persisted identity (node.key) + unique DB per node.
    let dir: PathBuf = std::env::temp_dir().join(format!(
        "xaeroflux-test-{}-{}-{}",
        sanitize(key),
        sanitize(name),
        next_seq()
    ));
    std::fs::create_dir_all(&dir).expect("create node temp dir");
    let db_path = dir.join("node.db");

    let xf = XaeroFlux::builder()
        .discovery_key(key)
        .db_path(db_path.to_string_lossy().to_string())
        .no_n0_discovery()
        .no_mdns()
        .disable_relay()
        .static_provider(provider.clone())
        .bootstrap_peers(bootstrap.to_vec())
        .build()
        .await
        .expect("build local node");

    // Publish our own address into the shared provider so mesh peers can dial us out-of-band.
    let addr = dialable_addr(&xf.endpoint)
        .await
        .expect("resolve node dialable address");
    provider.add_endpoint_info(addr);

    xf
}

/// The shared `StaticProvider` for a mesh/key — for tests that build their own raw peers and need
/// to register addresses into the same out-of-band addressing namespace as the `XaeroFlux` nodes.
pub fn mesh_provider(key: &str) -> StaticProvider {
    provider_for(key)
}

/// Build a raw, offline iroh `Endpoint` joined to the mesh's shared `StaticProvider`, advertising
/// the given ALPNs, and register its dialable address. For tests that must act as a raw QUIC /
/// gossip peer alongside `XaeroFlux` nodes (snapshot transfer, discovery-topic injection).
pub async fn offline_endpoint(key: &str, alpns: Vec<Vec<u8>>) -> Endpoint {
    let provider = provider_for(key);
    let endpoint = Endpoint::builder()
        .alpns(alpns)
        .relay_mode(iroh::RelayMode::Disabled)
        .discovery(provider.clone())
        .bind()
        .await
        .expect("bind offline raw endpoint");
    let addr = dialable_addr(&endpoint)
        .await
        .expect("resolve raw endpoint dialable address");
    provider.add_endpoint_info(addr);
    endpoint
}

/// Derive a gossip `TopicId` exactly as the engine does: `blake3(label)[..32]`.
pub fn topic_id(label: &str) -> TopicId {
    let hash = blake3::hash(label.as_bytes());
    let bytes: [u8; 32] = hash.as_bytes()[..32]
        .try_into()
        .expect("blake3 digest is 32 bytes");
    TopicId::from_bytes(bytes)
}

/// The engine's discovery topic for a discovery key (`cyan/discovery/{key}`).
pub fn discovery_topic(key: &str) -> TopicId {
    topic_id(&format!("cyan/discovery/{key}"))
}

/// The engine's per-group topic (`cyan/group/{gid}`).
pub fn group_topic(gid: &str) -> TopicId {
    topic_id(&format!("cyan/group/{gid}"))
}

/// Parse a node_id string into an iroh `PublicKey` (the engine's `EndpointId`).
pub fn pubkey(node_id: &str) -> PublicKey {
    node_id.parse().expect("node_id should parse as a PublicKey")
}

/// A raw, offline iroh-gossip peer — used to inject discovery-topic control messages
/// (`groups_exchange`) and group-topic events that the public `XaeroFlux` API cannot send, and to
/// observe broadcasts (`peer_introduction`) that the engine surfaces only on the discovery topic.
/// Mirrors the engine's own gossip wiring (iroh_gossip::ALPN + Router + Gossip).
pub struct RawGossip {
    pub endpoint: Endpoint,
    gossip: std::sync::Arc<Gossip>,
    _router: Router,
}

impl RawGossip {
    /// Spawn a raw gossip peer wired into the mesh's shared `StaticProvider`, fully offline.
    pub async fn spawn(key: &str) -> Self {
        let provider = provider_for(key);
        let endpoint = Endpoint::builder()
            .alpns(vec![iroh_gossip::ALPN.to_vec()])
            .relay_mode(iroh::RelayMode::Disabled)
            .discovery(provider.clone())
            .bind()
            .await
            .expect("bind raw gossip endpoint");
        let addr = dialable_addr(&endpoint)
            .await
            .expect("resolve raw gossip dialable address");
        provider.add_endpoint_info(addr);

        let gossip = std::sync::Arc::new(Gossip::builder().spawn(endpoint.clone()));
        let router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();

        Self {
            endpoint,
            gossip,
            _router: router,
        }
    }

    pub fn node_id(&self) -> String {
        self.endpoint.id().to_string()
    }

    /// Subscribe to a gossip topic, bootstrapping from `peers`. Returns the split sender/receiver.
    pub async fn subscribe(
        &self,
        topic: TopicId,
        peers: Vec<PublicKey>,
    ) -> (GossipSender, GossipReceiver) {
        let topic = self
            .gossip
            .subscribe(topic, peers)
            .await
            .expect("subscribe to gossip topic");
        topic.split()
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

/// Build an application `Event` authored by `source_node_id` with the given payload.
/// `source` must be the publisher's own `node_id` so receivers don't filter it as self-authored.
pub fn make_event(source_node_id: &str, payload: &str) -> Event {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    Event {
        id: generate_event_id(payload, source_node_id, ts),
        payload: payload.to_string(),
        source: source_node_id.to_string(),
        ts,
    }
}

/// Wait (bounded) for an event on `rx` matching `pred`. Returns the matching event, or an error on
/// timeout / channel close. Never an unbounded `recv()`.
pub async fn wait_for_event<P>(
    rx: &mut UnboundedReceiver<Event>,
    pred: P,
    t: Duration,
) -> Result<Event>
where
    P: Fn(&Event) -> bool,
{
    let fut = async {
        loop {
            match rx.recv().await {
                Some(ev) if pred(&ev) => return Ok(ev),
                Some(_) => continue,
                None => return Err(anyhow!("event channel closed before a matching event arrived")),
            }
        }
    };
    tokio::time::timeout(t, fut)
        .await
        .map_err(|_| anyhow!("timed out after {:?} waiting for a matching event", t))?
}

/// Drive the mesh to a connected state: repeatedly broadcast a fresh probe event from `publisher`
/// until every receiver in `receivers` has observed at least one probe (bounded by `t`).
///
/// This is the honest "mesh formed" oracle: `XaeroFlux`'s `PeerTracker` is private and is only
/// populated by `groups_exchange` discovery messages, which the engine receives but never emits —
/// so gossip connectivity is not observable via any public field. Successful event propagation
/// from `publisher` to all `receivers` proves the gossip mesh formed.
pub async fn establish_mesh(
    publisher: &XaeroFlux,
    receivers: &mut [&mut UnboundedReceiver<Event>],
    t: Duration,
) -> Result<()> {
    let prefix = format!("probe-{}", next_seq());
    let mut done = vec![false; receivers.len()];

    let driven = tokio::time::timeout(t, async {
        let mut tick = tokio::time::interval(Duration::from_millis(300));
        let mut n = 0u64;
        loop {
            tick.tick().await;
            n += 1;
            let probe = make_event(&publisher.node_id, &format!("{prefix}-{n}"));
            let _ = publisher.event_tx.send(probe);

            for (i, rx) in receivers.iter_mut().enumerate() {
                if done[i] {
                    continue;
                }
                while let Ok(ev) = rx.try_recv() {
                    if ev.payload.starts_with(&prefix) {
                        done[i] = true;
                        break;
                    }
                }
            }
            if done.iter().all(|d| *d) {
                return;
            }
        }
    })
    .await;

    if driven.is_err() {
        return Err(anyhow!(
            "mesh did not form within {:?}: receiver connectivity = {:?}",
            t,
            done
        ));
    }
    Ok(())
}

/// Count how many events matching `pred` arrive within `t` (drains until timeout). Used to assert
/// de-duplication (exactly-once delivery to `event_rx`).
pub async fn count_events<P>(rx: &mut UnboundedReceiver<Event>, pred: P, t: Duration) -> usize
where
    P: Fn(&Event) -> bool,
{
    let mut n = 0usize;
    let _ = tokio::time::timeout(t, async {
        while let Some(ev) = rx.recv().await {
            if pred(&ev) {
                n += 1;
            }
        }
    })
    .await;
    n
}
