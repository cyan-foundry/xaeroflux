// xaeroflux/src/swarm.rs
//
// Blob swarming for XaeroFlux (X8) — content-addressed, multi-source file distribution.
//
// Today the substrate moves data two ways: deltas via gossip and a full GroupSnapshot via direct
// QUIC (see `snapshot.rs`). Neither distributes *files*: plugin bundles (`.cyanplugin`) and large
// media need a content-addressed, multi-source swarm so a group can pull a blob from whichever peers
// hold it, in parallel, surviving holder churn. This module wires `iroh-blobs` 0.97 to provide that.
//
// Design (mirrors the standalone-primitive shape of `SnapshotProvider`/`SnapshotRequester`):
// - `BlobSwarm` owns an in-memory `iroh-blobs` store and mounts the blobs protocol on the blobs
//   ALPN over a caller-supplied iroh endpoint (its own `Router`). Any node can both serve and fetch.
// - Content addressing is Blake3: `iroh-blobs` identifies a blob by its Blake3 hash, so the hash *is*
//   the identity. `add` returns that hash; `fetch` verifies it on completion before surfacing bytes.
// - i-have / who-has negotiation is transport-agnostic: `BlobSwarm` produces and consumes
//   `SwarmMessage`s and maintains a holder registry, but does not own a gossip topic. Callers ride
//   the *existing* gossip channel (the engine's discovery/group topics) to carry these messages —
//   exactly as the snapshot protocol rides gossip for its `RequestSnapshot`/`SnapshotAvailable`.
// - Multi-source fetch + resume use `iroh-blobs`' `Downloader`, which tries holders in turn,
//   resuming from already-received chunks when a holder drops mid-transfer.
//
// This is additive and behavior-preserving for the `xaeroflux_bootstrap` binary: the binary does not
// construct a `BlobSwarm`, and nothing here changes the `NetworkActor`'s endpoint, gossip, or Router.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use iroh::{protocol::Router, Endpoint, PublicKey};
use iroh_blobs::store::mem::MemStore;
use iroh_blobs::BlobsProtocol;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// ALPN the blob swarm speaks. Re-exported so tests and downstream crates can bind endpoints for
/// blob transfer without taking a direct `iroh-blobs` dependency.
pub const BLOB_ALPN: &[u8] = iroh_blobs::ALPN;

/// The content-address type: a blob's Blake3 hash *is* its identity. Re-exported so tests and
/// downstream crates can name it without a direct `iroh-blobs` dependency.
pub use iroh_blobs::Hash;

/// Per-holder dial cap during a multi-source fetch. A departed holder's address lingers in discovery,
/// so an unbounded `connect` would retry it until QUIC's own long timeout; this bounds each attempt
/// so the fetch falls through to a live holder promptly.
const DIAL_TIMEOUT: Duration = Duration::from_secs(5);

// ============================================================================
// i-have / who-has negotiation messages (carried over the existing gossip channel)
// ============================================================================

/// A swarm control message exchanged over gossip to negotiate who holds a content-addressed blob.
/// Hashes are encoded as their Blake3 hex string so the message is plain JSON, like the snapshot
/// protocol's gossip messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SwarmMessage {
    /// "I have this blob" — `holder_node_id` holds the blob with this hash and will serve it.
    IHave { hash: String, holder_node_id: String },

    /// "Who has this blob?" — `requester_node_id` is looking for holders of this hash.
    WhoHas { hash: String, requester_node_id: String },
}

// ============================================================================
// BlobSwarm
// ============================================================================

/// A blob-swarm participant: a content-addressed store mounted on the blobs ALPN, plus the
/// negotiation core (holder registry + message handling) and a multi-source fetcher.
pub struct BlobSwarm {
    store: MemStore,
    endpoint: Endpoint,
    node_id: String,
    /// Keeps the blobs protocol accepting on the endpoint for this swarm's lifetime.
    _router: Router,
    /// hash (hex) -> set of node_ids that announced they hold it.
    holders: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl BlobSwarm {
    /// Mount a blob swarm on `endpoint` (which must advertise [`BLOB_ALPN`]). Builds an in-memory
    /// store and a `Router` accepting the blobs protocol — symmetric, so this node can both serve
    /// held blobs and fetch missing ones.
    pub fn new(endpoint: Endpoint, node_id: String) -> Self {
        let store = MemStore::new();
        let blobs = BlobsProtocol::new(&store, None);
        let router = Router::builder(endpoint.clone())
            .accept(BLOB_ALPN, blobs)
            .spawn();

        Self {
            store,
            endpoint,
            node_id,
            _router: router,
            holders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// This node's id (the holder/requester id carried in [`SwarmMessage`]s and dialed on fetch).
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    // ---- content addressing ------------------------------------------------

    /// Add bytes to the local store and return their Blake3 hash (the content's identity).
    pub async fn add(&self, data: impl Into<Bytes>) -> Result<Hash> {
        let tag = self
            .store
            .add_bytes(data.into())
            .await
            .map_err(|e| anyhow!("blob add failed: {e}"))?;
        Ok(tag.hash)
    }

    /// Whether the local store holds the blob for `hash`.
    pub async fn has(&self, hash: &Hash) -> Result<bool> {
        self.store
            .has(*hash)
            .await
            .map_err(|e| anyhow!("blob has() failed: {e}"))
    }

    /// Read a locally-held blob's full contents.
    pub async fn get(&self, hash: &Hash) -> Result<Bytes> {
        self.store
            .get_bytes(*hash)
            .await
            .map_err(|e| anyhow!("blob get_bytes failed: {e}"))
    }

    // ---- i-have / who-has negotiation --------------------------------------

    /// Build an `IHave` announcement for a blob this node holds.
    pub fn announce(&self, hash: &Hash) -> SwarmMessage {
        SwarmMessage::IHave {
            hash: hash.to_string(),
            holder_node_id: self.node_id.clone(),
        }
    }

    /// Build a `WhoHas` query for a blob this node wants.
    pub fn query(&self, hash: &Hash) -> SwarmMessage {
        SwarmMessage::WhoHas {
            hash: hash.to_string(),
            requester_node_id: self.node_id.clone(),
        }
    }

    /// Record that `holder` holds the blob identified by `hash_hex` (a holder this node observed via
    /// an `IHave`). Self-announcements are ignored so a node never lists itself as a remote holder.
    pub async fn record_holder(&self, hash_hex: &str, holder: &str) {
        if holder == self.node_id {
            return;
        }
        let mut holders = self.holders.write().await;
        holders
            .entry(hash_hex.to_string())
            .or_default()
            .insert(holder.to_string());
    }

    /// The remote holders this node currently knows for `hash` (its own observed state).
    pub async fn holders(&self, hash: &Hash) -> Vec<String> {
        let holders = self.holders.read().await;
        holders
            .get(&hash.to_string())
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Process one incoming negotiation message (received over gossip):
    /// - `IHave`  → record the holder; nothing to send back.
    /// - `WhoHas` → if this node holds the blob, return an `IHave` reply for the caller to broadcast.
    ///
    /// Pure negotiation logic over a serialized message; the caller owns the gossip transport.
    pub async fn on_message(&self, raw: &[u8]) -> Result<Option<SwarmMessage>> {
        let msg: SwarmMessage = serde_json::from_slice(raw)
            .map_err(|e| anyhow!("malformed swarm message: {e}"))?;
        match msg {
            SwarmMessage::IHave { hash, holder_node_id } => {
                self.record_holder(&hash, &holder_node_id).await;
                Ok(None)
            }
            SwarmMessage::WhoHas { hash, .. } => {
                let parsed = Hash::from_str(&hash)
                    .map_err(|e| anyhow!("WhoHas carried an unparseable hash: {e}"))?;
                if self.has(&parsed).await? {
                    Ok(Some(self.announce(&parsed)))
                } else {
                    Ok(None)
                }
            }
        }
    }

    // ---- multi-source fetch ------------------------------------------------

    /// Fetch the blob for `hash` from the given holders, trying them in turn and resuming across
    /// holder churn, then verify its Blake3 hash before returning the bytes.
    ///
    /// `iroh-blobs` does Blake3-verified streaming and writes verified chunks to the store as they
    /// arrive, tracking which ranges are present. So a fetch against a holder only pulls the *missing*
    /// ranges: if one holder drops mid-transfer (or is already gone when we dial it), we fall through
    /// to the next holder and it resumes from where the previous left off. A single holder leaving
    /// therefore never fails the download as long as some holder in the set can serve the rest.
    ///
    /// On completion we recompute the Blake3 hash of the assembled bytes and reject any mismatch
    /// (defence-in-depth on top of verified streaming) before surfacing the blob.
    pub async fn fetch(&self, hash: &Hash, holders: &[String]) -> Result<Bytes> {
        if holders.is_empty() {
            return Err(anyhow!("cannot fetch {hash}: no holders provided"));
        }

        let providers: Vec<PublicKey> = holders
            .iter()
            .map(|id| {
                id.parse::<PublicKey>()
                    .map_err(|e| anyhow!("holder id '{id}' is not a valid node id: {e}"))
            })
            .collect::<Result<_>>()?;

        let remote = self.store.remote();
        let mut last_err: Option<anyhow::Error> = None;
        for provider in &providers {
            // Resume short-circuit: a previous holder may already have delivered the whole blob.
            if self.has(hash).await? {
                break;
            }
            // Bounded dial: a holder that has left the swarm is no longer reachable, and a raw
            // `connect` would retry its stale address until QUIC's own (long) timeout. Cap each dial
            // so churn falls through to the next holder quickly instead of stalling the fetch.
            let conn = match tokio::time::timeout(
                DIAL_TIMEOUT,
                self.endpoint.connect(*provider, BLOB_ALPN),
            )
            .await
            {
                Ok(Ok(conn)) => conn,
                Ok(Err(e)) => {
                    last_err = Some(anyhow!("dial holder {provider} failed: {e}"));
                    continue;
                }
                Err(_) => {
                    last_err = Some(anyhow!("dial holder {provider} timed out (likely departed)"));
                    continue;
                }
            };
            // `fetch` pulls only the ranges still missing from the local store, so this resumes any
            // partial transfer left behind by an earlier holder that dropped.
            if let Err(e) = remote.fetch(conn, *hash).await {
                last_err = Some(anyhow!("fetch from holder {provider} failed: {e}"));
            }
        }

        if !self.has(hash).await? {
            return Err(last_err
                .unwrap_or_else(|| anyhow!("no holder in the set could serve {hash}")));
        }

        // Integrity gate: surface the blob only if the assembled content's Blake3 hash matches.
        let bytes = self.get(hash).await?;
        let computed = Hash::new(&bytes);
        if &computed != hash {
            return Err(anyhow!(
                "integrity check failed: fetched content hashes to {computed}, expected {hash}"
            ));
        }
        Ok(bytes)
    }
}
