// X8 — blob swarming (iroh-blobs content-addressed, multi-source distribution), fully offline.
//
// See XAEROFLUX_TEST_SPEC.md. `iroh-blobs` 0.97 is now wired as the `BlobSwarm` primitive
// (`xaeroflux::swarm`): an in-memory content-addressed store mounted on the blobs ALPN, i-have/
// who-has negotiation carried over the existing gossip channel, and a multi-source `Downloader`
// that resumes across holder churn and Blake3-verifies on completion.
//
// Discipline: offline only (no n0/relay/mDNS — a shared in-process `StaticProvider` addresses both
// the blob endpoints and the gossip endpoints), bounded waits (`tokio::time::timeout`), and every
// assertion is on a node's OWN observed state — its holder registry or its blob store.

// `clippy.toml` sets `allow-unwrap-in-tests = true`, but that allowance does not reach `unwrap()`
// calls emitted inside macro expansions (e.g. `serde_json::json!`) within an integration-test crate.
// This is test code where the project already permits unwrap, so allow it at the file level.
#![allow(clippy::disallowed_methods)]

mod support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use iroh_gossip::api::Event as GossipEvent;
use support::{offline_endpoint, pubkey, topic_id, unique_key, RawGossip, T};
use xaeroflux::swarm::{BlobSwarm, Hash, BLOB_ALPN};

/// Build a `BlobSwarm` over a fresh offline endpoint joined to the mesh's shared `StaticProvider`.
async fn spawn_swarm(key: &str) -> Arc<BlobSwarm> {
    let endpoint = offline_endpoint(key, vec![BLOB_ALPN.to_vec()]).await;
    let node_id = endpoint.id().to_string();
    Arc::new(BlobSwarm::new(endpoint, node_id))
}

/// Drive a gossip endpoint's receive loop: feed every received message into `swarm.on_message`, and
/// re-broadcast any reply (an `IHave` answering a `WhoHas`) back onto the same topic. This is the
/// transport glue — `BlobSwarm` owns the negotiation logic, the existing gossip channel owns delivery.
fn pump_gossip(
    swarm: Arc<BlobSwarm>,
    mut rx: iroh_gossip::api::GossipReceiver,
    send: iroh_gossip::api::GossipSender,
) {
    tokio::spawn(async move {
        while let Some(item) = rx.next().await {
            let Ok(GossipEvent::Received(msg)) = item else {
                continue;
            };
            if let Ok(Some(reply)) = swarm.on_message(&msg.content).await {
                if let Ok(bytes) = serde_json::to_vec(&reply) {
                    let _ = send.broadcast(Bytes::from(bytes)).await;
                }
            }
        }
    });
}

/// Deterministic blob payload of `len` bytes — large enough that a multi-source download splits work
/// across providers. No randomness, so the content hash is stable across runs.
fn blob_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// X8 — a provider advertises a blob (IHave), a requester asks (WhoHas), and they negotiate transfer.
///
/// Oracle: the requester's OWN holder registry. The requester broadcasts `WhoHas` over gossip; the
/// provider (which holds the blob) answers `IHave`; the requester records the provider as a holder.
/// We assert the requester's registry lists the provider's blob-endpoint id — its own observed state,
/// never a log line.
#[tokio::test]
async fn blob_ihave_whohas_negotiates() {
    let key = unique_key();

    // Blob-transfer participants (content-addressed stores on the blobs ALPN).
    let provider = spawn_swarm(&key).await;
    let requester = spawn_swarm(&key).await;
    let hash = provider
        .add(blob_bytes(4096))
        .await
        .expect("provider adds the blob");

    // Negotiation rides a gossip side channel (separate endpoints, shared StaticProvider). The two
    // gossip peers bootstrap from each other and share one blob-swarm topic.
    let prov_gossip = RawGossip::spawn(&key).await;
    let req_gossip = RawGossip::spawn(&key).await;
    let topic = topic_id("cyan/blobs/swarm-test");
    let (prov_send, prov_rx) = prov_gossip
        .subscribe(topic, vec![pubkey(&req_gossip.node_id())])
        .await;
    let (req_send, req_rx) = req_gossip
        .subscribe(topic, vec![pubkey(&prov_gossip.node_id())])
        .await;

    pump_gossip(provider.clone(), prov_rx, prov_send);
    pump_gossip(requester.clone(), req_rx, req_send.clone());

    // Requester drives WhoHas until it observes a holder (bounded). Retrying covers the gossip join
    // race the same way the mesh tests retry probes.
    let query = serde_json::to_vec(&requester.query(&hash)).expect("serialize WhoHas");
    let provider_id = provider.node_id().to_string();
    let negotiated = tokio::time::timeout(T, async {
        loop {
            let _ = req_send.broadcast(Bytes::from(query.clone())).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            if requester.holders(&hash).await.contains(&provider_id) {
                return;
            }
        }
    })
    .await;

    assert!(
        negotiated.is_ok(),
        "requester never learned a holder for the blob within {T:?}"
    );
    let holders = requester.holders(&hash).await;
    assert!(
        holders.contains(&provider_id),
        "requester's holder registry {holders:?} should list the provider {provider_id}"
    );
}

/// X8 — a blob is fetched from two providers concurrently (swarming / multi-source).
///
/// Two providers add the SAME bytes; content addressing means they produce the SAME hash. A fresh
/// requester (which does not hold the blob) fetches it from both holders. Oracle: the requester's
/// OWN store now holds the blob, and the returned bytes match — fetch Blake3-verifies on completion.
#[tokio::test]
async fn blob_fetched_from_two_providers() {
    let key = unique_key();
    let data = blob_bytes(512 * 1024);

    let provider_a = spawn_swarm(&key).await;
    let provider_b = spawn_swarm(&key).await;
    let hash_a = provider_a.add(data.clone()).await.expect("provider_a adds blob");
    let hash_b = provider_b.add(data.clone()).await.expect("provider_b adds blob");
    assert_eq!(
        hash_a, hash_b,
        "content addressing: identical bytes must hash to one identity"
    );

    let requester = spawn_swarm(&key).await;
    assert!(
        !requester.has(&hash_a).await.expect("has() query"),
        "fresh requester must not already hold the blob"
    );

    let holders = vec![
        provider_a.node_id().to_string(),
        provider_b.node_id().to_string(),
    ];
    let fetched = tokio::time::timeout(T, requester.fetch(&hash_a, &holders))
        .await
        .expect("multi-source fetch timed out")
        .expect("multi-source fetch failed");

    assert_eq!(
        fetched.as_ref(),
        data.as_slice(),
        "fetched bytes differ from the source content"
    );
    assert_eq!(
        Hash::new(&fetched),
        hash_a,
        "fetched content must hash back to the requested identity"
    );
    assert!(
        requester.has(&hash_a).await.expect("has() query"),
        "requester's own store must hold the blob after fetch"
    );
}

/// X8 (resume) — a holder dropping must not fail the download.
///
/// Two providers hold the blob; one is shut down before the fetch. The `Downloader` falls back to
/// (and resumes against) the surviving holder, so the download still completes and verifies. Oracle:
/// the requester's OWN store holds the verified blob despite a dead holder in the provider set.
#[tokio::test]
async fn blob_fetch_resumes_across_holder_churn() {
    let key = unique_key();
    let data = blob_bytes(512 * 1024);

    // The holder that will survive.
    let survivor = spawn_swarm(&key).await;
    let hash = survivor.add(data.clone()).await.expect("survivor adds blob");

    // The holder that will drop out. Keep its endpoint handle so we can close it (make it
    // undialable) before the fetch — simulating a holder that has left the swarm.
    let dead_endpoint = offline_endpoint(&key, vec![BLOB_ALPN.to_vec()]).await;
    let dead_id = dead_endpoint.id().to_string();
    {
        let dead = BlobSwarm::new(dead_endpoint.clone(), dead_id.clone());
        let dead_hash = dead.add(data.clone()).await.expect("dead holder adds blob");
        assert_eq!(dead_hash, hash, "both holders hold identical content");
        // Drop the swarm (and its blobs Router) and close the endpoint: this holder is now gone.
    }
    dead_endpoint.close().await;

    let requester = spawn_swarm(&key).await;
    // Provider set lists the dead holder first; the downloader must fall through to the survivor.
    let holders = vec![dead_id, survivor.node_id().to_string()];
    let fetched = tokio::time::timeout(T, requester.fetch(&hash, &holders))
        .await
        .expect("fetch with a churned holder timed out")
        .expect("fetch must survive a dropped holder");

    assert_eq!(
        fetched.as_ref(),
        data.as_slice(),
        "fetched bytes differ from the source content"
    );
    assert!(
        requester.has(&hash).await.expect("has() query"),
        "requester's own store must hold the blob after resuming past the dead holder"
    );
}
