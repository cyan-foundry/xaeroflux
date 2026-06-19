// X1, X2 — mesh formation + event propagation over offline iroh 0.95 gossip.
// See XAEROFLUX_TEST_SPEC.md. All nodes are offline (no n0 DNS, no mDNS, no relay) and addressed
// out-of-band via a shared in-process StaticProvider. Waits are bounded.

mod support;

use std::time::Duration;

use support::{
    count_events, establish_mesh, make_event, spawn_local_node, unique_key, wait_for_event, T,
};

/// X1 — a bootstrap node + two peers sharing a discovery key form one connected mesh.
///
/// Oracle: three distinct live node identities, plus proof of gossip connectivity by propagating a
/// probe event from one peer to both the bootstrap and the other peer (the peer tracker is private
/// and never populated in a pure-substrate mesh, so propagation is the only honest formation oracle).
#[tokio::test]
async fn mesh_bootstrap_forms() {
    let key = unique_key();

    let mut bootstrap = spawn_local_node("bootstrap", &key, &[]).await;
    let peer_a = spawn_local_node("peer_a", &key, &[bootstrap.node_id.clone()]).await;
    let mut peer_b = spawn_local_node("peer_b", &key, &[bootstrap.node_id.clone()]).await;

    // Three distinct, non-empty identities (proves the per-node temp-dir identity isolation works).
    assert!(!bootstrap.node_id.is_empty(), "bootstrap node_id empty");
    assert!(!peer_a.node_id.is_empty(), "peer_a node_id empty");
    assert!(!peer_b.node_id.is_empty(), "peer_b node_id empty");
    assert_ne!(bootstrap.node_id, peer_a.node_id, "bootstrap/peer_a share identity");
    assert_ne!(bootstrap.node_id, peer_b.node_id, "bootstrap/peer_b share identity");
    assert_ne!(peer_a.node_id, peer_b.node_id, "peer_a/peer_b share identity");

    // Connectivity: an event from peer_a reaches both the bootstrap and peer_b → mesh formed.
    establish_mesh(&peer_a, &mut [&mut bootstrap.event_rx, &mut peer_b.event_rx], T)
        .await
        .expect("mesh of bootstrap + peer_a + peer_b should form within T");
}

/// X2 — an event published by one peer reaches the bootstrap and the other peer exactly once.
#[tokio::test]
async fn event_propagates_to_all_peers() {
    let key = unique_key();

    let mut bootstrap = spawn_local_node("bootstrap", &key, &[]).await;
    let peer_a = spawn_local_node("peer_a", &key, &[bootstrap.node_id.clone()]).await;
    let mut peer_b = spawn_local_node("peer_b", &key, &[bootstrap.node_id.clone()]).await;

    // Warm up until the gossip mesh is connected (also exercises X1's path).
    establish_mesh(&peer_a, &mut [&mut bootstrap.event_rx, &mut peer_b.event_rx], T)
        .await
        .expect("mesh should form before measuring propagation");

    // Publish one distinct measured event from peer_a.
    let payload = format!("x2-measured-{}", &peer_a.node_id[..8]);
    let measured = make_event(&peer_a.node_id, &payload);
    peer_a
        .event_tx
        .send(measured.clone())
        .expect("publish measured event");

    // It reaches both the bootstrap and peer_b on their own event_rx.
    let at_bootstrap = wait_for_event(&mut bootstrap.event_rx, |e| e.payload == payload, T)
        .await
        .expect("bootstrap should receive peer_a's event");
    let at_peer_b = wait_for_event(&mut peer_b.event_rx, |e| e.payload == payload, T)
        .await
        .expect("peer_b should receive peer_a's event");

    assert_eq!(at_bootstrap.id, measured.id, "bootstrap saw a different event id");
    assert_eq!(at_peer_b.id, measured.id, "peer_b saw a different event id");
    assert_eq!(at_bootstrap.source, peer_a.node_id, "wrong source at bootstrap");

    // Dedup: exactly-once delivery to event_rx. We already consumed the single copy above, so no
    // further copies of that id should arrive in a short follow-up window.
    let extra_at_bootstrap =
        count_events(&mut bootstrap.event_rx, |e| e.id == measured.id, Duration::from_secs(2)).await;
    let extra_at_peer_b =
        count_events(&mut peer_b.event_rx, |e| e.id == measured.id, Duration::from_secs(2)).await;
    assert_eq!(extra_at_bootstrap, 0, "bootstrap received duplicate copies (dedup failed)");
    assert_eq!(extra_at_peer_b, 0, "peer_b received duplicate copies (dedup failed)");
}
