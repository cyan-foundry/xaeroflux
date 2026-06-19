// X1, X2 — mesh formation + event propagation over offline iroh 0.95 gossip.
// See XAEROFLUX_TEST_SPEC.md. All nodes are offline (no n0 DNS, no mDNS, no relay) and addressed
// out-of-band via a shared in-process StaticProvider. Waits are bounded.

// `clippy.toml` sets `allow-unwrap-in-tests = true`, but that allowance does not reach `unwrap()`
// calls emitted inside macro expansions (e.g. `serde_json::json!`) within an integration-test crate.
// This is test code where the project already permits unwrap, so allow it at the file level.
#![allow(clippy::disallowed_methods)]

mod support;

use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use iroh_gossip::api::Event as GossipEvent;
use support::{
    count_events, discovery_topic, establish_mesh, group_topic, make_event, pubkey, spawn_local_node,
    unique_key, wait_for_event, RawGossip, T,
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

/// X3 — when a peer announces a group via `groups_exchange`, the bootstrap auto-subscribes to that
/// group's topic and relays its events. A raw gossip peer injects the announce + a group event
/// (neither is expressible through the public `XaeroFlux` API), and the bootstrap surfaces the
/// relayed event on its own `event_rx` with `source == "group/<gid>"`.
#[tokio::test]
async fn group_topic_auto_subscribe_on_announce() {
    let key = unique_key();
    let mut bootstrap = spawn_local_node("bootstrap", &key, &[]).await;
    let bootstrap_pk = pubkey(&bootstrap.node_id);

    let raw = RawGossip::spawn(&key).await;
    let (disc_send, _disc_rx) = raw.subscribe(discovery_topic(&key), vec![bootstrap_pk]).await;
    let (grp_send, _grp_rx) = raw.subscribe(group_topic("g1"), vec![bootstrap_pk]).await;

    let announce = serde_json::json!({
        "msg_type": "groups_exchange",
        "node_id": raw.node_id(),
        "local_groups": ["g1"],
    })
    .to_string();

    // Drive the announce + group event until the bootstrap relays a g1 event to its event_rx.
    let relayed = tokio::time::timeout(T, async {
        let mut tick = tokio::time::interval(Duration::from_millis(400));
        let mut n = 0u64;
        loop {
            tick.tick().await;
            n += 1;
            let _ = disc_send.broadcast(Bytes::from(announce.clone())).await;
            let _ = grp_send
                .broadcast(Bytes::from(format!("g1-event-{n}")))
                .await;
            while let Ok(ev) = bootstrap.event_rx.try_recv() {
                if ev.source == "group/g1" {
                    return ev;
                }
            }
        }
    })
    .await;

    let ev = relayed.expect("bootstrap should auto-subscribe to g1 and relay a group event within T");
    assert_eq!(ev.source, "group/g1", "relayed event must be scoped to the announced group");
    assert!(
        ev.payload.starts_with("g1-event-"),
        "relayed payload should be the group message content, got {:?}",
        ev.payload
    );
}

/// X4 — with two peers online in a group, the bootstrap broadcasts a `peer_introduction` on the
/// discovery topic listing both peers' node_ids. The engine surfaces this only on the discovery
/// topic (never on a public `XaeroFlux` field), so a raw gossip observer announces the two peers
/// and reads the resulting introduction back off the discovery topic.
#[tokio::test]
async fn peer_introduction_lists_both_peers() {
    let key = unique_key();
    let bootstrap = spawn_local_node("bootstrap", &key, &[]).await;
    let bootstrap_pk = pubkey(&bootstrap.node_id);

    // Two distinct, valid peer identities to announce for group "g1".
    let peer_x = RawGossip::spawn(&key).await;
    let peer_y = RawGossip::spawn(&key).await;
    let id_x = peer_x.node_id();
    let id_y = peer_y.node_id();

    // Observer joins the discovery topic; it both announces the peers and reads the introduction.
    let observer = RawGossip::spawn(&key).await;
    let (disc_send, mut disc_rx) = observer
        .subscribe(discovery_topic(&key), vec![bootstrap_pk])
        .await;

    let announce = |id: &str| {
        serde_json::json!({
            "msg_type": "groups_exchange",
            "node_id": id,
            "local_groups": ["g1"],
        })
        .to_string()
    };
    let ann_x = announce(&id_x);
    let ann_y = announce(&id_y);

    let peers = tokio::time::timeout(T, async {
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let _ = disc_send.broadcast(Bytes::from(ann_x.clone())).await;
                    let _ = disc_send.broadcast(Bytes::from(ann_y.clone())).await;
                }
                item = disc_rx.next() => {
                    let Some(Ok(GossipEvent::Received(msg))) = item else { continue };
                    let Some(listed) = parse_peer_introduction(&msg.content) else { continue };
                    if listed.contains(&id_x) && listed.contains(&id_y) {
                        return listed;
                    }
                }
            }
        }
    })
    .await
    .expect("bootstrap should broadcast a peer_introduction listing both peers within T");

    assert!(peers.contains(&id_x), "introduction missing peer_x");
    assert!(peers.contains(&id_y), "introduction missing peer_y");
}

/// X5 — peer departure. **#[ignore]d: not observable through any public substrate oracle in-process.**
///
/// The engine marks a peer offline only on a gossip `NeighborDown` for the *discovery topic*, and
/// `PeerTracker::mark_offline` keys on the departing **neighbor's** node_id — not on the node_ids
/// carried in `groups_exchange` payloads (which is what populates the group rosters). So a tracked
/// peer cannot be driven offline by dropping a gossip neighbor unless that neighbor's own id was the
/// tracked id, and even then the result is only visible via the private `PeerTracker` or by the
/// *absence* of an id from a future `peer_introduction` — which the engine re-broadcasts only while
/// a group still has >1 peer. There is no public, positively-assertable departure signal. Re-enable
/// if the engine exposes peer-tracker state or emits an explicit departure event.
#[tokio::test]
#[ignore = "engine: peer departure is not observable via any public oracle in-process — mark_offline keys on the gossip neighbor id (not the groups_exchange node_id), PeerTracker is private, and post-departure peer_introduction only fires while >1 peer remains."]
async fn peer_departure_marks_offline() {
    // Intentionally minimal: see the doc comment above for why this is ignored rather than faked.
}

/// Parse a discovery-topic gossip payload as a `peer_introduction`, returning the listed peer
/// node_ids. Returns `None` for any other message shape.
fn parse_peer_introduction(content: &[u8]) -> Option<Vec<String>> {
    let json: serde_json::Value = serde_json::from_slice(content).ok()?;
    if json.get("msg_type").and_then(|v| v.as_str()) != Some("peer_introduction") {
        return None;
    }
    let peers = json
        .get("peers")?
        .as_array()?
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    Some(peers)
}
