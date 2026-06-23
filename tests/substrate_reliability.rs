// X7 — reliability: mesh formation is stable under repetition, and independent meshes running
// concurrently do not interfere. Offline, bounded waits. See XAEROFLUX_TEST_SPEC.md.

mod support;

use std::time::Duration;

use support::{
    count_events, establish_mesh, make_event, spawn_local_node, unique_key, wait_for_event, T,
};

/// X7 — form and tear down the full mesh 15× in a loop; every iteration must converge within T.
/// Each iteration's nodes are dropped at the end of the loop body (teardown) before the next forms.
#[tokio::test]
async fn repeat_mesh_forms_is_stable() {
    const ITERATIONS: usize = 15;
    for i in 0..ITERATIONS {
        let key = unique_key();
        let mut bootstrap = spawn_local_node("bootstrap", &key, &[]).await;
        let peer_a = spawn_local_node("peer_a", &key, &[bootstrap.node_id.clone()]).await;
        let mut peer_b = spawn_local_node("peer_b", &key, &[bootstrap.node_id.clone()]).await;

        establish_mesh(&peer_a, &mut [&mut bootstrap.event_rx, &mut peer_b.event_rx], T)
            .await
            .unwrap_or_else(|e| panic!("iteration {i}/{ITERATIONS}: mesh failed to form: {e}"));
        // bootstrap / peer_a / peer_b drop here, ending the iteration's mesh.
    }
}

/// X7 — several independent meshes (unique keys) run concurrently; all converge AND an event in one
/// mesh never leaks into another (topic/provider isolation by discovery key).
#[tokio::test]
async fn concurrent_meshes_do_not_interfere() {
    let key1 = unique_key();
    let key2 = unique_key();

    // Mesh 1.
    let mut b1 = spawn_local_node("bootstrap", &key1, &[]).await;
    let a1 = spawn_local_node("peer_a", &key1, &[b1.node_id.clone()]).await;
    let mut c1 = spawn_local_node("peer_b", &key1, &[b1.node_id.clone()]).await;

    // Mesh 2.
    let mut b2 = spawn_local_node("bootstrap", &key2, &[]).await;
    let a2 = spawn_local_node("peer_a", &key2, &[b2.node_id.clone()]).await;
    let mut c2 = spawn_local_node("peer_b", &key2, &[b2.node_id.clone()]).await;

    // Form both concurrently; both must converge within T. The receiver-borrow arrays live only for
    // this inner scope, so the borrows on b1/c1/b2/c2 are released before we reuse them below.
    let (r1, r2) = {
        let mut rx1 = [&mut b1.event_rx, &mut c1.event_rx];
        let mut rx2 = [&mut b2.event_rx, &mut c2.event_rx];
        tokio::join!(
            establish_mesh(&a1, &mut rx1, T),
            establish_mesh(&a2, &mut rx2, T),
        )
    };
    r1.expect("mesh 1 should converge concurrently");
    r2.expect("mesh 2 should converge concurrently");

    // Non-interference: an event published in mesh 1 reaches mesh 1's other peer but never mesh 2.
    let tag = format!("isolated-{}", &a1.node_id[..8]);
    a1.event_tx
        .send(make_event(&a1.node_id, &tag))
        .expect("publish isolated event in mesh 1");

    wait_for_event(&mut c1.event_rx, |e| e.payload == tag, T)
        .await
        .expect("mesh 1 peer_b should receive mesh 1's event");

    let leaked_b2 = count_events(&mut b2.event_rx, |e| e.payload == tag, Duration::from_secs(2)).await;
    let leaked_c2 = count_events(&mut c2.event_rx, |e| e.payload == tag, Duration::from_secs(2)).await;
    assert_eq!(leaked_b2, 0, "mesh 1 event leaked to mesh 2 bootstrap");
    assert_eq!(leaked_c2, 0, "mesh 1 event leaked to mesh 2 peer");
}
