// X6 — snapshot request/serve round-trip over direct QUIC, fully offline.
// See XAEROFLUX_TEST_SPEC.md. Two raw offline endpoints share a StaticProvider; the provider
// preloads a GroupSnapshot (via the public `update_from_event`) and serves it to the requester.

// `clippy.toml` sets `allow-unwrap-in-tests = true`, but that allowance does not reach `unwrap()`
// calls emitted inside macro expansions (e.g. `serde_json::json!`) within an integration-test crate.
// This is test code where the project already permits unwrap, so allow it at the file level.
#![allow(clippy::disallowed_methods)]

mod support;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use iroh::endpoint::{Connection, SendStream};
use support::{offline_endpoint, unique_key};
use xaeroflux::snapshot::{SnapshotMessage, SnapshotProvider, SnapshotRequester, SNAPSHOT_ALPN};
use xaeroflux::Event;

/// Synthesize an application `Event` whose JSON payload drives `SnapshotProvider::update_from_event`
/// (the only public way to populate a provider's in-memory `GroupSnapshot`).
fn ev(payload: serde_json::Value) -> Event {
    Event {
        id: blake3::hash(payload.to_string().as_bytes()).to_hex().to_string(),
        payload: payload.to_string(),
        source: "test".to_string(),
        ts: 42,
    }
}

/// Provider-side accept loop: accept the bi-stream the requester opened, read the group_id it
/// sends, and return the **send half of that same accepted stream** so the snapshot reply rides
/// back on the stream the requester is reading from.
async fn read_requested_group(conn: &Connection) -> Result<(SendStream, String)> {
    let (send, mut recv) = conn.accept_bi().await?;
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf).await?;
    let n = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; n];
    recv.read_exact(&mut buf).await?;
    Ok((send, String::from_utf8_lossy(&buf).to_string()))
}

/// X6 — full QUIC round-trip.
///
/// `SnapshotRequester::download_snapshot` opens a bi-stream, writes the group_id, then reads the
/// reply **on the recv half of that same stream**. `SnapshotProvider::serve_snapshot` now replies
/// on the send half of the *accepted* stream (the one `accept_bi()` produced for the requester's
/// open), so the two halves rendezvous and the snapshot transfers. (Previously `serve_snapshot`
/// replied on a fresh provider-initiated `conn.open_bi()` stream the requester never read, and the
/// transfer failed fast with "connection lost".) The in-memory snapshot model is exercised
/// separately by `snapshot_store_preload_and_serve_message` below.
#[tokio::test]
async fn snapshot_request_serve_round_trips() {
    let key = unique_key();
    let provider_ep = offline_endpoint(&key, vec![SNAPSHOT_ALPN.to_vec()]).await;
    let requester_ep = offline_endpoint(&key, vec![SNAPSHOT_ALPN.to_vec()]).await;
    let provider_id = provider_ep.id().to_string();
    let requester_id = requester_ep.id().to_string();

    // Preload a non-empty GroupSnapshot for "g1" through the public mutator.
    let provider = Arc::new(SnapshotProvider::new(provider_ep.clone(), provider_id.clone()));
    provider
        .update_from_event(
            "g1",
            &ev(serde_json::json!({
                "type": "GroupCreated", "id": "g1", "name": "Group One",
                "icon": "🌀", "color": "#0ff", "created_at": 1
            })),
        )
        .await;
    provider
        .update_from_event(
            "g1",
            &ev(serde_json::json!({
                "type": "WorkspaceCreated", "id": "ws1", "group_id": "g1",
                "name": "Workspace One", "created_at": 2
            })),
        )
        .await;
    let preloaded = provider.get_snapshot("g1").await.expect("snapshot preloaded");
    assert!(!preloaded.workspaces.is_empty(), "preload should create a workspace");

    // Drive the provider's accept loop in the background.
    let prov = provider.clone();
    let accept_ep = provider_ep.clone();
    let accept = tokio::spawn(async move {
        while let Some(incoming) = accept_ep.accept().await {
            let conn = match incoming.await {
                Ok(c) => c,
                Err(_) => continue,
            };
            if conn.alpn() != SNAPSHOT_ALPN {
                continue;
            }
            let (send, group_id) = match read_requested_group(&conn).await {
                Ok(g) => g,
                Err(_) => continue,
            };
            let _ = prov.serve_snapshot(send, &group_id).await;
            // Keep the connection alive until the requester has read the reply and closed,
            // so the finished stream's bytes are not lost to an eager connection drop.
            conn.closed().await;
        }
    });

    let requester = SnapshotRequester::new(requester_ep, requester_id);
    assert!(requester.needs_snapshot("g1").await, "fresh requester needs a snapshot");

    // Bounded: never an unbounded await on the network.
    let downloaded = tokio::time::timeout(
        Duration::from_secs(10),
        requester.download_snapshot(&provider_id, "g1"),
    )
    .await;
    accept.abort();

    let snapshot = downloaded
        .expect("snapshot download timed out")
        .expect("snapshot download failed");
    assert!(!snapshot.workspaces.is_empty(), "served snapshot must carry workspaces");
    assert_eq!(snapshot.group.id, "g1", "served snapshot is for the wrong group");
}

/// X6 (data model) — the snapshot *store* round-trips through the public API without the network:
/// preload via `update_from_event`, read back via `get_snapshot`, and confirm `handle_request`
/// reports the correct item count. Complements the over-the-wire QUIC round-trip in
/// `snapshot_request_serve_round_trips` above by exercising the in-memory model directly.
#[tokio::test]
async fn snapshot_store_preload_and_serve_message() {
    let key = unique_key();
    let endpoint = offline_endpoint(&key, vec![SNAPSHOT_ALPN.to_vec()]).await;
    let node_id = endpoint.id().to_string();
    let provider = SnapshotProvider::new(endpoint, node_id);

    provider
        .update_from_event(
            "g1",
            &ev(serde_json::json!({
                "type": "GroupCreated", "id": "g1", "name": "Group One",
                "icon": "🌀", "color": "#0ff", "created_at": 1
            })),
        )
        .await;
    provider
        .update_from_event(
            "g1",
            &ev(serde_json::json!({
                "type": "WorkspaceCreated", "id": "ws1", "group_id": "g1",
                "name": "Workspace One", "created_at": 2
            })),
        )
        .await;

    let snapshot = provider.get_snapshot("g1").await.expect("snapshot exists for g1");
    assert_eq!(snapshot.group.id, "g1");
    assert_eq!(snapshot.group.name, "Group One");
    assert_eq!(snapshot.workspaces.len(), 1, "one workspace preloaded");
    assert_eq!(snapshot.workspaces[0].id, "ws1");

    // handle_request advertises availability with item_count = 1 (group) + 1 (workspace) = 2.
    match provider.handle_request("g1").await {
        Some(SnapshotMessage::SnapshotAvailable { group_id, item_count, .. }) => {
            assert_eq!(group_id, "g1");
            assert_eq!(item_count, 2, "item_count = group + workspaces + boards + files + chats");
        }
        other => panic!("expected SnapshotAvailable for g1, got {other:?}"),
    }

    // Unknown group → no availability message.
    assert!(provider.handle_request("does-not-exist").await.is_none());
}
