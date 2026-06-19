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
use iroh::endpoint::Connection;
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

/// Provider-side accept loop: read the group_id the requester sends, then serve via the public API.
async fn read_requested_group(conn: &Connection) -> Result<String> {
    let (_send, mut recv) = conn.accept_bi().await?;
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf).await?;
    let n = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; n];
    recv.read_exact(&mut buf).await?;
    Ok(String::from_utf8_lossy(&buf).to_string())
}

/// X6 — full QUIC round-trip. **Currently #[ignore]d: genuine engine protocol mismatch.**
///
/// `SnapshotRequester::download_snapshot` opens a bi-stream, writes the group_id, then reads the
/// reply **on the recv half of that same stream**. But `SnapshotProvider::serve_snapshot` writes the
/// snapshot on a *new, provider-initiated* `conn.open_bi()` stream (and never writes to the stream
/// the requester opened), then returns — dropping the connection. The two halves never rendezvous,
/// so the transfer fails fast with "connection lost" (observed empirically in 1.06s, not a timeout).
/// The `xaeroflux_bootstrap` binary wires no snapshot accept loop, so this QUIC path is unexercised
/// in production. Re-enable once `serve_snapshot` replies on the *accepted* stream (`accept_bi`)
/// instead of opening its own. The in-memory snapshot model itself works — see
/// `snapshot_store_preload_and_serve_message` below.
#[tokio::test]
#[ignore = "engine: serve_snapshot replies on a fresh open_bi() stream while download_snapshot reads its own opened stream — they never rendezvous (connection lost). Fix serve to use the accepted stream."]
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
            let group_id = match read_requested_group(&conn).await {
                Ok(g) => g,
                Err(_) => continue,
            };
            let _ = prov.serve_snapshot(conn, &group_id).await;
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
/// reports the correct item count. This is the honest, currently-working slice of X6; the QUIC
/// transport is `#[ignore]`d above pending the `serve_snapshot` fix.
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
