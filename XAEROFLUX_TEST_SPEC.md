# XAEROFLUX_TEST_SPEC — bootstrap & mesh substrate tests

Companion to cyan-backend's `SUBSTRATE_TEST_SPEC.md`. This hardens the **xaeroflux
substrate**: the bootstrap peer, gossip discovery, event propagation, and snapshot
serve — the primitives the whole mesh stands on. In-process, offline, iroh 0.95.

## 0. What we prove

| ID | Guarantee |
|----|-----------|
| X1 | **Mesh forms** — a bootstrap node + 2 peers sharing a discovery key find each other (offline, no n0). |
| X2 | **Event propagation** — an event published by one peer reaches the bootstrap and the other peer. |
| X3 | **Group-topic subscription** — when a peer announces a group, the bootstrap subscribes and relays that group's events. |
| X4 | **Peer introduction** — the bootstrap's peer_introduction lists both peers. |
| X5 | **Peer departure** — a peer going away is marked offline / dropped from introductions. |
| X6 | **Snapshot round-trip** — a provider with a preloaded `GroupSnapshot` serves it to a requester over direct QUIC. |
| X7 | **Reliability** — X1/X2 hold repeatedly under load (no flakiness). |
| X8 | **Blob swarming** *(future, red)* — iroh-blobs IHave/WhoHas distribution (not yet integrated). |

Out of scope: the Iggy forwarding path in the bootstrap binary (enrichment, moving
to MCP/workflow) — don't test it. Focus on mesh + snapshot primitives.

## 1. Harness — spin local nodes in one process

The node API (confirm against `src/lib.rs`):
`XaeroFlux::builder().discovery_key(k).db_path(p).no_n0_discovery().bootstrap_peer(id).build().await`
→ `XaeroFlux { event_tx, event_rx, node_id, discovery_key }`.

`tests/support/mod.rs`:
```rust
pub async fn spawn_local_node(name: &str, key: &str, bootstrap: &[String]) -> XaeroFlux;
// builder + discovery_key(key) + unique temp/in-mem db + no_n0_discovery() + bootstrap_peers
pub async fn wait_for_event<P: Fn(&Event)->bool>(rx, pred, t: Duration) -> Result<Event>; // bounded
pub fn unique_key() -> String;     // isolate concurrent meshes
pub const T: Duration = Duration::from_secs(15);
```

**Address-resolution note (same lesson as cyan-backend):** with n0 disabled and no
relay, a peer dialing the bootstrap by `node_id` needs an address source. Prefer
mDNS (`discovery-local-network` is enabled). If in-process mDNS is flaky, add a
minimal **inert `StaticProvider`** seam (expose the endpoint; inject loopback
`EndpointAddr`s between nodes) — behavior-preserving, documented in STATUS. Do not
change any public signature the bootstrap binary uses.

**Per-node storage:** each `XaeroFlux` takes its own `db_path`, so unlike
cyan-backend you can likely assert on each node's **own** DB/state. Verify this;
if the DB turns out global, fall back to per-node `event_rx`/peer-tracker oracles
and note it.

## 2. Named tests (the backlog)

`tests/substrate_mesh.rs`
- `mesh_bootstrap_forms` (X1) — bootstrap + peer_a + peer_b, shared key, `no_n0_discovery`; assert all three alive and each peer's tracker sees the others within `T`.
- `event_propagates_to_all_peers` (X2) — peer_a publishes an `Event`; assert bootstrap and peer_b receive it on `event_rx` within `T`; dedup count == 1.
- `group_topic_auto_subscribe_on_announce` (X3) — peer announces group "g1"; bootstrap auto-subscribes; an event on g1's topic is relayed; assert source/group.
- `peer_introduction_lists_both_peers` (X4) — 2 peers online; the bootstrap's peer_introduction includes both node_ids.
- `peer_departure_marks_offline` (X5) — drop peer_a; assert it's marked offline / dropped from the next introduction. (May be `#[ignore]` if departure isn't observable in-process — note the reason.)

`tests/substrate_snapshot.rs`
- `snapshot_request_serve_round_trips` (X6) — provider preloads a `GroupSnapshot` for "g1"; requester `needs_snapshot` → connects → receives; assert non-empty workspaces/state.

`tests/substrate_reliability.rs`
- `repeat_mesh_forms_is_stable` (X7) — form/teardown the mesh 15× in a loop; every iteration meets within `T`.
- `concurrent_meshes_do_not_interfere` — several independent meshes (unique keys) concurrently; all converge.

`tests/substrate_swarm.rs` *(red scaffolds, `#[ignore]` + reason)*
- `blob_ihave_whohas_negotiates`, `blob_fetched_from_two_providers` — iroh-blobs swarming, not yet integrated.

## 3. Rules
Bounded waits only; offline/no-n0 only; never weaken assertions or edit this spec;
iroh 0.95; if a capability is missing, `#[ignore]` with a precise reason and keep going.
