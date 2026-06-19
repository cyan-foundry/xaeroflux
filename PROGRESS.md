# PROGRESS — xaeroflux substrate tests (feat/bootstrap-tests)

Branch: `feat/bootstrap-tests` off `feat/bootstrap-wip`. Never main.

## PHASE 0 — branch + baseline ✅
- Branch `feat/bootstrap-tests` created off `feat/bootstrap-wip`. Confirmed NOT on main.
- Baseline `cargo build` ✅ (3 pre-existing warnings in bootstrap bin).
- Baseline `cargo test` ✅ — 2 lib tests pass (`basic_integration_sanity`, `builder_with_custom_relay`).
- Baseline `cargo clippy --all-targets`: **29 pre-existing warning lines** (repo does not currently
  pass `-D warnings`; gate is differential — zero NEW warnings from my diff). `clippy.toml` allows
  unwrap/expect/panic in tests.

### Key engine facts established by reading src/ (before any change)
- Identity is persisted to `node.key` in the **parent dir of `db_path`** (lib.rs:164). `:memory:` →
  parent `.` → all nodes share `./node.key` → SAME node_id. **Harness must give each node its own temp dir.**
- `no_n0_discovery()` only flips `use_n0_discovery`; relay stays `RelayMode::Default` (n0 relays), mDNS stays on.
- `XaeroFlux` exposes only `event_tx`, `event_rx`, `node_id`, `discovery_key`. `PeerTracker` is private and is
  populated ONLY by `groups_exchange` discovery messages, which the engine receives but never sends. So the
  honest oracle for "mesh formed" is event propagation on `event_rx`, not the (private, unpopulated) tracker.

## PHASE 1 — harness + core mesh (X1, X2) ✅
- `tests/support/mod.rs`: `spawn_local_node`, `wait_for_event`, `unique_key`, `T`, plus `make_event`,
  `count_events`, `establish_mesh`. Offline: `no_n0_discovery()` + `no_mdns()` + `disable_relay()` +
  shared per-mesh `StaticProvider`. Each node gets a unique temp dir → unique `node.key` identity + DB.
- **Engine seam** (minimal, additive, behavior-preserving; documented in STATUS_OVERNIGHT.md):
  - `XaeroFluxBuilder::disable_relay()` → `RelayMode::Disabled` (opt-in; bootstrap binary unaffected).
  - `XaeroFluxBuilder::static_provider(StaticProvider)` → extra discovery service (opt-in).
  - `pub XaeroFlux::endpoint: Endpoint` (clone of the actor's endpoint) so the harness can read each
    node's `EndpointAddr`. No public signature the bootstrap binary uses was changed.
- mDNS was **not** needed: deterministic offline addressing via `StaticProvider` chosen over flaky
  in-process mDNS multicast (better for the reliability/concurrent phases).
- `tests/substrate_mesh.rs`: `mesh_bootstrap_forms` (X1) ✅, `event_propagates_to_all_peers` (X2) ✅.
  X1 oracle = 3 distinct identities + propagation (peer tracker is private & never populated by the
  pure substrate, so it is not an observable oracle). X2 asserts arrival at bootstrap + peer_b and
  exactly-once (dedup) delivery.
- GATE: `cargo build` ✅, full `cargo test` ✅ (4 pass, 0 new ignored), clippy **0 new warnings**
  (baseline 29 pre-existing, 0 in my files). Stable across 3 back-to-back runs (~9s each).

## PHASE 2 — discovery + snapshot (X3, X4, X5, X6) ✅
Added a raw co-resident gossip peer to the harness (`RawGossip` + `topic_id`/`discovery_topic`/
`group_topic`/`pubkey`/`offline_endpoint`/`mesh_provider`) — needed because `groups_exchange` and
group-topic events cannot be sent through the public `XaeroFlux` API, and `peer_introduction` is
only emitted on the discovery topic (never surfaced on a public field).

- `group_topic_auto_subscribe_on_announce` (X3) ✅ — raw peer announces group "g1" via
  `groups_exchange`; bootstrap auto-subscribes to `cyan/group/g1` and relays a group event to its
  own `event_rx` with `source == "group/g1"`. Oracle = bootstrap's `event_rx`.
- `peer_introduction_lists_both_peers` (X4) ✅ — observer announces two peer ids for "g1"; bootstrap
  broadcasts a `peer_introduction` on the discovery topic listing both; observer reads it back.
- `peer_departure_marks_offline` (X5) **#[ignore]** — honest finding: departure is not observable via
  any public oracle in-process. `mark_offline` keys on the gossip **neighbor** id, not the
  `groups_exchange` node_id that populates rosters; `PeerTracker` is private; post-departure
  `peer_introduction` only re-fires while a group still has >1 peer. Reason recorded in the test.
- `snapshot_request_serve_round_trips` (X6, QUIC) **#[ignore]** — genuine engine bug, confirmed
  empirically ("connection lost" in 1.06s, not a timeout): `serve_snapshot` replies on a fresh
  provider-initiated `open_bi()` stream while `download_snapshot` reads the stream **it** opened —
  the two never rendezvous. The bootstrap binary wires no snapshot accept loop, so this path is
  unexercised in production. Precise reason recorded in the test's `#[ignore]`.
- `snapshot_store_preload_and_serve_message` (X6 data model) ✅ — the working slice: preload via
  `update_from_event`, read back via `get_snapshot`, and `handle_request` reports `item_count == 2`.
- GATE: full `cargo test` ✅ (7 pass, 2 ignored, 0 fail), clippy **0 new warnings** (baseline 29).
  X3/X4 stable across 3 back-to-back runs.

## PHASE 3 — reliability + red scaffolds (X7, X8)
(in progress)
</content>
