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

## PHASE 2 — discovery + snapshot (X3, X4, X5, X6)
(in progress)
</content>
