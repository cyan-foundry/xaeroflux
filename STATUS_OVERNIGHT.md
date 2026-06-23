# STATUS — xaeroflux substrate test overnight run

Branch: **`feat/bootstrap-tests`** off `feat/bootstrap-wip`. `main` never touched (see end).
All work additive under `tests/` + a minimal, opt-in, behavior-preserving engine seam.

## Result summary

`cargo test`: **9 passed, 4 ignored, 0 failed.** `cargo clippy --all-targets`: **0 new warnings**
from this diff (repo carries 29 pre-existing warnings; my files add none). Fully offline — no n0 DNS,
no relay, no mDNS multicast; addressing is out-of-band via a shared in-process `StaticProvider`.

| ID | Test | File | Status | Notes |
|----|------|------|--------|-------|
| X1 | `mesh_bootstrap_forms` | substrate_mesh | ✅ green | 3 distinct identities + propagation proves formation |
| X2 | `event_propagates_to_all_peers` | substrate_mesh | ✅ green | reaches bootstrap + peer_b, exactly-once (dedup) |
| X3 | `group_topic_auto_subscribe_on_announce` | substrate_mesh | ✅ green | raw-gossip injects `groups_exchange` + group event; bootstrap relays to `event_rx` as `source="group/g1"` |
| X4 | `peer_introduction_lists_both_peers` | substrate_mesh | ✅ green | bootstrap broadcasts `peer_introduction` on discovery topic listing both peers; observed by a raw peer |
| X5 | `peer_departure_marks_offline` | substrate_mesh | ⏸ ignored | departure not observable via any public oracle in-process (see below) |
| X6 | `snapshot_request_serve_round_trips` (QUIC) | substrate_snapshot | ⏸ ignored | genuine engine protocol mismatch (see below) |
| X6 | `snapshot_store_preload_and_serve_message` (data model) | substrate_snapshot | ✅ green | preload → `get_snapshot` → `handle_request` item_count = 2 |
| X7 | `repeat_mesh_forms_is_stable` | substrate_reliability | ✅ green | 15× form/teardown, each converges within T |
| X7 | `concurrent_meshes_do_not_interfere` | substrate_reliability | ✅ green | 2 meshes converge concurrently + no cross-mesh leak |
| X8 | `blob_ihave_whohas_negotiates` | substrate_swarm | ⏸ ignored | iroh-blobs not integrated |
| X8 | `blob_fetched_from_two_providers` | substrate_swarm | ⏸ ignored | iroh-blobs not integrated |

Reliability numbers: `repeat_mesh_forms_is_stable` = 15 iterations, all green, ~73s total
(≈4.5s/iteration). `concurrent_meshes_do_not_interfere` converges well within T. `scripts/reliability.sh`
loops the mesh + reliability suites N× (default 20), failing on first red.

## Ignored tests — honest findings (never faked, precise reasons in-test)

- **X5 peer departure** — not observable through any public substrate oracle in-process. The engine
  marks a peer offline only on a gossip `NeighborDown` for the discovery topic, and
  `PeerTracker::mark_offline` keys on the **departing neighbor's** node_id — not the node_ids carried
  in `groups_exchange` payloads (which is what populates group rosters). `PeerTracker` is private, and
  the post-departure `peer_introduction` is re-broadcast only while a group still has >1 peer. There
  is no public, positively-assertable departure signal. Re-enable if the engine exposes peer-tracker
  state or emits an explicit departure event.

- **X6 snapshot QUIC round-trip** — genuine engine bug, confirmed empirically ("connection lost" in
  1.06s, not a timeout). `SnapshotRequester::download_snapshot` opens a bi-stream, writes the group_id,
  and reads the reply **on the recv half of that same stream**; `SnapshotProvider::serve_snapshot`
  instead writes the snapshot on a **new, provider-initiated `conn.open_bi()` stream** and never writes
  to the accepted stream, then returns (dropping the connection). The two halves never rendezvous. The
  `xaeroflux_bootstrap` binary wires no snapshot accept loop, so this path is unexercised in production.
  Fix: have `serve_snapshot` reply on the **accepted** stream (`accept_bi`) instead of opening its own.
  The in-memory snapshot model itself works and is covered green by `snapshot_store_preload_and_serve_message`.

- **X8 blob swarming (×2)** — `iroh-blobs` 0.97 is a declared dependency but not wired: the
  `NetworkActor`'s Router accepts only `iroh_gossip::ALPN`, and the engine exposes no content-addressed
  blob store, no multi-provider fetch, and no IHave/WhoHas API. Red scaffolds for a future rung.

## Per-node DB / identity isolation in-process — YES

Each node is built with its **own unique temp directory** (`spawn_local_node` / `offline_endpoint`),
so each gets its own `node.key` and its own SQLite DB. This was **required**, not optional: the engine
persists identity to `node.key` in the **parent dir of `db_path`** (lib.rs `from_config`), so a shared
`:memory:` path would have parent `.` and every in-process node would load the same `./node.key` and
collapse to one identity. The per-temp-dir scheme is proven by `mesh_bootstrap_forms` asserting three
**distinct** node_ids. Assertions are made on each node's **own `event_rx`** (the public oracle) rather
than its DB, because the DB and `PeerTracker` are not exposed through the public API.

## Engine seam — minimal, additive, behavior-preserving

Added to `src/lib.rs` (only — `src/snapshot.rs` and `src/bin/xaeroflux_bootstrap.rs` untouched):

1. `XaeroFluxConfig`: two new fields — `relay_disabled: bool` (default `false`),
   `static_provider: Option<StaticProvider>` (default `None`).
2. `XaeroFluxBuilder`: two new opt-in methods — `disable_relay()` → `RelayMode::Disabled`;
   `static_provider(StaticProvider)` → adds an extra discovery service.
3. `XaeroFlux`: new `pub endpoint: Endpoint` field (a clone of the endpoint the `NetworkActor` already
   built), so a harness can read each node's `EndpointAddr` for out-of-band addressing.
4. `NetworkActor::new`: a `relay_disabled` branch before the existing relay logic, and a
   `if let Some(provider) = config.static_provider { builder.discovery(provider) }` after the mdns block.

**Proof it's behavior-preserving for production:** every new behavior is gated behind opt-in builder
methods that default to off. The `xaeroflux_bootstrap` binary calls only
`discovery_key`/`db_path`/`relay_url`/`no_n0_discovery` — none of the new methods — so its config has
`relay_disabled = false` and `static_provider = None`, yielding identical endpoint construction (same
`relay_mode` selection, same discovery set) as before. The exposed `endpoint` is a clone of the same
Arc-backed endpoint the actor already created and drives; reading it changes nothing. **No public
signature the binary uses was changed.** The two pre-existing lib tests (`basic_integration_sanity`,
`builder_with_custom_relay`) still pass unchanged, and `cargo build` of the binary is warning-clean
relative to baseline.

`Cargo.toml`: added a `[dev-dependencies]` block (tokio, anyhow, serde, serde_json, blake3, bytes,
futures, iroh, iroh-gossip) — compiled only for tests; does not affect the library or the binary.

## Harness (`tests/support/mod.rs`)

`spawn_local_node`, `wait_for_event`, `unique_key`, `T` (per spec) + `make_event`, `count_events`,
`establish_mesh` (the formation oracle), `offline_endpoint`, `mesh_provider`, `RawGossip` (a raw
co-resident gossip peer for discovery-topic injection/observation), and topic-id helpers
(`topic_id`/`discovery_topic`/`group_topic`/`pubkey`). All waits are bounded `tokio::time::timeout`s.

## main was never touched

`main` == `origin/main` == `0757aa3` (unchanged). The three phase commits
(`phase1`/`phase2`/`phase3`) are all on `feat/bootstrap-tests`, which descends from
`feat/bootstrap-wip` → `main`. No merge/rebase/force-push; no PR opened. `feat/bootstrap-tests` was
pushed to origin after each green phase.
