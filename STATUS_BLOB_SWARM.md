# STATUS — Blob swarm (X8), Wave 2 §9

Branch: `feat/blob-swarm` (off `feat/snapshot-quic-fix`). Do **not** merge to `main`.

Greens the X8 red scaffolds: content-addressed, multi-source file (blob) swarming on
`iroh-blobs` 0.97 — the substrate primitive cyan-backend's plugin/media distribution builds on.
Before this, the real finding held: *"iroh-blobs 0.97 present but not wired"* — the `NetworkActor`
accepted only the gossip ALPN and exposed no blob store / content-addressed fetch / IHave-WhoHas.

## What was wired

New library primitive **`src/swarm.rs` → `xaeroflux::swarm::BlobSwarm`**, shaped exactly like the
existing standalone `SnapshotProvider`/`SnapshotRequester` (a type over an iroh `Endpoint`, additive,
not bolted into the `NetworkActor`). One symmetric type — any node can both serve and fetch.

- **ALPN / store.** Mounts `iroh-blobs`' `BlobsProtocol` on the blobs ALPN (`iroh_blobs::ALPN`,
  re-exported as `xaeroflux::swarm::BLOB_ALPN`) over its own `Router`, backed by an in-memory
  `iroh-blobs` `MemStore`.
- **Content addressing (Blake3 = identity).** `add(bytes) -> Hash` returns the blob's Blake3 hash;
  `iroh-blobs` identifies blobs by that hash, so the hash *is* the content's identity. `Hash` is
  re-exported as `xaeroflux::swarm::Hash` so tests/downstreams need no direct `iroh-blobs` dep.
- **i-have / who-has negotiation over the existing gossip channel.** `SwarmMessage::{IHave, WhoHas}`
  (plain JSON, hashes as hex — like the snapshot protocol's gossip messages). `BlobSwarm` owns the
  negotiation *logic* — a holder registry plus `on_message()` (records an `IHave`; answers a `WhoHas`
  with an `IHave` when it holds the blob) — and is **transport-agnostic**: callers ride the engine's
  existing gossip topics to carry the bytes. `announce()` / `query()` build the messages;
  `holders(hash)` is a node's own observed view of who holds what.
- **Multi-source fetch + resume.** `fetch(hash, holders)` tries the holders in turn over the
  `iroh-blobs` remote get API (Blake3-verified streaming). Because verified chunks are written to the
  store as they arrive and each fetch pulls only the *missing* ranges, a holder dropping mid-transfer
  (or already gone when dialed) just falls through to the next holder, which **resumes** from where
  the last left off. Each dial is bounded (`DIAL_TIMEOUT = 5s`) so a departed holder fails fast
  instead of stalling on its stale address.
- **Integrity gate.** On completion `fetch` recomputes the Blake3 hash of the assembled bytes and
  rejects any mismatch before surfacing the blob (defence-in-depth on top of verified streaming).

No `unwrap()`/`panic!` in the new engine path — all fallible steps use `?`/`map_err`.

## Tests green (`tests/substrate_swarm.rs`) — offline, bounded, own-state oracles

Un-`#[ignore]`d and green; a third test was **added** for the resume guarantee:

1. **`blob_ihave_whohas_negotiates`** — provider `add`s a blob; over a real gossip side channel
   (`RawGossip`, shared `StaticProvider`) the requester broadcasts `WhoHas` until it observes a
   holder. Oracle: the **requester's own holder registry** lists the provider's blob-endpoint id.
2. **`blob_fetched_from_two_providers`** — two providers `add` the *same* 512 KiB bytes → assert one
   shared hash (content addressing). A fresh requester fetches from both holders. Oracle: the
   **requester's own store** holds the blob afterward and the returned bytes hash back to the
   requested identity.
3. **`blob_fetch_resumes_across_holder_churn`** *(added)* — two holders hold the blob; one is
   `close()`d (departed) and listed *first* in the provider set. Oracle: the fetch falls through to
   the survivor and the **requester's own store** holds the verified blob — a dropped holder does not
   fail the download.

All waits are `tokio::time::timeout`; everything is `no_n0_discovery()` + relay-disabled + temp
in-memory stores, addressed out-of-band via the shared in-process `StaticProvider`.

Full suite: `cargo test` green — lib 2, mesh 4 (+1 pre-existing `#[ignore]` X5), reliability 2,
snapshot 2, swarm **3**. No regressions.

## Multi-holder + resume behavior (precise)

- **Multi-source** here means *multi-holder with fallback*: `fetch` is given N holders and pulls the
  blob from whichever can serve, advancing across the set as needed. It is **not** chunk-range
  parallelism across holders.
- **Why not the `Downloader`:** `iroh-blobs`' higher-level `api::downloader::Downloader` (which *does*
  split chunk ranges across holders, `SplitStrategy::Split`) **did not operate in this offline,
  `StaticProvider`-addressed harness** — it bailed `"Unable to download"` for every blob even with
  `SplitStrategy::None`, while the lower-level `store.remote().fetch(conn, hash)` over the *same*
  endpoints transferred the full 512 KiB cleanly (verified by direct diagnostic). So `fetch` uses the
  direct remote API with an explicit holder-fallback loop: simpler, one path, demonstrably working,
  and easy for a new hire to trace — consistent with the repo's simplicity rule. True range-split
  parallelism across holders is a future rung (revisit if/when the `Downloader` works under offline
  static addressing).
- **Resume** is real at holder granularity: verified partial chunks persist in the store, and the
  next holder's fetch resumes the missing ranges. The churn test exercises the connect-time case
  deterministically (a departed holder, bounded dial, fall-through to the survivor).

## Engine seam added (minimal, documented, behavior-preserving)

- **New file `src/swarm.rs`** + one line in `src/lib.rs` (`pub mod swarm;`). Nothing else in the
  library changed. The `xaeroflux_bootstrap` binary is untouched: it never constructs a `BlobSwarm`,
  and the `NetworkActor`'s endpoint / gossip / `Router` / ALPN set are unchanged. The blob swarm is
  an additive, opt-in primitive (same posture as `snapshot.rs`), ready for cyan-backend / the engine
  to mount onto a node's endpoint when plugin/media distribution lands.
- **No dependency bumps:** iroh 0.95 / iroh-blobs 0.97 only. `iroh-blobs` was already a declared
  dependency; this is the first code to use it.

## Clippy

Clippy-neutral. `cargo clippy --all-targets -- -D warnings` reports the **same pre-existing findings
with or without this diff** (verified by stashing): all in `src/lib.rs` / `src/snapshot.rs`
(`collapsible_if` under edition-2024 let-chains, macro-expansion `unwrap`s, dead fields) — unrelated
to blob swarm and the same debt flagged in `STATUS_SNAPSHOT_FIX.md`. The new files
(`src/swarm.rs`, `tests/substrate_swarm.rs`) add **zero** clippy findings. Flagged, not faked; a
separate clippy-hygiene PR should clear the pre-existing debt.
