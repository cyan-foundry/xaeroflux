# STATUS — Track A: snapshot QUIC round-trip fix

Branch: `feat/snapshot-quic-fix` (off `feat/bootstrap-tests`). Not merged to main.

## The bug

`SnapshotRequester::download_snapshot` opens a bi-stream (`conn.open_bi()`), writes
the length-prefixed `group_id`, finishes its send half, then reads the reply **on the
recv half of that same stream**.

`SnapshotProvider::serve_snapshot` instead replied on a *fresh, provider-initiated*
`conn.open_bi()` stream and never touched the stream the requester opened, then
returned and dropped the connection. The two halves never rendezvoused, so the
round-trip failed fast with "connection lost" (~1s — a real failure, not a timeout).

The `xaeroflux_bootstrap` binary wires no snapshot accept loop and never calls
`serve_snapshot`, so this QUIC path was unexercised in production — the round-trip
test (X6) was correctly `#[ignore]`d with this exact diagnosis.

## What changed

`src/snapshot.rs` — `serve_snapshot` now writes the reply on the **send half of the
accepted stream** rather than opening its own:

- Signature: `serve_snapshot(&self, conn: Connection, group_id)` →
  `serve_snapshot(&self, mut send: SendStream, group_id)`.
- Removed the `let (mut send, _recv) = conn.open_bi().await?;` line. The accept loop
  obtains `(send, recv)` from `conn.accept_bi()` (accepting the stream the requester
  opened), reads the `group_id` off `recv`, and hands `send` to `serve_snapshot`,
  which writes the length-prefixed snapshot there.
- Import: `iroh::endpoint::Connection` → `iroh::endpoint::SendStream` (Connection was
  no longer named in the file).

This is behavior-preserving for the deployed bootstrap peer: it never calls
`serve_snapshot`. The seam is documented here per CLAUDE.md.

`tests/substrate_snapshot.rs`:

- `read_requested_group` now returns `(SendStream, String)` — the send half of the
  accepted stream plus the requested group_id — so the reply rides back on the stream
  the requester is reading from.
- The accept loop passes that send half to `serve_snapshot`, then holds the
  connection open with `conn.closed().await` until the requester has read the reply
  and closed, so the finished stream's bytes aren't lost to an eager connection drop.
- `snapshot_request_serve_round_trips` (X6) un-`#[ignore]`d; stale bug-description
  doc comment replaced with the current behavior. All waits remain bounded
  (`tokio::time::timeout`, 10s); offline endpoints only.

No engine-path `unwrap()`/`panic!` introduced (`?`/`map_err` throughout).

## Test status — green

- `cargo build` — ok.
- `substrate_snapshot`: both tests pass (`snapshot_request_serve_round_trips`,
  `snapshot_store_preload_and_serve_message`). Round-trip re-run 5× for stability —
  green every time (~1.05s each), confirming no connection-drop race.
- `substrate_mesh`: 4 passed, 1 ignored (peer_departure — pre-existing).
- `substrate_reliability`: 2 passed (15× mesh form/teardown loop).
- `substrate_swarm`: 2 ignored (future blob-swarm red scaffolds — pre-existing).
- lib unit tests: 2 passed. Bootstrap bin: 0 tests.

## Clippy

This change is **clippy-neutral**: `cargo clippy --all-targets -- -D warnings`
reports the **same 19 findings with or without this diff** (verified by stashing the
change and re-running). None are introduced here — no new warnings.

Those 19 are **pre-existing** on the base branch under the current (stricter) clippy
toolchain, all in code unrelated to this bug:

- 4× `disallowed_methods` (unwrap) in `src/lib.rs` — these are emitted *inside*
  `serde_json::json!` macro expansions, not author-written unwraps (the same
  macro-expansion quirk already documented at the top of
  `tests/substrate_snapshot.rs`).
- 6× `collapsible_if` (let-chains) in `src/lib.rs` / `src/snapshot.rs::update_from_event`.
- 2× unused imports (`Duration`, `Bytes`) and 2× dead fields in `src/lib.rs` /
  `src/snapshot.rs`.

Clearing them would mean touching `lib.rs` engine paths the bootstrap binary relies
on (blanket `#[allow]`s or restructuring) — out of scope for this one located bug and
against the "minimal, behavior-preserving, one concern per commit" rule. Left as a
separate clippy-hygiene concern for a dedicated PR; flagged here rather than faked
green.
