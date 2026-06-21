# STATUS — xaeroflux hygiene (quarantine dead `snapshot.rs`)

Branch: `feat/xaeroflux-hygiene` (off `feat/blob-swarm` tip). **No code changed.**

## TL;DR — STOPPED, did not quarantine

The dead-code finding in MESH_HARDENING_SPEC.md §0b is **only partially correct**. `snapshot.rs`
is indeed unwired from the **deployed bootstrap binary and the production path** — but it is **NOT
free-floating dead code**: it is the lib public API exercised by the **X6 contract test**
(`snapshot_request_serve_round_trips`) documented in `XAEROFLUX_TEST_SPEC.md`, and the last several
commits actively fixed it and made X6 green.

Per the task's explicit guardrail ("If the grep reveals it IS used somewhere unexpected, STOP and
just write the finding in STATUS — do not force-remove") I changed **nothing**. Removing or gating
it off-by-default would break/disable the X6 contract test and **weaken `XAEROFLUX_TEST_SPEC.md`**,
which the substrate test discipline forbids.

## Fresh grep result

`grep -rn "SnapshotProvider|serve_snapshot|update_from_event|SnapshotRequester"` outside
`src/snapshot.rs` returns:

- **`tests/substrate_snapshot.rs`** — the X6 round-trip test. Actively consumes the lib public API:
  `use xaeroflux::snapshot::{SnapshotMessage, SnapshotProvider, SnapshotRequester, SNAPSHOT_ALPN};`
  and drives `update_from_event` (preload) + `serve_snapshot` (serve) + `download_snapshot` (fetch).
- **`src/swarm.rs`** — comments only (design notes referencing the snapshot primitive shape). No
  code dependency.

Additional confirming greps:

- **Bootstrap binary (`src/bin/xaeroflux_bootstrap.rs`)**: the only "snapshot" tokens are the
  `NetworkEvent::GroupSnapshotAvailable` gossip-event *variant* (decl line 61), which the bin
  **explicitly ignores** at line 466 (`NetworkEvent::GroupSnapshotAvailable { .. } => None`). The bin
  does NOT reference the `snapshot` module, `SnapshotProvider`, `serve_snapshot`, `SnapshotRequester`,
  `update_from_event`, or `SNAPSHOT_ALPN`. → §0b's *production* claim ("bootstrap is thin discovery,
  ignores `GroupSnapshotAvailable`") is TRUE.
- **No non-test `src/` code** references the snapshot API at all (comments excluded).

## Why this overturns "remove or gate"

`XAEROFLUX_TEST_SPEC.md` lists X6 as a first-class contract row:

> | X6 | **Snapshot round-trip** — a provider with a preloaded `GroupSnapshot` serves it to a
> requester over direct QUIC. |
> `snapshot_request_serve_round_trips` (X6) — provider preloads a `GroupSnapshot` for "g1";
> requester `needs_snapshot` → connects → receives; assert non-empty workspaces/state.

And the recent git history shows this is **actively maintained**, not abandoned:

- `d5b12b5 fix(snapshot): serve_snapshot replies on the accepted bi-stream`
- `c4bfe93 docs: STATUS_SNAPSHOT_FIX — serve_snapshot fix, X6 green`

So the module was *just debugged and greened*. Quarantining it now (removal **or**
`#[cfg(feature = "legacy_snapshot")]` off-by-default) would:

1. Break/disable the X6 contract test (`tests/substrate_snapshot.rs` would not compile by default),
2. Force editing/gating `XAEROFLUX_TEST_SPEC.md` to keep `cargo test` green — i.e. **weaken the
   contract**, explicitly forbidden by both CLAUDE.md and the task,
3. Undo the work in `d5b12b5`/`c4bfe93`.

Also, by the task's own quarantine criterion ("if truly unused by the `xaeroflux_bootstrap` binary
**and the lib's public API**"), `snapshot.rs` does **not** qualify: `xaeroflux::snapshot` IS the lib
public API and it IS used — by the X6 test.

## Reconciling with §0b

§0b is right that the **bootstrap stays thin** and serves no snapshots — nothing here changes that;
the deployed bin already ignores `GroupSnapshotAvailable`. The one inaccuracy is "referenced nowhere
outside itself (only a comment in `swarm.rs`)": it overlooked the X6 contract test in
`tests/substrate_snapshot.rs`. `snapshot.rs` is **test-covered substrate primitive**, dead only with
respect to the *deployed* path — not dead in the build/test sense.

## Recommendation

Leave `snapshot.rs` in place for now. It is harmless to the deployed bootstrap (unwired, the bin
ignores the event) and is needed to keep X6 green. Genuine quarantine should happen **together with
a test-spec decision** (retire the X6 row, or move the snapshot primitive + its test to wherever the
real cyan-backend snapshot engine lives) — a contract change that belongs to the human owner, not a
behavior-preserving hygiene pass.

## Verify

- `cargo build` — green (no code changed). Bootstrap bin (`cargo build --bin xaeroflux_bootstrap`)
  builds green; pre-existing dead-code warnings are unrelated to `snapshot.rs`.
- `cargo test` / `cargo clippy` — not re-run, since no code was modified; the tree is the
  `feat/blob-swarm` tip which was already green per the prior STATUS.

## Net change

Documentation only: this STATUS file. No source, no `Cargo.toml`, no test, no spec touched.
