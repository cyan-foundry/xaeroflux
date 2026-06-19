# xaeroflux — Agent Context

xaeroflux is the **P2P substrate + bootstrap engine** under Cyan: a Rust library
(iroh 0.95 QUIC + gossip, edition 2024) plus the `xaeroflux_bootstrap` binary that
runs as a well-known mesh peer for discovery. cyan-backend and Cyan Lens build on
the same primitives. Read this fully before changing anything.

## THIS IS STABLE, SHIPPING CODE — be careful

The bootstrap node + gossip discovery + snapshot serve are deployed (see cyan-iac).
Treat changes as production surgery.

- **Branch discipline.** Never commit to `main`. Never merge/rebase/force-push it.
  Work on a feature branch; leave PRs/merges to the human. If you're on `main`, STOP.
- **Prefer additive.** Test work adds files under `tests/`. If the test harness
  genuinely needs an engine seam (e.g. exposing the `Endpoint` or an inert
  `StaticProvider` for loopback addressing — exactly what the cyan-backend harness
  needed), keep it **minimal and behavior-preserving**: do not change the signature
  or behavior of any public API the `xaeroflux_bootstrap` binary relies on. Document
  any such seam in STATUS.
- **No `unwrap()`/`panic!`** in engine paths; use `?`/`map_err`.
- **iroh 0.95 only.** No 1.x APIs; do not bump iroh / iroh-gossip. (`iroh-blobs` 0.97
  is present but swarming is not yet integrated — that's a future rung, scaffold red.)
- **Small, reviewable diffs.** One concern per commit.

## Build & test

```bash
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
```

## Substrate test discipline (tests/substrate_*.rs, tests/support/)

- **`XAEROFLUX_TEST_SPEC.md` is the contract.** Don't edit it to make code pass; never weaken an assertion.
- **Bounded waits only** — every wait is a `tokio::time::timeout` with a clear failure; never an unbounded `recv()`, never `sleep`-as-sync.
- **Offline/local only** — tests use `no_n0_discovery()` and in-memory / temp DBs; never depend on a public relay or n0 DNS. A test that needs the internet is a bug.
- **Assert on each node's own observed state** (its `event_rx`, its peer tracker, its snapshot result) — not on log lines.
- **If the engine lacks a capability a test needs**, that's a real finding: note it, mark the test `#[ignore]` with the reason, keep going — don't fake a pass.

## Layout

`src/lib.rs` (the `XaeroFlux` builder/struct, `NetworkActor`, `PeerTracker`,
`StorageActor`), `src/snapshot.rs` (`SnapshotProvider`/`SnapshotRequester`,
`GroupSnapshot`), `src/bin/xaeroflux_bootstrap.rs` (the deployed bootstrap peer).
