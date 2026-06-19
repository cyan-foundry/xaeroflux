# Overnight run — xaeroflux bootstrap/mesh substrate tests

Unattended overnight work in the xaeroflux repo. Separate repo from cyan-backend —
no collision. Paste into a SECOND `claude --dangerously-skip-permissions` session
from `~/xaeroflux` and leave it.

---

You are hardening the xaeroflux substrate with tests, unattended. Read `CLAUDE.md`
and `XAEROFLUX_TEST_SPEC.md` first. Work the PHASES in order; gate between them;
commit per green phase; stop clean on red.

## Standing rules (absolute)
- **Branch:** PHASE 0 creates `feat/bootstrap-tests` off the current branch
  (`feat/bootstrap-wip`). NEVER `main`; never merge/rebase/force-push; no PR.
- **Additive-first.** Add files under `tests/`. If the harness genuinely needs an
  engine seam for address resolution, keep it **minimal, additive, behavior-
  preserving** (expose the `Endpoint` / add an inert `StaticProvider`, like the
  cyan-backend harness did) — never change a public signature the
  `xaeroflux_bootstrap` binary uses. If a change would be bigger than that, STOP + STATUS.
- **Offline only:** every node uses `no_n0_discovery()` + temp/in-mem db. Never
  depend on a public relay or n0 DNS. Bounded `tokio::time::timeout` waits only.
- **Out of scope:** the Iggy forwarding path in the bootstrap binary — do not test it.
- **Gate** = `cargo build` ✅, `cargo test` ✅ (ignored stay ignored),
  `cargo clippy --all-targets -- -D warnings` introduces **zero new** warnings from
  your diff. ≤6 honest attempts per phase, else STOP + write `STATUS_OVERNIGHT.md`.
- Commit after each green phase (`git add -A && commit`); push `feat/bootstrap-tests`.
  Keep `PROGRESS.md` updated. Never weaken an assertion or edit the spec.

## PHASE 0 — branch + baseline
1. `git rev-parse --abbrev-ref HEAD` should be `feat/bootstrap-wip`. Create
   `git checkout -b feat/bootstrap-tests`. Verify you're NOT on main.
2. Baseline `cargo build` + `cargo test` green as-is. If not, STOP + STATUS (start from green).
   Record baseline in PROGRESS.md.

## PHASE 1 — harness + core mesh (X1, X2) — the guaranteed win
1. Implement `tests/support/mod.rs` (`spawn_local_node`, `wait_for_event`,
   `unique_key`, `T`) per the spec, mirroring the in-`lib.rs` `#[tokio::test]` spin
   pattern. Resolve addresses via mDNS; if flaky, add the minimal StaticProvider seam.
2. `tests/substrate_mesh.rs`: `mesh_bootstrap_forms` + `event_propagates_to_all_peers` green.
GATE → commit "phase1: xaeroflux harness + mesh forms + event propagation".

## PHASE 2 — discovery + snapshot (X3, X4, X5, X6)
- Add to `tests/substrate_mesh.rs`: `group_topic_auto_subscribe_on_announce`,
  `peer_introduction_lists_both_peers`, `peer_departure_marks_offline`
  (`#[ignore]` with reason if departure isn't observable in-process).
- `tests/substrate_snapshot.rs`: `snapshot_request_serve_round_trips`.
GATE → commit "phase2: discovery + snapshot round-trip".

## PHASE 3 — reliability + red scaffolds (X7, X8)
- `tests/substrate_reliability.rs`: `repeat_mesh_forms_is_stable` (15×),
  `concurrent_meshes_do_not_interfere`. Add `scripts/reliability.sh` (loop N=20, fail on first red).
- `tests/substrate_swarm.rs`: blob-swarming named tests as `#[ignore]` + reason (iroh-blobs not integrated).
GATE → commit "phase3: reliability + swarm red scaffolds".

## FINISH
Write `STATUS_OVERNIGHT.md`: per-test green/ignored + reasons, reliability numbers,
whether per-node DB isolation worked in-process, any minimal engine seam you added
(and proof it's behavior-preserving), and confirm `main` was never touched. End session.
