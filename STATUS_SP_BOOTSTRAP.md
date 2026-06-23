# STATUS — Self-published signed rendezvous config (SUPER_PEER_COMPLETION_SPEC §5)

**Branch:** `feat/sp-bootstrap-config` (off the current tip `feat/xaeroflux-hygiene`,
which is `feat/blob-swarm` + the snapshot-quarantine hygiene commit — strictly ahead,
nothing dropped). **Additive + behavior-preserving.** Never touched `main`.

## Problem

Apps had to **hardcode the bootstrap `node_id`** to find the mesh. Every redeploy that
rotated `node.key` meant a per-app retune. §5 fix: the bootstrap, which on start already
knows its own `node_id` + `EndpointAddr` + `discovery_key` + relay, **self-publishes a
signed rendezvous config** to a well-known location. Apps fetch it, verify the signature,
and pin the `node_id` — no hardcoding, no per-deploy retune.

## Config shape

Published JSON is a `SignedRendezvousConfig`:

```json
{
  "config": {
    "env": "dev",
    "discovery_key": "cyan-dev",
    "bootstrap": {
      "node_id": "<hex ed25519 public key>",
      "addr": ["192.168.1.10:51234", "127.0.0.1:51234"]
    },
    "relay_url": "https://quic.dev.cyan.blockxaero.io",
    "ts": 1700000000
  },
  "signer": "<hex ed25519 public key of the signing node>",
  "signature": "<hex ed25519 signature over canonical JSON of `config`>"
}
```

- `bootstrap.node_id` — what apps pin.
- `bootstrap.addr` — the node's **own** bound direct socket addresses (from `endpoint.addr()`).
- `relay_url` — the configured relay (falls back to a relay observed on the endpoint addr).
- `ts` — publish timestamp (seconds); newer wins.

## Signing

- The bootstrap signs with its **own node key** (the same ed25519 `SecretKey` whose public
  half *is* the `node_id`). So a self-published config is self-certifying:
  `signer == config.bootstrap.node_id`.
- Verification: parse `signer` as a `PublicKey`, decode the hex `signature`, verify it over
  the canonical JSON bytes of `config`. `verify_config()` returns `Ok(())` only if it
  validates; a tampered config or wrong signer fails.
- App trust model: TOFU-pin `node_id` on first fetch, or compare `signer` against an
  org-pinned key distributed out-of-band. (No separate org key is required — the node
  self-signs — but the `signer` field makes an org-key check a one-liner if desired.)

## Sink

`trait ConfigSink { fn publish(&self, bytes: &[u8]) -> Result<()> }` — kept small and
network-free so the default is testable offline. Default impl is `FileSink` (writes the
JSON to a local path, creating the parent dir). A deploy that wants a PUT-URL / object-store
target adds one `impl ConfigSink` without touching the publish-on-start path.

## Publish-on-start (the bootstrap binary)

`src/bin/xaeroflux_bootstrap.rs`, immediately after the node reports running:

```
publish_rendezvous(&xf, env, relay_url, path)
  = xf.signed_rendezvous_config(env, relay_url, now) -> FileSink(path) -> publish_signed
```

- Runs **once on every (re)start**, so a fresh `node.key` / redeploy is reflected
  automatically — no per-deploy retune.
- **Non-fatal:** a publish failure is logged and the bootstrap keeps serving
  discovery / gossip / snapshots. Nothing in the deployed hot path changed.

New env vars (both optional, sensible defaults):
- `RENDEZVOUS_PATH` — where to write the signed config. Default: `<DB_PATH parent>/rendezvous.json`.
- `XAEROFLUX_ENV` — the `env` label in the config. Default: `dev`.

## What the deploy must do

1. Let the bootstrap write `RENDEZVOUS_PATH` (default `/opt/cyan/data/rendezvous.json`).
2. **Upload / serve that file at a well-known URL** (object store, or a static path the
   relay/edge already serves). It's just a JSON blob — no app needs the bootstrap's secret.
3. Apps fetch the URL, call the equivalent of `verify_config`, and pin
   `config.bootstrap.node_id` (+ dial `addr` / `relay_url`).
4. On redeploy, nothing to retune: the bootstrap re-signs and re-writes on start; re-upload
   (or have the deploy script re-push the file) and apps pick up the new identity.

## Engine seam (documented per CLAUDE.md)

`XaeroFlux` gained:
- a **private** `secret_key: SecretKey` field (cloned before it moves into the
  `NetworkActor`; never exposed to callers), and
- `pub fn signed_rendezvous_config(&self, env, relay_url, ts) -> Result<SignedRendezvousConfig>`
  which reads `node_id` + `discovery_key` from the node, direct addrs from the bound
  endpoint, and signs with the node's own key.

The secret key is **not** exposed — the only thing external code can do is ask the node to
sign the rendezvous config it already advertises. No public API the `xaeroflux_bootstrap`
binary relies on changed signature or behavior. `src/rendezvous.rs` is a new pure module
(`pub mod rendezvous`).

Added dependency: `hex = "0.4"` (already in `Cargo.lock` transitively) for hex
encode/decode of the signature in the published JSON.

## Tests (offline, file-writer sink) — green

`tests/rendezvous_publish.rs` (nodes built fully offline: `no_n0_discovery` / `no_mdns` /
`disable_relay`; bounded waits; assertions on each node's own observed state):

- `bootstrap_publishes_rendezvous_config_on_start` — file absent before, written after;
  round-trips to a signed config for this node.
- `config_is_signed_and_verifiable` — `signer == node_id`, `verify_config` passes; a
  tampered config fails.
- `config_carries_real_node_id_addr_relay_discovery_key` — carries the real `node_id`,
  `discovery_key`, configured `relay_url`, and the node's **own** bound `SocketAddr`s.
- `republish_on_restart_reflects_new_identity` — a fresh key dir yields a new `node_id`;
  republishing to the same sink overwrites it and verifies under the new key.

Plus 3 unit tests in `src/rendezvous.rs` (sign/verify roundtrip, tampered-config reject,
wrong-signer reject).

## Verify

- `cargo build` — clean (lib + `xaeroflux_bootstrap` bin).
- `cargo test` — all green (4 new integration + 3 new unit + existing suites; 1 pre-existing
  `#[ignore]` unchanged).
- `cargo clippy --all-targets -- -D warnings` — **my code (`src/rendezvous.rs`, the binary
  changes, the lib seam) adds ZERO findings.** The pre-existing clippy debt in
  `src/snapshot.rs` (quarantined dead code) and the `NetworkActor` discovery paths in
  `src/lib.rs` remains, exactly as documented in `STATUS_BLOB_SWARM.md` /
  `STATUS_SNAPSHOT_FIX.md` — a separate clippy-hygiene PR, flagged not faked.

## Behavior preservation

The deployed bootstrap's discovery / gossip / snapshot-serve / Iggy-forwarding loop is
untouched. The only addition is a single publish-on-start block (non-fatal) and a private
field + method on `XaeroFlux`. Existing tests stay green.
