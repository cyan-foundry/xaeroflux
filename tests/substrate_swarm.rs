// X8 — blob swarming (iroh-blobs IHave/WhoHas distribution). RED SCAFFOLDS — not yet integrated.
//
// `iroh-blobs` 0.97 is a declared dependency, but the engine does not wire it: the `NetworkActor`
// accepts only `iroh_gossip::ALPN` (+ the `xsp-1.0` ALPN on the endpoint) on its Router, and exposes
// no blob store, no content-addressed fetch, and no IHave/WhoHas negotiation. These tests name the
// future guarantees and stay `#[ignore]`d with a precise reason until swarming is integrated.

/// X8 — a provider advertises a blob (IHave), a requester asks (WhoHas), and they negotiate transfer.
#[tokio::test]
#[ignore = "iroh-blobs swarming not integrated: engine wires no blobs ALPN/protocol on its Router and exposes no IHave/WhoHas API. Future rung."]
async fn blob_ihave_whohas_negotiates() {
    // Scaffold only — see module comment. No engine capability to exercise yet.
}

/// X8 — a blob is fetched from two providers concurrently (swarming/multi-source).
#[tokio::test]
#[ignore = "iroh-blobs swarming not integrated: engine exposes no content-addressed blob store or multi-provider fetch. Future rung."]
async fn blob_fetched_from_two_providers() {
    // Scaffold only — see module comment. No engine capability to exercise yet.
}
