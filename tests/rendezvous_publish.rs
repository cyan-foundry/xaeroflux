// Rendezvous self-publish (SUPER_PEER_COMPLETION_SPEC §5), fully offline.
//
// The bootstrap, on start, writes a SIGNED rendezvous config advertising itself so apps discover
// it instead of hardcoding its node_id. These tests exercise the exact pieces the bootstrap binary
// composes — `XaeroFlux::signed_rendezvous_config` + the `FileSink` default — with NO network:
// nodes are built offline (no n0 / mDNS / relay) and the sink is a local file.
//
// Discipline (XAEROFLUX_TEST_SPEC): offline only, bounded waits, and every assertion is on the
// node's OWN observed state (its node_id, its bound endpoint address, the bytes it published).

#![allow(clippy::disallowed_methods)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use xaeroflux::XaeroFlux;
use xaeroflux::rendezvous::{SignedRendezvousConfig, FileSink, publish_signed, verify_config};

const RELAY: &str = "https://quic.dev.cyan.blockxaero.io";
const TS: u64 = 1_700_000_000;

/// Unique temp dir per node so each gets its own persisted identity (`node.key`) + DB.
fn unique_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("xaeroflux-rdv-{tag}-{nanos}"));
    std::fs::create_dir_all(&dir).expect("create node temp dir");
    dir
}

/// Build a fully-offline node and wait (bounded) for its bound endpoint to report a direct address.
async fn build_offline_node(dir: &Path, key: &str) -> XaeroFlux {
    let db_path = dir.join("node.db");
    let xf = XaeroFlux::builder()
        .discovery_key(key)
        .db_path(db_path.to_string_lossy().to_string())
        .no_n0_discovery()
        .no_mdns()
        .disable_relay()
        .build()
        .await
        .expect("build offline node");

    for _ in 0..100 {
        if !xf.endpoint.addr().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    xf
}

/// Publish `xf`'s signed rendezvous config to `path` (mirrors what the bootstrap binary does).
fn publish_to(xf: &XaeroFlux, path: &Path) -> SignedRendezvousConfig {
    let signed = xf
        .signed_rendezvous_config("test", Some(RELAY.to_string()), TS)
        .expect("build signed rendezvous config");
    let sink = FileSink::new(path);
    publish_signed(&sink, &signed).expect("publish signed config");
    signed
}

fn read_published(path: &Path) -> SignedRendezvousConfig {
    let bytes = std::fs::read(path).expect("read published config");
    serde_json::from_slice(&bytes).expect("parse published config")
}

#[tokio::test]
async fn bootstrap_publishes_rendezvous_config_on_start() {
    let dir = unique_dir("publish");
    let xf = build_offline_node(&dir, "cyan-test").await;
    let path = dir.join("rendezvous.json");

    // Nothing published before "start".
    assert!(!path.exists(), "config should not exist before publish");

    publish_to(&xf, &path);

    // The well-known file now exists and round-trips to a signed config for THIS node.
    assert!(path.exists(), "publish-on-start must write the rendezvous file");
    let published = read_published(&path);
    assert_eq!(published.config.bootstrap.node_id, xf.node_id);
    assert_eq!(published.config.discovery_key, "cyan-test");
}

#[tokio::test]
async fn config_is_signed_and_verifiable() {
    let dir = unique_dir("signed");
    let xf = build_offline_node(&dir, "cyan-test").await;
    let path = dir.join("rendezvous.json");

    publish_to(&xf, &path);
    let published = read_published(&path);

    // Self-published: the signer is this node, and the signature verifies.
    assert_eq!(published.signer, xf.node_id, "self-published config is signed by the node itself");
    verify_config(&published).expect("published config must verify");

    // A tampered config must NOT verify.
    let mut tampered = published.clone();
    tampered.config.bootstrap.addr = vec!["10.0.0.1:9999".to_string()];
    assert!(verify_config(&tampered).is_err(), "tampered config must fail verification");
}

#[tokio::test]
async fn config_carries_real_node_id_addr_relay_discovery_key() {
    let dir = unique_dir("carries");
    let xf = build_offline_node(&dir, "cyan-prod").await;
    let path = dir.join("rendezvous.json");

    publish_to(&xf, &path);
    let published = read_published(&path);
    let cfg = &published.config;

    // Real node identity + mesh key the node actually serves.
    assert_eq!(cfg.bootstrap.node_id, xf.node_id);
    assert_eq!(cfg.discovery_key, "cyan-prod");
    // The configured relay is carried through.
    assert_eq!(cfg.relay_url.as_deref(), Some(RELAY));
    // Real, dialable direct addresses from the bound endpoint.
    assert!(!cfg.bootstrap.addr.is_empty(), "offline node should bind at least one direct address");
    for a in &cfg.bootstrap.addr {
        a.parse::<SocketAddr>().unwrap_or_else(|_| panic!("addr `{a}` should be a real SocketAddr"));
    }
    // The advertised addresses are the node's OWN bound addresses.
    let observed: Vec<String> = xf.endpoint.addr().ip_addrs().map(|s| s.to_string()).collect();
    assert_eq!(cfg.bootstrap.addr, observed, "config must carry the node's own bound addresses");
}

#[tokio::test]
async fn republish_on_restart_reflects_new_identity() {
    // Same well-known sink path across a redeploy; a fresh identity (new node.key) must be reflected.
    let dir = unique_dir("restart");
    let path = dir.join("rendezvous.json");

    // First boot: identity A.
    let dir_a = dir.join("a");
    std::fs::create_dir_all(&dir_a).expect("dir a");
    let xf_a = build_offline_node(&dir_a, "cyan-test").await;
    publish_to(&xf_a, &path);
    let id_a = read_published(&path).config.bootstrap.node_id.clone();
    assert_eq!(id_a, xf_a.node_id);
    drop(xf_a);

    // Redeploy with a fresh key dir → new identity B, republished to the same path (overwrite).
    let dir_b = dir.join("b");
    std::fs::create_dir_all(&dir_b).expect("dir b");
    let xf_b = build_offline_node(&dir_b, "cyan-test").await;
    publish_to(&xf_b, &path);
    let published_b = read_published(&path);

    assert_ne!(id_a, xf_b.node_id, "a fresh key must yield a new node_id");
    assert_eq!(published_b.config.bootstrap.node_id, xf_b.node_id, "republish reflects the new identity");
    assert_eq!(published_b.signer, xf_b.node_id);
    verify_config(&published_b).expect("republished config must verify under the new key");
}
