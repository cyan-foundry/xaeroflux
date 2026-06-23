//! Self-published, signed rendezvous config (SUPER_PEER_COMPLETION_SPEC §5).
//!
//! On startup the bootstrap node already knows its own identity: `node_id`
//! (== its ed25519 public key), its dialable `EndpointAddr`, its `discovery_key`,
//! and its relay. Instead of every app hardcoding the bootstrap `node_id`, the
//! bootstrap **publishes a signed rendezvous config** to a configured sink (a local
//! file the deploy uploads, or — via the [`ConfigSink`] trait — a PUT URL / object
//! key). Apps fetch it, verify the signature, and pin the `node_id`.
//!
//! The bootstrap signs with its **own node key**, so a self-published config is
//! self-certifying: `signer == config.bootstrap.node_id`. Apps that already pin the
//! org/bootstrap key out-of-band can check `signer` against it.
//!
//! Everything here is pure and offline — no network, no I/O beyond the chosen sink —
//! so it is fully testable with the file-writer default.

use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Result, anyhow};
use iroh::{PublicKey, SecretKey, Signature};
use serde::{Deserialize, Serialize};

/// Where the bootstrap can be reached: its `node_id` (== ed25519 public key, hex)
/// and its dialable direct socket addresses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BootstrapInfo {
    /// The bootstrap's `node_id` — the verifying public key apps pin.
    pub node_id: String,
    /// Dialable direct socket addresses (`host:port`). May be empty if no direct
    /// address has been observed yet (relay-only reachability).
    pub addr: Vec<String>,
}

/// The rendezvous config apps fetch to discover the bootstrap peer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RendezvousConfig {
    /// Deployment environment label (e.g. `dev`, `prod`).
    pub env: String,
    /// The mesh discovery key this bootstrap serves.
    pub discovery_key: String,
    /// How to reach the bootstrap.
    pub bootstrap: BootstrapInfo,
    /// The relay URL the bootstrap is configured with, if any.
    pub relay_url: Option<String>,
    /// Unix timestamp (seconds) the config was published — newer wins.
    pub ts: u64,
}

/// A [`RendezvousConfig`] plus an ed25519 signature over its canonical bytes.
///
/// `signer` is the hex `node_id` of the signing key; `signature` is the hex ed25519
/// signature over the canonical JSON of `config`. An app verifies `signature` against
/// `signer` (see [`verify_config`]) and then pins `config.bootstrap.node_id`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedRendezvousConfig {
    pub config: RendezvousConfig,
    /// Hex-encoded ed25519 public key of the signer.
    pub signer: String,
    /// Hex-encoded ed25519 signature over [`canonical_bytes`] of `config`.
    pub signature: String,
}

/// Canonical byte representation of a config for signing/verifying.
///
/// `serde_json` serializes derived structs in field-declaration order, so this is
/// stable across sign and verify within a build.
fn canonical_bytes(config: &RendezvousConfig) -> Result<Vec<u8>> {
    serde_json::to_vec(config).context("serialize rendezvous config for signing")
}

/// Sign a [`RendezvousConfig`] with the bootstrap's secret key.
pub fn sign_config(
    config: RendezvousConfig,
    secret_key: &SecretKey,
) -> Result<SignedRendezvousConfig> {
    let bytes = canonical_bytes(&config)?;
    let signature = secret_key.sign(&bytes);
    Ok(SignedRendezvousConfig {
        config,
        signer: secret_key.public().to_string(),
        signature: hex::encode(signature.to_bytes()),
    })
}

/// Verify a [`SignedRendezvousConfig`]: the signature must validate against `signer`.
///
/// Returns `Ok(())` on success. Apps should additionally decide whether to trust
/// `signer` (TOFU pin on first fetch, or compare against an org-pinned key).
pub fn verify_config(signed: &SignedRendezvousConfig) -> Result<()> {
    let pubkey = PublicKey::from_str(&signed.signer)
        .map_err(|e| anyhow!("invalid signer public key: {e}"))?;
    let sig_bytes: [u8; Signature::LENGTH] = hex::decode(&signed.signature)
        .context("decode signature hex")?
        .try_into()
        .map_err(|_| anyhow!("signature must be {} bytes", Signature::LENGTH))?;
    let signature = Signature::from_bytes(&sig_bytes);
    let bytes = canonical_bytes(&signed.config)?;
    pubkey
        .verify(&bytes, &signature)
        .map_err(|e| anyhow!("rendezvous signature verification failed: {e}"))
}

/// A destination the signed rendezvous config is published to.
///
/// Kept small so the file-writer default is testable with no network; a deploy can
/// add a PUT-URL / object-store impl without touching the publish-on-start path.
pub trait ConfigSink {
    /// Publish the serialized signed config bytes. Idempotent: overwrites prior content.
    fn publish(&self, bytes: &[u8]) -> Result<()>;
}

/// Default sink: write the signed config to a local file the deploy uploads/serves
/// at the well-known URL.
pub struct FileSink {
    pub path: PathBuf,
}

impl FileSink {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl ConfigSink for FileSink {
    fn publish(&self, bytes: &[u8]) -> Result<()> {
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create rendezvous config dir {}", parent.display()))?;
        }
        std::fs::write(&self.path, bytes)
            .with_context(|| format!("write rendezvous config to {}", self.path.display()))
    }
}

/// Serialize a signed config and publish it to `sink`.
pub fn publish_signed(sink: &dyn ConfigSink, signed: &SignedRendezvousConfig) -> Result<()> {
    let json = serde_json::to_vec_pretty(signed).context("serialize signed rendezvous config")?;
    sink.publish(&json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand_chacha::rand_core::SeedableRng;

    fn key(seed: u8) -> SecretKey {
        let mut rng = rand_chacha::ChaCha8Rng::from_seed([seed; 32]);
        SecretKey::generate(&mut rng)
    }

    fn sample_config(node_id: String) -> RendezvousConfig {
        RendezvousConfig {
            env: "dev".to_string(),
            discovery_key: "cyan-dev".to_string(),
            bootstrap: BootstrapInfo {
                node_id,
                addr: vec!["127.0.0.1:4242".to_string()],
            },
            relay_url: Some("https://quic.dev.cyan.blockxaero.io".to_string()),
            ts: 1_700_000_000,
        }
    }

    #[test]
    fn sign_then_verify_roundtrips() {
        let sk = key(1);
        let signed = sign_config(sample_config(sk.public().to_string()), &sk).expect("sign");
        assert_eq!(signed.signer, sk.public().to_string());
        verify_config(&signed).expect("verify");
    }

    #[test]
    fn tampered_config_fails_verification() {
        let sk = key(2);
        let mut signed = sign_config(sample_config(sk.public().to_string()), &sk).expect("sign");
        signed.config.bootstrap.addr = vec!["10.0.0.1:9999".to_string()];
        assert!(verify_config(&signed).is_err(), "tampered config must not verify");
    }

    #[test]
    fn wrong_signer_fails_verification() {
        let sk = key(3);
        let other = key(4);
        let mut signed = sign_config(sample_config(sk.public().to_string()), &sk).expect("sign");
        signed.signer = other.public().to_string();
        assert!(verify_config(&signed).is_err(), "signature must not verify under a different key");
    }
}
