// Marabunta - Licensed under the MIT License.
//! Authentication and identity for the Marabunta Swarm.
//!
//! Three layers of authentication:
//!
//! 1. **Node identity** -- Ed25519 keypair derived from `ed25519-dalek`.
//!    The public key is hashed (SHA-256, truncated) to produce a deterministic
//!    [`NodeId`]. The keypair is persisted to `~/.marabunta/identity.key` so
//!    that a node keeps the same identity across restarts.
//!
//! 2. **Gossip message signing** -- Every [`SwarmMessage`] is wrapped in a
//!    [`SignedEnvelope`] that carries the sender's public key, a timestamp,
//!    and an Ed25519 signature over `payload || timestamp_bytes`. Receivers
//!    reject envelopes with stale timestamps (older than
//!    [`AUTH_MAX_MESSAGE_AGE`]).
//!
//! 3. **API bearer tokens** -- [`TokenStore`] manages SHA-256-hashed bearer
//!    tokens with per-token permissions ([`TokenPerms`]), optional expiry,
//!    and optional cost caps. Tokens are generated with a cryptographically
//!    random 32-byte base64-encoded string; only the hash is stored.
//!
//! 4. **Axum auth middleware** -- [`AuthLayer`] and [`AuthenticatedUser`]
//!    integrate token validation into axum request pipelines.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use ed25519_dalek::{Signer, Verifier};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::debug;

use super::config::{AUTH_MAX_MESSAGE_AGE, AUTH_TOKEN_DEFAULT_EXPIRY};
use super::types::{NodeId, SwarmError, SwarmMessage};
use crate::marabunta::identity::{FederationId, NodeId as DilithiumId, NodeIdentity as MarabuntaIdentity};

// Serde helpers for fixed-size byte arrays via base64 encoding
mod base64_32 {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 32], D::Error> {
        let s = String::deserialize(deserializer)?;
        let decoded = URL_SAFE_NO_PAD
            .decode(&s)
            .map_err(serde::de::Error::custom)?;
        decoded
            .try_into()
            .map_err(|_| serde::de::Error::custom("expected 32 bytes"))
    }
}

mod base64_64 {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8; 64], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 64], D::Error> {
        let s = String::deserialize(deserializer)?;
        let decoded = URL_SAFE_NO_PAD
            .decode(&s)
            .map_err(serde::de::Error::custom)?;
        decoded
            .try_into()
            .map_err(|_| serde::de::Error::custom("expected 64 bytes"))
    }
}

// ============================================================================
// 1. Node Identity -- Ed25519 keypair
// ============================================================================

/// Cryptographic identity for a swarm node.
///
/// Wraps an Ed25519 signing key and the corresponding verifying key.
/// The [`NodeId`] is derived deterministically from the public key so that
/// identity and routing are bound to the same key material.
pub struct NodeIdentity {
    pub node_id: NodeId,
    pub keypair: ed25519_dalek::SigningKey,
    pub public_key: ed25519_dalek::VerifyingKey,
}

impl NodeIdentity {
    /// Generate a brand-new identity with a random Ed25519 keypair.
    pub fn generate() -> Self {
        let mut rng = rand::thread_rng();
        let keypair = ed25519_dalek::SigningKey::generate(&mut rng);
        let public_key = keypair.verifying_key();
        let node_id = Self::node_id_from_pubkey(&public_key.to_bytes());
        Self {
            node_id,
            keypair,
            public_key,
        }
    }

    pub fn signing_key_bytes(&self) -> &[u8; 32] {
        self.keypair.as_bytes()
    }

    /// Load an existing identity from `path`, or generate a fresh one if the
    /// file does not exist.
    ///
    /// The key file stores the 32-byte Ed25519 seed (not the expanded key)
    /// encoded as unpadded URL-safe base64 on a single line. Parent
    /// directories are created automatically.
    pub fn load_or_generate(path: &Path) -> Result<Self, SwarmError> {
        if path.exists() {
            let data = std::fs::read_to_string(path).map_err(|e| {
                SwarmError::Auth(format!("failed to read identity file {}: {}", path.display(), e))
            })?;
            let seed_bytes = URL_SAFE_NO_PAD
                .decode(data.trim().as_bytes())
                .map_err(|e| {
                    SwarmError::Auth(format!("invalid base64 in identity file: {}", e))
                })?;
            if seed_bytes.len() != 32 {
                return Err(SwarmError::Auth(format!(
                    "identity seed must be 32 bytes, got {}",
                    seed_bytes.len()
                )));
            }
            let mut seed = [0u8; 32];
            seed.copy_from_slice(&seed_bytes);
            let keypair = ed25519_dalek::SigningKey::from_bytes(&seed);
            let public_key = keypair.verifying_key();
            let node_id = Self::node_id_from_pubkey(&public_key.to_bytes());
            debug!(node_id = %node_id, path = %path.display(), "loaded identity from file");
            Ok(Self {
                node_id,
                keypair,
                public_key,
            })
        } else {
            let identity = Self::generate();
            identity.save(path)?;
            debug!(node_id = %identity.node_id, path = %path.display(), "generated new identity");
            Ok(identity)
        }
    }

    /// Persist the signing key seed to `path`.
    ///
    /// Creates parent directories if they do not exist. The file is written
    /// with restrictive permissions on Unix (0600).
    pub fn save(&self, path: &Path) -> Result<(), SwarmError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                SwarmError::Auth(format!(
                    "failed to create identity directory {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }
        let encoded = URL_SAFE_NO_PAD.encode(self.keypair.to_bytes());
        std::fs::write(path, &encoded).map_err(|e| {
            SwarmError::Auth(format!(
                "failed to write identity file {}: {}",
                path.display(),
                e
            ))
        })?;

        // Restrict permissions on Unix.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(path, perms).map_err(|e| {
                SwarmError::Auth(format!("failed to set permissions on {}: {}", path.display(), e))
            })?;
        }

        Ok(())
    }

    /// Sign arbitrary bytes with this node's private key.
    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        let sig = self.keypair.sign(message);
        sig.to_bytes()
    }

    /// Verify a signature produced by a known public key.
    pub fn verify(public_key: &[u8; 32], message: &[u8], signature: &[u8; 64]) -> bool {
        let Ok(vk) = ed25519_dalek::VerifyingKey::from_bytes(public_key) else {
            return false;
        };
        let sig = ed25519_dalek::Signature::from_bytes(signature);
        vk.verify(message, &sig).is_ok()
    }

    /// Derive a deterministic [`NodeId`] from a 32-byte Ed25519 public key.
    ///
    /// The public key is hashed with SHA-256 and the first 16 bytes are
    /// used to construct a UUID (version 8, RFC 9562 custom).
    pub fn node_id_from_pubkey(public_key: &[u8; 32]) -> NodeId {
        let mut hasher = Sha256::new();
        hasher.update(public_key);
        let hash = hasher.finalize();
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&hash[..16]);
        NodeId(uuid::Uuid::from_bytes(bytes))
    }
}

// ============================================================================
// 2. Signed Envelope -- gossip message signing
// ============================================================================

/// A signed wrapper around a serialized [`SwarmMessage`].
///
/// The signature covers `payload || timestamp_rfc3339_bytes`, binding the
/// message content to the moment it was created. Receivers reject envelopes
/// whose timestamp is older than [`AUTH_MAX_MESSAGE_AGE`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedEnvelope {
    /// Serialized [`SwarmMessage`] (JSON).
    pub payload: Vec<u8>,
    /// NodeId of the sender.
    pub sender: NodeId,
    /// FederationId of the swarm.
    pub federation_id: FederationId,
    /// Raw 32-byte Ed25519 public key of the sender (base64-encoded for serde).
    #[serde(with = "base64_32")]
    pub public_key: [u8; 32],
    /// Ed25519 signature over `payload || timestamp_rfc3339` (base64-encoded for serde).
    #[serde(with = "base64_64")]
    pub signature: [u8; 64],
    /// When the envelope was created.
    pub timestamp: DateTime<Utc>,
}

impl SignedEnvelope {
    /// Create a signed envelope from a [`SwarmMessage`].
    pub fn sign(
        identity: &MarabuntaIdentity,
        federation_id: FederationId,
        message: &SwarmMessage,
    ) -> Result<Self, SwarmError> {
        let payload =
            serde_json::to_vec(message).map_err(|e| SwarmError::Auth(format!("serialize: {}", e)))?;
        let timestamp = Utc::now();
        let signing_input = Self::build_signing_input(&payload, &timestamp);
        let sig_bytes = identity.sign_ed25519(&signing_input);
        let mut signature = [0u8; 64];
        signature.copy_from_slice(&sig_bytes);

        Ok(Self {
            payload,
            sender: NodeId(uuid::Uuid::from_bytes([0u8; 16])), // Placeholder for UUID binding
            federation_id,
            public_key: identity.ed25519_verifying_key().to_bytes(),
            signature,
            timestamp,
        })
    }

    /// Verify the envelope's signature and timestamp, then deserialize the
    /// inner [`SwarmMessage`].
    pub fn verify_and_extract(
        &self,
        local_federation_id: FederationId,
    ) -> Result<SwarmMessage, SwarmError> {
        // Behavioral Requirement: Reject traffic from foreign federations (Cell Wall)
        if self.federation_id != local_federation_id {
            return Err(SwarmError::Auth(format!(
                "Federation mismatch: expected {}, received {}",
                local_federation_id, self.federation_id
            )));
        }
        // 1. Check timestamp freshness.
        let age = Utc::now()
            .signed_duration_since(self.timestamp)
            .to_std()
            .unwrap_or(Duration::ZERO);
        if age > AUTH_MAX_MESSAGE_AGE {
            return Err(SwarmError::Auth(format!(
                "envelope too old: {}s > {}s max",
                age.as_secs(),
                AUTH_MAX_MESSAGE_AGE.as_secs()
            )));
        }

        // 2. Verify that the sender claim matches the public key.
        let expected_id = NodeIdentity::node_id_from_pubkey(&self.public_key);
        if expected_id != self.sender {
            return Err(SwarmError::Auth(
                "sender NodeId does not match public key".into(),
            ));
        }

        // 3. Verify signature.
        let signing_input = Self::build_signing_input(&self.payload, &self.timestamp);
        if !NodeIdentity::verify(&self.public_key, &signing_input, &self.signature) {
            return Err(SwarmError::Auth("invalid envelope signature".into()));
        }

        // 4. Deserialize payload.
        let message: SwarmMessage = serde_json::from_slice(&self.payload)
            .map_err(|e| SwarmError::Auth(format!("payload deserialize: {}", e)))?;

        Ok(message)
    }

    /// Serialize the entire envelope to bytes (JSON).
    pub fn to_bytes(&self) -> Vec<u8> {
        // Serialization of our own struct should not fail unless OOM.
        serde_json::to_vec(self).expect("SignedEnvelope serialization should not fail")
    }

    /// Deserialize an envelope from bytes (JSON).
    pub fn from_bytes(data: &[u8]) -> Result<Self, SwarmError> {
        serde_json::from_slice(data)
            .map_err(|e| SwarmError::Auth(format!("envelope deserialize: {}", e)))
    }

    // -- internal --

    /// Build the byte string that gets signed: `payload || timestamp_rfc3339`.
    fn build_signing_input(payload: &[u8], timestamp: &DateTime<Utc>) -> Vec<u8> {
        let ts_str = timestamp.to_rfc3339();
        let mut input = Vec::with_capacity(payload.len() + ts_str.len());
        input.extend_from_slice(payload);
        input.extend_from_slice(ts_str.as_bytes());
        input
    }
}

// ============================================================================
// 2b. Gossip-level inline signing (Hardening A.2)
// ============================================================================

/// Build canonical signing input for a gossip message:
/// `sender_id_bytes(32) || timestamp_millis_le(8) || generation_le(8)`.
pub fn gossip_signing_input(
    sender_id: &DilithiumId,
    timestamp: &DateTime<Utc>,
    generation: u64,
) -> Vec<u8> {
    let mut input = Vec::with_capacity(48);
    input.extend_from_slice(&sender_id.0);
    input.extend_from_slice(&timestamp.timestamp_millis().to_le_bytes());
    input.extend_from_slice(&generation.to_le_bytes());
    input
}

/// Sign a gossip message's identity fields. Returns a Dilithium signature.
pub fn sign_gossip(
    identity: &MarabuntaIdentity,
    sender_id: &DilithiumId,
    timestamp: &DateTime<Utc>,
    generation: u64,
) -> Vec<u8> {
    let input = gossip_signing_input(sender_id, timestamp, generation);
    identity.sign_dilithium(&input).expect("Dilithium signing failed").to_vec()
}

/// Verify a gossip message's inline signature.
pub fn verify_gossip(
    pubkey: &[u8],
    sender_id: &DilithiumId,
    timestamp: &DateTime<Utc>,
    generation: u64,
    signature: &[u8],
) -> bool {
    let input = gossip_signing_input(sender_id, timestamp, generation);
    MarabuntaIdentity::verify_dilithium(pubkey, &input, signature)
}

/// Build canonical signing input for a witness report:
/// `reporter_bytes(16) || subject_bytes(16) || last_seen_millis_le(8)`.
pub fn witness_signing_input(
    reporter: &crate::swarm::types::NodeId,
    subject: &crate::swarm::types::NodeId,
    last_seen: &DateTime<Utc>,
) -> Vec<u8> {
    let mut input = Vec::with_capacity(40);
    input.extend_from_slice(reporter.0.as_bytes());
    input.extend_from_slice(subject.0.as_bytes());
    input.extend_from_slice(&last_seen.timestamp_millis().to_le_bytes());
    input
}

/// Sign a witness report. Returns a 64-byte Ed25519 signature.
pub fn sign_witness(
    identity: &NodeIdentity,
    reporter: &NodeId,
    subject: &NodeId,
    last_seen: &DateTime<Utc>,
) -> Vec<u8> {
    let input = witness_signing_input(reporter, subject, last_seen);
    identity.sign(&input).to_vec()
}

/// Verify a witness report signature.
pub fn verify_witness(
    pubkey: &[u8],
    reporter: &NodeId,
    subject: &NodeId,
    last_seen: &DateTime<Utc>,
    signature: &[u8],
) -> bool {
    if pubkey.len() != 32 || signature.len() != 64 {
        return false;
    }
    let mut pk = [0u8; 32];
    pk.copy_from_slice(pubkey);
    let mut sig = [0u8; 64];
    sig.copy_from_slice(signature);
    let input = witness_signing_input(reporter, subject, last_seen);
    NodeIdentity::verify(&pk, &input, &sig)
}

// ============================================================================
// 3. API Bearer Tokens
// ============================================================================

/// Per-token permission flags.
///
/// Each flag independently gates a category of API operations. The `admin`
/// flag implies all other permissions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenPerms {
    pub submit_jobs: bool,
    pub view_nodes: bool,
    pub manage_strategies: bool,
    pub manage_energy: bool,
    pub admin: bool,
}

impl TokenPerms {
    /// Full admin permissions (all flags set).
    pub fn admin() -> Self {
        Self {
            submit_jobs: true,
            view_nodes: true,
            manage_strategies: true,
            manage_energy: true,
            admin: true,
        }
    }

    /// Read-only permissions (view nodes only).
    pub fn read_only() -> Self {
        Self {
            submit_jobs: false,
            view_nodes: true,
            manage_strategies: false,
            manage_energy: false,
            admin: false,
        }
    }

    /// Check whether a specific capability is permitted.
    pub fn allows(&self, capability: &str) -> bool {
        if self.admin {
            return true;
        }
        match capability {
            "submit_jobs" => self.submit_jobs,
            "view_nodes" => self.view_nodes,
            "manage_strategies" => self.manage_strategies,
            "manage_energy" => self.manage_energy,
            "admin" => self.admin,
            _ => false,
        }
    }
}

impl Default for TokenPerms {
    fn default() -> Self {
        Self::read_only()
    }
}

/// A stored API token (hash only -- the plaintext is never persisted).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiToken {
    /// SHA-256 of the bearer token string.
    pub token_hash: [u8; 32],
    /// Human-readable name for management UIs.
    pub name: String,
    /// What this token is allowed to do.
    pub permissions: TokenPerms,
    /// When the token was created.
    pub created_at: DateTime<Utc>,
    /// When the token expires (`None` = never).
    pub expires_at: Option<DateTime<Utc>>,
    /// Optional maximum cumulative cost in USD.
    pub max_cost_usd: Option<f64>,
}

impl ApiToken {
    /// Returns `true` if the token has expired.
    pub fn is_expired(&self) -> bool {
        if let Some(exp) = self.expires_at {
            Utc::now() >= exp
        } else {
            false
        }
    }
}

/// Thread-safe store for API tokens.
///
/// Tokens are indexed by their SHA-256 hash. The plaintext is returned
/// exactly once at generation time and is never stored.
pub struct TokenStore {
    tokens: DashMap<[u8; 32], ApiToken>,
}

impl TokenStore {
    /// Create an empty token store.
    pub fn new() -> Self {
        Self {
            tokens: DashMap::new(),
        }
    }

    /// Generate a new bearer token.
    ///
    /// Returns `(plaintext_token, stored_token)`. The plaintext must be
    /// shown to the user exactly once; only its SHA-256 hash is retained.
    pub fn generate(
        &self,
        name: String,
        perms: TokenPerms,
        expires_in: Option<Duration>,
    ) -> (String, ApiToken) {
        // 32 random bytes -> 43-char URL-safe base64 token.
        let mut raw = [0u8; 32];
        {
            use rand::RngCore;
            let mut rng = rand::thread_rng();
            rng.fill_bytes(&mut raw);
        }
        let plaintext = format!("csw_{}", URL_SAFE_NO_PAD.encode(raw));

        let hash = Self::hash_token(&plaintext);

        let now = Utc::now();
        let expires_at = expires_in.map(|d| {
            now + chrono::Duration::from_std(d).unwrap_or(
                chrono::Duration::from_std(AUTH_TOKEN_DEFAULT_EXPIRY)
                    .expect("default expiry fits in chrono::Duration"),
            )
        });

        let token = ApiToken {
            token_hash: hash,
            name,
            permissions: perms,
            created_at: now,
            expires_at,
            max_cost_usd: None,
        };

        self.tokens.insert(hash, token.clone());
        (plaintext, token)
    }

    /// Validate a bearer token string.
    ///
    /// Returns a clone of the stored [`ApiToken`] if the token exists and
    /// has not expired. Returns `None` otherwise.
    pub fn validate(&self, token: &str) -> Option<ApiToken> {
        let hash = Self::hash_token(token);
        let entry = self.tokens.get(&hash)?;
        let api_token = entry.value();

        if api_token.is_expired() {
            // Expired tokens are lazily cleaned up here.
            drop(entry);
            self.tokens.remove(&hash);
            return None;
        }

        Some(api_token.clone())
    }

    /// Revoke a token by its hash. Returns `true` if the token was found
    /// and removed, `false` if it was not present.
    pub fn revoke(&self, token_hash: &[u8; 32]) -> bool {
        self.tokens.remove(token_hash).is_some()
    }

    /// List all stored tokens (without secrets).
    pub fn list(&self) -> Vec<ApiToken> {
        self.tokens.iter().map(|r| r.value().clone()).collect()
    }

    /// Number of tokens currently stored.
    pub fn count(&self) -> usize {
        self.tokens.len()
    }

    /// Compute the SHA-256 hash of a plaintext token string.
    fn hash_token(token: &str) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        let result = hasher.finalize();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&result);
        hash
    }
}

impl Default for TokenStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// 4. Axum Auth Middleware
// ============================================================================

/// Shared state injected into axum routes for token validation.
#[derive(Clone)]
pub struct AuthLayer {
    pub token_store: Arc<TokenStore>,
}

impl AuthLayer {
    pub fn new(token_store: Arc<TokenStore>) -> Self {
        Self { token_store }
    }
}

/// Axum extractor that validates the `Authorization: Bearer <token>` header
/// and provides the resolved [`ApiToken`] to route handlers.
#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub token: ApiToken,
}

/// Rejection type when authentication fails.
#[derive(Debug)]
pub enum AuthRejection {
    /// No `Authorization` header or not a Bearer token.
    MissingToken,
    /// Token not found, expired, or revoked.
    InvalidToken,
}

impl IntoResponse for AuthRejection {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            AuthRejection::MissingToken => {
                (StatusCode::UNAUTHORIZED, "missing or malformed Authorization header")
            }
            AuthRejection::InvalidToken => {
                (StatusCode::UNAUTHORIZED, "invalid or expired token")
            }
        };
        let body = serde_json::json!({ "error": msg, "code": status.as_u16() });
        (status, axum::Json(body)).into_response()
    }
}

#[axum::async_trait]
impl axum::extract::FromRequestParts<Arc<AuthLayer>> for AuthenticatedUser {
    type Rejection = AuthRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AuthLayer>,
    ) -> Result<Self, Self::Rejection> {
        let header_value = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or(AuthRejection::MissingToken)?;

        let token_str = header_value
            .strip_prefix("Bearer ")
            .or_else(|| header_value.strip_prefix("bearer "))
            .ok_or(AuthRejection::MissingToken)?;

        let api_token = state
            .token_store
            .validate(token_str)
            .ok_or(AuthRejection::InvalidToken)?;

        Ok(AuthenticatedUser { token: api_token })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tempfile::TempDir;
    use crate::marabunta::identity::{FederationId, NodeIdentity as MarabuntaIdentity};
    use super::NodeId as SwarmNodeId;

    // -- NodeIdentity tests --------------------------------------------------

    #[test]
    fn generate_identity_produces_valid_node_id() {
        let id = NodeIdentity::generate();
        // NodeId should be deterministically derived from the public key.
        let expected = NodeIdentity::node_id_from_pubkey(&id.public_key.to_bytes());
        assert_eq!(id.node_id, expected);
    }

    #[test]
    fn generate_identity_unique() {
        let a = NodeIdentity::generate();
        let b = NodeIdentity::generate();
        assert_ne!(a.node_id, b.node_id);
        assert_ne!(a.public_key.to_bytes(), b.public_key.to_bytes());
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let id = NodeIdentity::generate();
        let msg = b"hello marabunta swarm";
        let sig = id.keypair.sign(msg).to_bytes();
        assert!(id.public_key.verify(msg, &ed25519_dalek::Signature::from_bytes(&sig)).is_ok());
    }

    #[test]
    fn node_id_from_pubkey_deterministic() {
        let id = NodeIdentity::generate();
        let pk = id.public_key.to_bytes();
        let a = NodeIdentity::node_id_from_pubkey(&pk);
        let b = NodeIdentity::node_id_from_pubkey(&pk);
        assert_eq!(a, b);
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("identity.key");

        let original = NodeIdentity::generate();
        original.save(&path).unwrap();

        let loaded = NodeIdentity::load_or_generate(&path).unwrap();
        assert_eq!(original.node_id, loaded.node_id);
        assert_eq!(
            original.public_key.to_bytes(),
            loaded.public_key.to_bytes()
        );
    }

    // -- SignedEnvelope tests ------------------------------------------------

    #[test]
    fn envelope_sign_and_verify() {
        let id = MarabuntaIdentity::generate().unwrap();
        let msg = SwarmMessage::Ping {
            from: SwarmNodeId(uuid::Uuid::new_v4()),
            nonce: 42,
            pow_nonce: None,
        };

        let fed_id = FederationId::generate();
        let envelope = SignedEnvelope::sign(&id, fed_id, &msg).unwrap();
        let extracted = envelope.verify_and_extract(fed_id).unwrap();

        // The extracted message should match.
        match extracted {
            SwarmMessage::Ping { from: _, nonce, pow_nonce: _ } => {
                assert_eq!(nonce, 42);
            }
            other => panic!("unexpected message variant: {:?}", other),
        }
    }

    #[test]
    fn envelope_rejects_tampered_payload() {
        let id = MarabuntaIdentity::generate().unwrap();
        let msg = SwarmMessage::Ping {
            from: SwarmNodeId(uuid::Uuid::new_v4()),
            nonce: 1,
            pow_nonce: None,
        };

        let fed_id = FederationId::generate();
        let mut envelope = SignedEnvelope::sign(&id, fed_id, &msg).unwrap();
        // Flip a byte in the payload.
        if let Some(b) = envelope.payload.first_mut() {
            *b ^= 0xff;
        }
        assert!(envelope.verify_and_extract(fed_id).is_err());
    }

    #[test]
    fn envelope_rejects_tampered_timestamp() {
        let id = MarabuntaIdentity::generate().unwrap();
        let msg = SwarmMessage::Ping {
            from: SwarmNodeId(uuid::Uuid::new_v4()),
            nonce: 2,
            pow_nonce: None,
        };

        let fed_id = FederationId::generate();
        let mut envelope = SignedEnvelope::sign(&id, fed_id, &msg).unwrap();
        // Shift timestamp forward by 1 second (invalidates signature).
        envelope.timestamp = envelope.timestamp + chrono::Duration::seconds(1);
        assert!(envelope.verify_and_extract(fed_id).is_err());
    }

    #[test]
    fn envelope_rejects_stale_timestamp() {
        let id = MarabuntaIdentity::generate().unwrap();
        let msg = SwarmMessage::Ping {
            from: SwarmNodeId(uuid::Uuid::new_v4()),
            nonce: 3,
            pow_nonce: None,
        };

        let fed_id = FederationId::generate();
        let mut envelope = SignedEnvelope::sign(&id, fed_id, &msg).unwrap();
        // Set timestamp far in the past.
        let old_ts = Utc::now() - chrono::Duration::seconds(600);
        // Re-sign with the old timestamp so the signature itself is valid,
        // but the staleness check should reject it.
        let payload = &envelope.payload;
        let signing_input = SignedEnvelope::build_signing_input(payload, &old_ts);
        envelope.signature = id.sign_ed25519(&signing_input);
        envelope.timestamp = old_ts;

        let result = envelope.verify_and_extract(fed_id);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("too old"), "error was: {}", err_msg);
    }

    #[test]
    fn envelope_rejects_wrong_sender() {
        let id = MarabuntaIdentity::generate().unwrap();
        let fed_id = FederationId::generate();
        let other_id = SwarmNodeId(uuid::Uuid::new_v4());
        let msg = SwarmMessage::Ping {
            from: SwarmNodeId(uuid::Uuid::new_v4()),
            nonce: 4,
            pow_nonce: None,
        };

        let mut envelope = SignedEnvelope::sign(&id, fed_id, &msg).unwrap();
        // Claim a different sender while keeping the original public key.
        envelope.sender = other_id;
        let result = envelope.verify_and_extract(fed_id);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("does not match"),
            "error was: {}",
            err_msg
        );
    }

    #[test]
    fn envelope_serialization_roundtrip() {
        let id = MarabuntaIdentity::generate().unwrap();
        let fed_id = FederationId::generate();
        let msg = SwarmMessage::Pong {
            from: SwarmNodeId(uuid::Uuid::new_v4()),
            nonce: 99,
        };

        let envelope = SignedEnvelope::sign(&id, fed_id, &msg).unwrap();
        let bytes = envelope.to_bytes();
        let restored = SignedEnvelope::from_bytes(&bytes).unwrap();

        assert_eq!(envelope.sender, restored.sender);
        assert_eq!(envelope.public_key, restored.public_key);
        assert_eq!(envelope.signature, restored.signature);
        assert_eq!(envelope.payload, restored.payload);

        // The restored envelope should still verify.
        let extracted = restored.verify_and_extract(fed_id).unwrap();
        match extracted {
            SwarmMessage::Pong { from: _, nonce } => {
                assert_eq!(nonce, 99);
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn envelope_from_bytes_rejects_garbage() {
        let result = SignedEnvelope::from_bytes(b"not json");
        assert!(result.is_err());
    }

    // -- TokenPerms tests ----------------------------------------------------

    #[test]
    fn token_perms_admin_allows_everything() {
        let perms = TokenPerms::admin();
        assert!(perms.allows("submit_jobs"));
        assert!(perms.allows("view_nodes"));
        assert!(perms.allows("manage_strategies"));
        assert!(perms.allows("manage_energy"));
        assert!(perms.allows("admin"));
        assert!(perms.allows("anything_unknown"));
    }

    #[test]
    fn token_perms_read_only() {
        let perms = TokenPerms::read_only();
        assert!(!perms.allows("submit_jobs"));
        assert!(perms.allows("view_nodes"));
        assert!(!perms.allows("manage_strategies"));
        assert!(!perms.allows("admin"));
    }

    #[test]
    fn token_perms_custom() {
        let perms = TokenPerms {
            submit_jobs: true,
            view_nodes: true,
            manage_strategies: false,
            manage_energy: false,
            admin: false,
        };
        assert!(perms.allows("submit_jobs"));
        assert!(perms.allows("view_nodes"));
        assert!(!perms.allows("manage_strategies"));
        assert!(!perms.allows("manage_energy"));
        assert!(!perms.allows("admin"));
        assert!(!perms.allows("unknown_capability"));
    }

    #[test]
    fn token_perms_default_is_read_only() {
        let perms = TokenPerms::default();
        assert_eq!(perms, TokenPerms::read_only());
    }

    // -- TokenStore tests ----------------------------------------------------

    #[test]
    fn token_generate_and_validate() {
        let store = TokenStore::new();
        let (plaintext, created) =
            store.generate("test-token".into(), TokenPerms::admin(), None);

        assert!(plaintext.starts_with("csw_"));
        assert_eq!(created.name, "test-token");
        assert_eq!(created.permissions, TokenPerms::admin());
        assert!(created.expires_at.is_none());

        let validated = store.validate(&plaintext);
        assert!(validated.is_some());
        let validated = validated.unwrap();
        assert_eq!(validated.name, "test-token");
        assert_eq!(validated.token_hash, created.token_hash);
    }

    #[test]
    fn token_validate_rejects_unknown() {
        let store = TokenStore::new();
        assert!(store.validate("csw_nonexistent").is_none());
    }

    #[test]
    fn token_validate_rejects_expired() {
        let store = TokenStore::new();
        // Generate with a very short expiry that has already passed.
        let (plaintext, _) = store.generate(
            "ephemeral".into(),
            TokenPerms::read_only(),
            Some(Duration::from_secs(0)),
        );
        // The token was just created with expires_at = now + 0s, so it
        // should be expired immediately (or within a ms).
        // Give a tiny sleep to ensure we are past the expiry.
        std::thread::sleep(Duration::from_millis(10));
        assert!(store.validate(&plaintext).is_none());
    }

    #[test]
    fn token_revoke() {
        let store = TokenStore::new();
        let (plaintext, created) =
            store.generate("revokable".into(), TokenPerms::admin(), None);

        assert!(store.validate(&plaintext).is_some());
        assert!(store.revoke(&created.token_hash));
        assert!(store.validate(&plaintext).is_none());
    }

    #[test]
    fn token_revoke_nonexistent_returns_false() {
        let store = TokenStore::new();
        assert!(!store.revoke(&[0u8; 32]));
    }

    #[test]
    fn token_list() {
        let store = TokenStore::new();
        store.generate("a".into(), TokenPerms::admin(), None);
        store.generate("b".into(), TokenPerms::read_only(), None);
        store.generate("c".into(), TokenPerms::admin(), None);

        let list = store.list();
        assert_eq!(list.len(), 3);
        let names: HashSet<String> = list.iter().map(|t| t.name.clone()).collect();
        assert!(names.contains("a"));
        assert!(names.contains("b"));
        assert!(names.contains("c"));
    }

    #[test]
    fn token_count() {
        let store = TokenStore::new();
        assert_eq!(store.count(), 0);
        store.generate("x".into(), TokenPerms::admin(), None);
        assert_eq!(store.count(), 1);
        store.generate("y".into(), TokenPerms::admin(), None);
        assert_eq!(store.count(), 2);
    }

    #[test]
    fn token_store_default() {
        let store = TokenStore::default();
        assert_eq!(store.count(), 0);
    }

    #[test]
    fn token_with_expiry() {
        let store = TokenStore::new();
        let (plaintext, created) = store.generate(
            "expiring".into(),
            TokenPerms::admin(),
            Some(Duration::from_secs(3600)),
        );
        assert!(created.expires_at.is_some());
        // Should still be valid since we just created it.
        assert!(store.validate(&plaintext).is_some());
    }

    #[test]
    fn api_token_is_expired_none() {
        let token = ApiToken {
            token_hash: [0u8; 32],
            name: "test".into(),
            permissions: TokenPerms::admin(),
            created_at: Utc::now(),
            expires_at: None,
            max_cost_usd: None,
        };
        assert!(!token.is_expired());
    }

    #[test]
    fn api_token_is_expired_future() {
        let token = ApiToken {
            token_hash: [0u8; 32],
            name: "test".into(),
            permissions: TokenPerms::admin(),
            created_at: Utc::now(),
            expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
            max_cost_usd: None,
        };
        assert!(!token.is_expired());
    }

    #[test]
    fn api_token_is_expired_past() {
        let token = ApiToken {
            token_hash: [0u8; 32],
            name: "test".into(),
            permissions: TokenPerms::admin(),
            created_at: Utc::now() - chrono::Duration::hours(2),
            expires_at: Some(Utc::now() - chrono::Duration::hours(1)),
            max_cost_usd: None,
        };
        assert!(token.is_expired());
    }

    // -- AuthLayer / AuthRejection tests -------------------------------------

    #[test]
    fn auth_layer_construction() {
        let store = Arc::new(TokenStore::new());
        let layer = AuthLayer::new(store.clone());
        assert_eq!(Arc::strong_count(&layer.token_store), 2);
    }

    #[test]
    fn auth_rejection_missing_token_status() {
        let response = AuthRejection::MissingToken.into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn auth_rejection_invalid_token_status() {
        let response = AuthRejection::InvalidToken.into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // -- Concurrent token operations -----------------------------------------

    #[test]
    fn token_store_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(TokenStore::new());
        let mut handles = Vec::new();

        // Spawn 8 threads that each generate 10 tokens.
        for i in 0..8 {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                let mut tokens = Vec::new();
                for j in 0..10 {
                    let (pt, _) = store.generate(
                        format!("t-{}-{}", i, j),
                        TokenPerms::admin(),
                        None,
                    );
                    tokens.push(pt);
                }
                tokens
            }));
        }

        let all_tokens: Vec<String> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        assert_eq!(all_tokens.len(), 80);
        assert_eq!(store.count(), 80);

        // All tokens should validate.
        for t in &all_tokens {
            assert!(store.validate(t).is_some(), "token failed to validate: {}", t);
        }
    }
}
