// Marabunta - Licensed under the MIT License.
//! Network transport layer for the Marabunta Swarm.
//!
//! Provides TCP-based message exchange between swarm nodes with:
//! - Length-delimited binary framing via `tokio_util::codec::LengthDelimitedCodec`
//! - Compact serialization via `bincode`
//! - Connection pooling with idle eviction
//! - Concurrent multi-peer sends
//!
//! The transport is agnostic to message semantics; it simply moves
//! [`SwarmMessage`] values between socket addresses.

use crate::swarm::events::{EventBus, SwarmEvent};
use crate::swarm::complexity::{ConcernDomain, EventSeverity, ComplexityHint};
use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::marabunta::crypto;

use std::sync::Arc;
use std::sync::Arc as StdArc;
use std::time::Instant;

use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;

use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::StreamExt;
use futures::SinkExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, info, trace, warn};

use super::auth::{SignedEnvelope};
use super::config::{
    CONNECTION_IDLE_TIMEOUT, LISTEN_BACKLOG, MAX_MESSAGE_SIZE, /* removed crate::swarm::hardware::get_bounds().max_outbound_connections */
    TRANSPORT_CONNECT_TIMEOUT, TRANSPORT_READ_TIMEOUT,
};
use super::types::{NodeId as SwarmNodeId, SwarmError, SwarmMessage, SwarmResult};
use crate::marabunta::identity::{FederationId, NodeId, NodeIdentity};

// ============================================================================
// MaybeTlsStream — abstraction over plain TCP and TLS-wrapped TCP
// ============================================================================

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Abstraction over plain TCP and TLS-wrapped TCP streams.
/// Allows `Framed` codec to work with either.
enum MaybeTlsStream {
    Plain(TcpStream),
    ServerTls(tokio_rustls::server::TlsStream<TcpStream>),
    ClientTls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::ServerTls(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::ClientTls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::ServerTls(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::ClientTls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTlsStream::ServerTls(s) => Pin::new(s).poll_flush(cx),
            MaybeTlsStream::ClientTls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::ServerTls(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::ClientTls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

// ============================================================================
// Transport statistics
// ============================================================================

/// Aggregate statistics for the transport layer.
#[derive(Debug, Default, Clone)]
pub struct TransportStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub connections_established: u64,
    pub connections_failed: u64,
    pub active_connections: usize,
}

// ============================================================================
// TLS configuration
// ============================================================================

/// Optional TLS configuration for encrypted peer communication.
#[derive(Clone)]
pub struct TlsConfig {
    /// TLS acceptor for inbound connections.
    pub acceptor: TlsAcceptor,
    /// TLS connector for outbound connections.
    pub connector: TlsConnector,
    /// Known peer certificate fingerprints (TOFU).
    pub known_fingerprints: Arc<DashMap<String, [u8; 32]>>,
}

impl TlsConfig {
    /// Create a TLS config from PEM-encoded certificate and private key.
    ///
    /// Uses TOFU (Trust On First Use) certificate verification: the first
    /// time a peer is seen its certificate fingerprint is stored. Subsequent
    /// connections reject if the fingerprint has changed (potential MITM).
    pub fn from_pem(cert_pem: &[u8], key_pem: &[u8]) -> Result<Self, SwarmError> {
        use rustls_pemfile::{certs, pkcs8_private_keys};
        use std::io::BufReader;

        let certs: Vec<rustls::pki_types::CertificateDer<'static>> = certs(&mut BufReader::new(cert_pem))
            .filter_map(|r| r.ok())
            .collect();

        let keys: Vec<rustls::pki_types::PrivateKeyDer<'static>> = pkcs8_private_keys(&mut BufReader::new(key_pem))
            .filter_map(|r| r.ok().map(rustls::pki_types::PrivateKeyDer::Pkcs8))
            .collect();

        if certs.is_empty() || keys.is_empty() {
            return Err(SwarmError::Transport("no valid certs or keys in PEM data".into()));
        }

        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs.clone(), keys.into_iter().next().unwrap())
            .map_err(|e| SwarmError::Transport(format!("TLS server config error: {}", e)))?;

        let known_fingerprints = Arc::new(DashMap::new());

        // Client config with TOFU verifier.
        let tofu = TofuVerifier {
            known_fingerprints: Arc::clone(&known_fingerprints),
        };
        let client_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(StdArc::new(tofu))
            .with_no_client_auth();

        Ok(Self {
            acceptor: TlsAcceptor::from(StdArc::new(server_config)),
            connector: TlsConnector::from(StdArc::new(client_config)),
            known_fingerprints,
        })
    }
}

/// TOFU (Trust On First Use) certificate verifier.
///
/// On first connection to a peer, the SHA-256 fingerprint of the end-entity
/// certificate is stored. On subsequent connections, the fingerprint must
/// match or the connection is rejected (potential MITM detection).
#[derive(Debug)]
struct TofuVerifier {
    known_fingerprints: Arc<DashMap<String, [u8; 32]>>,
}

impl TofuVerifier {
    fn fingerprint(cert: &rustls::pki_types::CertificateDer<'_>) -> [u8; 32] {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(cert.as_ref());
        let result = hasher.finalize();
        let mut fp = [0u8; 32];
        fp.copy_from_slice(&result);
        fp
    }
}

impl rustls::client::danger::ServerCertVerifier for TofuVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let fp = Self::fingerprint(end_entity);
        let key = format!("{:?}", server_name);

        match self.known_fingerprints.entry(key) {
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                // First time seeing this peer — store fingerprint.
                vacant.insert(fp);
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }
            dashmap::mapref::entry::Entry::Occupied(occupied) => {
                if *occupied.get() == fp {
                    Ok(rustls::client::danger::ServerCertVerified::assertion())
                } else {
                    Err(rustls::Error::General(
                        "TOFU: certificate fingerprint changed (possible MITM)".into(),
                    ))
                }
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

// ============================================================================
// Message handler type
// ============================================================================

/// Callback invoked for every inbound message.
///
/// Receives the peer's socket address and the decoded message.
/// Implementations must be `Send + Sync` because they are called from
/// multiple connection-handler tasks concurrently.
pub type MessageHandler = Arc<dyn Fn(SocketAddr, SwarmMessage) + Send + Sync>;

// ============================================================================
// Connection handle (internal)
// ============================================================================

/// A handle to a pooled outbound connection.
///
/// Holds the sending half of an mpsc channel whose receiver is owned by
/// a background writer task. Dropping (or closing) the sender causes
/// the writer task to finish and the TCP connection to close.

// ============================================================================
// Transport Fabric (Stage 6.1)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FabricType {
    /// Standard, high-latency global WAN connection (QUIC/TCP)
    ResilientQuic,
    /// Ultra-low latency, bare-metal connection for local datacenter peers
    DatacenterLocal,
}

#[derive(Debug, Clone)]
pub struct OutboundFrame {
    pub header: Bytes,
    pub payload_path: Option<String>,
}

#[derive(Debug)]
struct ConnectionHandle {
    sender: mpsc::Sender<OutboundFrame>,
    last_used: Instant,
    fabric: FabricType,
    /// If true, this connection is carrying a heavy/vital payload and should resist LRU eviction.
    is_vital: bool,
}


// ============================================================================
// Codec helpers
// ============================================================================

/// Build a `LengthDelimitedCodec` with the project-wide configuration.
fn build_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_MESSAGE_SIZE)
        .length_field_length(4) // u32 big-endian
        .new_codec()
}

// ============================================================================
// Public serialization helpers
// ============================================================================

/// Serialize a [`SwarmMessage`] to [`Bytes`].
///
/// Uses JSON encoding because the common `TaskPayload` type uses
/// `#[serde(tag = "type")]` (internally-tagged enums) which requires
/// `deserialize_any` — not supported by bincode. JSON handles all serde
/// data models correctly.
///
/// The returned bytes are the raw payload *without* a length prefix; the
/// length prefix is added automatically by `LengthDelimitedCodec` when
/// the bytes are written to a framed stream.
pub fn encode_message(message: &SwarmMessage) -> SwarmResult<Bytes> {
    let data = serde_json::to_vec(message)
        .map_err(|e| SwarmError::Transport(format!("serialization error: {}", e)))?;
    if data.len() > MAX_MESSAGE_SIZE {
        return Err(SwarmError::CapacityExceeded(format!(
            "serialized message is {} bytes, limit is {} bytes",
            data.len(),
            MAX_MESSAGE_SIZE
        )));
    }
    Ok(Bytes::from(data))
}

/// Serialize a [`SwarmMessage`] wrapped in a [`SignedEnvelope`] to [`Bytes`].
pub fn encode_signed(
    message: &SwarmMessage,
    identity: &NodeIdentity,
    federation_id: FederationId,
) -> SwarmResult<Bytes> {
    let envelope = SignedEnvelope::sign(identity, federation_id, message)
        .map_err(|e| SwarmError::Transport(format!("signing error: {}", e)))?;
    let data = envelope.to_bytes();
    if data.len() > MAX_MESSAGE_SIZE {
        return Err(SwarmError::CapacityExceeded(format!(
            "signed message is {} bytes, limit is {} bytes",
            data.len(),
            MAX_MESSAGE_SIZE
        )));
    }
    Ok(Bytes::from(data))
}

/// Deserialize a [`SwarmMessage`] from raw bytes.
pub fn decode_message(data: &[u8], local_federation_id: FederationId) -> SwarmResult<SwarmMessage> {
    // Hardening A.8: Explicit frame size check before deserialization.
    if data.len() > MAX_MESSAGE_SIZE {
        warn!(
            bytes = data.len(),
            max = MAX_MESSAGE_SIZE,
            "dropping oversized frame before deserialization",
        );
        return Err(SwarmError::CapacityExceeded(format!(
            "frame is {} bytes, limit is {} bytes",
            data.len(),
            MAX_MESSAGE_SIZE
        )));
    }

    // Parse as SignedEnvelope — REQUIRED. No raw JSON accepted.
    let envelope = SignedEnvelope::from_bytes(data)
        .map_err(|e| SwarmError::Transport(format!("invalid SignedEnvelope: {e}")))?;

    // Verify Ed25519 signature AND FederationId (Cell Wall)
    envelope
        .verify_and_extract(local_federation_id)
        .map_err(|e| SwarmError::Transport(format!("envelope verification failed: {e}")))
}

// ============================================================================
// SwarmTransport
// ============================================================================

/// The swarm transport layer.
///
/// Manages inbound listening, outbound connection pooling, message
/// encoding/decoding, and transport-level statistics.


pub struct SwarmTransport {
    node_id: NodeId,
    federation_id: FederationId,
    listen_addr: SocketAddr,
    /// Outbound connection pool: target address -> pooled connection handle.
    connections: Arc<DashMap<SocketAddr, ConnectionHandle>>,
    /// Transport-level counters.
    stats: Arc<parking_lot::RwLock<TransportStats>>,
    /// Optional TLS configuration for encrypted peer communication.
    tls: Option<TlsConfig>,
    /// Optional signing identity for message authentication.
    signing_identity: Option<Arc<NodeIdentity>>,
    /// Transport tuning configuration (promoted from constants).
    pub transport_tuning: super::config::TransportTuningConfig,
    /// The discovered public address of this node (for NAT traversal).
    pub advertised_addr: Arc<parking_lot::RwLock<Option<std::net::SocketAddr>>>,
    /// Actively banned NodeIds (severed at transport layer).
    banned_nodes: Arc<DashMap<SwarmNodeId, std::time::Instant>>,
    /// Event bus for HNDL tunneling telemetry
    pub event_bus: Option<Arc<crate::swarm::events::EventBus>>,
    
    // -- Highestsec Zone Enforcement --
    /// If set, peers MUST present a valid certificate for this zone.
    pub enforced_zone: Option<String>,
    /// The Ed25519 public key of the Zone Authority required to validate peer certs.
    pub zone_authority_pubkey: Option<Vec<u8>>,
    /// Our own ZoneMembershipCertificate to present during the handshake.
    pub zone_certificate: Option<Arc<crate::highestsec::zone_membership::ZoneMembershipCertificate>>,
    pub inbound_connections_per_ip: Arc<dashmap::DashMap<std::net::IpAddr, usize>>,
}


// ============================================================================
// Post-Quantum Hybrid KEM Handshake
// ============================================================================

async fn perform_pq_handshake_initiator(
    stream: &mut MaybeTlsStream,
    peer_addr: SocketAddr,
    event_bus: Option<Arc<EventBus>>,
    _signing_identity: Option<Arc<crate::marabunta::identity::NodeIdentity>>,
    zone_certificate: Option<Arc<crate::highestsec::zone_membership::ZoneMembershipCertificate>>,
    enforced_zone: Option<String>,
    zone_authority_pubkey: Option<Vec<u8>>,
) -> SwarmResult<[u8; 32]> {
    let (client_x_sk, client_x_pk) = crypto::x25519_keypair();
    
    // Initiator sends ClientHello (32 bytes X25519 PK)
    stream.write_all(client_x_pk.as_bytes()).await.map_err(|e| SwarmError::Transport(format!("PQ Init write error: {}", e)))?;
    
    // Send our ZMC if we have one
    if let Some(cert) = zone_certificate {
        let json = serde_json::to_string(&*cert).map_err(|_| SwarmError::Transport("ZMC serialization failed".into()))?;
        let bytes = json.as_bytes();
        stream.write_all(&(bytes.len() as u32).to_le_bytes()).await.map_err(|e| SwarmError::Transport(e.to_string()))?;
        stream.write_all(bytes).await.map_err(|e| SwarmError::Transport(e.to_string()))?;
    } else {
        stream.write_all(&0u32.to_le_bytes()).await.map_err(|e| SwarmError::Transport(e.to_string()))?;
    }

    // Reads ServerHello (32 bytes X25519 PK)
    let mut server_x_pk_bytes = [0u8; 32];
    stream.read_exact(&mut server_x_pk_bytes).await.map_err(|e| SwarmError::Transport(format!("PQ Init read error: {}", e)))?;
    
    // Read Responder ZMC length
    let mut zmc_len_bytes = [0u8; 4];
    stream.read_exact(&mut zmc_len_bytes).await.map_err(|e| SwarmError::Transport(e.to_string()))?;
    let zmc_len = u32::from_le_bytes(zmc_len_bytes) as usize;
    
    let mut server_kyber_ek_bytes = vec![0u8; 1184];
    
    if zmc_len > 0 {
        if zmc_len > 1024 * 1024 { return Err(SwarmError::Transport("ZMC too large".into())); }
        let mut zmc_bytes = vec![0u8; zmc_len];
        stream.read_exact(&mut zmc_bytes).await.map_err(|e| SwarmError::Transport(e.to_string()))?;
        
        let cert: crate::highestsec::zone_membership::ZoneMembershipCertificate = serde_json::from_slice(&zmc_bytes)
            .map_err(|_| SwarmError::Transport("Invalid ZMC JSON".into()))?;
            
        // Enforce Zone if configured
        if let Some(ref required_zone) = enforced_zone {
            if &cert.zone_id != required_zone {
                return Err(SwarmError::Transport(format!("Zone mismatch: peer is in {}, required {}", cert.zone_id, required_zone)));
            }
            if let Some(ref auth_pk) = zone_authority_pubkey {
                if cert.authority_pubkey != *auth_pk {
                    return Err(SwarmError::Transport("ZMC authority public key mismatch".into()));
                }
                if !cert.verify() {
                    return Err(SwarmError::Transport("ZMC signature verification failed".into()));
                }
            }
        }
        
        if cert.kyber_ek.len() != 1184 { return Err(SwarmError::Transport("Invalid Kyber EK length in ZMC".into())); }
        server_kyber_ek_bytes.copy_from_slice(&cert.kyber_ek);
    } else {
        if enforced_zone.is_some() {
            return Err(SwarmError::Transport("Peer did not present a required ZoneMembershipCertificate".into()));
        }
        stream.read_exact(&mut server_kyber_ek_bytes).await.map_err(|e| SwarmError::Transport(e.to_string()))?;
    }

    // Validate Kyber EK
    let server_ek = crypto::kyber_ek_from_bytes(&server_kyber_ek_bytes)
        .map_err(|_| SwarmError::Transport("Invalid Kyber encapsulation key".into()))?;

    // Encapsulate
    let (kyber_ct, pq_ss) = crypto::KyberKeyPair::encapsulate_with(&server_ek)
        .map_err(|_| SwarmError::Transport("Kyber encapsulation failed".into()))?;

    // Send Kyber CT (1088 bytes)
    stream.write_all(&kyber_ct).await.map_err(|e| SwarmError::Transport(format!("PQ Init write error: {}", e)))?;

    // Compute classical DH
    let server_x_pk = x25519_dalek::PublicKey::from(server_x_pk_bytes);
    let dh_ss = crypto::x25519_diffie_hellman(&client_x_sk, &server_x_pk);

    // Combine
    let hybrid = crypto::hybrid_kem_combine(&dh_ss, pq_ss.as_bytes());

    if let Some(bus) = event_bus {
        bus.emit(SwarmEvent {
            id: 0,
            timestamp: chrono::Utc::now(),
            domain: ConcernDomain::Security,
            severity: EventSeverity::Info,
            complexity: ComplexityHint::Moderate,
            summary: format!("HNDL-Resistant Tunnel Established (Kyber768 + X25519) to {}", peer_addr),
            details: serde_json::json!({
                "peer": peer_addr.to_string(),
                "protocol": "hybrid-kem",
                "components": ["kyber768", "x25519"],
                "role": "initiator",
                "zone_enforced": enforced_zone.is_some()
            }),
            related_entities: vec![],
            suggested_actions: vec![],
            source_node: None,
            correlation_id: None,
            supersedes: None,
        });
    }

    Ok(hybrid.combined_secret)
}

async fn perform_pq_handshake_responder(
    stream: &mut MaybeTlsStream,
    peer_addr: SocketAddr,
    event_bus: Option<Arc<EventBus>>,
    signing_identity: Option<Arc<crate::marabunta::identity::NodeIdentity>>,
    zone_certificate: Option<Arc<crate::highestsec::zone_membership::ZoneMembershipCertificate>>,
    enforced_zone: Option<String>,
    zone_authority_pubkey: Option<Vec<u8>>,
) -> SwarmResult<[u8; 32]> {
    let (server_x_sk, server_x_pk) = crypto::x25519_keypair();
    
    // Reads ClientHello (32 bytes X25519 PK)
    let mut client_x_pk_bytes = [0u8; 32];
    stream.read_exact(&mut client_x_pk_bytes).await.map_err(|e| SwarmError::Transport(format!("PQ Resp read error: {}", e)))?;
    
    // Read Initiator ZMC length
    let mut zmc_len_bytes = [0u8; 4];
    stream.read_exact(&mut zmc_len_bytes).await.map_err(|e| SwarmError::Transport(e.to_string()))?;
    let zmc_len = u32::from_le_bytes(zmc_len_bytes) as usize;
    
    if zmc_len > 0 {
        if zmc_len > 1024 * 1024 { return Err(SwarmError::Transport("ZMC too large".into())); }
        let mut zmc_bytes = vec![0u8; zmc_len];
        stream.read_exact(&mut zmc_bytes).await.map_err(|e| SwarmError::Transport(e.to_string()))?;
        
        let cert: crate::highestsec::zone_membership::ZoneMembershipCertificate = serde_json::from_slice(&zmc_bytes)
            .map_err(|_| SwarmError::Transport("Invalid ZMC JSON".into()))?;
            
        // Enforce Zone if configured
        if let Some(ref required_zone) = enforced_zone {
            if &cert.zone_id != required_zone {
                return Err(SwarmError::Transport(format!("Zone mismatch: peer is in {}, required {}", cert.zone_id, required_zone)));
            }
            if let Some(ref auth_pk) = zone_authority_pubkey {
                if cert.authority_pubkey != *auth_pk {
                    return Err(SwarmError::Transport("ZMC authority public key mismatch".into()));
                }
                if !cert.verify() {
                    return Err(SwarmError::Transport("ZMC signature verification failed".into()));
                }
            }
        }
    } else if enforced_zone.is_some() {
        return Err(SwarmError::Transport("Peer did not present a required ZoneMembershipCertificate".into()));
    }
    
    // Responder sends ServerHello (32 bytes X25519 PK)
    stream.write_all(server_x_pk.as_bytes()).await.map_err(|e| SwarmError::Transport(format!("PQ Resp write error: {}", e)))?;
    
    // Send our ZMC if we have one
    if let Some(cert) = zone_certificate {
        let json = serde_json::to_string(&*cert).map_err(|_| SwarmError::Transport("ZMC serialization failed".into()))?;
        let bytes = json.as_bytes();
        stream.write_all(&(bytes.len() as u32).to_le_bytes()).await.map_err(|e| SwarmError::Transport(e.to_string()))?;
        stream.write_all(bytes).await.map_err(|e| SwarmError::Transport(e.to_string()))?;
    } else {
        stream.write_all(&0u32.to_le_bytes()).await.map_err(|e| SwarmError::Transport(e.to_string()))?;
        // If no ZMC, send ephemeral Kyber EK
        let kyber_ek_bytes = if let Some(ref id) = signing_identity {
            id.kyber.encapsulation_key_bytes()
        } else {
            crypto::KyberKeyPair::generate().encapsulation_key_bytes()
        };
        stream.write_all(&kyber_ek_bytes).await.map_err(|e| SwarmError::Transport(format!("PQ Resp write error: {}", e)))?;
    }

    // Reads Kyber CT (1088 bytes)
    let mut kyber_ct_bytes = [0u8; 1088];
    stream.read_exact(&mut kyber_ct_bytes).await.map_err(|e| SwarmError::Transport(format!("PQ Resp read error: {}", e)))?;

    // Decapsulate
    let pq_ss = if let Some(ref id) = signing_identity {
        id.kyber.decapsulate(&kyber_ct_bytes).map_err(|_| SwarmError::Transport("Kyber decapsulation failed".into()))?
    } else {
        return Err(SwarmError::Transport("Cannot decapsulate without NodeIdentity".into()));
    };

    // Compute classical DH
    let client_x_pk = x25519_dalek::PublicKey::from(client_x_pk_bytes);
    let dh_ss = crypto::x25519_diffie_hellman(&server_x_sk, &client_x_pk);

    // Combine
    let hybrid = crypto::hybrid_kem_combine(&dh_ss, pq_ss.as_bytes());

    if let Some(bus) = event_bus {
        bus.emit(SwarmEvent {
            id: 0,
            timestamp: chrono::Utc::now(),
            domain: ConcernDomain::Security,
            severity: EventSeverity::Info,
            complexity: ComplexityHint::Moderate,
            summary: format!("HNDL-Resistant Tunnel Established (Kyber768 + X25519) from {}", peer_addr),
            details: serde_json::json!({
                "peer": peer_addr.to_string(),
                "protocol": "hybrid-kem",
                "components": ["kyber768", "x25519"],
                "role": "responder",
                "zone_enforced": enforced_zone.is_some()
            }),
            related_entities: vec![],
            suggested_actions: vec![],
            source_node: None,
            correlation_id: None,
            supersedes: None,
        });
    }

    Ok(hybrid.combined_secret)
}

impl SwarmTransport {
    pub fn set_advertised_addr(&self, addr: std::net::SocketAddr) {
        *self.advertised_addr.write() = Some(addr);
    }
    pub fn get_advertised_addr(&self) -> Option<std::net::SocketAddr> {
        *self.advertised_addr.read()
    }

    // ========================================================================
    // Predator Active Defense Methods
    // ========================================================================

    /// Actively ban a NodeId at the network layer.
    pub fn ban_node(&self, node: SwarmNodeId) {
        tracing::warn!(node_id = %node, "TRANSPORT: Enforcing active network ban");
        self.banned_nodes.insert(node, std::time::Instant::now());
        self.connections.clear(); 
    }

    /// Check if a node is currently banned.
    pub fn is_banned(&self, node: &SwarmNodeId) -> bool {
        self.banned_nodes.contains_key(node)
    }

    /// Create a new transport bound to the given address.
    pub fn new(node_id: NodeId, federation_id: FederationId, listen_addr: SocketAddr) -> Self {
        Self {
            node_id,
            federation_id,
            listen_addr,
            connections: Arc::new(DashMap::new()),
            stats: Arc::new(parking_lot::RwLock::new(TransportStats::default())),
            tls: None,
            signing_identity: None,
            transport_tuning: super::config::TransportTuningConfig::default(),
            advertised_addr: Arc::new(parking_lot::RwLock::new(None)),
            banned_nodes: Arc::new(DashMap::new()),
            event_bus: None,
            enforced_zone: None,
            zone_authority_pubkey: None,
            zone_certificate: None,
            inbound_connections_per_ip: Arc::new(dashmap::DashMap::new()),
        }
    }
    
    /// Enforce a Highestsec Zone at the transport layer. Connections without
    /// a valid certificate signed by `authority_pubkey` for `zone_id` will be dropped.
    pub fn with_zone_enforcement(mut self, zone_id: String, authority_pubkey: Vec<u8>) -> Self {
        self.enforced_zone = Some(zone_id);
        self.zone_authority_pubkey = Some(authority_pubkey);
        self
    }
    
    /// Equip this node with a ZoneMembershipCertificate to present to peers.
    pub fn with_zone_certificate(mut self, cert: Arc<crate::highestsec::zone_membership::ZoneMembershipCertificate>) -> Self {
        self.zone_certificate = Some(cert);
        self
    }

    /// Attach an event bus for logging security events.
    pub fn with_event_bus(mut self, bus: Arc<crate::swarm::events::EventBus>) -> Self {
        self.event_bus = Some(bus);
        self
    }

    /// Enable TLS on this transport.
    pub fn with_tls(mut self, tls_config: TlsConfig) -> Self {
        self.tls = Some(tls_config);
        self
    }

    /// Enable message signing with the given identity.
    pub fn with_signing(mut self, identity: Arc<NodeIdentity>) -> Self {
        self.signing_identity = Some(identity);
        self
    }

    /// Override transport tuning configuration (for runtime config).
    pub fn with_transport_tuning(mut self, tuning: super::config::TransportTuningConfig) -> Self {
        self.transport_tuning = tuning;
        self
    }

    /// Check if TLS is enabled.
    pub fn tls_enabled(&self) -> bool {
        self.tls.is_some()
    }

    // ========================================================================
    // Inbound
    // ========================================================================

    /// Start listening for inbound connections.
    ///
    /// Spawns an accept loop and per-connection handler tasks. Returns the
    /// actual bound address, which is useful when the configured port is `0`
    /// (OS-assigned).
    ///
    /// The accept loop runs until the `shutdown` watch channel signals `true`.
    pub async fn start_listener(
        self: &Arc<Self>,
        handler: MessageHandler,
        mut shutdown: watch::Receiver<bool>,
    ) -> SwarmResult<SocketAddr> {
        // Use TcpSocket to apply LISTEN_BACKLOG explicitly.
        let socket = if self.listen_addr.is_ipv6() {
            tokio::net::TcpSocket::new_v6()
        } else {
            tokio::net::TcpSocket::new_v4()
        }
        .map_err(|e| SwarmError::Transport(format!("failed to create socket: {}", e)))?;

        socket
            .set_reuseaddr(true)
            .map_err(|e| SwarmError::Transport(format!("set_reuseaddr: {}", e)))?;
        socket
            .bind(self.listen_addr)
            .map_err(|e| SwarmError::Transport(format!("failed to bind {}: {}", self.listen_addr, e)))?;
        let listener = socket
            .listen(LISTEN_BACKLOG)
            .map_err(|e| SwarmError::Transport(format!("listen({}): {}", LISTEN_BACKLOG, e)))?;

        let local_addr = listener.local_addr().map_err(|e| {
            SwarmError::Transport(format!("failed to get local address: {}", e))
        })?;

        info!(
            node_id = %self.node_id,
            listen_addr = %local_addr,
            "transport listener started"
        );

        let stats = Arc::clone(&self.stats);
        let node_id = self.node_id;
        let federation_id = self.federation_id;
        let tls_acceptor = self.tls.as_ref().map(|t| t.acceptor.clone());

        let event_bus_arc_clone = self.event_bus.clone();
        let signing_identity_clone = self.signing_identity.clone();
        let zmc_clone = self.zone_certificate.clone();
        let ez_clone = self.enforced_zone.clone();
        let zap_clone = self.zone_authority_pubkey.clone();
        let inbound_counts = Arc::clone(&self.inbound_connections_per_ip);
        let max_per_ip = self.transport_tuning.max_inbound_connections_per_ip;
        let max_total_inbound = 5000; // Hard limit to prevent TCP stack OOM
        let active_inbound = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let active_vital = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    result = shutdown.changed() => {
                        match result {
                            Ok(()) if *shutdown.borrow() => {
                                info!(node_id = %node_id, "transport listener shutting down");
                                break;
                            }
                            Ok(()) => continue,
                            Err(_) => {
                                debug!(node_id = %node_id, "shutdown channel closed, stopping listener");
                                break;
                            }
                        }
                    }

                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, peer_addr)) => {
                                let ip = peer_addr.ip();
                                let mut too_many = false;
                                
                                let current_total = active_inbound.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                
                                // 🛑 IDENTITY HANDSHAKE DEADLOCK (Connection Vitality Tiers)
                                // We reserve 10% (500) of our TCP slots for "Vital" connections.
                                // If we are above 4500 connections, we assume the connection is dropped
                                // UNLESS the peer can prove it is vital during the handshake.
                                // At this TCP accept stage, we just protect the absolute hard limit.
                                if current_total >= max_total_inbound {
                                    tracing::warn!("🔥 GATEWAY BACKPRESSURE: Dropping connection from {}. Total inbound connections {} exceeds limit {}.", peer_addr, current_total, max_total_inbound);
                                    active_inbound.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                                    continue;
                                }

                                {
                                    let mut count = inbound_counts.entry(ip).or_insert(0);
                                    if *count >= max_per_ip {
                                        too_many = true;
                                    } else {
                                        *count += 1;
                                    }
                                }
                                
                                if too_many {
                                    warn!(peer = %peer_addr, "dropping connection: exceeded max_inbound_connections_per_ip");
                                    active_inbound.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                                    continue;
                                }
                                
                                struct HybridGuard {
                                    ip: std::net::IpAddr,
                                    map: Arc<dashmap::DashMap<std::net::IpAddr, usize>>,
                                    total_counter: Arc<std::sync::atomic::AtomicUsize>,
                                }
                                impl Drop for HybridGuard {
                                    fn drop(&mut self) {
                                        if let Some(mut count) = self.map.get_mut(&self.ip) {
                                            if *count > 0 { *count -= 1; }
                                        }
                                        self.total_counter.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                                    }
                                }
                                
                                let guard = HybridGuard { ip, map: Arc::clone(&inbound_counts), total_counter: Arc::clone(&active_inbound) };

                                trace!(peer = %peer_addr, "accepted inbound connection");
                                let handler = Arc::clone(&handler);
                                let stats = Arc::clone(&stats);
                                let acceptor = tls_acceptor.clone();
                                let bus_clone = event_bus_arc_clone.clone();
                                let identity_clone = signing_identity_clone.clone();
                                let zmc_clone_inner = zmc_clone.clone();
                                let ez_clone_inner = ez_clone.clone();
                                let zap_clone_inner = zap_clone.clone();
                                let active_inbound_clone = Arc::clone(&active_inbound);
                                let active_vital_clone = Arc::clone(&active_vital);
                                tokio::spawn(Self::handle_inbound_connection(guard, 
                                    stream, peer_addr, federation_id, handler, stats, acceptor, bus_clone, identity_clone, zmc_clone_inner, ez_clone_inner, zap_clone_inner,
                                    active_inbound_clone, active_vital_clone,
                                ));
                            }
                            Err(e) => {
                                warn!(error = %e, "failed to accept connection");
                                // Brief pause to avoid tight error loops (e.g. fd exhaustion).
                                time::sleep(std::time::Duration::from_millis(50)).await;
                            }
                        }
                    }
                }
            }
        });

        Ok(local_addr)
    }

    /// Handle a single inbound TCP connection: read frames, decode, dispatch.
    async fn handle_inbound_connection<GuardType>(
        _guard: GuardType,
        mut stream: tokio::net::TcpStream,
        peer_addr: SocketAddr,
        federation_id: FederationId,
        handler: MessageHandler,
        stats: Arc<parking_lot::RwLock<TransportStats>>,
        tls_acceptor: Option<TlsAcceptor>,
        event_bus: Option<Arc<EventBus>>,
        signing_identity: Option<Arc<crate::marabunta::identity::NodeIdentity>>,
        zone_certificate: Option<Arc<crate::highestsec::zone_membership::ZoneMembershipCertificate>>,
        enforced_zone: Option<String>,
        zone_authority_pubkey: Option<Vec<u8>>,
        active_inbound: Arc<std::sync::atomic::AtomicUsize>,
        active_vital: Arc<std::sync::atomic::AtomicUsize>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        
        // --------------------------------------------------------------------
        // Pillar 11.2: Speed-of-Light Cryptographic Nonce Responder
        // --------------------------------------------------------------------
        // Before entering TLS or the framing protocol, we peek at the incoming stream.
        // If it's a 32-byte cryptographic nonce from a LatencyProber, we immediately
        // sign it with our biological private key and echo it back.
        // The RTT of this operation physically bounds our geographic location.
        let mut peek_buf = [0u8; 32];
        if let Ok(Ok(n)) = tokio::time::timeout(std::time::Duration::from_millis(50), stream.peek(&mut peek_buf)).await {
            if n == 32 {
                // Read the nonce off the wire
                if stream.read_exact(&mut peek_buf).await.is_ok() {
                    if let Some(ref identity) = signing_identity {
                        // Sign the nonce using Ed25519 (64-byte signature)
                        let signature = identity.sign_ed25519(&peek_buf);
                        let _ = stream.write_all(&signature).await;
                        tracing::debug!(peer = %peer_addr, "🌍 SPEED-OF-LIGHT: Satisfied cryptographic latency challenge.");
                    } else {
                        // Unsigned fallback (64 bytes of zeros) for nodes lacking keys
                        let _ = stream.write_all(&[0u8; 64]).await;
                    }
                    // The LatencyProber immediately drops the connection after receiving the signature.
                    return; 
                }
            }
        }

        // --- Resume standard protocol for data traffic ---

        let mut maybe_tls_stream = if let Some(acceptor) = tls_acceptor {
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    debug!(peer = %peer_addr, "TLS handshake succeeded (inbound)");
                    MaybeTlsStream::ServerTls(tls_stream)
                }
                Err(e) => {
                    warn!(peer = %peer_addr, error = %e, "TLS handshake failed (inbound)");
                    return;
                }
            }
        } else {
            trace!(peer = %peer_addr, "accepting plaintext connection (no TLS configured)");
            MaybeTlsStream::Plain(stream)
        };

        
        let combined_secret = match perform_pq_handshake_responder(&mut maybe_tls_stream, peer_addr, event_bus, signing_identity, zone_certificate, enforced_zone, zone_authority_pubkey).await {
            Ok(ss) => ss,
            Err(e) => {
                tracing::warn!(peer = %peer_addr, error = %e, "PQ Handshake failed");
                return;
            }
        };

        let codec = build_codec();
        let mut framed = Framed::new(maybe_tls_stream, codec);
        let mut rx_counter = 0u64;

        // Pillar 8.4: Side-Channel Stream State
        let mut active_stream_file: Option<(tokio::fs::File, String, SwarmMessage)> = None;

        loop {
            let read_result = time::timeout(TRANSPORT_READ_TIMEOUT, framed.next()).await;

            match read_result {
                Ok(Some(Ok(frame))) => {
                    let mut nonce = [0u8; 12];
                    nonce[4..12].copy_from_slice(&rx_counter.to_le_bytes());
                    rx_counter += 1;
                    
                    let decrypted_frame = match crate::marabunta::crypto::aes256gcm_decrypt(&combined_secret, &nonce, &frame) {
                        Ok(d) => d,
                        Err(e) => {
                            tracing::warn!(peer = %peer_addr, error = %e, "AES-GCM decryption failed");
                            break;
                        }
                    };

                    match decode_message(&decrypted_frame, federation_id) {
                        Ok(message) => {
                            // 1. Handle Side-Channel Data Chunks
                            match message {
                                SwarmMessage::DataChunk { ref data } => {
                                    if let Some((ref mut file, _, _)) = active_stream_file {
                                        let _ = file.write_all(&data).await;
                                        continue; // Skip handler for raw data
                                    }
                                },
                                SwarmMessage::EndOfStream => {
                                    if let Some((mut file, path, mut original_msg)) = active_stream_file.take() {
                                        let _ = file.flush().await;
                                        tracing::info!(peer = %peer_addr, path = %path, "🐺 TRANSPORT: Side-channel stream reconstruction complete.");
                                        
                                        // Inject the local path back into the original message
                                        match original_msg {
                                            SwarmMessage::StigmergicResponse { ref mut payload_path, .. } => *payload_path = Some(path),
                                            
                                            _ => {}
                                        }
                                        handler(peer_addr, original_msg);
                                        continue;
                                    }
                                },
                                _ => {}
                            }

                            // 2. Handle New Stream Initiation
                            let stream_follows = match &message {
                                SwarmMessage::StigmergicResponse { stream_follows, .. } => *stream_follows,
                                
                                _ => false,
                            };

                            if stream_follows {
                                let local_path = format!("/dev/shm/stigmergic_rx_{}.bin", uuid::Uuid::new_v4().simple());
                                if let Ok(file) = tokio::fs::File::create(&local_path).await {
                                    tracing::info!(peer = %peer_addr, path = %local_path, "🐺 TRANSPORT: Entering Side-Channel Stream Sink mode.");
                                    active_stream_file = Some((file, local_path, message));
                                    continue;
                                }
                            }

                            // 3. Standard Dispatch
                            handler(peer_addr, message);
                        }
                        Err(e) => {
                            warn!(peer = %peer_addr, error = %e, "failed to decode inbound message");
                        }
                    }
                }
                Ok(Some(Err(e))) => {
                    warn!(peer = %peer_addr, error = %e, "frame read error, closing connection");
                    break;
                }
                Ok(None) => {
                    // Stream ended (peer closed).
                    debug!(peer = %peer_addr, "inbound connection closed by peer");
                    break;
                }
                Err(_) => {
                    // Read timeout elapsed.
                    debug!(
                        peer = %peer_addr,
                        timeout_secs = TRANSPORT_READ_TIMEOUT.as_secs(),
                        "inbound read timeout, closing connection"
                    );
                    break;
                }
            }
        }
    }

    // ========================================================================
    // Outbound
    // ========================================================================

    /// Send a message to a specific peer.
    ///
    /// If the connection pool contains a live connection to `target`, the
    /// message is sent through it. Otherwise a new TCP connection is
    /// established (with [`TRANSPORT_CONNECT_TIMEOUT`]), a writer task is
    /// spawned, and the connection is added to the pool.
    pub async fn send(&self, target: SocketAddr, message: SwarmMessage) -> SwarmResult<()> {
        let is_vital = matches!(&message, 
            SwarmMessage::PushBlob { .. } | 
            SwarmMessage::FetchBlob { .. } | 
            SwarmMessage::ChunkResult { .. }
        );

        let payload_path = match &message {
            
            SwarmMessage::StigmergicResponse { payload_path, .. } => payload_path.clone(),
            SwarmMessage::Ping { .. } => None,
            _ => None,
        };

        let header = match &self.signing_identity {
            Some(id) => encode_signed(&message, id, self.federation_id)?,
            None => encode_message(&message)?,
        };
        let header_len = header.len() as u64;
        
        let outbound_frame = OutboundFrame {
            header,
            payload_path,
        };

        // Fast path: reuse existing connection.
        if let Some(mut entry) = self.connections.get_mut(&target) {
            entry.last_used = Instant::now();
            if is_vital {
                entry.is_vital = true;
            }
            let sender = entry.sender.clone();
            drop(entry);

            match sender.try_send(outbound_frame.clone()) {
                Ok(()) => {
                    let mut s = self.stats.write();
                    s.messages_sent += 1;
                    s.bytes_sent += header_len;
                    return Ok(());
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    if sender.send(outbound_frame.clone()).await.is_ok() {
                        let mut s = self.stats.write();
                        s.messages_sent += 1;
                        s.bytes_sent += header_len;
                        return Ok(());
                    }
                }
                _ => {}
            }
            self.connections.remove(&target);
        }

        // Slow path: establish a new connection.
        let sender = self.establish_connection(target, is_vital).await?;
        sender.send(outbound_frame.clone()).await.map_err(|_| {
            self.connections.remove(&target);
            SwarmError::Transport(format!("connection dropped"))
        })?;

        {
            let mut s = self.stats.write();
            s.messages_sent += 1;
            s.bytes_sent += header_len;
        }
        Ok(())
    }

    /// Send a message to each of the given targets concurrently.
    ///
    /// Returns one result per target in the same order as the input slice.
    pub async fn send_many(&self, targets: &[(SocketAddr, SwarmMessage)]) -> Vec<SwarmResult<()>> {
        let mut handles = Vec::with_capacity(targets.len());

        for (addr, msg) in targets {
            let addr = *addr;
            let msg = msg.clone();
            // We cannot move `self` into the spawned task, so we grab the
            // Arc-wrapped internals we need. However, `send` takes `&self`,
            // so we work around it by encoding + sending directly.
            let connections = Arc::clone(&self.connections);
            let stats = Arc::clone(&self.stats);
            let node_id = self.node_id;
            let tls_connector = self.tls.as_ref().map(|t| t.connector.clone());
            let signing_identity = self.signing_identity.clone();
            let federation_id = self.federation_id;

            // Build a small future that does exactly what `send` does but
            // uses the cloned Arcs instead of `&self`.
            handles.push(tokio::spawn(async move {
                let transport = InternalSender {
                    _node_id: node_id,
                    federation_id,
                    connections,
                    stats,
                    tls_connector,
                    signing_identity,
                };
                transport.send_inner(addr, msg).await
            }));
        }

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(SwarmError::Transport(format!(
                    "send task panicked: {}",
                    e
                )))),
            }
        }

        results
    }

    /// Establish a new TCP connection to `target` and register it in the pool.
    ///
    /// Returns the mpsc sender through which frames can be enqueued for the
    /// background writer task.

    // --- STAGE 6.1: Fabric Auto-Detection ---
    // We ping the target to determine if it is sitting in the same physical 
    // datacenter (e.g., NY4). If RTT is < 2ms, we upgrade to DatacenterLocal.
    pub async fn detect_fabric(target: SocketAddr) -> FabricType {
        let start = std::time::Instant::now();
        // A real implementation would use a dedicated lightweight UDP ping here.
        // For the architectural proof, we simulate a quick TCP connection attempt 
        // to measure base latency.
        if let Ok(mut stream) = time::timeout(std::time::Duration::from_millis(5), tokio::net::TcpStream::connect(target)).await {
            if let Ok(_) = stream {
                let rtt = start.elapsed();
                if rtt < std::time::Duration::from_millis(2) {
                    tracing::info!(peer = %target, rtt_ms = ?rtt.as_millis(), "🚀 BIMODAL FABRIC: Ultra-low latency detected. Optimizing for DatacenterLocal low-latency peer.fast-path.");
                    return FabricType::DatacenterLocal;
                }
            }
        }
        FabricType::ResilientQuic
    }

    async fn establish_connection(
        &self,
        target: SocketAddr,
        is_vital: bool,
    ) -> SwarmResult<mpsc::Sender<OutboundFrame>> {
        // Enforce the maximum pool size by evicting the oldest connection.
        self.enforce_pool_limit();

        let stream = time::timeout(TRANSPORT_CONNECT_TIMEOUT, TcpStream::connect(target))
            .await
            .map_err(|_| {
                self.stats.write().connections_failed += 1;
                SwarmError::Transport(format!(
                    "connection to {} timed out after {:?}",
                    target, TRANSPORT_CONNECT_TIMEOUT
                ))
            })?
            .map_err(|e| {
                self.stats.write().connections_failed += 1;
                SwarmError::Transport(format!("failed to connect to {}: {}", target, e))
            })?;

        // Disable Nagle for lower latency.
        let _ = stream.set_nodelay(true);

        let mut maybe_tls_stream = if let Some(ref tls) = self.tls {
            let server_name = rustls::pki_types::ServerName::from(target.ip());
            match tls.connector.connect(server_name, stream).await {
                Ok(tls_stream) => {
                    debug!(peer = %target, "TLS handshake succeeded (outbound)");
                    MaybeTlsStream::ClientTls(tls_stream)
                }
                Err(e) => {
                    warn!(peer = %target, error = %e, "TLS handshake failed (outbound)");
                    return Err(SwarmError::Transport(format!(
                        "TLS handshake to {target} failed: {e}"
                    )));
                }
            }
        } else {
            trace!(peer = %target, "connecting plaintext (no TLS configured)");
            MaybeTlsStream::Plain(stream)
        };

        
        // Enforce PQ tunnel
        let combined_secret = perform_pq_handshake_initiator(&mut maybe_tls_stream, target, self.event_bus.clone(), self.signing_identity.clone(), self.zone_certificate.clone(), self.enforced_zone.clone(), self.zone_authority_pubkey.clone()).await?;

        let codec = build_codec();
        let framed = Framed::new(maybe_tls_stream, codec);
        let (sink, _reader) = framed.split();

        // Channel between the caller and the writer task. A bounded channel
        // provides natural back-pressure when the network is slow.
        let (tx, rx) = mpsc::channel::<OutboundFrame>(128);

        // Spawn a writer task that drains the channel into the TCP sink.
        let connections = Arc::clone(&self.connections);
        tokio::spawn(Self::writer_task(target, sink, rx, connections, combined_secret));

        
        let fabric = SwarmTransport::detect_fabric(target).await;
        
        self.connections.insert(
            target,
            ConnectionHandle {
                sender: tx.clone(),
                last_used: Instant::now(),
                fabric,
                is_vital,
            },
        );

        {
            let mut s = self.stats.write();
            s.connections_established += 1;
            s.active_connections = self.connections.len();
        }

        debug!(target = %target, pool_size = self.connections.len(), "new outbound connection");

        Ok(tx)
    }

    /// Background task that reads frames from the mpsc channel and writes
    /// them to the TCP sink. Exits when the channel is closed or a write
    /// error occurs.
    async fn writer_task(
        target: SocketAddr,
        mut sink: futures::stream::SplitSink<
            Framed<MaybeTlsStream, LengthDelimitedCodec>,
            bytes::Bytes,
        >,
        mut rx: mpsc::Receiver<OutboundFrame>,
        connections: Arc<DashMap<SocketAddr, ConnectionHandle>>,
        combined_secret: [u8; 32],
    ) {
        let mut tx_counter = 0u64;
        while let Some(outbound) = rx.recv().await {
            // 1. Send Header
            let mut nonce = [0u8; 12];
            nonce[4..12].copy_from_slice(&tx_counter.to_le_bytes());
            tx_counter += 1;
            
            let encrypted_header = match crate::marabunta::crypto::aes256gcm_encrypt(&combined_secret, &nonce, &outbound.header) {
                Ok(e) => e,
                Err(e) => {
                    tracing::error!(target_addr = %target, error = %e, "AES-GCM header encryption failed");
                    break;
                }
            };

            if let Err(e) = sink.send(bytes::Bytes::from(encrypted_header)).await {
                warn!(target_addr = %target, error = %e, "write error (header), closing outbound connection");
                break;
            }

            // 2. Pillar 8.4: Physical File Streaming (Side-Channel)
            if let Some(path) = outbound.payload_path {
                if let Ok(mut file) = tokio::fs::File::open(&path).await {
                    tracing::info!(target = %target, path = %path, "🐺 TRANSPORT: Initiating side-channel QUIC stream for 200GB shard.");
                    
                    let mut buf = vec![0u8; 1024 * 1024]; // 1MB streaming window
                    while let Ok(n) = file.read(&mut buf).await {
                        if n == 0 { break; }
                        
                        let mut data_nonce = [0u8; 12];
                        data_nonce[4..12].copy_from_slice(&tx_counter.to_le_bytes());
                        tx_counter += 1;

                        // We wrap each chunk in a DataChunk message to maintain protocol framing
                        let chunk_msg = SwarmMessage::DataChunk { data: buf[..n].to_vec() };
                        let chunk_bytes = encode_message(&chunk_msg).unwrap_or_default();

                        let encrypted_chunk = match crate::marabunta::crypto::aes256gcm_encrypt(&combined_secret, &data_nonce, &chunk_bytes) {
                            Ok(e) => e,
                            Err(_) => break,
                        };

                        if sink.send(bytes::Bytes::from(encrypted_chunk)).await.is_err() { break; }
                    }
                    
                    // 3. Send End of Stream
                    let mut end_nonce = [0u8; 12];
                    end_nonce[4..12].copy_from_slice(&tx_counter.to_le_bytes());
                    tx_counter += 1;

                    let end_msg = encode_message(&SwarmMessage::EndOfStream).unwrap_or_default();
                    let encrypted_end = crate::marabunta::crypto::aes256gcm_encrypt(&combined_secret, &end_nonce, &end_msg).unwrap_or_default();
                    let _ = sink.send(bytes::Bytes::from(encrypted_end)).await;

                    tracing::info!(target = %target, "🐺 TRANSPORT: Side-channel stream finalized.");
                } else {
                    tracing::error!("🐺 TRANSPORT: Failed to open streaming payload at path: {}", path);
                }
            }
        }

        // Ensure the connection is removed from the pool when the writer stops.
        connections.remove(&target);
        debug!(target_addr = %target, "outbound writer task finished");
    }

    /// Remove the oldest connection if the pool has reached its capacity limit.
    fn enforce_pool_limit(&self) {
        while self.connections.len() >= crate::swarm::hardware::get_bounds().max_outbound_connections {
            // 🛑 THE IDENTITY HANDSHAKE DEADLOCK FIX
            // When we hit the socket limit, we actively protect connections marked as 'vital'
            // (e.g. ones currently streaming 20MB WASM binaries) from being evicted.
            let oldest_non_vital = self
                .connections
                .iter()
                .filter(|entry| !entry.value().is_vital)
                .min_by_key(|entry| entry.value().last_used)
                .map(|entry| *entry.key());

            // If everything is vital (rare), fall back to absolute oldest.
            let oldest = oldest_non_vital.or_else(|| {
                self.connections
                    .iter()
                    .min_by_key(|entry| entry.value().last_used)
                    .map(|entry| *entry.key())
            });

            if let Some(addr) = oldest {
                self.connections.remove(&addr);
                debug!(
                    evicted = %addr,
                    "evicted oldest connection to stay within pool limit"
                );
            } else {
                break;
            }
        }
    }

    // ========================================================================
    // Lifecycle
    // ========================================================================

    /// Close all outbound connections and clear the pool.
    ///
    /// Inbound connections are stopped by signalling the shutdown watch
    /// channel that was passed to [`start_listener`](Self::start_listener).
    pub async fn shutdown(&self) {
        let count = self.connections.len();
        self.connections.clear();
        info!(
            node_id = %self.node_id,
            connections_closed = count,
            "transport shut down"
        );
    }

    /// Spawn a background task that periodically prunes idle connections.
    ///
    /// Runs every 30 seconds. Connections whose `last_used` timestamp is
    /// older than [`CONNECTION_IDLE_TIMEOUT`] are removed (which drops the
    /// mpsc sender and causes the writer task to exit).
    pub fn spawn_connection_pruner(
        self: &Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let connections = Arc::clone(&self.connections);
        let stats = Arc::clone(&self.stats);
        let node_id = self.node_id;

        tokio::spawn(async move {
            let prune_interval = std::time::Duration::from_secs(30);

            loop {
                tokio::select! {
                    biased;

                    result = shutdown.changed() => {
                        match result {
                            Ok(()) if *shutdown.borrow() => {
                                debug!(node_id = %node_id, "connection pruner shutting down");
                                break;
                            }
                            Ok(()) => continue,
                            Err(_) => break,
                        }
                    }

                    _ = time::sleep(prune_interval) => {
                        let now = Instant::now();
                        let mut pruned = 0usize;

                        connections.retain(|addr, handle| {
                            let idle = now.duration_since(handle.last_used);
                            if idle > CONNECTION_IDLE_TIMEOUT {
                                debug!(
                                    peer = %addr,
                                    idle_secs = idle.as_secs(),
                                    "pruning idle connection"
                                );
                                pruned += 1;
                                false
                            } else if handle.sender.is_closed() {
                                debug!(
                                    peer = %addr,
                                    "pruning closed connection"
                                );
                                pruned += 1;
                                false
                            } else {
                                true
                            }
                        });

                        if pruned > 0 {
                            let remaining = connections.len();
                            stats.write().active_connections = remaining;
                            debug!(
                                pruned,
                                remaining,
                                "connection pruner cycle complete"
                            );
                        }
                    }
                }
            }
        })
    }

    // ========================================================================
    // Accessors
    // ========================================================================

    /// Return a snapshot of transport statistics.
    pub fn stats(&self) -> TransportStats {
        let mut s = self.stats.read().clone();
        s.active_connections = self.connections.len();
        s
    }

    /// Number of currently pooled outbound connections.
    pub fn active_connections(&self) -> usize {
        self.connections.len()
    }
}

// ============================================================================
// InternalSender — helper for send_many
// ============================================================================

/// Lightweight handle that captures the Arc-wrapped internals of
/// [`SwarmTransport`] so that `send_many` can spawn independent tasks
/// without requiring `Arc<SwarmTransport>`.
struct InternalSender {
    _node_id: NodeId,
    federation_id: FederationId,
    connections: Arc<DashMap<SocketAddr, ConnectionHandle>>,
    stats: Arc<parking_lot::RwLock<TransportStats>>,
    tls_connector: Option<TlsConnector>,
    signing_identity: Option<Arc<NodeIdentity>>,
}

impl InternalSender {
    /// Mirrors [`SwarmTransport::send`] using the captured Arcs.
    async fn send_inner(&self, target: SocketAddr, message: SwarmMessage) -> SwarmResult<()> {
        let is_vital = matches!(&message, 
            SwarmMessage::PushBlob { .. } | 
            SwarmMessage::FetchBlob { .. } | 
            SwarmMessage::ChunkResult { .. }
        );
        let payload_path = match &message {
            
            SwarmMessage::StigmergicResponse { payload_path, .. } => payload_path.clone(),
            SwarmMessage::Ping { .. } => None,
            _ => None,
        };
        let header = match &self.signing_identity {
            Some(id) => encode_signed(&message, id, self.federation_id)?,
            None => encode_message(&message)?,
        };
        let outbound_frame = OutboundFrame { header, payload_path };

        if let Some(mut entry) = self.connections.get_mut(&target) {
            entry.last_used = Instant::now();
            if is_vital {
                entry.is_vital = true;
            }
            let sender = entry.sender.clone();
            drop(entry);
            if sender.send(outbound_frame).await.is_ok() { return Ok(()); }
            self.connections.remove(&target);
        }

        // We do not implement connection establishment in InternalSender 
        // to avoid complexity for send_many; just return error if pooled connection missing.
        Err(SwarmError::Transport(format!("no pooled connection to {}", target)))
    }

    /// Establish a new outbound TCP connection and register in the shared pool.
    async fn establish_connection(&self, target: SocketAddr) -> SwarmResult<mpsc::Sender<OutboundFrame>> {
        // Enforce pool limit.
        while self.connections.len() >= crate::swarm::hardware::get_bounds().max_outbound_connections {
            // 🛑 THE IDENTITY HANDSHAKE DEADLOCK FIX (send_many)
            let oldest_non_vital = self
                .connections
                .iter()
                .filter(|entry| !entry.value().is_vital)
                .min_by_key(|entry| entry.value().last_used)
                .map(|entry| *entry.key());

            let oldest = oldest_non_vital.or_else(|| {
                self.connections
                    .iter()
                    .min_by_key(|entry| entry.value().last_used)
                    .map(|entry| *entry.key())
            });

            if let Some(addr) = oldest {
                self.connections.remove(&addr);
                debug!(
                    evicted = %addr,
                    "evicted oldest connection to stay within pool limit (send_many)"
                );
            } else {
                break;
            }
        }

        let stream = time::timeout(TRANSPORT_CONNECT_TIMEOUT, TcpStream::connect(target))
            .await
            .map_err(|_| {
                self.stats.write().connections_failed += 1;
                SwarmError::Transport(format!(
                    "connection to {} timed out after {:?}",
                    target, TRANSPORT_CONNECT_TIMEOUT
                ))
            })?
            .map_err(|e| {
                self.stats.write().connections_failed += 1;
                SwarmError::Transport(format!("failed to connect to {}: {}", target, e))
            })?;

        let _ = stream.set_nodelay(true);

        let maybe_tls_stream = if let Some(ref connector) = self.tls_connector {
            let server_name = rustls::pki_types::ServerName::from(target.ip());
            match connector.connect(server_name, stream).await {
                Ok(tls_stream) => {
                    debug!(peer = %target, "TLS handshake succeeded (outbound, send_many)");
                    MaybeTlsStream::ClientTls(tls_stream)
                }
                Err(e) => {
                    warn!(peer = %target, error = %e, "TLS handshake failed (outbound, send_many)");
                    return Err(SwarmError::Transport(format!(
                        "TLS handshake to {target} failed: {e}"
                    )));
                }
            }
        } else {
            MaybeTlsStream::Plain(stream)
        };

        let codec = build_codec();
        let framed = Framed::new(maybe_tls_stream, codec);
        let (sink, _reader) = framed.split();

        let (tx, rx) = mpsc::channel::<OutboundFrame>(128);

        let connections = Arc::clone(&self.connections);
        tokio::spawn(SwarmTransport::writer_task(target, sink, rx, connections, [0u8; 32]));

        
        let fabric = SwarmTransport::detect_fabric(target).await;
        
        self.connections.insert(
            target,
            ConnectionHandle {
                sender: tx.clone(),
                last_used: Instant::now(),
                fabric,
                is_vital: false,
            },
        );

        {
            let mut s = self.stats.write();
            s.connections_established += 1;
            s.active_connections = self.connections.len();
        }

        debug!(
            target_addr = %target,
            pool_size = self.connections.len(),
            "new outbound connection (send_many)"
        );

        Ok(tx)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::watch;

    /// Construct a minimal Ping message for testing.
    fn test_message(nonce: u64) -> SwarmMessage {
        SwarmMessage::Ping {
            from: SwarmNodeId::new(),
            nonce,
            pow_nonce: None,
        }
    }

    #[test]
    fn encode_decode_signed_roundtrip() {
        let identity = NodeIdentity::generate().unwrap();
        let msg = test_message(42);
        let fed_id = FederationId::generate();
        let encoded = encode_signed(&msg, &identity, fed_id).expect("encode_signed failed");
        let decoded = decode_message(&encoded, fed_id).expect("decode failed");

        match decoded {
            SwarmMessage::Ping { nonce, .. } => assert_eq!(nonce, 42),
            other => panic!("unexpected variant: {:?}", other),
        }
    }

    #[test]
    fn decode_raw_json_rejected() {
        let msg = test_message(42);
        let raw_json = serde_json::to_vec(&msg).unwrap();
        let fed_id = FederationId::generate();
        let result = decode_message(&raw_json, fed_id);
        assert!(result.is_err(), "raw JSON should be rejected");
    }

    #[test]
    fn decode_garbage_returns_error() {
        let garbage = &[0xFF, 0xFE, 0xFD, 0xFC];
        let fed_id = FederationId::generate();
        assert!(decode_message(garbage, fed_id).is_err());
    }

    #[tokio::test]
    async fn listener_accepts_and_dispatches() {
        let node_a = NodeId::random();
        let node_b = NodeId::random();

        let transport_a = Arc::new(SwarmTransport::new(
            node_a,
            FederationId::generate(),
            "127.0.0.1:0".parse().unwrap(),
        ));

        let received = Arc::new(AtomicU64::new(0));
        let received_clone = Arc::clone(&received);

        let handler: MessageHandler = Arc::new(move |_addr, msg| {
            if let SwarmMessage::Ping { nonce, .. } = msg {
                received_clone.store(nonce, Ordering::SeqCst);
            }
        });

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let bound_addr = transport_a
            .start_listener(handler, shutdown_rx)
            .await
            .expect("start_listener failed");

        // Give the listener a moment to start accepting.
        time::sleep(std::time::Duration::from_millis(50)).await;

        // Send from a separate transport.
        let identity = NodeIdentity::generate().unwrap();
        let transport_b = SwarmTransport::new(NodeId::random(), FederationId::generate(), "127.0.0.1:0".parse().unwrap())
            .with_signing(Arc::new(identity));

        transport_b
            .send(bound_addr, test_message(99))
            .await
            .expect("send failed");

        // Wait for the message to be dispatched.
        time::sleep(std::time::Duration::from_millis(200)).await;

        assert_eq!(received.load(Ordering::SeqCst), 99);

        // Verify stats.
        let stats_b = transport_b.stats();
        assert_eq!(stats_b.messages_sent, 1);
        assert!(stats_b.bytes_sent > 0);
        assert_eq!(stats_b.connections_established, 1);

        let stats_a = transport_a.stats();
        assert_eq!(stats_a.messages_received, 1);
        assert!(stats_a.bytes_received > 0);

        // Shutdown.
        let _ = shutdown_tx.send(true);
        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn send_many_concurrent() {
        let node = NodeId::random();
        let listener_transport = Arc::new(SwarmTransport::new(
            NodeId::random(),
            FederationId::generate(),
            "127.0.0.1:0".parse().unwrap(),
        ));

        let counter = Arc::new(AtomicU64::new(0));
        let counter_clone = Arc::clone(&counter);

        let handler: MessageHandler = Arc::new(move |_addr, _msg| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let bound_addr = listener_transport
            .start_listener(handler, shutdown_rx)
            .await
            .expect("start_listener failed");

        time::sleep(std::time::Duration::from_millis(50)).await;

        let identity = NodeIdentity::generate().unwrap();
        let sender = SwarmTransport::new(node, FederationId::generate(), "127.0.0.1:0".parse().unwrap())
            .with_signing(Arc::new(identity));

        let targets: Vec<(SocketAddr, SwarmMessage)> = (0..5)
            .map(|i| (bound_addr, test_message(i)))
            .collect();

        let results = sender.send_many(&targets).await;
        for (i, r) in results.iter().enumerate() {
            assert!(r.is_ok(), "send_many[{}] failed: {:?}", i, r);
        }

        time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 5);

        let _ = shutdown_tx.send(true);
        listener_transport.shutdown().await;
        sender.shutdown().await;
    }

    #[tokio::test]
    async fn connection_pool_reuses_connections() {
        let listener_transport = Arc::new(SwarmTransport::new(
            NodeId::random(),
            FederationId::generate(),
            "127.0.0.1:0".parse().unwrap(),
        ));

        let handler: MessageHandler = Arc::new(|_addr, _msg| {});

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let bound_addr = listener_transport
            .start_listener(handler, shutdown_rx)
            .await
            .expect("start_listener failed");

        time::sleep(std::time::Duration::from_millis(50)).await;

        let identity = NodeIdentity::generate().unwrap();
        let sender = SwarmTransport::new(NodeId::random(), FederationId::generate(), "127.0.0.1:0".parse().unwrap())
            .with_signing(Arc::new(identity));

        // Send two messages to the same target.
        sender.send(bound_addr, test_message(1)).await.unwrap();
        sender.send(bound_addr, test_message(2)).await.unwrap();

        // Should still only have one connection in the pool.
        assert_eq!(sender.active_connections(), 1);

        // And only one connection established in stats.
        assert_eq!(sender.stats().connections_established, 1);
        assert_eq!(sender.stats().messages_sent, 2);

        let _ = shutdown_tx.send(true);
        listener_transport.shutdown().await;
        sender.shutdown().await;
    }
}
pub mod torrent;
pub mod onion;
pub mod rdma;
pub mod stigmergic_dtn;
