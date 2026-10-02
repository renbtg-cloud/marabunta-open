// Marabunta - Licensed under the MIT License.
//! Phantom Protocol - Anonymous BYOD Participation in Marabunta Compute
//!
//! The Phantom Protocol enables workers to contribute compute resources to
//! Marabunta Compute while maintaining cryptographic privacy guarantees. This
//! module provides the infrastructure for anonymous participation without
//! revealing worker identity.
//!
//! # Core Concepts
//!
//! ## Anonymous Contribution
//!
//! Workers can contribute compute power without revealing their identity.
//! The system uses blind signatures to create verifiable contribution
//! receipts that cannot be traced back to specific workers.
//!
//! ## Tiered Disclosure
//!
//! When redeeming contributions for rewards or recognition, workers can
//! choose how much information to reveal:
//!
//! - **Anonymous**: Prove contributions exist without any identifying info
//! - **Tier Only**: Reveal contribution tier (Bronze/Silver/Gold/Platinum)
//! - **Amount Only**: Reveal total compute units contributed
//! - **Full**: Complete disclosure with employee ID
//!
//! ## Double-Spend Prevention
//!
//! The protocol prevents workers from claiming the same contribution
//! multiple times through cryptographic key images that can detect reuse
//! without revealing the user's identity.
//!
//! # Architecture
//!
//! The Phantom Protocol consists of several layers:
//!
//! ```text
//! ┌───────────────────────────────────────────────────────────────┐
//! │                      Application Layer                        │
//! │  (Token Wallet, Redemption Packages, Disclosure Controls)     │
//! ├───────────────────────────────────────────────────────────────┤
//! │                      Protocol Layer                           │
//! │  (Blind Signatures, Ring Signatures, Zero-Knowledge Proofs)   │
//! ├───────────────────────────────────────────────────────────────┤
//! │                    Cryptographic Layer                        │
//! │  (RSA, Elliptic Curves, Hash Functions, Secure Random)        │
//! └───────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Modules
//!
//! - [`crypto`]: Cryptographic primitives (signatures, proofs, tokens)
//!
//! # Security Considerations
//!
//! - All cryptographic keys are stored encrypted
//! - Private keys are zeroized when dropped
//! - Timing-safe comparisons are used for sensitive operations
//! - Minimum key sizes enforce security requirements
//!
//! # Example Usage
//!
//! ```rust,ignore
//! use marabunta_compute::phantom::crypto::{
//!     BlindSignatureKeyPair,
//!     TokenWallet,
//!     DisclosureLevel,
//! };
//!
//! // Setup
//! let coordinator_keys = BlindSignatureKeyPair::generate(3072)?;
//! let mut wallet = TokenWallet::new("secure_password")?;
//!
//! // Contribute and collect tokens...
//!
//! // Redeem anonymously
//! let package = wallet.prepare_redemption(DisclosureLevel::TierOnly)?;
//! ```

pub mod crypto;
pub mod network;

// Re-export the crypto module's public API at the phantom level
pub use crypto::{
    // Blind signatures
    BlindSignature,
    BlindSignatureKeyPair,
    BlindSignaturePublicKey,
    BlindedMessage,
    // Zero-knowledge proofs
    BlindingFactor,
    // Tokens
    ContributionReceipt,
    ContributionTier,
    ContributionToken,
    // Errors
    CryptoError,
    DisclosureLevel,
    PedersenCommitment,
    RangeProof,
    RedemptionPackage,
    // Ring signatures
    RingParams,
    RingPrivateKey,
    RingPublicKey,
    RingSignature,
    RingSignatureScheme,
    TierProof,
    TokenError,
    TokenWallet,
    UnblindedSignature,
};

// Re-export the network module's public API
pub use network::{
    // Onion routing
    CircuitId,
    CircuitInfo,
    CircuitState,
    // Cover traffic
    CoverConfig,
    // Errors
    CoverError,
    CoverStats,
    CoverStrategy,
    CoverTrafficGenerator,
    CoverTrafficSystem,
    // Fingerprint security_domain
    SecurityDomainConfig,
    SecurityDomainConfigBuilder,
    SecurityDomainError,
    DeviceCharacteristics,
    FakeProfile,
    FingerprintSecurityDomain,
    GossipError,
    // Peer mesh
    GossipProtocol,
    IntegratedSecurityDomain,
    LatencyNormalizer,
    MeshConfig,
    MeshConfigBuilder,
    MeshError,
    // Integrated system
    NetworkStats,
    OnionCell,
    OnionConfig,
    OnionError,
    OnionRouter,
    PeerAdvertisement,
    PeerCapabilities,
    PeerConnection,
    PeerId,
    PeerInfo,
    PeerMesh,
    PhantomNetwork,
    Priority,
    RelayConfig,
    RelayId,
    RelayNode,
    RelayRegistry,
    RelayService,
    RemoveReason,
    SymmetricKey,
    TokenBucket,
    TrafficShaper,
    X25519PrivateKey,
    X25519PublicKey,
    CELL_SIZE,
    MAX_PAYLOAD_SIZE,
};
