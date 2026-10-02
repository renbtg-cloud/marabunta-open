// Marabunta - Licensed under the MIT License.
//! Phantom Protocol Cryptographic Primitives
//!
//! This module provides the cryptographic foundations for anonymous BYOD
//! (Bring Your Own Device) participation in Marabunta Compute. The primitives
//! enable workers to contribute compute resources while maintaining privacy.
//!
//! # Overview
//!
//! The Phantom Protocol enables:
//!
//! - **Anonymous contribution**: Workers prove work without revealing identity
//! - **Blind signatures**: Coordinators sign receipts without seeing content
//! - **Ring signatures**: Group membership proofs without individual identification
//! - **Zero-knowledge proofs**: Prove properties without revealing exact values
//! - **Secure tokens**: Encrypted storage and tiered disclosure
//!
//! # Architecture
//!
//! ```text
//!                          ┌─────────────────┐
//!                          │   Coordinator   │
//!                          │  (Signs blind)  │
//!                          └────────┬────────┘
//!                                   │
//!                    ┌──────────────┼──────────────┐
//!                    │              │              │
//!                    ▼              ▼              ▼
//!             ┌───────────┐  ┌───────────┐  ┌───────────┐
//!             │  Worker A │  │  Worker B │  │  Worker C │
//!             │  (Anon)   │  │  (Anon)   │  │  (Anon)   │
//!             └─────┬─────┘  └─────┬─────┘  └─────┬─────┘
//!                   │              │              │
//!                   └──────────────┼──────────────┘
//!                                  │
//!                                  ▼
//!                          ┌─────────────────┐
//!                          │  Token Wallet   │
//!                          │   (Encrypted)   │
//!                          └────────┬────────┘
//!                                   │
//!                                   ▼
//!                          ┌─────────────────┐
//!                          │   Redemption    │
//!                          │ (Tier/Full/etc) │
//!                          └─────────────────┘
//! ```
//!
//! # Security Properties
//!
//! - **Perfect forward secrecy**: Compromised keys don't reveal past contributions
//! - **Unlinkability**: Different tokens from same worker appear independent
//! - **Non-repudiation**: Signed tokens prove coordinator accepted contribution
//! - **Double-spend prevention**: Key images detect reuse
//!
//! # Usage Example
//!
//! ```rust,ignore
//! use marabunta_compute::phantom::crypto::{
//!     blind_signature::BlindSignatureKeyPair,
//!     token::{TokenWallet, ContributionReceipt, DisclosureLevel},
//! };
//!
//! // 1. Coordinator generates signing key
//! let coordinator_keys = BlindSignatureKeyPair::generate(2048)?;
//!
//! // 2. Worker creates a contribution receipt
//! let receipt = ContributionReceipt::new(
//!     100,  // compute units
//!     "ml_training".to_string(),
//!     coordinator_id,
//! );
//!
//! // 3. Worker blinds the receipt
//! let blinded = coordinator_keys.public.blind(&receipt.to_signable_bytes())?;
//!
//! // 4. Coordinator signs (cannot see original content)
//! let blind_sig = coordinator_keys.sign_blinded(&blinded)?;
//!
//! // 5. Worker unblinds to get valid token
//! let signature = blinded.unblind_with_message(
//!     &blind_sig,
//!     &coordinator_keys.public,
//!     &receipt.to_signable_bytes(),
//! )?;
//!
//! // 6. Store in wallet
//! let mut wallet = TokenWallet::new("secure_password")?;
//! wallet.store(ContributionToken::new(receipt, signature))?;
//!
//! // 7. Redeem with chosen disclosure
//! let package = wallet.prepare_redemption(DisclosureLevel::TierOnly)?;
//! ```
//!
//! # Module Contents
//!
//! - [`blind_signature`]: RSA blind signatures (Chaum scheme)
//! - [`ring_signature`]: Ring signatures for anonymous group membership
//! - [`zkp`]: Zero-knowledge range proofs and tier proofs
//! - [`token`]: Contribution tokens and wallet management
//! - [`errors`]: Error types for cryptographic operations
//!
//! # Security Notes
//!
//! - All cryptographic operations use constant-time algorithms where possible
//! - Private keys are automatically zeroized when dropped
//! - Minimum RSA key size is 2048 bits; 3072+ recommended for long-term security
//! - Blinding factors and nonces must never be reused
//!
//! # Implementation Notes
//!
//! This implementation prioritizes clarity and correctness over raw performance.
//! For production use with high throughput requirements, consider:
//!
//! - Using hardware security modules (HSMs) for signing keys
//! - Batch verification for ring signatures
//! - Precomputed tables for elliptic curve operations
//!
//! The elliptic curve operations in this module use simplified hash-based
//! derivations. For production deployment, integrate with `curve25519-dalek`
//! or similar audited EC libraries.

pub mod blind_signature;
pub mod errors;
pub mod ring_signature;
pub mod token;
pub mod zkp;

// Re-export commonly used types
pub use blind_signature::{
    BlindSignature, BlindSignatureKeyPair, BlindSignaturePublicKey, BlindedMessage,
    UnblindedSignature,
};
pub use errors::{CryptoError, TokenError};
pub use ring_signature::{
    EllipticCurve, HashAlgorithm, RingParams, RingPrivateKey, RingPublicKey, RingSignature,
    RingSignatureScheme,
};
pub use token::{
    ContributionReceipt, ContributionToken, DisclosureLevel, RedemptionPackage, TokenKeypair,
    TokenWallet,
};
pub use zkp::{
    BlindingFactor, BulletproofRangeProof, ContributionTier, PedersenCommitment, RangeProof,
    TierProof,
};

/// Protocol version for compatibility checking.
pub const PROTOCOL_VERSION: u32 = 1;

/// Minimum supported protocol version for backward compatibility.
pub const MIN_SUPPORTED_VERSION: u32 = 1;

/// Check if a protocol version is supported.
pub fn is_version_supported(version: u32) -> bool {
    version >= MIN_SUPPORTED_VERSION && version <= PROTOCOL_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_support() {
        assert!(is_version_supported(1));
        assert!(!is_version_supported(0));
        assert!(!is_version_supported(100));
    }

    #[test]
    fn test_full_workflow() {
        // This test demonstrates the complete anonymous contribution workflow

        // 1. Setup: Coordinator generates signing keys
        let coordinator_keys = BlindSignatureKeyPair::generate(2048).unwrap();
        let coordinator_id = [1u8; 32];

        // 2. Worker creates a contribution receipt
        let receipt = ContributionReceipt::new(150, "ml_training".to_string(), coordinator_id);

        // 3. Worker blinds the receipt
        let signable = receipt.to_signable_bytes();
        let blinded = coordinator_keys.public.blind(&signable).unwrap();

        // 4. Coordinator signs the blinded receipt
        // (Coordinator cannot see the original content!)
        let blind_sig = coordinator_keys.sign_blinded(&blinded).unwrap();

        // 5. Worker unblinds to get a valid signature
        let signature = blinded
            .unblind_with_message(&blind_sig, &coordinator_keys.public, &signable)
            .unwrap();

        // 6. Verify the signature is valid
        assert!(coordinator_keys.public.verify(&signature).unwrap());

        // 7. Create and store the token
        let token = ContributionToken::new(receipt, signature);
        let mut wallet = TokenWallet::new("test_password").unwrap();
        wallet.store(token).unwrap();

        // 8. Verify wallet state
        assert_eq!(wallet.total_compute_units(), 150);
        assert_eq!(wallet.current_tier(), Some(ContributionTier::Silver));

        // 9. Prepare redemption with tier-only disclosure
        let package = wallet
            .prepare_redemption(DisclosureLevel::TierOnly)
            .unwrap();
        assert!(package.tier_proof.is_some());
        assert_eq!(package.tier(), Some(ContributionTier::Silver));
        assert!(package.total_units.is_none()); // Amount not disclosed

        // 10. Export and import wallet
        let backup = wallet.export_encrypted("backup_password").unwrap();
        let mut wallet2 = TokenWallet::new("other_password").unwrap();
        let imported = wallet2
            .import_encrypted(&backup, "backup_password")
            .unwrap();
        assert_eq!(imported, 1);
        assert_eq!(wallet2.total_compute_units(), 150);
    }

    #[test]
    fn test_ring_signature_integration() {
        // Test ring signatures for anonymous group membership

        let scheme = RingSignatureScheme::default_scheme();

        // Generate a ring of participants
        let members: Vec<_> = (0..5).map(|_| scheme.generate_keypair().unwrap()).collect();
        let ring: Vec<_> = members.iter().map(|(_, pk)| pk.clone()).collect();

        // Worker signs a contribution claim
        let message = b"I contributed to job XYZ";
        let signer_index = 3;

        let signature = scheme
            .sign(message, &ring, &members[signer_index].0, signer_index)
            .unwrap();

        // Verifier can confirm someone in the ring signed
        // but cannot determine WHO signed
        assert!(scheme.verify(message, &ring, &signature).unwrap());

        // Key image allows double-spend detection
        let mut spent_images = std::collections::HashSet::new();
        assert!(!scheme.is_key_image_spent(&signature.key_image, &spent_images));

        spent_images.insert(signature.key_image);

        // Same signer signing again will be detected
        let sig2 = scheme
            .sign(
                b"Another message",
                &ring,
                &members[signer_index].0,
                signer_index,
            )
            .unwrap();
        assert!(scheme.is_key_image_spent(&sig2.key_image, &spent_images));
    }

    #[test]
    fn test_range_proof_integration() {
        // Test zero-knowledge range proofs

        let actual_hours = 350u64;
        let blinding = BlindingFactor::random();

        // Prove contribution is in Silver tier range [100, 499]
        // without revealing exact amount
        let proof = RangeProof::prove(actual_hours, 100, 499, blinding.as_bytes()).unwrap();

        // Verifier can confirm value is in range
        assert!(proof.verify(100, 499).unwrap());

        // But cannot determine exact value from the proof
        // (The commitment hides the actual value)

        // Tier proof provides even simpler interface
        let tier_proof = TierProof::prove(actual_hours, blinding.as_bytes()).unwrap();
        assert_eq!(tier_proof.tier, ContributionTier::Silver);
        assert!(tier_proof.verify().unwrap());
    }
}
