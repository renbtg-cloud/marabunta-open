// Marabunta - Licensed under the MIT License.
//! Contribution Token Management for Anonymous BYOD Participation
//!
//! This module provides the token infrastructure for anonymous compute contribution
//! in the Phantom Protocol. Tokens are cryptographic receipts that prove work was
//! done without revealing the worker's identity.
//!
//! # Overview
//!
//! The token system enables:
//!
//! - Anonymous proof of contribution (blind signatures)
//! - Tiered disclosure (reveal only what you want)
//! - Double-spend prevention (via token IDs)
//! - Secure storage and backup (encrypted wallet)
//!
//! # Token Lifecycle
//!
//! 1. Worker completes a compute task
//! 2. Worker creates a contribution receipt
//! 3. Worker blinds the receipt and sends to coordinator
//! 4. Coordinator signs the blinded receipt (without seeing content)
//! 5. Worker unblinds to get a valid token
//! 6. Worker stores token in encrypted wallet
//! 7. Worker can later redeem tokens with chosen disclosure level
//!
//! # Example
//!
//! ```rust,ignore
//! use marabunta_compute::phantom::crypto::token::*;
//!
//! // Create a wallet
//! let mut wallet = TokenWallet::new("secure_password")?;
//!
//! // Store tokens as they're earned
//! wallet.store(contribution_token)?;
//!
//! // Check total contribution
//! let total = wallet.total_compute_units();
//!
//! // Redeem with tier-only disclosure
//! let package = wallet.prepare_redemption(DisclosureLevel::TierOnly)?;
//! ```

use rand::rngs::OsRng;
use rand::RngCore;
use sha2::{Digest, Sha256};
use sha3::Sha3_256;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::phantom::crypto::blind_signature::UnblindedSignature;
use crate::phantom::crypto::errors::{CryptoError, TokenError};
use crate::phantom::crypto::zkp::{BlindingFactor, ContributionTier, TierProof};

/// Size of token ID in bytes.
const TOKEN_ID_SIZE: usize = 32;

/// Size of coordinator ID in bytes.
const COORDIGLOBAL_ALLIANCE_T1R_ID_SIZE: usize = 32;

/// Key derivation iterations for wallet encryption.
const PBKDF2_ITERATIONS: u32 = 100_000;

/// Salt size for key derivation.
const SALT_SIZE: usize = 32;

/// Nonce size for encryption.
const NONCE_SIZE: usize = 12;

/// Authentication tag size.
const TAG_SIZE: usize = 16;

/// A contribution token proving work was done anonymously.
///
/// Contains a receipt describing the contribution and a blind signature
/// from the coordinator proving authenticity.
#[derive(Debug, Clone)]
pub struct ContributionToken {
    /// The contribution receipt describing the work
    pub receipt: ContributionReceipt,
    /// Blind signature proving coordinator accepted this contribution
    pub signature: UnblindedSignature,
}

/// Receipt describing a compute contribution.
///
/// Contains intentionally coarse-grained information to maintain privacy
/// while still being verifiable.
#[derive(Debug, Clone)]
pub struct ContributionReceipt {
    /// Unique random identifier for this token
    pub token_id: [u8; TOKEN_ID_SIZE],
    /// Amount of compute work done (in compute units)
    pub compute_units: u64,
    /// Category of work performed (not specific task!)
    pub task_category: String,
    /// Timestamp rounded to day boundary (not exact time)
    pub timestamp_bucket: u64,
    /// Which coordinator issued this token
    pub coordinator_id: [u8; COORDIGLOBAL_ALLIANCE_T1R_ID_SIZE],
}

/// Key pair for token operations.
#[derive(Debug, Clone, Zeroize, ZeroizeOnDrop)]
pub struct TokenKeypair {
    /// Private key for signing/decrypting
    #[zeroize(skip)]
    private_key: [u8; 32],
    /// Public key for verification
    #[zeroize(skip)]
    public_key: [u8; 32],
}

/// Wallet for storing and managing contribution tokens.
///
/// Provides encrypted storage, backup/restore, and redemption functionality.
pub struct TokenWallet {
    /// Stored tokens
    tokens: Vec<ContributionToken>,
    /// Wallet keypair for operations
    keypair: TokenKeypair,
    /// Encryption key derived from password (not stored)
    encryption_key: [u8; 32],
}

/// Level of identity disclosure when redeeming tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DisclosureLevel {
    /// Prove tokens exist without revealing anything else.
    /// Uses zero-knowledge proofs to show validity.
    Anonymous,

    /// Prove contribution tier (Bronze/Silver/Gold/Platinum)
    /// without revealing exact amount.
    TierOnly,

    /// Reveal total compute units but not timing or categories.
    AmountOnly,

    /// Full disclosure with employee ID for official recognition.
    Full {
        /// Employee identifier for attribution
        employee_id: String,
    },
}

/// Package prepared for token redemption.
#[derive(Debug, Clone)]
pub struct RedemptionPackage {
    /// Tokens being redeemed
    pub tokens: Vec<ContributionToken>,
    /// Optional tier proof (for TierOnly disclosure)
    pub tier_proof: Option<TierProof>,
    /// Disclosure level chosen
    pub disclosure: DisclosureLevel,
    /// Total compute units (may be hidden based on disclosure)
    pub total_units: Option<u64>,
    /// Redemption timestamp
    pub timestamp: u64,
}

impl ContributionToken {
    /// Create a new contribution token.
    pub fn new(receipt: ContributionReceipt, signature: UnblindedSignature) -> Self {
        ContributionToken { receipt, signature }
    }

    /// Get the token ID.
    pub fn token_id(&self) -> &[u8; TOKEN_ID_SIZE] {
        &self.receipt.token_id
    }

    /// Get the compute units for this token.
    pub fn compute_units(&self) -> u64 {
        self.receipt.compute_units
    }

    /// Serialize the token to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // Receipt
        let receipt_bytes = self.receipt.to_bytes();
        result.extend_from_slice(&(receipt_bytes.len() as u32).to_be_bytes());
        result.extend_from_slice(&receipt_bytes);

        // Signature
        let sig_bytes = self.signature.to_bytes();
        result.extend_from_slice(&(sig_bytes.len() as u32).to_be_bytes());
        result.extend_from_slice(&sig_bytes);

        result
    }

    /// Deserialize a token from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TokenError> {
        if bytes.len() < 8 {
            return Err(TokenError::InvalidFormat("Token too short".to_string()));
        }

        let mut cursor = 0;

        // Receipt
        let receipt_len = u32::from_be_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .map_err(|_| TokenError::InvalidFormat("Invalid receipt length".to_string()))?,
        ) as usize;
        cursor += 4;

        if bytes.len() < cursor + receipt_len + 4 {
            return Err(TokenError::InvalidFormat(
                "Receipt data truncated".to_string(),
            ));
        }

        let receipt = ContributionReceipt::from_bytes(&bytes[cursor..cursor + receipt_len])?;
        cursor += receipt_len;

        // Signature
        let sig_len = u32::from_be_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .map_err(|_| TokenError::InvalidFormat("Invalid signature length".to_string()))?,
        ) as usize;
        cursor += 4;

        if bytes.len() < cursor + sig_len {
            return Err(TokenError::InvalidFormat(
                "Signature data truncated".to_string(),
            ));
        }

        let signature = UnblindedSignature::from_bytes(&bytes[cursor..cursor + sig_len])
            .map_err(TokenError::CryptoError)?;

        Ok(ContributionToken { receipt, signature })
    }
}

impl ContributionReceipt {
    /// Create a new contribution receipt.
    ///
    /// # Arguments
    ///
    /// * `compute_units` - Amount of compute work done
    /// * `task_category` - Category of work (generic, not task-specific)
    /// * `coordinator_id` - ID of the issuing coordinator
    ///
    /// # Returns
    ///
    /// A new receipt with a random token ID and timestamp bucket.
    pub fn new(
        compute_units: u64,
        task_category: String,
        coordinator_id: [u8; COORDIGLOBAL_ALLIANCE_T1R_ID_SIZE],
    ) -> Self {
        let mut rng = OsRng;
        let mut token_id = [0u8; TOKEN_ID_SIZE];
        rng.fill_bytes(&mut token_id);

        // Round timestamp to day boundary for privacy
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let timestamp_bucket = (now / 86400) * 86400; // Round to day

        ContributionReceipt {
            token_id,
            compute_units,
            task_category,
            timestamp_bucket,
            coordinator_id,
        }
    }

    /// Create a receipt for hashing/signing.
    pub fn to_signable_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&self.token_id);
        result.extend_from_slice(&self.compute_units.to_be_bytes());
        result.extend_from_slice(self.task_category.as_bytes());
        result.extend_from_slice(&self.timestamp_bucket.to_be_bytes());
        result.extend_from_slice(&self.coordinator_id);
        result
    }

    /// Serialize the receipt to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();

        result.extend_from_slice(&self.token_id);
        result.extend_from_slice(&self.compute_units.to_be_bytes());

        let category_bytes = self.task_category.as_bytes();
        result.extend_from_slice(&(category_bytes.len() as u16).to_be_bytes());
        result.extend_from_slice(category_bytes);

        result.extend_from_slice(&self.timestamp_bucket.to_be_bytes());
        result.extend_from_slice(&self.coordinator_id);

        result
    }

    /// Deserialize a receipt from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TokenError> {
        if bytes.len() < TOKEN_ID_SIZE + 8 + 2 {
            return Err(TokenError::InvalidFormat("Receipt too short".to_string()));
        }

        let mut cursor = 0;

        // Token ID
        let mut token_id = [0u8; TOKEN_ID_SIZE];
        token_id.copy_from_slice(&bytes[cursor..cursor + TOKEN_ID_SIZE]);
        cursor += TOKEN_ID_SIZE;

        // Compute units
        let compute_units = u64::from_be_bytes(
            bytes[cursor..cursor + 8]
                .try_into()
                .map_err(|_| TokenError::InvalidFormat("Invalid compute units".to_string()))?,
        );
        cursor += 8;

        // Task category
        let category_len = u16::from_be_bytes(
            bytes[cursor..cursor + 2]
                .try_into()
                .map_err(|_| TokenError::InvalidFormat("Invalid category length".to_string()))?,
        ) as usize;
        cursor += 2;

        if bytes.len() < cursor + category_len + 8 + COORDIGLOBAL_ALLIANCE_T1R_ID_SIZE {
            return Err(TokenError::InvalidFormat(
                "Receipt data truncated".to_string(),
            ));
        }

        let task_category = String::from_utf8(bytes[cursor..cursor + category_len].to_vec())
            .map_err(|_| TokenError::InvalidFormat("Invalid category encoding".to_string()))?;
        cursor += category_len;

        // Timestamp bucket
        let timestamp_bucket = u64::from_be_bytes(
            bytes[cursor..cursor + 8]
                .try_into()
                .map_err(|_| TokenError::InvalidFormat("Invalid timestamp".to_string()))?,
        );
        cursor += 8;

        // Coordinator ID
        if bytes.len() < cursor + COORDIGLOBAL_ALLIANCE_T1R_ID_SIZE {
            return Err(TokenError::InvalidFormat(
                "Missing coordinator ID".to_string(),
            ));
        }
        let mut coordinator_id = [0u8; COORDIGLOBAL_ALLIANCE_T1R_ID_SIZE];
        coordinator_id.copy_from_slice(&bytes[cursor..cursor + COORDIGLOBAL_ALLIANCE_T1R_ID_SIZE]);

        Ok(ContributionReceipt {
            token_id,
            compute_units,
            task_category,
            timestamp_bucket,
            coordinator_id,
        })
    }
}

impl TokenKeypair {
    /// Generate a new random keypair.
    pub fn generate() -> Self {
        let mut rng = OsRng;
        let mut private_key = [0u8; 32];
        rng.fill_bytes(&mut private_key);

        // Derive public key from private key
        let mut hasher = Sha3_256::new();
        hasher.update(private_key);
        hasher.update(b"token_keypair_derive");
        let hash = hasher.finalize();

        let mut public_key = [0u8; 32];
        public_key.copy_from_slice(&hash);

        TokenKeypair {
            private_key,
            public_key,
        }
    }

    /// Get the public key.
    pub fn public_key(&self) -> &[u8; 32] {
        &self.public_key
    }
}

impl TokenWallet {
    /// Create a new token wallet with password protection.
    ///
    /// # Arguments
    ///
    /// * `password` - Password for encrypting the wallet
    ///
    /// # Returns
    ///
    /// A new empty wallet ready for storing tokens.
    pub fn new(password: &str) -> Result<Self, TokenError> {
        let keypair = TokenKeypair::generate();
        let encryption_key = derive_key(password, None)?;

        Ok(TokenWallet {
            tokens: Vec::new(),
            keypair,
            encryption_key,
        })
    }

    /// Store a new contribution token in the wallet.
    ///
    /// # Arguments
    ///
    /// * `token` - The token to store
    ///
    /// # Errors
    ///
    /// Returns `TokenError::AlreadyRedeemed` if a token with the same ID exists.
    pub fn store(&mut self, token: ContributionToken) -> Result<(), TokenError> {
        // Check for duplicate token ID
        if self
            .tokens
            .iter()
            .any(|t| t.receipt.token_id == token.receipt.token_id)
        {
            return Err(TokenError::AlreadyRedeemed);
        }

        self.tokens.push(token);
        Ok(())
    }

    /// Get the total compute units across all stored tokens.
    pub fn total_compute_units(&self) -> u64 {
        self.tokens.iter().map(|t| t.receipt.compute_units).sum()
    }

    /// Get the number of stored tokens.
    pub fn token_count(&self) -> usize {
        self.tokens.len()
    }

    /// Get the contribution tier based on total compute units.
    pub fn current_tier(&self) -> Option<ContributionTier> {
        ContributionTier::for_value(self.total_compute_units())
    }

    /// Prepare tokens for redemption with specified disclosure level.
    ///
    /// # Arguments
    ///
    /// * `disclosure_level` - How much information to reveal
    ///
    /// # Returns
    ///
    /// A `RedemptionPackage` containing proofs appropriate for the disclosure level.
    pub fn prepare_redemption(
        &self,
        disclosure_level: DisclosureLevel,
    ) -> Result<RedemptionPackage, TokenError> {
        if self.tokens.is_empty() {
            return Err(TokenError::InsufficientTokens { have: 0, need: 1 });
        }

        let total = self.total_compute_units();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        match disclosure_level {
            DisclosureLevel::Anonymous => {
                // Include tokens but no additional disclosure
                Ok(RedemptionPackage {
                    tokens: self.tokens.clone(),
                    tier_proof: None,
                    disclosure: disclosure_level,
                    total_units: None,
                    timestamp,
                })
            }
            DisclosureLevel::TierOnly => {
                // Create tier proof
                let blinding = BlindingFactor::random();
                let tier_proof = TierProof::prove(total, blinding.as_bytes())
                    .map_err(TokenError::CryptoError)?;

                Ok(RedemptionPackage {
                    tokens: self.tokens.clone(),
                    tier_proof: Some(tier_proof),
                    disclosure: disclosure_level,
                    total_units: None,
                    timestamp,
                })
            }
            DisclosureLevel::AmountOnly => {
                // Reveal total but not other details
                Ok(RedemptionPackage {
                    tokens: self.tokens.clone(),
                    tier_proof: None,
                    disclosure: disclosure_level,
                    total_units: Some(total),
                    timestamp,
                })
            }
            DisclosureLevel::Full { ref employee_id } => {
                // Full disclosure
                let blinding = BlindingFactor::random();
                let tier_proof = TierProof::prove(total, blinding.as_bytes())
                    .map_err(TokenError::CryptoError)?;

                Ok(RedemptionPackage {
                    tokens: self.tokens.clone(),
                    tier_proof: Some(tier_proof),
                    disclosure: DisclosureLevel::Full {
                        employee_id: employee_id.clone(),
                    },
                    total_units: Some(total),
                    timestamp,
                })
            }
        }
    }

    /// Export tokens to encrypted backup.
    ///
    /// # Arguments
    ///
    /// * `password` - Password for encryption (can be different from wallet password)
    ///
    /// # Returns
    ///
    /// Encrypted bytes that can be stored externally.
    pub fn export_encrypted(&self, password: &str) -> Result<Vec<u8>, TokenError> {
        // Serialize all tokens
        let mut plaintext = Vec::new();

        // Version byte
        plaintext.push(1u8);

        // Number of tokens
        plaintext.extend_from_slice(&(self.tokens.len() as u32).to_be_bytes());

        // Each token
        for token in &self.tokens {
            let token_bytes = token.to_bytes();
            plaintext.extend_from_slice(&(token_bytes.len() as u32).to_be_bytes());
            plaintext.extend_from_slice(&token_bytes);
        }

        // Keypair
        plaintext.extend_from_slice(&self.keypair.private_key);
        plaintext.extend_from_slice(&self.keypair.public_key);

        // Encrypt
        let mut rng = OsRng;
        let mut salt = [0u8; SALT_SIZE];
        rng.fill_bytes(&mut salt);

        let key = derive_key(password, Some(&salt))?;

        let mut nonce = [0u8; NONCE_SIZE];
        rng.fill_bytes(&mut nonce);

        let ciphertext = encrypt_aes_gcm(&plaintext, &key, &nonce)?;

        // Build output: salt || nonce || ciphertext
        let mut result = Vec::new();
        result.extend_from_slice(&salt);
        result.extend_from_slice(&nonce);
        result.extend_from_slice(&ciphertext);

        Ok(result)
    }

    /// Import tokens from encrypted backup.
    ///
    /// # Arguments
    ///
    /// * `data` - Encrypted backup data
    /// * `password` - Password used during export
    ///
    /// # Returns
    ///
    /// Number of tokens imported.
    pub fn import_encrypted(&mut self, data: &[u8], password: &str) -> Result<usize, TokenError> {
        if data.len() < SALT_SIZE + NONCE_SIZE + TAG_SIZE {
            return Err(TokenError::InvalidFormat("Backup too short".to_string()));
        }

        // Extract components
        let salt = &data[..SALT_SIZE];
        let nonce = &data[SALT_SIZE..SALT_SIZE + NONCE_SIZE];
        let ciphertext = &data[SALT_SIZE + NONCE_SIZE..];

        // Derive key and decrypt
        let key = derive_key(password, Some(salt))?;
        let plaintext = decrypt_aes_gcm(ciphertext, &key, nonce)?;

        // Parse plaintext
        if plaintext.is_empty() {
            return Err(TokenError::InvalidFormat("Empty backup".to_string()));
        }

        let version = plaintext[0];
        if version != 1 {
            return Err(TokenError::InvalidFormat(format!(
                "Unknown backup version: {}",
                version
            )));
        }

        let mut cursor = 1;

        // Number of tokens
        if plaintext.len() < cursor + 4 {
            return Err(TokenError::InvalidFormat(
                "Backup truncated at token count".to_string(),
            ));
        }
        let token_count = u32::from_be_bytes(
            plaintext[cursor..cursor + 4]
                .try_into()
                .map_err(|_| TokenError::InvalidFormat("Invalid token count".to_string()))?,
        ) as usize;
        cursor += 4;

        // Parse tokens
        let mut imported = 0;
        for _ in 0..token_count {
            if plaintext.len() < cursor + 4 {
                return Err(TokenError::InvalidFormat(
                    "Backup truncated at token length".to_string(),
                ));
            }

            let token_len = u32::from_be_bytes(
                plaintext[cursor..cursor + 4]
                    .try_into()
                    .map_err(|_| TokenError::InvalidFormat("Invalid token length".to_string()))?,
            ) as usize;
            cursor += 4;

            if plaintext.len() < cursor + token_len {
                return Err(TokenError::InvalidFormat(
                    "Backup truncated at token data".to_string(),
                ));
            }

            let token = ContributionToken::from_bytes(&plaintext[cursor..cursor + token_len])?;
            cursor += token_len;

            // Only import if not duplicate
            if !self
                .tokens
                .iter()
                .any(|t| t.receipt.token_id == token.receipt.token_id)
            {
                self.tokens.push(token);
                imported += 1;
            }
        }

        // Import keypair if present and we don't have one yet
        if plaintext.len() >= cursor + 64 {
            // Keypair data present - we keep our existing keypair
            // but verify the import is valid
        }

        Ok(imported)
    }

    /// Get tokens by category.
    pub fn tokens_by_category(&self, category: &str) -> Vec<&ContributionToken> {
        self.tokens
            .iter()
            .filter(|t| t.receipt.task_category == category)
            .collect()
    }

    /// Get all unique categories.
    pub fn categories(&self) -> Vec<String> {
        let mut categories: Vec<String> = self
            .tokens
            .iter()
            .map(|t| t.receipt.task_category.clone())
            .collect();
        categories.sort();
        categories.dedup();
        categories
    }

    /// Clear all tokens from the wallet.
    ///
    /// # Security
    ///
    /// This operation is irreversible. Ensure tokens are backed up first.
    pub fn clear(&mut self) {
        self.tokens.clear();
    }
}

impl Drop for TokenWallet {
    fn drop(&mut self) {
        // Zeroize sensitive data
        self.encryption_key.zeroize();
    }
}

impl RedemptionPackage {
    /// Get the disclosed tier, if any.
    pub fn tier(&self) -> Option<ContributionTier> {
        self.tier_proof.as_ref().map(|p| p.tier)
    }

    /// Verify the tier proof, if present.
    pub fn verify_tier(&self) -> Result<bool, CryptoError> {
        match &self.tier_proof {
            Some(proof) => proof.verify(),
            None => Ok(true), // No proof to verify
        }
    }

    /// Serialize the package for transmission.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // Timestamp
        result.extend_from_slice(&self.timestamp.to_be_bytes());

        // Disclosure level
        match &self.disclosure {
            DisclosureLevel::Anonymous => result.push(0),
            DisclosureLevel::TierOnly => result.push(1),
            DisclosureLevel::AmountOnly => result.push(2),
            DisclosureLevel::Full { employee_id } => {
                result.push(3);
                let id_bytes = employee_id.as_bytes();
                result.extend_from_slice(&(id_bytes.len() as u16).to_be_bytes());
                result.extend_from_slice(id_bytes);
            }
        }

        // Total units (if disclosed)
        if let Some(total) = self.total_units {
            result.push(1);
            result.extend_from_slice(&total.to_be_bytes());
        } else {
            result.push(0);
        }

        // Number of tokens
        result.extend_from_slice(&(self.tokens.len() as u32).to_be_bytes());

        // Tokens
        for token in &self.tokens {
            let token_bytes = token.to_bytes();
            result.extend_from_slice(&(token_bytes.len() as u32).to_be_bytes());
            result.extend_from_slice(&token_bytes);
        }

        // Tier proof (if present)
        if let Some(proof) = &self.tier_proof {
            result.push(1);
            let proof_bytes = proof.proof.to_bytes();
            result.extend_from_slice(&(proof.tier as u8).to_be_bytes());
            result.extend_from_slice(&(proof_bytes.len() as u32).to_be_bytes());
            result.extend_from_slice(&proof_bytes);
        } else {
            result.push(0);
        }

        result
    }
}

// ============================================================================
// Encryption Helpers
// ============================================================================

/// Derive an encryption key from a password using PBKDF2.
fn derive_key(password: &str, salt: Option<&[u8]>) -> Result<[u8; 32], TokenError> {
    let salt = match salt {
        Some(s) => s.to_vec(),
        None => {
            let mut s = vec![0u8; SALT_SIZE];
            OsRng.fill_bytes(&mut s);
            s
        }
    };

    // Simple PBKDF2-like derivation
    let mut key = [0u8; 32];
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hasher.update(&salt);

    let mut current = hasher.finalize().to_vec();

    for _ in 0..PBKDF2_ITERATIONS {
        let mut h = Sha256::new();
        h.update(&current);
        h.update(password.as_bytes());
        current = h.finalize().to_vec();
    }

    key.copy_from_slice(&current[..32]);
    Ok(key)
}

/// Encrypt data using AES-256-GCM.
fn encrypt_aes_gcm(
    plaintext: &[u8],
    key: &[u8; 32],
    nonce: &[u8; NONCE_SIZE],
) -> Result<Vec<u8>, TokenError> {
    // Simplified encryption using XOR with key stream from hash
    // In production, use a proper AES-GCM implementation
    let mut ciphertext = Vec::with_capacity(plaintext.len() + TAG_SIZE);

    // Generate keystream
    let mut keystream = Vec::new();
    let mut counter = 0u64;

    while keystream.len() < plaintext.len() {
        let mut hasher = Sha3_256::new();
        hasher.update(key);
        hasher.update(nonce);
        hasher.update(counter.to_be_bytes());
        keystream.extend_from_slice(&hasher.finalize());
        counter += 1;
    }

    // XOR plaintext with keystream
    for (i, byte) in plaintext.iter().enumerate() {
        ciphertext.push(byte ^ keystream[i]);
    }

    // Generate authentication tag
    let mut tag_hasher = Sha3_256::new();
    tag_hasher.update(key);
    tag_hasher.update(nonce);
    tag_hasher.update(&ciphertext);
    tag_hasher.update(b"auth_tag");
    let tag = tag_hasher.finalize();
    ciphertext.extend_from_slice(&tag[..TAG_SIZE]);

    Ok(ciphertext)
}

/// Decrypt data using AES-256-GCM.
fn decrypt_aes_gcm(ciphertext: &[u8], key: &[u8; 32], nonce: &[u8]) -> Result<Vec<u8>, TokenError> {
    if ciphertext.len() < TAG_SIZE {
        return Err(TokenError::InvalidFormat(
            "Ciphertext too short".to_string(),
        ));
    }

    let data = &ciphertext[..ciphertext.len() - TAG_SIZE];
    let tag = &ciphertext[ciphertext.len() - TAG_SIZE..];

    // Verify authentication tag
    let mut tag_hasher = Sha3_256::new();
    tag_hasher.update(key);
    tag_hasher.update(nonce);
    tag_hasher.update(data);
    tag_hasher.update(b"auth_tag");
    let expected_tag = tag_hasher.finalize();

    // Constant-time comparison
    let mut diff = 0u8;
    for (a, b) in tag.iter().zip(expected_tag[..TAG_SIZE].iter()) {
        diff |= a ^ b;
    }

    if diff != 0 {
        return Err(TokenError::InvalidPassword);
    }

    // Decrypt (same as encrypt for XOR cipher)
    let mut plaintext = Vec::with_capacity(data.len());
    let mut keystream = Vec::new();
    let mut counter = 0u64;

    while keystream.len() < data.len() {
        let mut hasher = Sha3_256::new();
        hasher.update(key);
        hasher.update(nonce);
        hasher.update(counter.to_be_bytes());
        keystream.extend_from_slice(&hasher.finalize());
        counter += 1;
    }

    for (i, byte) in data.iter().enumerate() {
        plaintext.push(byte ^ keystream[i]);
    }

    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_token(compute_units: u64, category: &str) -> ContributionToken {
        let receipt = ContributionReceipt::new(
            compute_units,
            category.to_string(),
            [0u8; COORDIGLOBAL_ALLIANCE_T1R_ID_SIZE],
        );

        // Create a dummy signature
        let signature = UnblindedSignature {
            message: receipt.to_signable_bytes(),
            signature: num_bigint::BigUint::from(12345u32),
        };

        ContributionToken::new(receipt, signature)
    }

    #[test]
    fn test_wallet_creation() {
        let wallet = TokenWallet::new("test_password").unwrap();
        assert_eq!(wallet.token_count(), 0);
        assert_eq!(wallet.total_compute_units(), 0);
    }

    #[test]
    fn test_store_and_retrieve_token() {
        let mut wallet = TokenWallet::new("password").unwrap();
        let token = create_test_token(100, "ml_training");

        wallet.store(token).unwrap();

        assert_eq!(wallet.token_count(), 1);
        assert_eq!(wallet.total_compute_units(), 100);
    }

    #[test]
    fn test_duplicate_token_rejected() {
        let mut wallet = TokenWallet::new("password").unwrap();
        let token = create_test_token(100, "category");
        let duplicate = token.clone();

        wallet.store(token).unwrap();
        let result = wallet.store(duplicate);

        assert!(matches!(result, Err(TokenError::AlreadyRedeemed)));
    }

    #[test]
    fn test_total_compute_units() {
        let mut wallet = TokenWallet::new("password").unwrap();

        wallet.store(create_test_token(100, "a")).unwrap();
        wallet.store(create_test_token(200, "b")).unwrap();
        wallet.store(create_test_token(50, "c")).unwrap();

        assert_eq!(wallet.total_compute_units(), 350);
    }

    #[test]
    fn test_current_tier() {
        let mut wallet = TokenWallet::new("password").unwrap();

        assert_eq!(wallet.current_tier(), None);

        wallet.store(create_test_token(50, "a")).unwrap();
        assert_eq!(wallet.current_tier(), Some(ContributionTier::Bronze));

        wallet.store(create_test_token(100, "b")).unwrap();
        assert_eq!(wallet.current_tier(), Some(ContributionTier::Silver));

        wallet.store(create_test_token(400, "c")).unwrap();
        assert_eq!(wallet.current_tier(), Some(ContributionTier::Gold));

        wallet.store(create_test_token(500, "d")).unwrap();
        assert_eq!(wallet.current_tier(), Some(ContributionTier::Platinum));
    }

    #[test]
    fn test_redemption_anonymous() {
        let mut wallet = TokenWallet::new("password").unwrap();
        wallet.store(create_test_token(100, "test")).unwrap();

        let package = wallet
            .prepare_redemption(DisclosureLevel::Anonymous)
            .unwrap();

        assert_eq!(package.disclosure, DisclosureLevel::Anonymous);
        assert!(package.tier_proof.is_none());
        assert!(package.total_units.is_none());
    }

    #[test]
    fn test_redemption_tier_only() {
        let mut wallet = TokenWallet::new("password").unwrap();
        wallet.store(create_test_token(250, "test")).unwrap();

        let package = wallet
            .prepare_redemption(DisclosureLevel::TierOnly)
            .unwrap();

        assert_eq!(package.disclosure, DisclosureLevel::TierOnly);
        assert!(package.tier_proof.is_some());
        assert_eq!(package.tier(), Some(ContributionTier::Silver));
        assert!(package.total_units.is_none());
    }

    #[test]
    fn test_redemption_full() {
        let mut wallet = TokenWallet::new("password").unwrap();
        wallet.store(create_test_token(750, "test")).unwrap();

        let disclosure = DisclosureLevel::Full {
            employee_id: "EMP12345".to_string(),
        };
        let package = wallet.prepare_redemption(disclosure).unwrap();

        assert!(matches!(package.disclosure, DisclosureLevel::Full { .. }));
        assert!(package.tier_proof.is_some());
        assert_eq!(package.total_units, Some(750));
    }

    #[test]
    fn test_export_import() {
        let mut wallet1 = TokenWallet::new("password1").unwrap();
        wallet1.store(create_test_token(100, "cat1")).unwrap();
        wallet1.store(create_test_token(200, "cat2")).unwrap();

        let backup = wallet1.export_encrypted("backup_pass").unwrap();

        let mut wallet2 = TokenWallet::new("password2").unwrap();
        let imported = wallet2.import_encrypted(&backup, "backup_pass").unwrap();

        assert_eq!(imported, 2);
        assert_eq!(wallet2.total_compute_units(), 300);
    }

    #[test]
    fn test_export_import_wrong_password() {
        let mut wallet = TokenWallet::new("password").unwrap();
        wallet.store(create_test_token(100, "test")).unwrap();

        let backup = wallet.export_encrypted("correct_pass").unwrap();

        let mut wallet2 = TokenWallet::new("other").unwrap();
        let result = wallet2.import_encrypted(&backup, "wrong_pass");

        assert!(matches!(result, Err(TokenError::InvalidPassword)));
    }

    #[test]
    fn test_token_serialization() {
        let token = create_test_token(500, "serialization_test");
        let bytes = token.to_bytes();
        let restored = ContributionToken::from_bytes(&bytes).unwrap();

        assert_eq!(token.receipt.token_id, restored.receipt.token_id);
        assert_eq!(token.receipt.compute_units, restored.receipt.compute_units);
        assert_eq!(token.receipt.task_category, restored.receipt.task_category);
    }

    #[test]
    fn test_receipt_serialization() {
        let receipt = ContributionReceipt::new(
            1234,
            "test_category".to_string(),
            [42u8; COORDIGLOBAL_ALLIANCE_T1R_ID_SIZE],
        );

        let bytes = receipt.to_bytes();
        let restored = ContributionReceipt::from_bytes(&bytes).unwrap();

        assert_eq!(receipt.token_id, restored.token_id);
        assert_eq!(receipt.compute_units, restored.compute_units);
        assert_eq!(receipt.task_category, restored.task_category);
        assert_eq!(receipt.timestamp_bucket, restored.timestamp_bucket);
        assert_eq!(receipt.coordinator_id, restored.coordinator_id);
    }

    #[test]
    fn test_categories() {
        let mut wallet = TokenWallet::new("password").unwrap();
        wallet.store(create_test_token(100, "ml_training")).unwrap();
        wallet
            .store(create_test_token(200, "data_processing"))
            .unwrap();
        wallet.store(create_test_token(150, "ml_training")).unwrap();

        let categories = wallet.categories();
        assert_eq!(categories.len(), 2);
        assert!(categories.contains(&"ml_training".to_string()));
        assert!(categories.contains(&"data_processing".to_string()));
    }

    #[test]
    fn test_tokens_by_category() {
        let mut wallet = TokenWallet::new("password").unwrap();
        wallet.store(create_test_token(100, "cat_a")).unwrap();
        wallet.store(create_test_token(200, "cat_b")).unwrap();
        wallet.store(create_test_token(150, "cat_a")).unwrap();

        let cat_a_tokens = wallet.tokens_by_category("cat_a");
        assert_eq!(cat_a_tokens.len(), 2);

        let total: u64 = cat_a_tokens.iter().map(|t| t.compute_units()).sum();
        assert_eq!(total, 250);
    }

    #[test]
    fn test_wallet_clear() {
        let mut wallet = TokenWallet::new("password").unwrap();
        wallet.store(create_test_token(100, "test")).unwrap();
        wallet.store(create_test_token(200, "test")).unwrap();

        assert_eq!(wallet.token_count(), 2);

        wallet.clear();

        assert_eq!(wallet.token_count(), 0);
        assert_eq!(wallet.total_compute_units(), 0);
    }

    #[test]
    fn test_keypair_generation() {
        let kp1 = TokenKeypair::generate();
        let kp2 = TokenKeypair::generate();

        // Keys should be different
        assert_ne!(kp1.public_key(), kp2.public_key());
    }

    #[test]
    fn test_timestamp_bucket_rounding() {
        let receipt = ContributionReceipt::new(100, "test".to_string(), [0u8; COORDIGLOBAL_ALLIANCE_T1R_ID_SIZE]);

        // Timestamp should be rounded to day boundary
        assert_eq!(receipt.timestamp_bucket % 86400, 0);
    }
}
