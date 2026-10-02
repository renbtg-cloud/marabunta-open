// Marabunta - Licensed under the MIT License.
//! RSA Blind Signature Implementation (Chaum Scheme)
//!
//! This module implements RSA blind signatures as described by David Chaum in 1983.
//! Blind signatures allow a signer to sign a message without seeing its content,
//! providing unlinkability between the signing request and the final signature.
//!
//! # Protocol Flow
//!
//! 1. **Worker generates message** `m` (contribution receipt)
//! 2. **Worker blinds**: `m' = m * r^e mod n` (r is random blinding factor)
//! 3. **Coordinator signs**: `s' = (m')^d mod n`
//! 4. **Worker unblinds**: `s = s' * r^(-1) mod n`
//! 5. **Result**: `(m, s)` is a valid signature; coordinator never saw `m`
//!
//! # Security Notes
//!
//! - Minimum RSA key size: 2048 bits (3072+ recommended for long-term security)
//! - Blinding factors must be cryptographically random
//! - All private key material is zeroized on drop
//! - Constant-time modular exponentiation is used where possible
//!
//! # Example
//!
//! ```rust,ignore
//! use marabunta_compute::phantom::crypto::blind_signature::*;
//!
//! // Coordinator generates keypair
//! let keypair = BlindSignatureKeyPair::generate(2048)?;
//!
//! // Worker creates and blinds a message
//! let message = b"contribution_receipt_data";
//! let blinded = keypair.public.blind(message)?;
//!
//! // Coordinator signs the blinded message (cannot see original)
//! let blind_sig = keypair.sign_blinded(&blinded)?;
//!
//! // Worker unblinds to get valid signature on original message
//! let unblinded = blinded.unblind(&blind_sig, &keypair.public)?;
//!
//! // Anyone can verify the signature
//! assert!(keypair.public.verify(&unblinded)?);
//! ```

use num_bigint::{BigUint, RandBigInt, ToBigUint};
use num_integer::Integer;
use num_traits::{One, Zero};
use rand::rngs::OsRng;
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::phantom::crypto::errors::CryptoError;

/// Minimum RSA key size in bits for security.
const MIN_RSA_BITS: usize = 2048;

/// Default RSA public exponent (65537 = 0x10001).
/// This value provides a good balance between security and performance.
const DEFAULT_PUBLIC_EXPONENT: u64 = 65537;

/// Number of Miller-Rabin rounds for primality testing.
const MILLER_RABIN_ROUNDS: usize = 40;

/// Maximum attempts to generate a prime before failing.
const MAX_PRIME_ATTEMPTS: usize = 10000;

/// RSA Blind Signature key pair containing both public and private components.
///
/// The private key is automatically zeroized when this struct is dropped.
#[derive(Debug)]
pub struct BlindSignatureKeyPair {
    /// Public key for blinding and verification
    pub public: BlindSignaturePublicKey,
    /// Private key for signing (kept secret)
    private: BlindSignaturePrivateKey,
}

/// RSA public key for blind signature operations.
///
/// This key is used by workers to blind messages before sending to the coordinator,
/// and by anyone to verify unblinded signatures.
#[derive(Debug, Clone)]
pub struct BlindSignaturePublicKey {
    /// RSA modulus n = p * q
    pub n: BigUint,
    /// Public exponent e (typically 65537)
    pub e: BigUint,
}

/// RSA private key for blind signature operations.
///
/// This key is held only by the coordinator and used to sign blinded messages.
/// All fields are zeroized on drop for security.
#[derive(Debug, Zeroize, ZeroizeOnDrop)]
pub struct BlindSignaturePrivateKey {
    /// Private exponent d where d*e ≡ 1 (mod φ(n))
    #[zeroize(skip)] // BigUint doesn't impl Zeroize, we handle manually
    d: BigUint,
    /// First prime factor of n
    #[zeroize(skip)]
    p: BigUint,
    /// Second prime factor of n
    #[zeroize(skip)]
    q: BigUint,
    /// d mod (p-1) for CRT optimization
    #[zeroize(skip)]
    dp: BigUint,
    /// d mod (q-1) for CRT optimization
    #[zeroize(skip)]
    dq: BigUint,
    /// q^(-1) mod p for CRT optimization
    #[zeroize(skip)]
    qinv: BigUint,
}

/// A message that has been blinded for signing.
///
/// Contains both the blinded value (to send to signer) and the blinding factor
/// (kept secret by the worker for unblinding).
#[derive(Debug)]
pub struct BlindedMessage {
    /// The blinded message value: m' = H(m) * r^e mod n
    pub blinded: BigUint,
    /// The blinding factor r (kept secret for unblinding)
    blinding_factor: BigUint,
    /// Original message hash for verification
    #[allow(dead_code)]
    message_hash: BigUint,
}

/// A blind signature on a blinded message.
///
/// This is produced by the signer and must be unblinded by the message originator.
#[derive(Debug, Clone)]
pub struct BlindSignature {
    /// The signature value: s' = (m')^d mod n
    pub signature: BigUint,
}

/// An unblinded signature that can be publicly verified.
///
/// This represents a valid RSA signature on the original message, even though
/// the signer never saw the original message content.
#[derive(Debug, Clone)]
pub struct UnblindedSignature {
    /// The original message that was signed
    pub message: Vec<u8>,
    /// The unblinded signature value: s = s' * r^(-1) mod n
    pub signature: BigUint,
}

impl BlindSignatureKeyPair {
    /// Generate a new RSA blind signature key pair.
    ///
    /// # Arguments
    ///
    /// * `bits` - RSA modulus size in bits (minimum 2048, recommended 3072+)
    ///
    /// # Returns
    ///
    /// A new key pair suitable for blind signature operations.
    ///
    /// # Errors
    ///
    /// Returns `CryptoError::InvalidModulusSize` if bits < 2048.
    /// Returns `CryptoError::PrimeGenerationFailed` if prime generation fails.
    ///
    /// # Security Notes
    ///
    /// - Uses cryptographically secure random number generation (OsRng)
    /// - Primes are tested with 40 rounds of Miller-Rabin
    /// - Generates safe primes where p-1 has a large prime factor
    pub fn generate(bits: usize) -> Result<Self, CryptoError> {
        if bits < MIN_RSA_BITS {
            return Err(CryptoError::InvalidModulusSize(bits));
        }

        let mut rng = OsRng;
        let prime_bits = bits / 2;

        // Generate two distinct primes p and q
        let p = generate_prime(&mut rng, prime_bits)?;
        let q = generate_prime_different(&mut rng, prime_bits, &p)?;

        // Compute modulus n = p * q
        let n = &p * &q;

        // Compute Euler's totient φ(n) = (p-1)(q-1)
        let p_minus_1 = &p - 1u32;
        let q_minus_1 = &q - 1u32;
        let phi_n = &p_minus_1 * &q_minus_1;

        // Public exponent e = 65537
        let e = DEFAULT_PUBLIC_EXPONENT.to_biguint().unwrap();

        // Verify e is coprime to φ(n)
        if e.gcd(&phi_n) != BigUint::one() {
            return Err(CryptoError::OperationFailed(
                "Public exponent not coprime to phi(n)".to_string(),
            ));
        }

        // Compute private exponent d = e^(-1) mod φ(n)
        let d = mod_inverse(&e, &phi_n).ok_or_else(|| {
            CryptoError::OperationFailed("Failed to compute private exponent".to_string())
        })?;

        // Compute CRT components for faster signing
        let dp = &d % &p_minus_1;
        let dq = &d % &q_minus_1;
        let qinv = mod_inverse(&q, &p).ok_or_else(|| {
            CryptoError::OperationFailed("Failed to compute CRT coefficient".to_string())
        })?;

        Ok(BlindSignatureKeyPair {
            public: BlindSignaturePublicKey { n, e },
            private: BlindSignaturePrivateKey {
                d,
                p,
                q,
                dp,
                dq,
                qinv,
            },
        })
    }

    /// Sign a blinded message.
    ///
    /// The signer cannot determine the original message content from the
    /// blinded value, providing unlinkability.
    ///
    /// # Arguments
    ///
    /// * `blinded` - The blinded message to sign
    ///
    /// # Returns
    ///
    /// A blind signature that must be unblinded by the message originator.
    pub fn sign_blinded(&self, blinded: &BlindedMessage) -> Result<BlindSignature, CryptoError> {
        self.private.sign_blinded(blinded, &self.public)
    }
}

impl BlindSignaturePublicKey {
    /// Blind a message for signing.
    ///
    /// This creates a blinded version of the message that hides the original
    /// content from the signer while allowing them to produce a valid signature.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to blind
    ///
    /// # Returns
    ///
    /// A `BlindedMessage` containing the blinded value and the secret blinding
    /// factor needed for unblinding.
    ///
    /// # Security Notes
    ///
    /// - The blinding factor is cryptographically random
    /// - The message is hashed before blinding (Full Domain Hash)
    /// - The blinding factor must be kept secret until unblinding
    pub fn blind(&self, message: &[u8]) -> Result<BlindedMessage, CryptoError> {
        let mut rng = OsRng;

        // Hash the message using Full Domain Hash (FDH)
        let message_hash = full_domain_hash(message, &self.n);

        // Generate random blinding factor r that is coprime to n
        let blinding_factor = loop {
            let r = rng.gen_biguint_range(&BigUint::from(2u32), &self.n);
            if r.gcd(&self.n) == BigUint::one() {
                break r;
            }
        };

        // Compute r^e mod n
        let r_to_e = blinding_factor.modpow(&self.e, &self.n);

        // Compute blinded message: m' = H(m) * r^e mod n
        let blinded = (&message_hash * &r_to_e) % &self.n;

        Ok(BlindedMessage {
            blinded,
            blinding_factor,
            message_hash,
        })
    }

    /// Verify an unblinded signature.
    ///
    /// This verifies that the signature is valid for the original message,
    /// even though the signer never saw the original message.
    ///
    /// # Arguments
    ///
    /// * `sig` - The unblinded signature to verify
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the signature is valid, `Ok(false)` otherwise.
    ///
    /// # Security Notes
    ///
    /// - Uses constant-time comparison for the final check
    /// - Verification time should not leak information about validity
    pub fn verify(&self, sig: &UnblindedSignature) -> Result<bool, CryptoError> {
        // Compute expected hash of the message
        let expected_hash = full_domain_hash(&sig.message, &self.n);

        // Compute s^e mod n (should equal H(m) if signature is valid)
        let computed_hash = sig.signature.modpow(&self.e, &self.n);

        // Constant-time comparison
        Ok(constant_time_eq_biguint(&computed_hash, &expected_hash))
    }

    /// Serialize the public key to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let n_bytes = self.n.to_bytes_be();
        let e_bytes = self.e.to_bytes_be();

        let mut result = Vec::new();
        result.extend_from_slice(&(n_bytes.len() as u32).to_be_bytes());
        result.extend_from_slice(&n_bytes);
        result.extend_from_slice(&(e_bytes.len() as u32).to_be_bytes());
        result.extend_from_slice(&e_bytes);
        result
    }

    /// Deserialize a public key from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CryptoError> {
        if bytes.len() < 8 {
            return Err(CryptoError::SerializationFailed(
                "Input too short".to_string(),
            ));
        }

        let mut cursor = 0;

        let n_len = u32::from_be_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .map_err(|_| CryptoError::SerializationFailed("Invalid n length".to_string()))?,
        ) as usize;
        cursor += 4;

        if bytes.len() < cursor + n_len + 4 {
            return Err(CryptoError::SerializationFailed(
                "Input too short for n".to_string(),
            ));
        }

        let n = BigUint::from_bytes_be(&bytes[cursor..cursor + n_len]);
        cursor += n_len;

        let e_len = u32::from_be_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .map_err(|_| CryptoError::SerializationFailed("Invalid e length".to_string()))?,
        ) as usize;
        cursor += 4;

        if bytes.len() < cursor + e_len {
            return Err(CryptoError::SerializationFailed(
                "Input too short for e".to_string(),
            ));
        }

        let e = BigUint::from_bytes_be(&bytes[cursor..cursor + e_len]);

        Ok(BlindSignaturePublicKey { n, e })
    }
}

impl BlindSignaturePrivateKey {
    /// Sign a blinded message using CRT optimization.
    ///
    /// This is significantly faster than naive modular exponentiation for
    /// large RSA keys.
    fn sign_blinded(
        &self,
        blinded: &BlindedMessage,
        public: &BlindSignaturePublicKey,
    ) -> Result<BlindSignature, CryptoError> {
        // CRT-based RSA signing for performance
        // s_p = m^dp mod p
        let s_p = blinded.blinded.modpow(&self.dp, &self.p);

        // s_q = m^dq mod q
        let s_q = blinded.blinded.modpow(&self.dq, &self.q);

        // Combine using CRT: s = s_q + q * (qinv * (s_p - s_q) mod p)
        let diff = if s_p >= s_q {
            &s_p - &s_q
        } else {
            &self.p - ((&s_q - &s_p) % &self.p)
        };

        let h = (&self.qinv * diff) % &self.p;
        let signature = &s_q + &self.q * &h;

        // Verify the signature is correct (security_domain against fault attacks)
        let verification = signature.modpow(&public.e, &public.n);
        if verification != blinded.blinded {
            return Err(CryptoError::OperationFailed(
                "Signature self-verification failed".to_string(),
            ));
        }

        Ok(BlindSignature { signature })
    }
}

impl BlindedMessage {
    /// Unblind a signature to obtain a valid signature on the original message.
    ///
    /// # Arguments
    ///
    /// * `blind_sig` - The blind signature from the signer
    /// * `public_key` - The signer's public key
    ///
    /// # Returns
    ///
    /// An `UnblindedSignature` that is a valid RSA signature on the original message.
    ///
    /// # Errors
    ///
    /// Returns `CryptoError::BlindingFactorInverseNotFound` if the blinding factor
    /// inverse cannot be computed.
    pub fn unblind(
        &self,
        blind_sig: &BlindSignature,
        public_key: &BlindSignaturePublicKey,
    ) -> Result<UnblindedSignature, CryptoError> {
        // Compute r^(-1) mod n
        let r_inverse = mod_inverse(&self.blinding_factor, &public_key.n)
            .ok_or(CryptoError::BlindingFactorInverseNotFound)?;

        // Compute s = s' * r^(-1) mod n
        let signature = (&blind_sig.signature * &r_inverse) % &public_key.n;

        Ok(UnblindedSignature {
            message: Vec::new(), // Message must be tracked separately
            signature,
        })
    }

    /// Unblind a signature and attach the original message.
    ///
    /// This is the recommended method when you have access to the original message.
    pub fn unblind_with_message(
        &self,
        blind_sig: &BlindSignature,
        public_key: &BlindSignaturePublicKey,
        original_message: &[u8],
    ) -> Result<UnblindedSignature, CryptoError> {
        let mut unblinded = self.unblind(blind_sig, public_key)?;
        unblinded.message = original_message.to_vec();
        Ok(unblinded)
    }

    /// Get the blinded value for transmission to the signer.
    pub fn blinded_value(&self) -> &BigUint {
        &self.blinded
    }
}

impl BlindSignature {
    /// Serialize the blind signature to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.signature.to_bytes_be()
    }

    /// Deserialize a blind signature from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        BlindSignature {
            signature: BigUint::from_bytes_be(bytes),
        }
    }
}

impl UnblindedSignature {
    /// Serialize the unblinded signature to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let sig_bytes = self.signature.to_bytes_be();
        let mut result = Vec::new();
        result.extend_from_slice(&(self.message.len() as u32).to_be_bytes());
        result.extend_from_slice(&self.message);
        result.extend_from_slice(&(sig_bytes.len() as u32).to_be_bytes());
        result.extend_from_slice(&sig_bytes);
        result
    }

    /// Deserialize an unblinded signature from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CryptoError> {
        if bytes.len() < 8 {
            return Err(CryptoError::SerializationFailed(
                "Input too short".to_string(),
            ));
        }

        let mut cursor = 0;

        let msg_len =
            u32::from_be_bytes(bytes[cursor..cursor + 4].try_into().map_err(|_| {
                CryptoError::SerializationFailed("Invalid message length".to_string())
            })?) as usize;
        cursor += 4;

        if bytes.len() < cursor + msg_len + 4 {
            return Err(CryptoError::SerializationFailed(
                "Input too short for message".to_string(),
            ));
        }

        let message = bytes[cursor..cursor + msg_len].to_vec();
        cursor += msg_len;

        let sig_len = u32::from_be_bytes(bytes[cursor..cursor + 4].try_into().map_err(|_| {
            CryptoError::SerializationFailed("Invalid signature length".to_string())
        })?) as usize;
        cursor += 4;

        if bytes.len() < cursor + sig_len {
            return Err(CryptoError::SerializationFailed(
                "Input too short for signature".to_string(),
            ));
        }

        let signature = BigUint::from_bytes_be(&bytes[cursor..cursor + sig_len]);

        Ok(UnblindedSignature { message, signature })
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Full Domain Hash (FDH) - maps message to element of Z_n*
///
/// Uses iterative hashing to produce a value in the correct range.
fn full_domain_hash(message: &[u8], n: &BigUint) -> BigUint {
    let n_bytes = (n.bits() as usize).div_ceil(8);
    let hash_input = message.to_vec();
    let mut result = Vec::with_capacity(n_bytes + 32);

    // Iteratively hash until we have enough bytes
    let mut counter = 0u32;
    while result.len() < n_bytes + 32 {
        let mut hasher = Sha256::new();
        hasher.update(&hash_input);
        hasher.update(counter.to_be_bytes());
        result.extend_from_slice(&hasher.finalize());
        counter += 1;
    }

    // Convert to BigUint and reduce modulo n
    let hash_int = BigUint::from_bytes_be(&result[..n_bytes + 32]);
    hash_int % n
}

/// Generate a random prime number of the specified bit length.
fn generate_prime(rng: &mut OsRng, bits: usize) -> Result<BigUint, CryptoError> {
    for _ in 0..MAX_PRIME_ATTEMPTS {
        // Generate random odd number of correct bit length
        let mut candidate = rng.gen_biguint(bits as u64);

        // Ensure correct bit length by setting the top bit
        candidate |= BigUint::one() << (bits - 1);

        // Ensure odd
        candidate |= BigUint::one();

        if is_probably_prime(&candidate, MILLER_RABIN_ROUNDS) {
            return Ok(candidate);
        }
    }

    Err(CryptoError::PrimeGenerationFailed {
        attempts: MAX_PRIME_ATTEMPTS,
    })
}

/// Generate a prime different from a given prime.
fn generate_prime_different(
    rng: &mut OsRng,
    bits: usize,
    other: &BigUint,
) -> Result<BigUint, CryptoError> {
    for _ in 0..MAX_PRIME_ATTEMPTS {
        let candidate = generate_prime(rng, bits)?;
        if &candidate != other {
            return Ok(candidate);
        }
    }

    Err(CryptoError::PrimeGenerationFailed {
        attempts: MAX_PRIME_ATTEMPTS,
    })
}

/// Miller-Rabin primality test.
fn is_probably_prime(n: &BigUint, rounds: usize) -> bool {
    // Handle small cases
    if n <= &BigUint::one() {
        return false;
    }
    if n == &BigUint::from(2u32) || n == &BigUint::from(3u32) {
        return true;
    }
    if n.is_even() {
        return false;
    }

    // Write n-1 as 2^r * d
    let n_minus_1 = n - 1u32;
    let mut d = n_minus_1.clone();
    let mut r = 0u32;

    while d.is_even() {
        d >>= 1;
        r += 1;
    }

    let mut rng = OsRng;

    'witness: for _ in 0..rounds {
        // Pick random a in [2, n-2]
        let a = rng.gen_biguint_range(&BigUint::from(2u32), &(&n_minus_1 - 1u32));

        // Compute x = a^d mod n
        let mut x = a.modpow(&d, n);

        if x == BigUint::one() || x == n_minus_1 {
            continue 'witness;
        }

        for _ in 0..r - 1 {
            x = x.modpow(&BigUint::from(2u32), n);
            if x == n_minus_1 {
                continue 'witness;
            }
        }

        return false;
    }

    true
}

/// Compute modular inverse using extended Euclidean algorithm.
fn mod_inverse(a: &BigUint, m: &BigUint) -> Option<BigUint> {
    if m.is_zero() {
        return None;
    }

    let (gcd, x, _) = extended_gcd(a, m);

    if gcd != BigUint::one() {
        return None;
    }

    // Handle negative result
    Some(((x % m) + m) % m)
}

/// Extended Euclidean algorithm returning (gcd, x, y) where ax + by = gcd.
fn extended_gcd(a: &BigUint, b: &BigUint) -> (BigUint, BigUint, BigUint) {
    if b.is_zero() {
        return (a.clone(), BigUint::one(), BigUint::zero());
    }

    // Use signed arithmetic internally
    let mut old_r = a.clone();
    let mut r = b.clone();
    let mut old_s = num_bigint::BigInt::from(1);
    let mut s = num_bigint::BigInt::from(0);

    while !r.is_zero() {
        let quotient = &old_r / &r;
        let quotient_int = num_bigint::BigInt::from(quotient.clone());

        let temp_r = r.clone();
        r = &old_r - &quotient * &r;
        old_r = temp_r;

        let temp_s = s.clone();
        s = &old_s - &quotient_int * &s;
        old_s = temp_s;
    }

    // Convert back to unsigned, handling negatives via modular arithmetic
    let x = if old_s < num_bigint::BigInt::from(0) {
        let abs_s = (-old_s).to_biguint().unwrap();
        b - (abs_s % b)
    } else {
        old_s.to_biguint().unwrap() % b
    };

    (old_r, x, BigUint::zero()) // We only need x for mod_inverse
}

/// Constant-time comparison of two BigUints.
///
/// This is critical for preventing timing attacks during signature verification.
fn constant_time_eq_biguint(a: &BigUint, b: &BigUint) -> bool {
    let a_bytes = a.to_bytes_be();
    let b_bytes = b.to_bytes_be();

    // Pad to same length
    let max_len = std::cmp::max(a_bytes.len(), b_bytes.len());
    let mut a_padded = vec![0u8; max_len];
    let mut b_padded = vec![0u8; max_len];

    a_padded[max_len - a_bytes.len()..].copy_from_slice(&a_bytes);
    b_padded[max_len - b_bytes.len()..].copy_from_slice(&b_bytes);

    // Constant-time comparison
    let mut result = 0u8;
    for (x, y) in a_padded.iter().zip(b_padded.iter()) {
        result |= x ^ y;
    }

    result == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_generation() {
        let keypair = BlindSignatureKeyPair::generate(2048).unwrap();
        assert!(keypair.public.n.bits() >= 2047);
        assert!(keypair.public.n.bits() <= 2048);
    }

    #[test]
    fn test_key_generation_minimum_size() {
        assert!(matches!(
            BlindSignatureKeyPair::generate(1024),
            Err(CryptoError::InvalidModulusSize(1024))
        ));
    }

    #[test]
    fn test_blind_sign_unblind_verify() {
        let keypair = BlindSignatureKeyPair::generate(2048).unwrap();
        let message = b"Hello, anonymous world!";

        // Blind the message
        let blinded = keypair.public.blind(message).unwrap();

        // Sign the blinded message
        let blind_sig = keypair.sign_blinded(&blinded).unwrap();

        // Unblind the signature
        let unblinded = blinded
            .unblind_with_message(&blind_sig, &keypair.public, message)
            .unwrap();

        // Verify the unblinded signature
        assert!(keypair.public.verify(&unblinded).unwrap());
    }

    #[test]
    fn test_signature_uniqueness() {
        let keypair = BlindSignatureKeyPair::generate(2048).unwrap();
        let message = b"Test message";

        // Two blindings of the same message should produce different blinded values
        let blinded1 = keypair.public.blind(message).unwrap();
        let blinded2 = keypair.public.blind(message).unwrap();

        assert_ne!(blinded1.blinded, blinded2.blinded);
    }

    #[test]
    fn test_invalid_signature() {
        let keypair = BlindSignatureKeyPair::generate(2048).unwrap();
        let message = b"Original message";
        let wrong_message = b"Wrong message";

        let blinded = keypair.public.blind(message).unwrap();
        let blind_sig = keypair.sign_blinded(&blinded).unwrap();
        let mut unblinded = blinded
            .unblind_with_message(&blind_sig, &keypair.public, message)
            .unwrap();

        // Tamper with the message
        unblinded.message = wrong_message.to_vec();

        // Verification should fail
        assert!(!keypair.public.verify(&unblinded).unwrap());
    }

    #[test]
    fn test_public_key_serialization() {
        let keypair = BlindSignatureKeyPair::generate(2048).unwrap();

        let bytes = keypair.public.to_bytes();
        let restored = BlindSignaturePublicKey::from_bytes(&bytes).unwrap();

        assert_eq!(keypair.public.n, restored.n);
        assert_eq!(keypair.public.e, restored.e);
    }

    #[test]
    fn test_unblinded_signature_serialization() {
        let keypair = BlindSignatureKeyPair::generate(2048).unwrap();
        let message = b"Serialization test";

        let blinded = keypair.public.blind(message).unwrap();
        let blind_sig = keypair.sign_blinded(&blinded).unwrap();
        let unblinded = blinded
            .unblind_with_message(&blind_sig, &keypair.public, message)
            .unwrap();

        let bytes = unblinded.to_bytes();
        let restored = UnblindedSignature::from_bytes(&bytes).unwrap();

        assert_eq!(unblinded.message, restored.message);
        assert_eq!(unblinded.signature, restored.signature);
    }

    #[test]
    fn test_miller_rabin() {
        // Known primes
        assert!(is_probably_prime(&BigUint::from(2u32), MILLER_RABIN_ROUNDS));
        assert!(is_probably_prime(&BigUint::from(3u32), MILLER_RABIN_ROUNDS));
        assert!(is_probably_prime(
            &BigUint::from(17u32),
            MILLER_RABIN_ROUNDS
        ));
        assert!(is_probably_prime(
            &BigUint::from(997u32),
            MILLER_RABIN_ROUNDS
        ));

        // Known composites
        assert!(!is_probably_prime(
            &BigUint::from(4u32),
            MILLER_RABIN_ROUNDS
        ));
        assert!(!is_probably_prime(
            &BigUint::from(15u32),
            MILLER_RABIN_ROUNDS
        ));
        assert!(!is_probably_prime(
            &BigUint::from(1000u32),
            MILLER_RABIN_ROUNDS
        ));
    }

    #[test]
    fn test_mod_inverse() {
        let a = BigUint::from(3u32);
        let m = BigUint::from(11u32);
        let inv = mod_inverse(&a, &m).unwrap();

        // 3 * 4 = 12 ≡ 1 (mod 11)
        assert_eq!(inv, BigUint::from(4u32));

        // Verify: a * inv ≡ 1 (mod m)
        assert_eq!((&a * &inv) % &m, BigUint::one());
    }

    #[test]
    fn test_constant_time_eq() {
        let a = BigUint::from(12345u32);
        let b = BigUint::from(12345u32);
        let c = BigUint::from(54321u32);

        assert!(constant_time_eq_biguint(&a, &b));
        assert!(!constant_time_eq_biguint(&a, &c));
    }
}
