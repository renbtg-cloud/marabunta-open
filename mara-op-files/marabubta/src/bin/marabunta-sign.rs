// Marabunta - Licensed under the MIT License.
//! Binary signing CLI tool for Marabunta highestsec compliance.
//!
//! Appends a trailer containing a SHA-256 hash and Ed25519 signature
//! to a binary, and writes a detached `.sig` file alongside it.
//!
//! Usage: marabunta-sign <binary_path> <base64_signing_key>

use std::{env, fs, process};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};

/// Trailer magic: "CMBRSIG\0" (8 bytes).
const MAGIC: &[u8; 8] = b"CMBRSIG\0";
/// Trailer version.
const VERSION: u8 = 1;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: marabunta-sign <binary_path> <base64_signing_key>");
        process::exit(1);
    }

    let binary_path = &args[1];
    let key_b64 = &args[2];

    // Decode the signing key from base64.
    let key_bytes = STANDARD.decode(key_b64).unwrap_or_else(|e| {
        eprintln!("Error: invalid base64 signing key: {}", e);
        process::exit(1);
    });
    if key_bytes.len() != 32 {
        eprintln!("Error: signing key must be 32 bytes (got {})", key_bytes.len());
        process::exit(1);
    }
    let mut key_array = [0u8; 32];
    key_array.copy_from_slice(&key_bytes);
    let signing_key = SigningKey::from_bytes(&key_array);

    // Read the binary.
    let binary = fs::read(binary_path).unwrap_or_else(|e| {
        eprintln!("Error: cannot read {}: {}", binary_path, e);
        process::exit(1);
    });

    // Compute SHA-256 hash of the binary content.
    let mut hasher = Sha256::new();
    hasher.update(&binary);
    let hash: [u8; 32] = hasher.finalize().into();

    // Sign the hash with Ed25519.
    let signature = signing_key.sign(&hash);
    let sig_bytes = signature.to_bytes();

    // Build the trailer: magic(8) + hash(32) + signature(64) + version(1) = 105 bytes.
    let mut trailer = Vec::with_capacity(105);
    trailer.extend_from_slice(MAGIC);
    trailer.extend_from_slice(&hash);
    trailer.extend_from_slice(&sig_bytes);
    trailer.push(VERSION);

    // Append trailer to the binary.
    let mut signed_binary = binary;
    signed_binary.extend_from_slice(&trailer);
    fs::write(binary_path, &signed_binary).unwrap_or_else(|e| {
        eprintln!("Error: cannot write {}: {}", binary_path, e);
        process::exit(1);
    });

    // Write detached .sig file.
    let sig_path = format!("{}.sig", binary_path);
    fs::write(&sig_path, &trailer).unwrap_or_else(|e| {
        eprintln!("Error: cannot write {}: {}", sig_path, e);
        process::exit(1);
    });

    println!("Signed: {}", binary_path);
    println!("  SHA-256:   {}", hex::encode(hash));
    println!("  Signature: {}", hex::encode(&sig_bytes));
    println!("  Detached:  {}", sig_path);
    println!("  Trailer:   {} bytes (version {})", trailer.len(), VERSION);
}
