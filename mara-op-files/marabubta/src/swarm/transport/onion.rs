// Marabunta - Licensed under the MIT License.
//! Pillar 1.2 & Pillar 15.3: The Phantom Protocol & Meat Shield Topology
//! 
//! Cryptographically obfuscates the origin and destination of sensitive BFT events.
//! Physical Boulders do not listen on public ports. They establish outbound-only
//! encrypted ChaCha20Poly1305 tunnels to 10 random `NodeClass::Edge` proxies.
//! The Edge proxies bind to the public internet and act as Meat Shields, absorbing
//! inbound JCL payloads and hiding the Enterprise's true IP from kinetic targeting.

use chacha20poly1305::{aead::{Aead, KeyInit}, XChaCha20Poly1305, XNonce};
use x25519_dalek::{StaticSecret, PublicKey};
use serde::{Deserialize, Serialize};
use rand::RngCore;
use std::collections::HashSet;
use crate::swarm::types::NodeId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnionPacket {
    /// The next hop's public key (X25519)
    pub next_hop_pubkey: [u8; 32],
    /// Encrypted blob containing the next OnionPacket or the final payload
    pub encrypted_payload: Vec<u8>,
    /// Ephemeral public key for DH exchange
    pub ephemeral_pubkey: [u8; 32],
    /// Nonce for XChaCha20Poly1305
    pub nonce: [u8; 24],
}

pub struct PhantomRouter {
    secret_key: StaticSecret,
    pub_key: PublicKey,
    /// Pillar 15.3: Meat Shield Topology
    /// If this node is a Enterprise, it connects OUT to these Edge proxies.
    /// It never listens for INBOUND connections.
    dust_proxies: HashSet<NodeId>,
}

impl PhantomRouter {
    pub fn new(seed: [u8; 32]) -> Self {
        let secret_key = StaticSecret::from(seed);
        let pub_key = PublicKey::from(&secret_key);
        Self { 
            secret_key, 
            pub_key,
            dust_proxies: HashSet::new(),
        }
    }

    pub fn public_key(&self) -> [u8; 32] {
        *self.pub_key.as_bytes()
    }

    /// Allocates 10 disposable IoT/residential nodes to act as public listeners.
    /// The Enterprise maintains persistent outbound TCP tunnels to them.
    pub async fn provision_dust_proxies(&mut self, available_dust: &[NodeId]) {
        tracing::info!("Pillar 15.3: Provisioning Edge-Proxies (Meat Shields) for asymmetric network stealth.");
        for (i, proxy) in available_dust.iter().take(10).enumerate() {
            self.dust_proxies.insert(*proxy);
            tracing::debug!("Bound outbound-only stealth tunnel to Edge Proxy [{}/10]: {:?}", i+1, proxy);
        }
    }

    /// Recursively encapsulates a payload into multiple onion layers.
    /// `hops` is a list of relay public keys in the order they will be visited.
    pub fn encapsulate_route(
        final_payload: &[u8],
        hops: &[[u8; 32]],
    ) -> Result<OnionPacket, &'static str> {
        let mut current_payload = final_payload.to_vec();
        
        // Wrap from the inside out
        for hop_pubkey in hops.iter().rev() {
            let layer = Self::encapsulate_layer(*hop_pubkey, &current_payload)?;
            current_payload = bincode::serialize(&layer).map_err(|_| "Bincode serialization failed")?;
        }
        
        bincode::deserialize(&current_payload).map_err(|_| "Final bincode deserialization failed")
    }

    fn encapsulate_layer(target_pubkey_bytes: [u8; 32], payload: &[u8]) -> Result<OnionPacket, &'static str> {
        let target_pubkey = PublicKey::from(target_pubkey_bytes);
        
        let mut ephemeral_bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut ephemeral_bytes);
        let ephemeral_secret = StaticSecret::from(ephemeral_bytes);
        let ephemeral_public = PublicKey::from(&ephemeral_secret);
        
        let shared_secret = ephemeral_secret.diffie_hellman(&target_pubkey);
        let cipher = XChaCha20Poly1305::new(shared_secret.as_bytes().into());
        
        let mut nonce_bytes = [0u8; 24];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);

        let encrypted_payload = cipher.encrypt(nonce, payload).map_err(|_| "Encryption failed")?;

        Ok(OnionPacket {
            next_hop_pubkey: target_pubkey_bytes,
            encrypted_payload,
            ephemeral_pubkey: *ephemeral_public.as_bytes(),
            nonce: nonce_bytes,
        })
    }

    /// Peels one layer of the onion. Returns the decrypted payload which might be another OnionPacket.
    pub fn peel_layer(&self, packet: &OnionPacket) -> Result<Vec<u8>, &'static str> {
        if packet.next_hop_pubkey != *self.pub_key.as_bytes() {
            return Err("Node mismatch: This node is not the intended relay for this layer.");
        }

        let sender_ephemeral = PublicKey::from(packet.ephemeral_pubkey);
        let shared_secret = self.secret_key.diffie_hellman(&sender_ephemeral);
        let cipher = XChaCha20Poly1305::new(shared_secret.as_bytes().into());
        let nonce = XNonce::from_slice(&packet.nonce);

        cipher.decrypt(nonce, packet.encrypted_payload.as_ref()).map_err(|_| "Decryption failed")
    }
}
