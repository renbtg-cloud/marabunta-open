<!-- Marabunta - Licensed under the MIT License.
## Chapter 3: Post-Quantum Sovereign Identity

In a zero-trust network, IP addresses are meaningless. An IP is simply a transient artifact of an ISP's physical routing infrastructure. When a node hops from a corporate WiFi network in London to a 5G cellular connection in a moving train, its IP address changes. If the network relies on IP addresses for identity or security, the network shatters.

In Marabunta, the node *is* the cryptographic key. A node is identified permanently and immutably by a `bls12_381` pairing-friendly elliptic curve keypair. 

However, we are not building a system for the next five years; we are building a system for the next fifty. And within the next two decades, standard elliptic curves (like `secp256k1` or `ed25519`) will be broken by sufficiently stable quantum computers executing Shor's algorithm.

If we deploy a global infrastructure secured entirely by classical elliptic curves, we are building a glass house in a stone-throwing contest. 

### 3.1 The Janis & Jim Analysis: "Harvest Now, Decrypt Later"

Jim sat across from Janis in the War Room, reviewing the proposed cryptographic suite for the swarm's initial deployment. 

"Janis," Jim said, "I'm looking at the transport layer. You've ripped out standard TLS 1.3. You replaced the entire Diffie-Hellman key exchange with ML-KEM. Why? Standard Elliptic Curve Diffie-Hellman (ECDH) is perfectly secure. It's what the entire internet uses."

Janis didn't look up from her monitor. "It's secure today, Jim. It won't be secure in fifteen years."

"We aren't storing nuclear launch codes," Jim argued. "We are storing intermediate PyTorch gradients and compiled WASM payloads. If a quantum computer breaks the encryption in fifteen years, who cares? The data will be useless by then."

Janis turned to face him. "You are forgetting the threat model of the Spot Market. We are executing workloads on anonymous laptops in adversarial nation-states. You are worried about them reading the data in fifteen years. I am worried about them stealing the data *today*."

Janis stepped to the whiteboard and wrote: **Harvest Now, Decrypt Later (HNDL)**.

"State-sponsored actors are currently vacuuming up petabytes of encrypted internet traffic," Janis explained. "They store it in massive, cold-storage datacenters in places like Utah or Beijing. They can't read it today. But they are stockpiling it. The moment a cryptographically relevant quantum computer comes online, they will use Shor's algorithm to retroactively break the ECDH handshake, retrieve the symmetric AES key, and decrypt the entire historical archive."

Jim frowned. "But you just said the data will be useless by then."

"The *data* might be useless," Janis said. "But the *algorithms* won't be. If a quantitative hedge fund runs a proprietary trading algorithm on our Spot Market, they are transmitting the compiled WASM binary across the network. If an adversary harvests that encrypted traffic today and decrypts it in a decade, they steal the IP. They reverse-engineer the trading logic. We cannot allow our clients' IP to have a 10-year expiration date."

### 3.2 The ML-KEM Encapsulation Mechanism

To mathematically neutralize the HNDL threat, Marabunta enforces **ML-KEM (Kyber768)**, the NIST FIPS 203 post-quantum standard, for every single TCP/QUIC handshake in the swarm.

"So how does ML-KEM fix it?" Jim asked. "Is it just a bigger elliptic curve?"

"No," Janis said. "It abandons curves entirely. It relies on Learning With Errors (LWE) over polynomial rings. It's a math problem that even a quantum computer cannot solve efficiently."

Janis drew a diagram of the handshake.

```text
=== POST-QUANTUM HANDSHAKE (ML-KEM + CHACHA20) ===

[Node A (Alice)]                                  [Node B (Bob)]
      |                                                 |
      | 1. Bob generates ML-KEM Public Key (pk)         |
      |    and Secret Key (sk).                         |
      | <--------- (Sends Public Key `pk`) ------------ |
      |                                                 |
      | 2. Alice generates a random 256-bit             |
      |    shared secret (SS).                          |
      | 3. Alice "encapsulates" SS using Bob's `pk`.    |
      |    Produces Ciphertext (ct).                    |
      | ---------- (Sends Ciphertext `ct`) -----------> |
      |                                                 |
      |                               4. Bob "decapsulates" `ct` |
      |                                  using his `sk` to       |
      |                                  recover the exact SS.   |
      |                                                 |
      | 5. Both derive AEAD Key via HKDF(SS).           |
      |                                                 |
[Secure, Quantum-Resistant ChaCha20-Poly1305 Tunnel Established]
```

"Notice the difference," Janis pointed out. "In traditional Diffie-Hellman, both sides contribute math to generate the shared secret. In a Key Encapsulation Mechanism (KEM), Alice simply invents a random 256-bit secret, locks it in a mathematical box that only Bob can open, and ships the box across the network."

To demonstrate the mathematical boundaries, consider this reference implementation mapping the cryptography using the `pqcrypto_kyber` crate:

```rust
// The ML-KEM Key Encapsulation Mechanism Reference
use pqcrypto_kyber::kyber768::*;

/// Node A encapsulates a shared secret for Node B.
pub fn encapsulate_payload(public_key: &PublicKey) -> (Ciphertext, SharedSecret) {
    // The ML-KEM algorithm generates both the ciphertext to transmit
    // and the shared secret to retain locally.
    let (ciphertext, shared_secret) = encapsulate(public_key);
    
    // The shared secret is subsequently passed through HKDF to derive 
    // the ChaCha20Poly1305 AEAD key for symmetric payload encryption.
    (ciphertext, shared_secret)
}

/// Node B decapsulates the payload to retrieve the shared secret.
pub fn decapsulate_payload(ciphertext: &Ciphertext, secret_key: &SecretKey) -> SharedSecret {
    // The math guarantees that a quantum computer cannot derive the 
    // shared secret from the ciphertext alone.
    decapsulate(ciphertext, secret_key)
}
```

### 3.3 The Role of BLS12-381 (Threshold Signatures)

Jim reviewed the Rust logic. "Okay. ML-KEM secures the transport layer. The data is safe from quantum decryption. But if we are a post-quantum network, why are we still using `bls12_381` for the Node IDs? That's a classical elliptic curve."

"Because of the Hashgraph," Janis replied.

"If we want the 100 Virtual Boulders to achieve Byzantine Fault Tolerance," Janis explained, "they have to sign every single event in the DAG. If we used the post-quantum signature standard (ML-DSA / Dilithium), every signature would be 2.4 Kilobytes. Multiply that by 100 Boulders, gossiping thousands of events a second... the network would drown in its own signature overhead."

"So we use `bls12_381` because it's small?"

"We use `bls12_381` because it supports **Signature Aggregation**," Janis corrected. "We can take 100 distinct signatures from 100 different Boulders, and mathematically compress them into a single, 48-byte string. It doesn't matter if 10 nodes sign it or 10,000 nodes sign it; the final aggregated signature is always exactly 48 bytes."

Jim nodded slowly. "We use ML-KEM to protect the data from quantum computers. We use `bls12_381` to protect the network from drowning in its own consensus."

"Exactly," Janis said. "We enforce quantum resistance where the data is vulnerable (the transport layer). We enforce classical, pairing-friendly curves where the network requires absolute speed (the consensus layer). It is a hybrid cryptographic immune system."

With the transport layer mathematically hardened against the HNDL threat, the network can safely deploy its encrypted WASM payloads across anonymous, untrusted silicon. The question then becomes: how do millions of anonymous nodes find each other without a central router?

[Continue to Chapter 4: Biomimetics and Stigmergy](../03-topology/01-stigmergy.md)
