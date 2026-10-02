<!-- Marabunta - Licensed under the MIT License.
## Chapter 11: Compliance Fenced Boundaries (Zone Certificates)

The Asymmetric Data Diode detailed in Chapter 8 allows a corporation to safely interact with the global, anonymous swarm. However, certain workloads are legally prohibited from leaving specific geographic or regulatory boundaries.

A European healthcare provider analyzing anonymized patient data cannot allow that workload to be executed by a node physically located in a jurisdiction that does not enforce the General Data Protection Regulation (GDPR). 

Similarly, a US defense contractor running fluid dynamics simulations on a classified aerospace component cannot allow that payload to be processed by a Spot Market node located in an adversarial nation-state, even if the payload is encrypted and the node is cheap.

In a decentralized, anonymous swarm spanning 100 million IPs, how do you mathematically guarantee geography?

We implement **Zone Certificates**.

### 11.1 The Janis & Jim Analysis: The WireGuard Vulnerability

Jim, the VP of Engineering, stared at the compliance requirements for the bank's new risk analysis model. 

"Janis," Jim said, "Legal just threw a flag. We can't use the Marabunta Spot Market for the European derivatives calculation. The data must stay physically within the EU. We have to drop back to our our centralized Frankfurt deployment."

Janis shook her head. "We don't need the Hyperscaler for compliance. We just configure the JCL manifest to only accept bids from nodes with EU IP addresses."

Jim laughed. "IP geolocation? In a zero-trust network? Janis, I could defeat that in five minutes."

Jim grabbed a marker and drew a diagram on the whiteboard.

```text
[Malicious Actor in North Korea] ====== (WireGuard VPN) ======> [Cheap VPS in Frankfurt]
        |                                                           |
        +-- (Actual Execution)                                      |
                                                                    v
                                                        [Marabunta Swarm (Thinks EU)]
```

"I'm an offshore Spot Market farmer," Jim explained. "I want to steal your high-paying banking workloads, but my IP is blacklisted. So, I rent a $5-a-month Virtual Private Server (VPS) in Frankfurt. I set up a WireGuard tunnel. I route my Marabunta node's traffic through the German IP. Your system queries the MaxMind database, sees a German IP, and hands me the workload. I pull it back through the tunnel, execute the math in my unauthorized jurisdiction, and send the result back. You just violated GDPR, and I got paid."

Janis stared at the diagram. Jim was right. In a hostile network, IP geolocation is useless. The geography of the internet is fluid. 

"Okay," Janis said, erasing the VPS from the board. "We discard IP geolocation. Geography isn't defined by a router. It must be defined by cryptography."

### 11.2 Cryptographic Jurisdictions

Marabunta solves the WireGuard vulnerability through strict cryptographic issuance.

When an organization (e.g., a hospital network or a defense contractor) deploys a `ClearNet` node intended to handle highly regulated workloads, that node cannot simply join the network and claim to be in a specific zone. It must be issued a **Zone Certificate** by a trusted, centralized auditing authority (e.g., an internal corporate compliance officer, or an external government regulator).

A Zone Certificate is a strict JSON payload, cryptographically signed using the ED25519 private key of the trusted authority. 

The auditor physically verifies the location and security of the datacenter (e.g., checking the biometric locks on the server rack in Frankfurt) before issuing the certificate to the node's `bls12_381` public key.

```json
{
  "node_pubkey": "0x8F9A2B4C90D1E2F3A4B5C6D7E8F9A0B1C2D3E4F5...",
  "jurisdiction": "EU_STRATEGIC_PACT",
  "requires_clearnet": true,
  "audit_level": "SOC2_TYPE_2",
  "hardware_attestation": "INTEL_SGX_ENCLAVE",
  "issued_at": "2026-04-04T12:00:00Z",
  "expires_at": "2027-04-04T12:00:00Z",
  "authority_signature": "7d8f9a2b5c6d..."
}
```

### 11.3 The Rust Verification Engine

When the node receives a JCL workload, the Marabunta Swarm does not check its IP address. It executes a rigorous, multi-signature verification protocol.

To demonstrate the mathematical boundaries, consider this reference implementation mapping the cryptography, demonstrating how the `ed25519_dalek` crate is used to mathematically prove geography:

```rust
use ed25519_dalek::{Verifier, VerifyingKey, Signature};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ZoneCertificate {
    pub node_pubkey: String,
    pub jurisdiction: String,
    pub expires_at: u64,
    // The raw bytes of the auditor's signature
    pub authority_signature: [u8; 64], 
}

pub fn verify_geographic_compliance(
    cert: &ZoneCertificate, 
    auditor_pubkey_bytes: &[u8; 32],
    current_timestamp: u64
) -> Result<(), &'static str> {

    // 1. Verify the certificate hasn't expired
    if current_timestamp > cert.expires_at {
        return Err("Zone Certificate Expired. Node is non-compliant.");
    }

    // 2. Reconstruct the auditor's public key
    let auditor_pubkey = VerifyingKey::from_bytes(auditor_pubkey_bytes)
        .map_err(|_| "Invalid Auditor Public Key")?;

    // 3. Extract the signature
    let signature = Signature::from_bytes(&cert.authority_signature);

    // 4. Re-serialize the payload (excluding the signature) to check the hash
    let mut payload = cert.node_pubkey.clone().into_bytes();
    payload.extend_from_slice(cert.jurisdiction.as_bytes());
    payload.extend_from_slice(&cert.expires_at.to_be_bytes());

    // 5. The absolute mathematical boundary
    auditor_pubkey.verify(&payload, &signature)
        .map_err(|_| "FATAL: Cryptographic signature mismatch. Geography spoofing detected.")
}
```

If Jim's rogue offshore farmer attempts to forge an `EU_STRATEGIC_PACT` certificate, the `verify` function will trap the mathematical mismatch instantly. The bid is dropped. 

### 11.4 Bidding and Execution Fencing

When a data scientist submits a JCL manifest for a highly regulated workload, they explicitly declare the compliance boundary using the exact strings verified by the auditor:

```yaml
job:
  id: "urn:mrb:job:eu-genomics-alpha"
  compliance_constraint: 
    required_jurisdiction: "EU_STRATEGIC_PACT"
    enforce_clearnet_fencing: true
    require_hardware_enclave: true
```

When this job is broadcast to the Spot Market, the `MarketplaceEngine` enforces the boundary at the protocol level.

1.  **The Bid Rejection:** The anonymous DarkNet node running the WireGuard VPN in Frankfurt attempts to bid on the job. The sending node inspects the bidder's profile. Because the bidder cannot produce a valid, ED25519-signed Zone Certificate issued by the European Auditor, the bid is mathematically dropped. The VPN is useless.
2.  **The Authorized Execution:** A `ClearNet` node physically located in the audited Frankfurt datacenter bids on the job. It attaches its valid Zone Certificate. The sending node verifies the signature. The signature is valid. The payload is transmitted.

### 11.5 The Deep Research Vulnerability (Hostile TEE Extraction)

As we scale this system, we must address the ultimate paranoia of the enterprise architect: **Hostile Node Memory Extraction**.

Even if a node possesses a valid Zone Certificate and executes the math perfectly, what stops a rogue SysAdmin working inside the audited Frankfurt datacenter from simply reading the RAM of the server to steal the proprietary trading algorithm or the unencrypted DNA sequences? 

While WebAssembly protects the *host* from a malicious *payload* (as detailed in Chapter 6), it does not protect the *payload* from a malicious *host*.

If the JCL manifest includes `require_hardware_enclave: true`, the Marabunta binary enforces absolute Intellectual Property protection by demanding execution within a **Trusted Execution Environment (TEE)**, such as Intel SGX or AMD SEV.

1.  **Remote Attestation:** Before the Embassy transmits the decrypted WASM payload to the worker node, it executes a hardware-level cryptographic handshake with the worker's CPU. The CPU provides a signed attestation proving that the specific memory enclave is mathematically blind, even from the root operating system (the hypervisor).
2.  **The Execution Vault:** The encrypted payload is transmitted directly into the CPU enclave. It is decrypted *inside* the silicon, executed, and the result is re-encrypted before leaving the chip.
3.  **The Result:** Even if the SysAdmin uses `ptrace` or a physical hardware probe to dump the RAM of the server during execution, they will only extract encrypted static. 

Through the combination of **Zone Certificates** (to prove geography) and **Hardware Enclaves** (to prove memory sealing), Marabunta guarantees absolute regulatory compliance and IP protection within a decentralized, zero-trust topology.

[Continue to Chapter 10: MMX Spot Market & The Thermodynamic Ledger](../07-economics/01-thermodynamic-ledger.md)

