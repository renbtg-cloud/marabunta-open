<!-- Marabunta - Licensed under the MIT License.
## Chapter 6: ClearNet Embassies and DarkNet Proxies

A globally regulated entity (e.g., a Fortune 500 Bank) cannot expose its internal network to a decentralized swarm. Relying on traditional VPNs or DMZs introduces the **Monolithic Membrane Anti-Pattern**, where a single breach compromises the entire internal infrastructure.

Marabunta resolves this via the **Asymmetric Data Diode**.

```text
  [ClearNet Corporate Boundary]           [The DarkNet Proxy]           [The Swarm DHT]
 
  +-----------------------+              +-------------------+          +-------------+
  |  Write-Only Embassy   | <=== UDP === |  Phantom Router   | <------- |  Boulder A  |
  |  (Static IP: 1.1.1.1) |   (Encrypted)|  (Ephemeral IP)   | (Gossip) |  (Executor) |
  |                       |              |                   |          |             |
  |  > JCL Payload (Enc)  |              | > ml_kem Terminate|          | > wasmtime  |
  |  > ZKP Result (Dec)   | === UDP ===> | > Payload Push    | -------> | > ptrace    |
  +-----------------------+              +-------------------+          +-------------+
       | (Air-Gapped)
  +-----------------------+
  | Internal Ledger       |
  +-----------------------+
```

The `ClearNet Embassy` sits inside the bank's firewall. It *never* initiates an outbound connection to the public darknet, and it *never* accepts an unauthenticated inbound stream. 

Anonymous `DarkNet Proxies` (disposable meat shields) reach *across* the physical boundary via outbound-only UDP tunnels. They pull the encrypted workload from the Embassy, execute it on cheap offshore hardware, and push the Zero-Knowledge Proof (ZKP) verified result back. Security is enforced by topology and cryptography, not firewalls. 
