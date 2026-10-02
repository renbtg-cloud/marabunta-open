<!-- Marabunta - Licensed under the MIT License.
## Chapter 12: Behavioral Immune System & Elo Reputation

In a centralized datacenter, network defense is defined by the perimeter. You erect a massive firewall (WAF), inspect incoming packets with Deep Packet Inspection (DPI), and implement aggressive rate-limiting on your edge routers. You assume the inside of your network is safe, and the outside is hostile.

In a decentralized Spot Market consisting of 100 million anonymous laptops, gaming PCs, and IoT devices, there is no perimeter. The network is entirely composed of hostile, untrusted actors. If there is no firewall, how do you prevent a malicious actor from launching a DDoS attack from *inside* the swarm?

Marabunta implements a **Behavioral Immune System**, powered by cryptographic **Elo Reputation**.

### 12.1 The Janis & Jim Analysis: The Sybil Vulnerability

Jim, the VP of Engineering, brought a new threat model to the War Room. 

"Janis," Jim said, "I've been looking at the Kademlia DHT. It's permissionless. Anyone can generate a `bls12_381` keypair and join the swarm as a Dust node."

Janis nodded. "Yes. That is the definition of a decentralized Spot Market."

"Then we have a fatal vulnerability," Jim argued. "It's a classic Sybil attack. What stops a malicious state-actor from renting a massive server farm in St. Petersburg and spinning up 100,000 fake Marabunta nodes? They generate 100,000 keypairs, flood our Kademlia routing tables, and launch a massive Distributed Denial of Service (DDoS) attack from the inside. They drown the Boulders in fake Hashgraph gossip, and the consensus layer collapses."

Janis didn't look concerned. "You are assuming that just because a node joins the network, the network actually listens to it."

"If it's in the routing table, we have to listen to it," Jim said.

"No," Janis corrected. "We let them join. We let them sit in the routing table. But we don't let them speak. In Marabunta, you have to earn your bandwidth."

### 12.2 The Mathematics of Elo Reputation

Janis explained that every single node in the swarm maintains a local, independent ledger of the behavior of its peers. There is no global "reputation score" stored on a central server, because a central server could be hacked to artificially boost a malicious node. 

The scoring system is based on the **Elo rating system**, originally designed to calculate the relative skill levels of chess players. 

"Every time a new node joins the network, its default Elo score is 1000," Janis explained. 

*   **Positive Reinforcement (Earning Elo):** If Node A routes a Kademlia payload to Node B, and Node B successfully forwards it to the target without dropping the TCP connection, Node A increases Node B's Elo score. If Node B executes a WASM payload and returns a Zero-Knowledge Proof (ZKP) that mathematically verifies, its Elo score increases significantly.
*   **Negative Slashing (Losing Elo):** If Node B drops a TCP connection, returns a malformed Hashgraph event, or provides an invalid ZKP, Node A mathematically slashes Node B's Elo score.

To demonstrate the mathematical boundaries, consider this reference implementation mapping the Elo adjustment logic:

```rust
pub struct PeerReputation {
    pub elo_score: f64,
    pub total_interactions: u64,
    pub successful_executions: u64,
}

impl PeerReputation {
    /// Adjust the Elo score based on the outcome of a WASM execution or routing event.
    /// Uses a dynamic K-factor that stabilizes as the node proves its reliability over time.
    pub fn update_elo(&mut self, outcome_success: bool, expected_probability: f64) {
        self.total_interactions += 1;
        
        // High volatility for new nodes, rigid stability for veterans.
        let k_factor = if self.total_interactions < 100 { 32.0 } else { 10.0 };
        
        let actual_score = if outcome_success { 
            self.successful_executions += 1;
            1.0 
        } else { 
            0.0 
        };

        // Standard Elo adjustment formula
        let delta = k_factor * (actual_score - expected_probability);
        self.elo_score = (self.elo_score + delta).clamp(0.0, 3000.0);
    }
}
```

"Notice the `clamp`," Janis pointed out. "A node's score can never drop below 0, and it can never exceed 3000. It is a strictly bounded mathematical reality."

### 12.3 The TrafficShaper Chokehold

Jim looked at the Rust logic. "Okay, so the North Korean server farm spins up 100,000 nodes. They all start with an Elo of 1000. They try to flood the Heavy Boulders in Moldova with fake Hashgraph gossip. How does the Elo score actually stop the DDoS attack?"

Janis wrote the **Bandwidth Allocation Formula** on the whiteboard.

$$ \text{Allowed Packets/Sec} = \left( \frac{\text{Peer Elo}}{1000} \right)^3 \times \text{Baseline Limit} $$

"The Elo score doesn't just sit in a database," Janis said. "It is directly hardwired into the `TrafficShaper` at the TCP/QUIC connection layer."

Janis pointed to the formula. "A new node with an Elo of 1000 is allowed to send exactly 1x the baseline limit—maybe 10 packets per second. If that node tries to send 11 packets, the `TrafficShaper` drops the 11th packet at the socket layer. It never even reaches the application logic."

"But a Heavy Boulder," Janis continued, "that has been reliably executing WASM payloads and weaving the Hashgraph for six months, might have an Elo of 2500."

Janis did the math: $(2.5)^3 \approx 15.6$.

"The Boulder is permitted to send 15 times more traffic than the new node. The swarm biologically prioritizes traffic from proven, high-reputation nodes."

Jim nodded slowly. "So the 100,000 North Korean Sybil nodes..."

"They hit the `TrafficShaper` chokehold," Janis finished. "Because they haven't spent months executing real workloads and building their Elo, they are throttled to 10 packets a second. They can't flood the network. To launch a successful DDoS attack, the adversary would have to run 100,000 nodes legitimately for six months, paying real electricity costs to build up their Elo scores to a dangerous level."

"And the moment they launch the attack..." Jim realized.

"The moment they launch the attack," Janis smiled, "they start dropping invalid packets. The honest nodes instantly slash their Elo scores back to 1000. Six months of thermodynamic investment is vaporized in 400 milliseconds."

### 12.4 The 500 Elo Guillotine (Amputation)

The immune system has one final, brutal defense mechanism. 

If a node consistently lies, drops connections, or attempts to spoof Zone Certificates (Chapter 11), its Elo score drops below 500. 

When a node hits this threshold, the `TrafficShaper` does not just drop its packets. It issues a `TCP RST` (Reset) packet, abruptly terminating the connection. The honest node then deletes the malicious node's `bls12_381` public key from its Kademlia routing table entirely. 

The malicious node is cryptographically amputated from the organism. It is mathematically blinded and isolated, screaming into a void where no honest node will ever route its packets again.

This is the Behavioral Immune System. It guarantees that the decentralized supercomputer can safely ingest 100 million anonymous nodes, because the network automatically quarantines the infection before the pathogen can breach the consensus layer.

[Continue to Chapter 13: Court of Arbitration & ZKP Slashing](./04-arbitration-court.md)
