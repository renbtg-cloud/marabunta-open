# The Physics of the Swarm: Revisiting the Basics (Part 02)

Following the successful implementation of the Storage TTL, Capitalist GC, and Ingress PoW, a second-pass audit of the core Marabunta physics engine was conducted. The goal: identify any remaining mechanical gaps that would prevent a 10-million-node swarm from achieving stable equilibrium.

## 1. The Purgatory Black Hole (Missing Atonement Vector)
*   **Current State:** When a node commits fraud, the Ledger issues a `SlashingDirective`. The node goes into Purgatory. To escape, it must complete 1,000 *unpaid* chunks.
*   **The Gap:** If the node is in Purgatory, its `ask_price` is forced to `0`. However, the Kademlia Orderbook explicitly drops bids if `budget_valid == false`. If an Enterprise submits a high-paying job, the Orchestrator will never award chunks to a 0-bidding node because the pricing logic is mismatched. Furthermore, if there are no `verify_mode == None` jobs available, the Purgatory node cannot complete its sentence.
*   **The Fix:** Implement a dedicated "Community Service" queue. The network must artificially generate low-priority, high-compute validation jobs (or default to Charity Swarms) explicitly designed to absorb Purgatory compute power.

## 2. The Floating ZKP Dilemma (Orphaned Audits)
*   **Current State:** When an edge node finishes math, it puts its ZKP in the `unverified_zkps` queue. The next node asking for work is forced to audit it (The 1:1 Tollbooth).
*   **The Gap:** What happens if the network is overwhelmingly idle? If there are 1,000 ZKPs waiting, but zero nodes are asking for new paid work (because there are no new jobs), the ZKPs will sit in the queue forever. The original workers will never get paid.
*   **The Fix:** The Orchestrator must inject a "Timeout Trigger" on the `unverified_zkps` queue. If a ZKP sits for >5 minutes, the Orchestrator must proactively push `SwarmMessage::VerifyThisProof` to randomly selected idle nodes, bypassing the Tollbooth waiting game.

## 3. The Membrane Choke (Gateway Asphyxiation)
*   **Current State:** DarkNet nodes behind symmetric NAT use ClearNet Ambassador nodes as relays (`MembraneGateway`).
*   **The Gap:** If 50,000 Brazilian Gamer nodes (DarkNet) all select the exact same Petrobras Ambassador (ClearNet) as their relay, the Ambassador`s inbound Tokio TCP queue will instantly crash under the weight of 50,000 simultaneous `EgressRelay` messages.
*   **The Fix:** The Membrane Gateway requires a Backpressure Load Balancer. If concurrent relay connections exceed `MAX_RELAY_CONNECTIONS`, the Ambassador must return a specific `SwarmMessage::RelayCapacityExceeded` with a randomized backoff delay, forcing the DarkNet nodes to dynamically discover a new Ambassador.
