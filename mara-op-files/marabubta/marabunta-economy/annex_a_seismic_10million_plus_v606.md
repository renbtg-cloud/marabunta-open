# Annex A (v606): Narrative & Edge-Case Code Audit

## Abstract
An audit was conducted against the newly proposed narrative constraints for Annex A, incorporating the critical requirement for strict capacity agreements during Inter-Swarm Wormholing (Norway to Brazil).

## Discovered Bottlenecks

### 1. The Rehydrator DDOS (Thundering Herd)
**Location:** `src/swarm/mod.rs` (Settlement Rehydrator Loop)
**The Narrative Trigger:** The British Orchestrator goes offline for 2 days. 100,000 UK Edge nodes accumulate thousands of `ChunkResult` invoices in their local SQLite `dead_letter_ledger`.
**The Code Flaw:** When the Orchestrator comes back online, the `rehydrator` loop in every edge node wakes up and immediately runs a `for` loop over *every single pending invoice*, calling `outbound_tx.try_send()` as fast as the CPU allows. 100,000 nodes simultaneously blasting 5,000 cached messages will instantly overwhelm the Orchestrator`s TCP sockets and OOM the networking stack.
**The Fix:** Implement "Jitter" and "Pagination" (batching) in the Rehydrator loop. Nodes should trickle their backlogged invoices over a randomized 10-minute window, rather than a 1-millisecond blast.

### 2. The Unregulated Wormhole (The Capacity Breach)
**Location:** `src/marabunta/federation.rs` and `src/swarm/work.rs`
**The Narrative Trigger:** Norway uses the Wormhole to send computation to Brazil, but must mathematically guarantee they do not overwhelm Petrobras`s Kademlia DHT.
**The Code Flaw:** Currently, the Wormhole logic in `submit_job` hardcodes a 40% export split without checking Brazil`s actual availability. If Norway submits a 100-Billion-chunk job, they will blindly dump 40 Billion chunks onto Brazil`s Ambassador node, crashing Petrobras`s ingress router.
**The Fix:** The `DiplomaticTreaty` must enforce the `capacity_cap_pct` (which already exists but is unused for Wormholes). The Orchestrator must query the Federation Manager for the exact number of chunks Brazil has cryptographically agreed to accept *per minute*, and shape the exported Kademlia pheromones to perfectly match that bandwidth.
