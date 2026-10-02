# Annex A (v605): Narrative & Edge-Case Code Audit

## Abstract
An audit was conducted against the newly proposed narrative constraints for Annex A: 
1. Cross-swarm Wormholing (Norway to Brazil)
2. Economic spot-market dynamics and Fraud (ZKP faking)
3. A 48-hour Orchestrator Outage (The Great British Disconnect)

While the architecture supports all these events mathematically, two severe implementation bottlenecks were discovered in the code that would cause the network to crash or behave rigidly under these specific narrative conditions.

## Discovered Bottlenecks

### 1. The Rehydrator DDOS (Thundering Herd)
**Location:** `src/swarm/mod.rs` (Settlement Rehydrator Loop)
**The Narrative Trigger:** The British Orchestrator goes offline for 2 days. 100,000 UK Edge nodes accumulate thousands of `ChunkResult` invoices in their local SQLite `dead_letter_ledger`.
**The Code Flaw:** When the Orchestrator comes back online, the `rehydrator` loop in every edge node wakes up and immediately runs a `for` loop over *every single pending invoice*, calling `outbound_tx.try_send()` as fast as the CPU allows. 100,000 nodes simultaneously blasting 5,000 cached messages will instantly overwhelm the Orchestrator`s TCP sockets and OOM the networking stack.
**The Fix:** Implement "Jitter" and "Pagination" (batching) in the Rehydrator loop. Nodes should trickle their backlogged invoices over a randomized 10-minute window, rather than a 1-millisecond blast.

### 2. The Rigid Wormhole (Hardcoded 60/40 Split)
**Location:** `src/swarm/work.rs` (submit_job)
**The Narrative Trigger:** Norway uses the Wormhole to send computation to Brazil.
**The Code Flaw:** Currently, if a `wormhole_treaty` is present, the codebase hardcodes a mathematical split: `let split_idx = (tasks.len() as f64 * 0.6) as usize;`. 60% stays local, 40% goes foreign. This is too rigid. If Norway`s swarm is 100% full, they might want to route 100% of the job to Brazil.
**The Fix:** Add a `wormhole_routing_pct: u8` parameter to `JobSubmissionRequest` and `DiplomaticTreaty` so the submitter (or the Orchestrator) can dynamically dial the exact percentage of the job to export across the Wormhole.
