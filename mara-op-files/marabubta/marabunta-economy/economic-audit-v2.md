# Economic Audit v2: Achieving Complete System Integrity

Following the successful implementation of the 1:1 Civic Duty Tollbooth and the 3-Tier Sovereign Identity, a final code-level audit was conducted to identify "Phantom Logic" and "Mathematical Gaps."

### Apocalyptic Economic Vulnerabilities Discovered

#### 1. The Passive DAS Loophole (Missing Challenge Issuance)
**The Problem:** We implemented the *response* to Data Availability Sampling (DAS), but the Orchestrator has no background task to actually *issue* the challenges. 
**The Risk:** Malicious nodes know that if no one is asking for the bytes, they can delete the 5TB dataset with zero risk of being caught. The "Slashing Directive" we built is a loaded gun with no one to pull the trigger.
**The Fix:** Implement a `SlaMonitor` background loop that periodically selects random `BlobHash`es from the `KnowledgeStore` and fires `ProveBlobAvailability` messages to the storing nodes.

#### 2. The Mocked BFT Ledger (Settlement Amnesia)
**The Problem:** The `SettlementClaim` handler in `src/swarm/mod.rs` currently only logs a success message. It does not physically call `HashgraphEngine::propose_transaction`. 
**The Risk:** MMX tokens are "minted" in logs, but never actually recorded in the Merkle State Trie. The economy has zero persistence. If the node reboots, all "payments" are lost.
**The Fix:** Physically wire the `SettlementClaim` into the `HashgraphEngine`. Every verified ZKP must generate a `DagEvent` that requires a 67% BFT supermajority to settle into the Merkle Trie.

#### 3. The Sybil-Resistance Debt (Missing PoW Validation)
**The Problem:** In `marabunta-cli apologize`, we built a Hashcash loop. However, the Ledger side of `SubmitAtonement` does not actually *verify* that the `pow_nonce` matches the node`s ID at the required difficulty.
**The Risk:** A script kiddie can send a random number as a nonce, and the Ledger will unban them for free.
**The Fix:** Implement a `verify_atonement_pow` function in the Ledger that mathematically confirms the CPU burn before clearing the `InPurgatory` flag.

---

### Implementation Sequence

1. **The DAS Trigger:** `DONE`
2. **The BFT Settlement Bridge:** `DONE`
3. **The Atonement Verifier:** `DONE`
