# Thermodynamic Civic Duty: The 1:1 Verification Tollbooth

## The Verification Deficit Problem
In a hyper-capitalist Spot Market, nodes are financially incentivized to spend 100% of their CPU cycles on paid computations. Zero-Knowledge Proofs (ZKPs) require milliseconds of CPU time to verify. If verification is uncompensated (or under-compensated), rational nodes will refuse to act as Arbiters. If no one arbitrates, the Ledger cannot safely mint tokens, and the economy halts.

## The Solution: Forced 1:1 Matching
Marabunta eliminates the "Tragedy of the Commons" by implementing a **1:1 Civic Duty Ratio**. 

Before a node can pull a batch of paid chunks (`RequestWorkBatch`), the Orchestrator evaluates the global `unverified_zkp` queue. If the queue is populated, the Orchestrator refuses to assign paid work. Instead, it forcefully assigns a ZKP verification task to the requesting node. 

Only after the node successfully verifies the ZKP and submits the `SpotCheckPassed` signal does its Kademlia `civic_duty_score` increment, unlocking its ability to pull profitable chunks.

## Edge Case Analysis: The "100-Node Synchronization Spike"
If a homogeneous swarm of 100 nodes all finish a 2-hour chunk at exactly the same microsecond:
1. **The Flood:** 100 ZKPs hit the Ledger simultaneously.
2. **The Halt:** The Orchestrator freezes all paid chunk distribution.
3. **The Crowd-Audit:** The 100 nodes, desperate for more work, send `RequestWorkBatch`. The Orchestrator intercepts this and hands exactly 1 ZKP to each of the 100 nodes.
4. **The Evaporation:** Because ZKP verification is highly asymmetrical (taking ~50ms vs. the 2-hour chunk generation time), the entire 100-ZKP backlog is processed in parallel and cleared in exactly 50 milliseconds.
5. **The Resumption:** The network instantly unfreezes, payments are minted, and the next batch of 2-hour chunks is distributed.

As the swarm scales to 10 million heterogeneous nodes across different geographic latencies, runtimes, and hardware tiers, the probability of perfect synchronization approaches zero. The verification load becomes a continuous, unnoticeable background hum of 50ms interruptions, ensuring 100% global cryptographic security at effectively zero opportunity cost to the node operators.
