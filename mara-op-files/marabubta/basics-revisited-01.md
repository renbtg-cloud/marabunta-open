# The Physics of the Swarm: Revisiting the Basics (Part 01)

Before extending the Marabunta architecture into application layers like DiLoCo, the fundamental physics of the network must be watertight. A $10 Billion compute bourse cannot possess structural leaks in its core resource management.

## 1. The Storage Lease (The TTL Problem)
*   **Current State:** The network penalizes nodes (via `SlashingDirective`) if they delete output datasets. 
*   **The Gap:** Infinite storage is physically impossible. There is no mechanism defining *when* an edge node is legally permitted to free up its SSD space.
*   **The Fix:** Implement a cryptographic `StorageTTL` (Time To Live). When the TTL expires, nodes can safely garbage-collect the blobs, and Orchestrators must cease issuing Data Availability Sampling (DAS) challenges.

## 2. Kademlia Pheromone Eviction (The Bourse Floor)
*   **Current State:** To prevent OOM crashes, the edge node`s `KnowledgeStore` hard-caps at 5,000 active jobs (`MAX_KNOWN_JOBS`).
*   **The Gap:** What happens at job 5,001? If eviction is random or FIFO (First-In, First-Out), a node might drop a high-paying enterprise job to make room for a zero-pay charity job.
*   **The Fix:** Implement an Economic Eviction Policy. The 5,000 slots must operate as a hyper-competitive leaderboard, continuously dropping the lowest-paying/lowest-priority jobs to ensure maximum capital efficiency.

## 3. Sybil Resistance on Ingress (The Bootstorm)
*   **Current State:** Nodes caught committing fraud are thrown into Digital Purgatory and must perform thermodynamic PoW to escape.
*   **The Gap:** Generating a brand new `NodeId` is cryptographically free. A malicious actor can bypass Purgatory simply by deleting their keys and generating 100,000 new identities, flooding the Kademlia DHT.
*   **The Fix:** Introduce an Ingress Proof-of-Work (PoW). A 5-second SHA-256 handicap on the initial Kademlia handshake prevents botnets from executing Sybil attacks without burning massive amounts of capital (electricity).
