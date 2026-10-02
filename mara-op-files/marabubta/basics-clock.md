# The Physics of Time: The Block Height Architecture

## The Decision
To prevent "Chrono-Squatting" (where malicious nodes alter their BIOS clock to prematurely expire storage contracts or trigger slashing events), Marabunta is transitioning from absolute biological time (`Utc::now()`) to relative cryptographic time (**BFT Block Heights**) for all core economic enforcement.

## The Tradeoffs
*   **Pros:** Absolute immunity to local clock manipulation. Absolute immunity to Eclipse attacks (if isolated, time "stops" rather than proceeding into a false future).
*   **Cons:** High architectural impact. Requires translation layers between human-readable UX (hours/days) and network physics (blocks).
*   **UX Mitigation:** The API remains unchanged. Users submit jobs using `storage_ttl_hours: 72`. The Orchestrator calculates the target Block Height based on the current block + (72 hours / block_time). 

## Implementation Steps

### Step 1: The Network Metronome
*   **Status:** `[DONE]`
*   **Action:** Add an `AtomicU64` named `latest_bft_block` to the `KnowledgeStore`. Update the `BftLedger` event handler in `src/swarm/mod.rs` to increment this counter whenever a valid consensus block is processed. 

### Step 2: The Data Plane Translation
*   **Status:** `[DONE]`
*   **Action:** Modify `BlobMetadata` in `src/swarm/blobstore.rs`. Change `expires_at: Option<DateTime<Utc>>` to `expires_at_block: Option<u64>`.

### Step 3: The API & Orchestrator Bridge
*   **Status:** `[DONE]`
*   **Action:** Update `execute_chunk_inner` in `src/swarm/work.rs`. When a chunk completes and the `BlobMetadata` is written, calculate the target expiration block. 
*   **The Fairness Fix:** Instead of hardcoding 5s/block, the Orchestrator must use the `actual_avg_block_time` (derived from Hashgraph telemetry) to ensure the Worker is not held "hostage" by a slow network. 
*   **Formula:** `current_block + (ttl_hours * 3600 / actual_avg_block_time)`.

### Step 4: The Capitalist Garbage Collector
*   **Status:** `[DONE]`
*   **Action:** Rewrite the `evict_lru_for` economic deletion logic in `src/swarm/blobstore.rs`. Instead of checking `Utc::now()`, it must read `knowledge.latest_bft_block` and evaluate if `expires_at_block <= current_block`.

### Step 5: The Tollbooth Sweeper (Orphaned ZKPs)
*   **Status:** `[DONE]`
*   **Action:** Update the 5-minute ZKP timeout in `src/swarm/sla.rs` and `knowledge.rs`. Instead of using `DateTime`, stamp ZKPs with their submission block height. The sweeper triggers if `current_block - submission_block > 60` (approx 5 minutes).
