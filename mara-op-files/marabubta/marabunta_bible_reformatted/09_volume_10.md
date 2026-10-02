# VOLUME 10: RUNTIME EXTENSIBILITY & MCL REFERENCE

10.0 The Extensible Swarm

A planetary-scale infrastructure cannot be a monolithic, immutable black box. To support the diverse requirements of sovereign states, global banks, and edge consumers, Marabunta is architected as a modular substrate.

The system utilizes a high-performance WASM-based Plugin Architecture. This allows administrators to inject custom logic into the marabunta-visor daemon at runtime—without recompiling the binary or restarting the node.

10.1 The Plugin Architecture

Marabunta provides clean, opaque API surfaces for third-party extensions across five critical domains: 
1. Transport Plugins: Define custom networking protocols (e.g., Satellite, LoRa, or proprietary radio links). 
2. Storage Plugins: Integrate new backend persistence layers (e.g., IPFS, Arweave, or localized NVMe arrays). 
3. PubSub Plugins: Inject custom telemetry brokers for real-time monitoring. 
4. Scatter/Gather Plugins: Define novel MapReduce partitioning strategies. 
5. Migration Plugins: Control how WASM memory states are transferred between nodes during preemption.

Implementation: src/plugin/mod.rs

The PluginHost orchestrator manages the full lifecycle of these modules: spawning isolated processes, establishing secure data channels, and monitoring health.

pub struct PluginHost { /// Registry of all active and standby plugins. registry: Arc<PluginRegistry>, /// Lifecycle state machine for each plugin process. lifecycles: RwLock<HashMap<PluginId, PluginLifecycle>>, }

impl PluginHost {

/// Dynamically loads a signed WASM plugin into the Swarm. pub async fn load_plugin(&self, binary: Vec<u8>, config: PluginConfig) -> Result<(), PluginError> { // 
1. Verify the cryptographic signature of the plugin. // 
2. Instantiate the isolated WASM sandbox for the plugin. // 
3. Register the plugin's exported capabilities into the Swarm's trait map. Ok(()) } }

This extensibility ensures that Marabunta remains future-proof. If a superior encryption algorithm or a more efficient transport protocol is developed, it can be deployed epidemically across the Swarm as a plugin.

The Plugin Security Model (Capability-Based Isolation)

Hot-swapping executable plugins at runtime introduces severe vector risks. A malicious or poorly written storage plugin could attempt a directory traversal attack ( ../../../etc/ shadow ) on the host edge node.

Marabunta secures the runtime via WASI-CAP (Capability-Based Security). Plugins are not granted blanket host access. When a transport plugin is initialized, the PluginHost binds it to an exclusive, randomized port matrix. When a storage plugin is loaded, it is passed a virtualized, chroot-jailed block device descriptor. The plugin physically cannot address memory or filesystem paths outside its explicitly granted capabilities.

10.2 Marabunta Command Language (MCL)

To interact with this modular infrastructure, developers and DevOps engineers utilize the Marabunta Command Language (MCL).

MCL is a formal, deterministic markup (typically expressed in YAML or JSON) that codifies the physical, economic, and geopolitical requirements of a computational workload. It serves as the "Instruction Manual" for the biological routing layer.

The Formal Specification: JobSubmissionRequest

The following is the high-level schema of an MCL manifest, demonstrating the precision with which a user can define their execution physics.

pub struct MclManifest { pub name: String, pub script_type: ScriptType, // e.g., "Python", "Rust", "Shell", "Cooperative" pub script: String, pub input_blobs: Vec<BlobRef>, pub chunk_strategy: ChunkStrategy, // e.g., "MapReduce", "Single", "Fractal" pub requirements: JobRequirements, pub energy_budget: Option<EnergyBudget>, pub priority: u8, // 0-255 }

10.3 The JobRequirements Block

The requirements block allows for granular hardware and geographic targeting: * min_vram_gb : Minimum GPU memory required. * require_tee : Boolean flag for Intel SGX/AMD SEV enforcement. * allowed_zones : A list of ISO country codes (e.g., ["US", "CH"] ) enforced via PoL RTT triangulation. * max_rtt_ms : Maximum permissible network latency for the aggregation quorum.

10.4 The EnergyBudget and MMX Bidding

MCL introduces Thermodynamic Bidding. Instead of a fixed cost, the user defines their maximum spend in MMX tokens and their bidding_strategy . * Urgent: Bids at the 90th percentile of the current spot market to ensure immediate preemption of other workloads. * Lazy: Bids only when energy prices go negative (e.g., during solar peaks in Texas), ensuring the lowest possible execution cost.

MCL is not merely a configuration format; it is a mathematical contract. Once signed and submitted to a Gateway Assimilator, the MCL manifest is immutable, providing the ZK- STARK pipeline with the baseline constraints required to verify the integrity of the result.

