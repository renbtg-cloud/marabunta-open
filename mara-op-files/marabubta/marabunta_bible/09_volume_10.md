# VOLUME 10: RUNTIME EXTENSIBILITY & MCL REFERENCE

     10.0 The Extensible Swarm

     A planetary-scale infrastructure cannot be a monolithic, immutable black box. To support the
     diverse requirements of sovereign states, global banks, and edge consumers, Marabunta is
     architected as a modular substrate.

     The system utilizes a high-performance WASM-based Plugin Architecture. This allows
     administrators to inject custom logic into the marabunta-visor daemon at runtime—without
     recompiling the binary or restarting the node.




     10.1 The Plugin Architecture

     Marabunta provides clean, opaque API surfaces for third-party extensions across five critical
     domains: 1. Transport Plugins: Define custom networking protocols (e.g., Satellite, LoRa, or
     proprietary radio links). 2. Storage Plugins: Integrate new backend persistence layers (e.g.,
     IPFS, Arweave, or localized NVMe arrays). 3. PubSub Plugins: Inject custom telemetry
     brokers for real-time monitoring. 4. Scatter/Gather Plugins: Define novel MapReduce
     partitioning strategies. 5. Migration Plugins: Control how WASM memory states are
     transferred between nodes during preemption.


     Implementation: src/plugin/mod.rs

     The PluginHost orchestrator manages the full lifecycle of these modules: spawning isolated
     processes, establishing secure data channels, and monitoring health.


        pub struct PluginHost {
             /// Registry of all active and standby plugins.
             registry: Arc<PluginRegistry>,
             /// Lifecycle state machine for each plugin process.
             lifecycles: RwLock<HashMap<PluginId, PluginLifecycle>>,
        }


        impl PluginHost {



             /// Dynamically loads a signed WASM plugin into the Swarm.
             pub async fn load_plugin(&self, binary: Vec<u8>, config:
        PluginConfig) -> Result<(), PluginError> {
                  // 1. Verify the cryptographic signature of the plugin.
                  // 2. Instantiate the isolated WASM sandbox for the plugin.
                  // 3. Register the plugin's exported capabilities into the
        Swarm's trait map.
                  Ok(())
             }
        }



     This extensibility ensures that Marabunta remains future-proof. If a superior encryption
     algorithm or a more efficient transport protocol is developed, it can be deployed epidemically
     across the Swarm as a plugin.



     The Plugin Security Model (Capability-Based Isolation)

     Hot-swapping executable plugins at runtime introduces severe vector risks. A malicious or
     poorly written storage plugin could attempt a directory traversal attack ( ../../../etc/
     shadow ) on the host edge node.


     Marabunta secures the runtime via WASI-CAP (Capability-Based Security). Plugins are not
     granted blanket host access. When a transport plugin is initialized, the PluginHost binds it to
     an exclusive, randomized port matrix. When a storage plugin is loaded, it is passed a
     virtualized, chroot-jailed block device descriptor. The plugin physically cannot address
     memory or filesystem paths outside its explicitly granted capabilities.



     10.2 Marabunta Command Language (MCL)

     To interact with this modular infrastructure, developers and DevOps engineers utilize the
     Marabunta Command Language (MCL).

     MCL is a formal, deterministic markup (typically expressed in YAML or JSON) that codifies
     the physical, economic, and geopolitical requirements of a computational workload. It serves as
     the "Instruction Manual" for the biological routing layer.


     The Formal Specification: JobSubmissionRequest

     The following is the high-level schema of an MCL manifest, demonstrating the precision with
     which a user can define their execution physics.




        pub struct MclManifest {
             pub name: String,
             pub script_type: ScriptType, // e.g., "Python", "Rust", "Shell",
        "Cooperative"
             pub script: String,
             pub input_blobs: Vec<BlobRef>,
             pub chunk_strategy: ChunkStrategy, // e.g., "MapReduce",
        "Single", "Fractal"
             pub requirements: JobRequirements,
             pub energy_budget: Option<EnergyBudget>,
             pub priority: u8, // 0-255
        }



     10.3 The JobRequirements Block

     The requirements block allows for granular hardware and geographic targeting: *
      min_vram_gb : Minimum GPU memory required. * require_tee : Boolean flag for Intel
     SGX/AMD SEV enforcement. * allowed_zones : A list of ISO country codes (e.g., ["US",
     "CH"] ) enforced via PoL RTT triangulation. * max_rtt_ms : Maximum permissible network
     latency for the aggregation quorum.


     10.5 Decentralized Infrastructure Management (The Cache Warming Trap)

     Marabunta transcends transient computation to become a declarative Infrastructure-as-Code
     (IaC) substrate. Because the swarm treats storage as an economic commodity, administrators
     can proactively mold the physical state of the global fleet using "Cache Warming" primitives.

     The mechanic utilizes the Storage TTL and Capitalist Garbage Collection physics as a
     deployment engine. If an enterprise anticipates a massive surge in demand for a specific
     runtime (e.g., JRE 17, a 12GB PyTorch environment, or a proprietary C++ library), they do not
     wait for the day of execution. They submit an Infrastructure Provisioning Job.

     The Provisioning Job consists of:
     1. A "dummy" script (e.g., a 1ms liveness check).
     2. A massive dependency list (The required environment Blobs).
     3. A high-value Economic Lease (e.g., a 720-hour Storage TTL).

     The swarm's economic incentives force target nodes to download the heavy environments and
     lock them to their local SSDs to collect the storage premium. By the time the actual
     computation is submitted, the global fleet is already pre-configured, resulting in near-zero
     network latency and instantaneous "Cold Start" times.

     This primitive serves as the foundation for the upcoming Infrastructure Management System
     (IMS). In this evolved state, Marabunta allows fleet operators to remotely inspect, mutate, and
     purge installed software stacks across authorized edge topologies, turning 10 million untrusted
     PCs into a single, manageable, and highly liquid corporate data center.




