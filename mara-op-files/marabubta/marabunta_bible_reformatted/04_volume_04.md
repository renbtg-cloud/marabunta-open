# VOLUME 04: THE PHANTOM OVERLAY (UNIDENTIFIABILITY & ONION ROUTING)

4.0 The Sovereign Egress Diode

Traditional distributed systems rely on the IP-routing layer for data distribution, inherently exposing the network's topology to deep packet inspection (DPI) and traffic analysis by intermediate ISPs or hostile intelligence agencies. Even with TLS encryption, metadata—such as the origin, destination, and timing of packet bursts—remains visible, allowing adversaries to map computational clusters and infer dataset sensitivity.

Marabunta neutralizes traffic analysis by natively embedding the Phantom Overlay, a compact, cryptographic onion-routing protocol specifically optimized for high-throughput computational payloads.

When a regulated institution (e.g., a central bank or genomic lab) deploys a Marabunta node, it functions as an Asymmetric Data Diode. The node binds no listening ports to the public internet, eliminating the possibility of external port-scanning or remote exploitation. It exclusively initiates outbound UDP streams (QUIC) into the global mesh, masked by continuous cover traffic.

4.1 Sphinx: Bit-Level Unidentifiability

The cornerstone of the Phantom Overlay is the Sphinx packet format. Sphinx provides forward secrecy and bit-level unidentifiability, ensuring that as a packet traverses multiple relay nodes, its bitwise representation changes completely at each hop.

The Implementation: src/swarm/crypto/sphinx.rs

The following extraction demonstrates the multi-hop nested encryption using x25519-dalek for ephemeral key exchange and ChaCha20 for high-speed stream ciphering.

// Sphinx Packet Nesting: From Exit to Entry for _ in hops.iter().rev() {

let mut key = [0u8; 32]; rng.fill_bytes(&mut key); // Deriving ephemeral hop key

let mut nonce = [0u8; 12]; rng.fill_bytes(&mut nonce);

// Encapsulate payload layer using ChaCha20 let mut cipher = ChaCha20::new(&key.into(), &nonce.into()); cipher.apply_keystream(&mut current_payload);

// Prepend nonce for the subsequent hop's decryption let mut next_p = nonce.to_vec(); next_p.extend_from_slice(&current_payload); current_payload = next_p; }

Architectural Analysis: Metadata Eradication

1. Ephemeral Handoff: Notice the use of hops.iter().rev() . Each layer of the onion is encrypted using a unique, ephemeral symmetric key negotiated via X25519 with each intermediate relay. A relay node (e.g., a consumer laptop in Argentina) only possesses the key to peel its specific layer. It can see the address of the next hop, but it cannot see the ultimate destination, the original source, or the contents of the payload. 
2. Fixed Cell Size: Sphinx packets are padded to a fixed size (typically 1300 bytes). This prevents observers from using packet length to distinguish between a tiny command- and-control signal and a massive Cooperative gradient shard.

Egress Diode Backpressure (QUIC Congestion Collapse)

Continuous UDP cover traffic lacks native TCP congestion control. If a Gateway blasts a 10GB tensor over Sphinx UDP to a slow edge node in Argentina, intermediate ISP routers will drop 90% of the packets, crashing the subnet in a self-inflicted DDoS.

Marabunta mitigates this by embedding TCP-Friendly Rate Control (TFRC) and BBR congestion algorithms directly inside the Sphinx QUIC tunnels ( src/phantom/network/ onion.rs ). The Swarm dynamically throttles cryptographic injection rates based on real-time packet-loss telemetry from the DHT, ensuring high-throughput operations gracefully back off before saturating the physical carrier lines.

4.2 The Continuous Cover Traffic Vector

Even with onion routing, the timing of transmissions (e.g., synchronous weight updates every 4 hours in a federated learning run) creates a statistical "cadence" that signals intelligence agencies can detect.

Marabunta neutralizes this through Continuous Cover Traffic.

1. The White Noise Stream: Marabunta nodes generate a constant, uniform-rate stream of UDP packets (fake traffic) between peers in the DHT. 
2. The Payload Injection: When a real computational payload (e.g., a Sphinx-encrypted tensor) needs to be transmitted, it is fragmented and injected seamlessly into this pre- existing white-noise stream. 
3. The Result: To a Tier-1 ISP or an intelligence agency tapping an undersea fiber cable, the Marabunta Swarm appears as a flat, meaningless line of entropy. The origin, destination, and exact start/stop times of a scientific or financial sync are mathematically unidentifiable.

Implementation: src/phantom/network/onion.rs

// Traffic Analysis Resistance: Fixed cell size with random padding pub const CELL_SIZE: usize = 512;

impl OnionRouter { /// Wrap payload in onion layers (one encryption per hop). fn wrap_onion(&self, keys: &[SymmetricKey], payload: &[u8], rng: &mut impl Rng) -> Result<Vec<u8>, OnionError> { let mut data = vec![0u8; 2 + payload.len()]; // Add length prefix and payload...

// Encrypt from exit to guard (outermost layer) for key in keys.iter().rev() { data = key.encrypt(&data, rng); } Ok(data) }

/// Add timing jitter to prevent timing analysis. async fn add_jitter(&self, rng: &mut impl Rng) { let jitter_ms = rng.gen_range(0..=self.config.max_jitter_ms); tokio::time::sleep(Duration::from_millis(jitter_ms)).await; } }

By combining Fixed-Size Cells, Timing Jitter, and Onion Layering, Marabunta provides an execution environment that remains invisible to the physical infrastructure of the internet. The network is not just encrypted; it is topologically anonymous.

4.3 Absolute Infiltration: DNS-over-HTTPS (DoH) Tunneling

While Sphinx UDP routing provides optimal throughput, totalitarian nation-states (e.g., China, Iran) deploy Deep Packet Inspection (DPI) firewalls capable of executing "default-deny" policies on all unrecognized UDP payloads. If a sovereign actor kinetically severs standard port traffic, the Swarm must maintain command-and-control connectivity.

Marabunta natively embeds Fallback 5 (Last Resort): The DNS-over-HTTPS (DoH) Tunneling Transport.

Implementation: src/marabunta/transport/doh.rs

If the marabunta-visor detects that the Sovereign Egress Diode is fully blackholed by an ISP, it autonomously down-shifts the transport protocol.

It encodes the 1300-byte Sphinx computational frames into Base64 strings and fragments them into raw DNS TXT record queries. These queries are routed explicitly through whitelisted, global HTTPS providers (e.g., Google 8.8.8.8 or Cloudflare 1.1.1.1 ).

// DNS TXT limits dictate fragmentation geometry const DNS_TXT_MAX_BYTES: usize = 255; const QUERIES_PER_FRAME: usize = FRAME_SIZE.div_ceil(DNS_TXT_MAX_BYTES);

// Inbound/Outbound Queues structure the Base64 DNS encoded streams pub struct DohTransport { codec: FrameCodec, connected: bool, inbound: VecDeque<Frame>, outbound: VecDeque<String>, }

Architectural Analysis: The Un-Censorable Heartbeat

This transport layer operates at a severely constrained bandwidth (approximately ~1 KB/s effective throughput). It cannot be used to synchronize a 350PB Cooperative tensor gradient.

However, it is mathematically sufficient to sustain the minimum viable cryptographic heartbeat of the Swarm. A node trapped behind a national firewall can use the DoH transport to slowly bleed out execution receipts, authenticate MclManifest command definitions, and maintain its Kademlia routing presence.

To block this traffic, the host nation-state would be forced to permanently block all HTTPS requests to global DNS resolvers, effectively disconnecting their entire populace from the modern internet. The Swarm weaponizes the adversary's reliance on fundamental infrastructure to guarantee its own survival.

4.4 Stigmergic Delay-Tolerant Networking (DTN)

The hyperscaler paradigm assumes constant, low-latency terrestrial fiber connectivity. If an AWS region loses uplink to the internet, all compute halts until the physical connection is repaired.

Marabunta assumes a radically hostile connectivity environment. The Swarm is engineered to operate across deep-space telemetry links, air-gapped secure facilities, and highly volatile intermittent connections (e.g., ships at sea or disconnected IoT edge devices).

Marabunta natively embeds a Stigmergic Delay-Tolerant Networking (DTN) router.

Implementation: src/swarm/transport/stigmergic_dtn.rs

The DTN router mathematically decouples the Kademlia routing plane from the physical transport layer. It operates on a "store-and-forward" cryptographic caching model.

// The DTN Router handles massive temporal partitions, bridging air- gapped or deep-space environments. pub struct DtnRouter { storage: Arc<DtnStorage>, /// Pending frames waiting for physical connectivity to a target or intermediate relay. pending_frames: DashMap<NodeId, VecDeque<StoredFrame>>, }

impl DtnRouter { /// Commits a cryptographic frame to persistent NVMe storage when the target /// is physically unreachable (e.g., satellite occultation or physical air-gap). pub async fn store_for_forwarding(&self, target: NodeId, frame: Frame) -> Result<(), DtnError> { let stored_frame = StoredFrame {

frame, stored_at: Instant::now(), expires_at: Instant::now() + self.config.max_storage_time, retries: 0, };

self.pending_frames.entry(target).or_default().push_back(stored_frame); // Persist to local BlobStore to survive hard reboots during the partition self.storage.persist_frame(target, &stored_frame).await?;

Ok(()) } }

Architectural Analysis: Asynchronous Physics

If an edge node (e.g., a drone) enters an area with zero connectivity for two weeks, it does not discard computational payloads. It executes the WASM logic, generates the STARK receipts, and routes the packets to the local DtnRouter .

The node writes the encrypted Sphinx packets to physical NVMe storage. When the node eventually detects an intermittent connection (e.g., passing within range of a LEO satellite or establishing a brief STUN connection via a 3G tower), the DtnRouter autonomously flushes the pending_frames queue into the Kademlia DHT.

The Swarm relies on Epidemic Stigmergy. The packets are passed from node to node, held in storage during outages, and forwarded when links are re-established. The CRDT IsomorphicStateRing is mathematically indifferent to time. It seamlessly merges a tensor gradient computed 14 days ago on an isolated drone with the live global model, neutralizing the hyperscaler assumption of constant connectivity.

