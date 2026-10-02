# ANNEX B: Planetary Asymmetric Data Acquisition

B.0 The Sovereign Scraping Imperative

Modern scientific and economic models are governed by the volume and quality of their data. To perform massive-scale simulations or market analysis, institutions require continuous, unbounded access to public and specialized data sources across the global internet.

However, the internet is not open. It is actively defended.

If a sovereign research institute attempts to scrape 50 terabytes of data from a protected domain using a centralized IP block (e.g., an AWS datacenter in us-east-1 or an Azure cluster in Europe), they will be immediately neutralized.

Modern Web Application Firewalls (WAFs) and centralized reverse-proxies operate on autonomous, behavioral rate-limiting. They detect the high-velocity requests originating from a unified IP block, categorize the traffic as a "botnet," and issue IP-level bans or endless CAPTCHA challenges, severing the data supply.

Marabunta bypasses these defenses natively. It utilizes the Swarm to execute Planetary Asymmetric Data Acquisition.

B.1 The Ephemeral Edge Advantage

The fundamental architecture of Marabunta is a 100-million node peer-to-peer mesh. Crucially, the vast majority of these nodes (the "Ephemeral" cohort) do not reside in recognizable datacenters. They are consumer laptops, IoT devices, and smartphones operating on residential IP addresses (e.g., Comcast, Vodafone, China Telecom) across 190 countries.

This topology is indistinguishable from organic, human web traffic.

When an Administrator submits an MCL (Marabunta Command Language) payload requesting a massive data extraction operation, the Gateway Assimilator shreds the target URLs into millions of micro-tasks.

The Implementation: src/swarm/detection/webserver.rs

The Edge nodes execute the HTTP requests directly from their local network interfaces. Because the requests originate from residential ISP IP addresses, they mathematically bypass datacenter IP blocklists.

// Execution occurs locally on the Edge Node via the WebServerDetector use std::collections::HashMap; use async_trait::async_trait;

pub struct WebServerDetector { pub host: String, pub port: u16, }

impl WebServerDetector { /// Executes a distributed web request from a residential edge node. pub async fn execute_extraction(&self, url: &str) -> Result<Vec<u8>, &'static str> { let response = crate::swarm::detection::http_get_raw(&self.host, self.port).await .map_err(|_| "Target unreachable from local residential node")?;

// The target domain serves the data, unaware that the residential // request is part of a coordinated, global swarm. Ok(response) } }

The Ethics of Extration: Consent and Autonomous Throttling

Routing heavy, asymmetric data extraction payloads through the residential IP addresses of unwitting consumers (the "Ephemeral" nodes) introduces severe legal and ethical liabilities. If a target domain detects the scraping pattern and blacklists the IP address, a consumer in Praia Grande or Mumbai could find their home internet banned from accessing standard web services.

Marabunta prevents this through Autonomous Legal Throttling ( src/swarm/detection/ webserver.rs ).

1. Opt-In Capability Matrix: A consumer node must explicitly declare its willingness to participate in external web-scraping within its local Marabunta configuration file ( config_consent.rs ). It is not a default capability. 
2. Fractional Saturation: The Swarm routing layer mathematically guarantees that no single residential IP address executes more than 3 HTTP requests to the same target domain within a 24-hour window. 
3. The Law of Large Numbers: Because the Swarm possesses 100 million or more (e.g., 15 billion) potential egress IPs, it can scrape a 1-million page target domain without any single node firing more than a single HTTP GET request.

This renders the scraping traffic functionally indistinguishable from organic human browsing. The target domain's WAF (Web Application Firewall) perceives 1 million unique, geographically distributed human visitors reading one page each. The residential IP is never flagged as a botnet, preserving the integrity of the consumer's connection.

B.2 Bypassing Geographic Subpoenas (Geo-Spoofing)

Certain nation-states or corporate entities employ strict geoblocking. If a target domain is hosted in China and blocks all IP addresses originating from the United States, an American research institution cannot acquire the data using domestic infrastructure.

Marabunta solves this mathematically.

The Administrator writes an MCL manifest with a strict Jurisdiction Fence: allowed_zones: ["CN", "HK", "MO"] .

The Swarm routes the extraction WASM payload exclusively to nodes that mathematically prove their physical location within the target jurisdiction via RTT Triangulation (as detailed in Volume 05).

The Chinese target domain receives HTTP requests originating from organic, residential Chinese IP addresses (e.g., a laptop in Shenzhen). The domain serves the data. The Shenzhen node processes the HTML, extracts the required payload, encrypts it via the Sphinx protocol, and routes it back through the Phantom Overlay to the United States.

B.3 Kinetic DDoS Deflection and the "Fluid CDN"

The inverse is also true. Marabunta provides ultimate resilience for content hosted within the Swarm, effectively serving as an Un-Censorable Content Delivery Network (CDN) that bypasses centralized vendors.

If a sovereign government issues a subpoena to a centralized DNS provider to seize a domain, or if a botnet launches a 500Gbps DDoS attack against a specific server, traditional architectures collapse.

In Marabunta, static assets are shredded into Reed-Solomon chunks and injected into the Kademlia DHT. Users access the content via its cryptographic hash ( blake3 ) over the Sphinx UDP overlay.

Stigmergic Evaporation & Load Dissipation

How does a network without a center survive a DDoS attack?

If a botnet floods the Swarm with requests for a specific cryptographic hash, the Wild Dogs daemon detects the artificial latency spike in that specific XOR bucket. The Kademlia routing table dynamically shatters.

The Crocodile and Plumtree daemons clone the requested data chunks to 100,000 surrounding nodes in milliseconds to absorb the impact. The harder the botnet hits the target hash, the more the Swarm replicates the target to dissipate the thermal load.

The attack energy is literally weaponized to strengthen the network's availability.

