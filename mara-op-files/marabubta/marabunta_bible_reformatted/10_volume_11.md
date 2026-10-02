# VOLUME 11: THE PROGRAMMATIC CONTROL PLANE (API & ROUTING MATRIX)

11.0 The Death of the "Fire and Forget" Batch Job

In traditional High-Performance Computing (HPC), a researcher submits a payload to a job scheduler (e.g., Slurm) and waits. If the code contains a syntax error, a missing dependency, or a mathematically unsolvable equation, the system may run for 12 hours on a 10,000-node cluster before failing. The researcher is billed for the wasted compute time.

Marabunta eliminates this inefficiency through the Programmatic Control Plane.

The Gateway Assimilators do not blindly accept payloads. They provide a synchronous, deterministic API that performs localized Dry-Run Compilation and Mathematical Classification before injecting the payload into the planetary Swarm.

11.1 The Switchboard Routing Matrix

When a complex mathematical expression or workflow is submitted to the API, it is routed through the Switchboard.

The Switchboard does not rely on a single execution backend. It utilizes a multi-oracle routing architecture. If an expression is submitted in LaTeX or plain text, the ingest module parses it into an AST, extracts the symbols, and attempts to resolve it using the primary oracle.

If the primary oracle fails (e.g., due to an unsolvable symbolic constraint), the payload does not crash. It gracefully degrades through a mathematically defined 7-Level Failure Hierarchy.

Implementation: src/switchboard/failure.rs

pub enum FailureLevel { /// Total success, all execution backends responded correctly L0, /// Partial success, primary backend succeeded but secondary verification failed

L1, /// Fallback used, primary failed but fallback oracle succeeded L2, /// Degraded result, only a partial computational answer is mathematically available L3, /// Classification only, execution failed but the expression was structurally classified L4, /// Input accepted, parsing failed but the raw payload was stored for asynchronous retry L5, /// Total failure, the expression is mathematically invalid L6, }

impl FailureLevel { /// Returns true if this failure level is considered acceptable. /// L0-L3 are acceptable, L4-L6 require escalation or user intervention. pub fn is_acceptable(&self) -> bool { matches!(self, Self::L0 | Self::L1 | Self::L2 | Self::L3) } }

This matrix guarantees that an enterprise user paying for compute is never billed for an L6 catastrophic failure, because the Switchboard traps and rejects the payload synchronously at the Gateway before it ever hits the MMX spot market.

11.2 The mrb submit Pipeline

The standard interaction model for an enterprise developer is the Marabunta Command Line Interface ( mrb ).

When a developer executes $ mrb submit --manifest aml_job.mcl , the CLI initiates a highly orchestrated sequence with the ApiServer operating on the Gateway Assimilator.

1. Authentication & RBAC: The Gateway validates the developer's WebAuthn PKI token, ensuring they possess the submit_jobs permission within the specific Jurisdiction defined in the MCL manifest. 
2. Synchronous AST Dry-Run: The Gateway compiles the Python/Rust payload into wasm32-wasi . It verifies the memory bounds and ensures all imported host functions are safely stubbed by the MantisJournal . If the code contains an infinite loop

detectable via static analysis, the deployment is rejected with an HTTP 400 Bad Request . 
3. Thermodynamic Escrow Lock: The Gateway evaluates the EnergyBudget block of the MCL manifest. It calculates the projected cost based on the current global MMX spot market pricing and locks the required funds in a cryptographic escrow smart contract. 
4. The Epidemic Handoff: Only after the payload achieves an L0 or L1 Switchboard verification status does the API return an HTTP 201 Created response to the developer. The Gateway then injects the WASM binary and the MCL constraints into the Kademlia DHT.

From the developer's perspective, the API response is instantaneous and familiar. Behind the Gateway, the payload has transitioned into an unstoppable biological virus, propagating across 100 million or more (e.g., 15 billion) nodes via Plumtree gossip.

11.3 Enterprise Integration (Federated Identity & Asynchronous Event Streams)

While the underlying Swarm operates in a zero-trust, cryptographic vacuum, requiring enterprise developers to manually manage ed25519 keypairs to interact with the Gateway Assimilator introduces catastrophic usability friction.

Marabunta bridges this gap by natively supporting legacy enterprise IT frameworks without compromising the cryptographic integrity of the mesh.

Federated Identity (OIDC/SAML)

The Gateway Assimilator is not a silo. It natively integrates with centralized enterprise Identity Providers (IdP) such as Okta, Microsoft Entra ID, or Ping Identity via the AuthLayer module ( src/swarm/auth.rs ).

When a developer executes $ mrb submit --manifest aml_job.mcl , they do not need to provide a raw private key. They authenticate via an OpenID Connect (OIDC) JWT token. The Gateway validates the cryptographic signature of the IdP, maps the developer's corporate group (e.g., JPMorgan_Data_Science_L3 ) to the corresponding Marabunta Jurisdiction permissions, and transparently signs the MCL manifest on their behalf using the Gateway's HSM (Hardware Security Module).

The Swarm remains cryptographically verified, but the enterprise maintains standard SSO (Single Sign-On) lifecycle management over its engineers.

Asynchronous Event Streams (Webhooks)

A 100-million node network does not execute linearly. A massive MapReduce aggregation or a 350PB Cooperative gradient sync may take minutes or hours to clear the Kademlia DHT. If an enterprise CI/CD pipeline or SIEM (Security Information and Event Management) dashboard is forced to continuously poll the Gateway API for status updates, the resulting traffic storm will degrade the Control Plane.

Marabunta solves this via Asynchronous Webhooks ( src/swarm/webhooks.rs ).

An administrator can subscribe a corporate endpoint (e.g., a Splunk ingestor or a PagerDuty listener) to specific Swarm events.

When the Harpy daemon mathematically verifies a Cryptographic Hash trace, or when the IsomorphicStateRing achieves global consensus on a CRDT merger, the Gateway immediately pushes a secure HTTPS POST request containing the event metadata and the cryptographic receipt directly to the subscribed corporate endpoint.

The enterprise is updated instantaneously, without ever polling the network, allowing Marabunta to function as a seamless, event-driven coprocessor for existing IT architectures.

The Dashboard API

Once deployed, the developer does not SSH into an edge node to view logs. The Control Plane API exposes aggregate telemetry while preserving the anonymity of the Phantom Overlay.

# Get the latency heatmap for a specific distributed training run
$ curl http://localhost:8080/api/jobs/wp_aml_syndicate_99/latency

# Filter active nodes by thermodynamic region
$ curl "http://localhost:8080/api/nodes?region=us- east-1&status=online"

The infrastructure endpoints provide full detail on public "Aggregator" nodes, while the "Ephemeral" node telemetry is heavily obfuscated and aggregated to protect the physical locations of consumer hardware participating in the Swarm.

