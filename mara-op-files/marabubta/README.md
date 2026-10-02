# Marabunta

A Decentralized, Peer-to-Peer Compute Swarm.

Marabunta allows you to coordinate computational tasks across a globally distributed network of unreliable nodes. It provides high-performance execution in hardened sandboxes, a self-healing distributed file system, and an automated infrastructure transition engine.

---

## 1. Setup and Installation

### Prerequisites
*   **Rust:** Latest stable toolchain.
*   **Linux:** Required for hardened sandboxing and process monitoring features.
*   **Python 3.10+:** Required for running simulation scripts.

### Compiling from Source
Build the entire workspace (Node, CLI, and SDK) in release mode:
```bash
cargo build --release
```
The primary binaries will be located in `./target/release/`:
*   `marabunta-swarm`: The background worker node.
*   `marabunta-cli`: The primary orchestrator and administrative tool.

---

## 2. Launching the Swarm

### Kickstart a Local Development Swarm
To rapidly spawn a virtualized 5-node swarm on your local machine for testing and script development:
```bash
./target/release/marabunta-cli dev
```
This command automatically generates ephemeral identities, assigns ports, and wires the Kademlia DHT bootstrap pointers.

### Running a Production Node
To join a global swarm or run a dedicated node, you must configure a `config.toml`:
1.  Copy the example: `cp config/marabunta.example.toml config.toml`
2.  Edit the `listen_addr` and add `bootstrap_servers` IPs.
3.  Launch the node:
```bash
./target/release/marabunta-swarm --config config.toml
```

---

## 3. Submitting Jobs (User Action Guide)

### Level 1: Simple Python Simulation (Markov Chain)
You can submit arbitrary Python scripts without writing any boilerplate.

1.  **Write your script** (`markov.py`):
    ```python
    import random
    states = ["Ready", "Computing", "Idle"]
    current = "Idle"
    for i in range(10):
        current = random.choice(states)
        print(f"Step {i}: State shifted to {current}")
    ```

2.  **Submit to the Swarm**:
    ```bash
    ./target/release/marabunta-cli di-lo-co run \
        --name "markov-v1" \
        --script-file ./markov.py
    ```

### Level 2: Cooperative Parameter Sweeps
For massive parallel execution, use the `di-lo-co` pipeline to run shards of a script across many nodes simultaneously.

```bash
./target/release/marabunta-cli di-lo-co run \
    --name "global-climate-sweep" \
    --script-file ./simulation.py \
    --sync-interval 100 \
    --dataset-shard-uri "https://storage.provider.com/data-part-1.bin"
```
*   `--sync-interval`: Number of local steps before the node gossips its state to the swarm.
*   `--dataset-shard-uri`: Unique data source for this specific execution shard.

### Level 3: Native High-Performance WASM
Compile your Rust or C++ code to `wasm32-wasi` and run it directly in the hardened sandbox:
```bash
./target/release/marabunta-cli execute --wasm-file ./engine.wasm --input ./input.bin
```

---

## 4. The Great Assimilation (IaC Conversion)

Marabunta can automatically translate centralized infrastructure files into decentralized Swarm Job definitions.

### Converting Kubernetes Manifests
Extracts `nodeSelector` and `resource limits` into Kademlia routing constraints:
```bash
./target/release/marabunta-cli assimilate --file deployment.yaml --out swarm-k8s.json
```

### Converting Docker Compose
Maps container images to individual WASM sandbox tasks:
```bash
./target/release/marabunta-cli assimilate --file docker-compose.yml --out swarm-docker.json
```

### Converting Terraform
Maps `aws_instance` requests into Swarm spot market bidding logic:
```bash
./target/release/marabunta-cli assimilate --file main.tf --out swarm-tf.json
```

---

## 5. Storage Operations (Planetary FS)

Marabunta manages files via a "Planetary Storage" engine. Any file larger than **1GB** is automatically shredded into 14 Reed-Solomon shards and distributed across the DHT.

*   **Posing a File:**
    ```bash
    ./target/release/marabunta-cli storage upload --file ./massive-dataset.bin
    ```
*   **Retrieving a File:**
    The system handles this transparently. If a local file is corrupted or missing shards, the node concurrently pulls shards from its XOR-metric peers and reconstructs the file in memory.
*   **Checking Health:**
    Query the local node to see the status of sharded fragments:
    ```bash
    ./target/release/marabunta-cli storage status --hash <BLOB_HASH>
    ```

---

## 6. Dashboards and War Games

The node hosts a local administrative suite accessible via your web browser when a node is running.

### Ports and URLs
*   **Node API/Dashboard:** `http://localhost:8080`
*   **Planetary Redis Gateway:** `localhost:6379`
*   **BFT Ledger DB (PgWire):** `localhost:5432`

### The UI Suite
1.  **Management UI (`/management-ui/index.html`):** The master control panel. Use this to trigger **War Games** simulations:
    *   **Kill Node:** Manually trigger a `SIGKILL` on local Boulders to verify BFT Hashgraph consensus recovery.
    *   **Partition Mesh:** Simulate a transatlantic fiber cut to watch the Anti-Entropy daemon re-shard the database.
2.  **Cluster Telemetry (`/dashboard/index.html`):** Real-time geographic heatmap of the active swarm.
3.  **Job Logs (`/job-ui/index.html`):** View standard output and fuel consumption for every active WASM task.

---

## 7. Economics (Mercantile Exchange)

Nodes participate in the **Marabunta Mercantile Exchange (MMX)** to earn or spend compute credits.

*   **Setting Your Ask Price:**
    Configure your node's minimum price for executing jobs in `config.toml`:
    ```toml
    [mmx]
    min_bid_price_cents = 5.0
    ```
*   **Checking Reputation:**
    Your "Trust Score" is based on BFT consensus participation and valid ZK-STARK proofs:
    ```bash
    ./target/release/marabunta-cli node info
    ```

---

## 8. CLI Command Reference

| Category | Command | Action |
| :--- | :--- | :--- |
| **Swarm** | `mrb dev` | Spawn local test network |
| **Jobs** | `mrb di-lo-co run` | Submit Python/Parameter sweeps |
| **Admin** | `mrb zone create` | Establish new authority zone |
| **Audit** | `mrb audit list` | View immutable execution receipts |
| **Infra** | `mrb assimilate` | Convert K8s/Docker/Terraform |
| **Identity**| `mrb sign` | Cryptographically sign a payload |
