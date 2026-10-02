<!-- Marabunta - Licensed under the MIT License.
# Section V: Execution and Autonomous Defense

## Chapter 8: The Fuel-Metered WASM Sandbox

In the previous chapters, we established how the Marabunta Swarm routes data using Kademlia and Plumtree gossip, and how it agrees on the absolute chronological order of events using the BFT Hashgraph. We proved how the 5TB global state is mathematically compressed into a 32-byte Merkle-Patricia Root Hash. 

However, a decentralized supercomputer is useless if it cannot safely execute untrusted code. 

If a quantitative hedge fund submits a proprietary Python trading algorithm, and a genomic research facility submits a C++ fluid dynamics simulation, the swarm must execute those payloads across anonymous hardware located in adversarial nation-states. 

We must execute untrusted code on untrusted silicon. The execution boundary must be absolute.

### 8.1 The Janis & Jim Analysis: The Death of Docker

Jim, the VP of Engineering, reviewed the architecture diagram for the execution nodes. 

"Janis," Jim said, "I don't see the Kubernetes control plane. And where is the Docker daemon?"

"There is no Docker," Janis replied. 

"Janis, every enterprise on Earth uses Docker containers. Why are we reinventing the wheel?"

Janis pulled up a telemetry chart. "Because Docker is thermodynamically and chronologically incompatible with a Spot Market swarm."

Janis pointed to the lifecycle metrics of the 10,000-node simulation. "In a centralized Californian datacenter, you spin up a leased instance, you pull your 500MB Docker image containing a full Ubuntu userland, and that container stays alive for three months. Cold-start latency doesn't matter."

"But in the Swarm," Janis continued, "our nodes are highly volatile. A consumer might close their laptop after 15 minutes. If we assign a Map-Reduce job to that node, and it takes 2 minutes just to pull the massive Docker image across their residential ISP, the node might disconnect before the computation even begins. Furthermore, Docker relies on Linux kernel namespaces (`cgroups`) for isolation. Hypervisor escapes are discovered constantly. It is not a zero-trust boundary."

"So what do we use?" Jim asked.

"We compile everything—Python, Rust, C++, Fortran—to **WebAssembly (WASM)**. Specifically, the `wasm32-wasip1` target."

Janis typed a command into the terminal. A 2.1MB binary appeared.

"This is the entire execution payload," Janis said. "There is no OS userland. There is no bloat. It transmits across the Kademlia DHT in milliseconds, and the `wasmtime` engine cold-starts the execution in under 50 microseconds. That is a 40,000x improvement in cold-start latency over a container."

### 8.2 The Hardware MMU (The Guard Regions)

Jim studied the 2MB binary. "Okay, so it's fast. But how is it secure? If it's just raw byte-code executing on the host CPU, what stops the payload from writing to memory addresses outside its sandbox and stealing the host's private keys?"

"The hardware itself," Janis said. "We don't use slow software checks. We use the silicon."

This abstract architectural pattern defines the strict memory bounds required to initialize the host engine safely:

```rust
use wasmtime::{Config, Engine};

pub fn configure_darwin_engine() -> Engine {
    let mut config = Config::new();
    
    // 1. Enforce strictly deterministic execution (Fuel Metering)
    config.consume_fuel(true);
    
    // 2. Allocate the 4GB Guard Region utilizing the CPU's MMU
    config.static_memory_maximum_size(4 * 1024 * 1024 * 1024); // 4GB Maximum Virtual Memory
    config.static_memory_guard_size(2 * 1024 * 1024 * 1024);   // 2GB Unmapped Buffer
    
    // 3. Disable multi-threading to prevent side-channel timing attacks
    config.wasm_threads(false);
    
    Engine::new(&config).expect("FATAL: Failed to initialize execution sandbox")
}
```

"Look at the `static_memory_guard_size`," Janis explained, tapping the screen. "When the `wasmtime` engine allocates the linear memory for the untrusted payload, it deliberately allocates a massive, 2-Gigabyte 'Guard Region' of unmapped virtual memory immediately following it."

"Why?" Jim asked.

"Because if the malicious WASM code tries to read or write an array out of bounds, the instruction will hit that unmapped Guard Region. We don't have to write code to check every single array index. We just let the CPU execute it. The physical Memory Management Unit (MMU) of the processor will detect the invalid address and trigger a hardware Page Fault (`SIGSEGV`). The engine catches the hardware trap instantly and kills the process. We get bare-metal execution speeds with absolute isolation."

### 8.3 Deterministic Fuel Metering (The End of the Infinite Loop)

"Alright," Jim conceded. "The memory is blind. But what about the CPU? What if I submit a WASM payload that just contains a `while(true)` infinite loop? I could submit 10 million infinite loops and permanently lock up every CPU core in the entire swarm."

Janis smiled. "You could try. But you would bankrupt yourself in about three seconds."

Janis brought up a JCL (Job Control Language) manifest on the screen.

```yaml
job:
  id: "urn:mrb:job:monte-carlo-v1"
payloads:
  - id: "map-worker"
    type: "wasm32-wasip1"
    source: "s3://marabunta-clearnet/jobs/monte-carlo.wasm"
    execution_bounds:
      max_fuel: 500000000 # 500 Million Opcodes
      max_memory_mb: 256
```

"In Marabunta," Janis explained, "time does not exist. Time is relative. You can't kill a job because it took 'too long' in seconds, because an Intel i9 processes faster than a Raspberry Pi."

"We measure execution in **Fuel**."

Before the `wasmtime` engine compiles the WASM byte-code into native machine code (JIT), it injects accounting instructions. Every time the code branches, loops, or executes a mathematical operation, it physically "burns" fuel. 

"Fuel is a deterministic count of opcodes," Janis said. "A simple integer addition (`i32.add`) might burn 1 unit of fuel. A complex vectorized floating-point operation (`v128.load`) might burn 10 units. It is perfectly deterministic."

"So," Jim reasoned, "if I declare `max_fuel: 500,000,000`..."

"If your payload hits 500,000,001 instructions," Janis said, "the engine instantly generates a `Trap` exception. The execution halts. The node reclaims the CPU core. And because you staked MMX tokens to pay for those 500 million opcodes, the swarm keeps your money."

Janis leaned forward. "If you submit an infinite loop to the swarm, you are simply paying us for the privilege of warming up our CPUs until your wallet hits zero."

### 8.4 The WASI Blacklist (Crippling the Kernel)

"One last vector," Jim said. "If the code is executing, it still needs to talk to the operating system. It needs to read files. It needs to open network sockets."

"No, it doesn't," Janis replied coldly. 

"The WebAssembly System Interface (WASI) defines how WASM code interacts with the host OS. In Marabunta, we deliberately cripple the WASI implementation. We actively blacklist the `sock_open` and `fd_read` system calls."

"If a malicious payload tries to establish a reverse-shell, or read the `/etc/passwd` file of the host machine," Janis continued, "the engine simply returns an `ENOSYS` (Function not implemented) error. The payload is deaf, dumb, and blind. It can only execute pure mathematics on the input data it was explicitly handed in memory."

By combining MMU Hardware Guard Regions, Deterministic Fuel Metering, and a crippled WASI interface, Marabunta guarantees that an anonymous laptop in a coffee shop can safely execute a state-sponsored cyberweapon without ever compromising the host operating system.

However, relying entirely on the `wasmtime` engine running in user-space is still a theoretical vulnerability if a zero-day exploit is found in the WASM compiler itself. To achieve true, unkillable resilience, we must monitor the execution from the outside, looking down from the Operating System kernel.

[Continue to Chapter 9: Crash-Only Darwin Auto-Healing](./02-darwin-auto-healing.md)
