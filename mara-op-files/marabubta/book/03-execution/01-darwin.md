<!-- Marabunta - Licensed under the MIT License.
## Chapter 5: Crash-Only Darwin Auto-Healing

Traditional distributed systems attempt to handle network timeouts and memory leaks gracefully. This is the **Fragile Consensus Anti-Pattern**. Graceful degradation in a high-state environment is an illusion; attempting to recover compromised memory introduces severe coupling between the supervisor daemon and the untrusted payload.

Marabunta enforces the **Crash-Only Principle**. Nodes are assumed to be highly ephemeral and hostile. 

The `DarwinEngine` monitors the execution sub-processes at the Linux kernel level using UNIX `ptrace`. 

### 5.1 The Anatomy of a wasmtime Trap (The Internal Engine)

Before we can monitor failure from the Operating System, we must understand how failure is defined within the WebAssembly Sandbox.

WebAssembly (WASM) is a stack-based virtual machine. It was designed to run untrusted code safely. However, standard software-level bounds checking (inserting an `if index < array.length` instruction before every memory access) incurs a massive performance penalty, often slowing execution by 20% to 50%. 

To achieve bare-metal performance, Marabunta utilizes the `wasmtime` engine, powered by the Cranelift Just-In-Time (JIT) compiler. 

Cranelift eliminates software bounds checking entirely. Instead, it relies on the physical hardware of the host processor—specifically, the **Memory Management Unit (MMU)**.

When the `wasmtime` engine instantiates a module, it does not allocate just the required memory (e.g., 256MB). It allocates a massive, contiguous block of virtual memory (typically 4GB or 6GB) using `mmap`. 

1.  **The Accessible Region:** The first 256MB is marked as `PROT_READ | PROT_WRITE`. This is the sandbox. The WASM code can read and write here freely at native CPU speeds.
2.  **The Guard Region:** The remaining 4GB is mapped with `PROT_NONE` (no access). This is the abyss.

If a malicious WASM payload attempts an out-of-bounds memory access (e.g., trying to read memory address `256MB + 1` to steal a cryptographic key from the host process), the CPU hardware instantly triggers a Page Fault. 

Because the memory is marked `PROT_NONE`, the Linux kernel generates a `SIGSEGV` (Segmentation Fault) signal. 

The `wasmtime` engine intercepts this `SIGSEGV`, verifies that the faulting address was within the 4GB guard region, and translates the hardware fault into a WebAssembly **Trap**. The execution halts instantly, with zero software overhead during normal operation.

This abstract architectural pattern defines the strict memory bounds required to initialize the host engine safely:

```rust
use wasmtime::{Config, Engine};

pub fn configure_darwin_engine() -> Engine {
    let mut config = Config::new();
    
    // Enforce strictly deterministic execution (Fuel Metering)
    config.consume_fuel(true);
    
    // Allocate a 4GB guard region to utilize hardware MMU bounds checking
    // eliminating the need for slow software instrumentation.
    config.static_memory_maximum_size(4 * 1024 * 1024 * 1024); // 4GB Guard
    config.static_memory_guard_size(2 * 1024 * 1024 * 1024);   // 2GB Buffer
    
    // Disable multi-threading to prevent side-channel timing attacks
    config.wasm_threads(false);
    
    Engine::new(&config).expect("Failed to initialize Darwin Engine")
}
```

### 5.2 The ptrace State Machine (The OS Layer)

While the `wasmtime` trap is highly effective, it relies entirely on the engine running in user-space. If a zero-day exploit is discovered in the Cranelift compiler itself, a sophisticated attacker could potentially bypass the guard regions and execute arbitrary code on the host machine.

To achieve true, unkillable resilience, the Marabunta node must monitor the execution from the *outside*, looking down from the Operating System kernel.

When a Marabunta node accepts a JCL payload, it does not execute it within the main daemon process. It `fork()`s a completely isolated child process to run the `wasmtime` engine. The parent **Darwin Engine** then uses the `ptrace` system call to attach to this child process.

`ptrace` allows a parent process to observe and control the execution of another process, examining its core image and registers. It is the exact mechanism used by debuggers like `gdb` and `strace`.

The Darwin Engine operates a complex state machine, waiting for the Linux kernel to deliver signals regarding the child process. It explicitly sets `PTRACE_O_TRACESYSGOOD` to distinguish between genuine system calls and signal-delivery stops, ensuring it only reacts to catastrophic events.

```rust
use nix::sys::ptrace;
use nix::sys::wait::{waitpid, WaitStatus};
use nix::sys::signal::Signal;
use nix::unistd::Pid;

pub fn execute_with_darwin_oversight(child_pid: Pid) {
    // 1. Attach to the untrusted child process.
    ptrace::attach(child_pid).expect("FATAL: Darwin failed to attach to WASM sandbox.");
    
    // 2. Configure ptrace to differentiate syscalls from signals.
    ptrace::setoptions(child_pid, ptrace::Options::PTRACE_O_TRACESYSGOOD)
        .expect("Failed to set ptrace options");

    loop {
        // 3. Block and wait for the kernel to signal a state change in the child.
        match waitpid(child_pid, None) {
            Ok(WaitStatus::Signaled(_, Signal::SIGSEGV, _)) => {
                // A Segmentation Fault occurred. 
                // Either a WASM trap triggered, or a Zero-Day was attempted.
                tracing::error!("Pillar 12: Critical Memory Violation (SIGSEGV).");
                handle_catastrophic_failure(child_pid);
                break;
            }
            Ok(WaitStatus::Signaled(_, Signal::SIGILL, _)) => {
                // Illegal Instruction detected.
                tracing::error!("Pillar 12: Illegal Instruction (SIGILL).");
                handle_catastrophic_failure(child_pid);
                break;
            }
            Ok(WaitStatus::Exited(_, exit_code)) => {
                // The WASM execution completed normally within its fuel bounds.
                tracing::info!("Execution complete. Exit code: {}", exit_code);
                break;
            }
            Ok(WaitStatus::Stopped(pid, sig)) if sig == Signal::SIGTRAP | 0x80 => {
                 // Benign syscall stop (due to PTRACE_O_TRACESYSGOOD)
                 ptrace::syscall(pid, None).expect("Failed to resume syscall");
            }
            _ => {
                // Ignore benign signals (e.g., SIGSTOP from ptrace attach)
                // and instruct the kernel to resume child execution.
                ptrace::cont(child_pid, None).expect("Failed to resume child");
                continue;
            }
        }
    }
}
```

### 5.3 Forensic Memory Extraction (The Post-Mortem)

When the `WaitStatus::Signaled` match arm triggers `handle_catastrophic_failure(child_pid)`, the Darwin Engine does not simply kill the process and move on. It must preserve the crime scene.

If the Swarm is going to mathematically penalize (slash) the node that submitted the malicious payload, the Court of Arbitration requires cryptographic proof of the exploit attempt.

The Darwin Engine performs a **Zero-Trust Forensic Extraction**.

It must extract the exact state of the WASM linear memory and the CPU registers at the exact millisecond the payload attempted the hypervisor escape.

1.  **Register Extraction:** The engine uses `ptrace(PTRACE_GETREGS)` to pull the CPU state. It records the Instruction Pointer (`RIP` on x86_64) to identify exactly which compiled machine code instruction triggered the fault, and the Faulting Address (`CR2`) to identify exactly which memory boundary was breached.
2.  **Memory Extraction:** To dump the 256MB WASM memory space, the engine does *not* use `PTRACE_PEEKDATA`, which reads memory one word at a time and is excruciatingly slow. Instead, it uses the highly optimized Linux `process_vm_readv` system call, which transfers massive blocks of memory directly between the address spaces of the child and the parent in a single context switch.
3.  **Compression:** A 256MB memory dump is too large to gossip efficiently over the DHT. The Darwin Engine instantly streams the extracted memory buffer through the `zstd` compression algorithm (Level 3). Because WASM linear memory is often sparsely populated (containing many contiguous zeroes), `zstd` compresses the 256MB dump down to 2-3 Megabytes in milliseconds.

```rust
use nix::sys::uio::{process_vm_readv, IoVec, RemoteIoVec};

fn extract_forensic_memory(child_pid: Pid, target_addr: *mut libc::c_void, length: usize) -> Vec<u8> {
    let mut buffer = vec![0u8; length];
    
    let local_iov = IoVec::from_mut_slice(&mut buffer);
    let remote_iov = RemoteIoVec { base: target_addr as usize, len: length };
    
    // Rapidly extract the memory space in a single syscall
    process_vm_readv(child_pid, &[local_iov], &[remote_iov])
        .expect("Failed to extract WASM memory state");
        
    // Stream the buffer through zstd compression before network transmission
    zstd::bulk::compress(&buffer, 3).expect("ZSTD compression failed")
}
```

### 5.4 The Stigmergic Quarantine Protocol

Once the forensic artifact (the compressed memory dump and the CPU registers) is secured, the Darwin Engine executes the child process (`Signal::SIGKILL`).

There is no `try_catch` block attempting to reset the memory pointers. There is no logic attempting to salvage the execution state. The payload is annihilated.

However, killing the process locally does not protect the global 100-million node swarm from the malicious actor. The Darwin Engine must connect the OS-level `SIGKILL` back to the global Kademlia routing table.

It executes the **Stigmergic Quarantine Protocol**.

The local node generates a specialized UDP payload and gossips it to its immediate neighbors:
`Message::Quarantine { node_id: <Attacker_BLS12_381>, payload_hash: <Blake3_Digest>, forensic_cid: <IPFS_CID> }`

When the neighboring nodes receive this Quarantine message, they do not blindly trust it. They query the DHT for the `forensic_cid`, pull the 2MB compressed memory dump, and submit it to a local Court of Arbitration Jury. 

The Jury inspects the CPU registers and the WASM execution trace. They mathematically verify that the payload intentionally violated its memory bounds.

Once verified, every node in the local sector of the XOR metric space applies the penalty:
*   The attacker's **Elo Reputation Score** is slashed by an absolute scalar of `0.25`.
*   Because the Kademlia `TrafficShaper` mathematically severs any TCP connection to a node with an Elo rating below `0.50`, the attacker is instantly, mathematically excommunicated from the routing tree. 

The attacker is isolated. The poison pill is neutralized. The network heals organically, at the routing layer, because the local node had the courage to let the process die.

[Continue to Chapter 6: ClearNet Embassies and DarkNet Proxies](../04-playbooks/01-membrane.md)
