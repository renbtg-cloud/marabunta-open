# VOLUME 02: ABSOLUTE DETERMINISM (THE EXECUTION SANDBOX)

2.0 The Hypervisor Trap

Executing mission-critical, proprietary workloads on volatile, heterogeneous edge nodes (from consumer smartphones to specialized datacenter racks) necessitates an execution environment completely devoid of traditional Operating System trust assumptions.

Traditional distributed systems rely on OS-level virtualization (Docker or Kubernetes). This approach introduces three fundamental vulnerabilities: 
1. Massive Attack Surfaces: Container escapes and shared kernel exploits compromise host integrity. 
2. Bandwidth Bloat: Deploying a simple Python script often requires pulling a 1GB Ubuntu base image across the network. 
3. Non-Deterministic Execution: Standard OS environments allow payloads to read external state (e.g., host system clocks, hardware random number generators, or DNS resolvers), making mathematical verification of the output impossible.

Marabunta circumvents these vulnerabilities by enforcing absolute execution determinism and isolation using a heavily metered WebAssembly (WASM) sandbox.

Workloads are compiled to mathematically pure Abstract Syntax Trees (ASTs) in the wasm32- wasi format. The resulting payloads are typically under 2MB, allowing instantaneous distribution across the Swarm.

2.1 The wasm32-wasi Determinism Paradigm

If execution is non-deterministic, Byzantine Fault Tolerance (BFT) is impossible. You cannot mathematically verify a computation across a 10,000-node quorum if the expected output is permitted to drift based on localized, host-specific variables (like whether the script is executed at 12:00 in Tokyo or 18:00 in New York).

The Marabunta Wasmtime JIT compiler acts as a hypervisor. The WASM payload believes it is interacting with a standard POSIX-compliant Operating System.

When the payload executes an assembly instruction like call $wasi_snapshot_preview1.random_get , the Wasmtime engine traps the execution. Instead of passing the request to the underlying Linux or Windows kernel, Marabunta intercepts it. It generates a deterministic pseudo-random byte slice, seeded from the node's Chrysalis Grinder identity, and provides it to the payload.

This guarantees that a WASM payload executed 10,000 times across 10,000 different edge nodes will result in a mathematically identical final memory state and output hash.

The wasm32 4GB Memory Ceiling (Zero-Copy Streaming)

The strict wasm32 architecture imposes a hard, physical limit of 4 Gigabytes of linear memory per instance. Loading a 40GB Simulation payload into this sandbox results in an immediate Out-Of- Memory (OOM) panic.

Marabunta resolves this via Memory-Mapped Tensor Streaming and the wasm64 extension proposal. Massive datasets reside on the host NVMe drives and are mathematically streamed into the WASM execution window in discrete, pointer-referenced 2GB chunks, achieving zero- copy I/O bounded only by the physical PCIe bus speed.

The Floating-Point Determinism Paradox

WebAssembly guarantees structural determinism, but the underlying physical CPUs (x86 vs ARM) handle NaN (Not-a-Number) bit patterns and Fused Multiply-Add (FMA) instructions differently. This minute hardware drift destroys Cryptographic Hash BFT consensus.

The Marabunta SDK enforces Canonical NaNs and explicitly disables hardware FMA during the LLVM compilation phase ( StrictFP ), guaranteeing identical bit-level state transitions across wildly heterogeneous silicon.

The Deterministic Clock Paradox

If the MantisJournal mocks clock_time_get to a static 0 to ensure determinism, payloads verifying TLS certificates or JWT expiration dates will permanently fail. Conversely, passing real wall-clock time breaks STARK reproducibility.

Marabunta utilizes Isomorphic Monotonic Clocks. During the live execution, the payload receives the true wall-clock time, which is strictly serialized into the .mrb-dump tape. During the Cryptographic Hash proof generation, the JIT compiler is fed the exact recorded timestamp from the tape, resolving the paradox between secure network verification and mathematical reproducibility.

Cryptographic Entropy Exhaustion

A static 32-byte PoW seed will eventually loop a standard Pseudo-Random Number Generator (PRNG) during massive Monte Carlo simulations, ruining scientific integrity. Marabunta prevents entropy depletion by utilizing HKDF-SHA256 (HMAC-based Extract-and-Expand Key Derivation). The static identity seed is continuously expanded with an internal monotonic counter, providing an infinite, cryptographically secure byte stream without repeating structural patterns.

2.2 The Mantis Journal: Intercepting Reality

Because all workloads execute inside the strict wasm32-wasi sandbox, the Marabunta node intercepts every single interaction between the payload and the host machine via the Mantis Journal.

The payload is blind; it only perceives the reality that the Journal explicitly constructs for it.

The Implementation: src/swarm/mantis_journal.rs

// Core WASI Hypercall Interception: The Virtual Filesystem Trap linker.func_wrap( "wasi_snapshot_preview1", "path_open", move |mut caller: Caller<'_, WasiCtx>, fd: i32, dirflags: i32, path_ptr: i32, path_len: i32, oflags: i32, fs_rights_base: i64, fs_rights_inheriting: i64, fdflags: i32, opened_fd_ptr: i32| -> i32 {

// 
1. We never allow the WASM payload to touch the host disk. // 
2. We resolve the requested string pointer against the in- memory Virtual Filesystem (VFS).

let mut tape = tape_clone_open.lock().unwrap(); tape.push(HypercallRecord { call_name: "path_open".to_string(), timestamp_ns: 0, // Mocked for absolute determinism bytes_written: 0, bytes_read: 0, payload: None, });

// 
3. Return a mock Virtual File Descriptor (e.g., VFD 100) // 
4. Write VFD 100 into the WASM linear memory at `opened_fd_ptr` // 
5. Return WASI_ESUCCESS (0)

0 }, )?;

Architectural Analysis: The Flight Data Recorder

1. The Virtual Filesystem (VFS): Notice the path_open trap. If a data scientist's PyTorch code attempts to execute open("/data/shard_01.csv", "r") , the sandbox does not interact with the host machine's hard drive. It traps the hypercall, serializes the request into a HypercallRecord , and returns a mock file descriptor. The payload is perfectly, cryptographically isolated from the physical hardware. 
2. The .mrb-dump Journal Tape: Every interaction (a read, a write, a clock query) is serialized and appended to a continuous tape. Upon a crash or an explicit checkpoint request, this tape is isolated into a highly compressed .mrb-dump file. This file contains the entire execution history, allowing a developer thousands of miles away to hit "Replay" in their IDE. They can reverse-step through the exact crash deterministically, because the IDE feeds the payload the identical sequence from the tape instead of executing live OS calls.

State Snapshotting and Tape Compaction

A naive implementation of a Flight Data Recorder would result in catastrophic memory bloat. If a fluid dynamics simulation executes 50,000 I/O operations per second for 72 hours, the resulting continuous tape of hypercall interceptions would consume Terabytes of RAM, triggering an Out-Of-Memory (OOM) kill on the edge node.

Marabunta prevents this through Deterministic State Snapshotting.

The MantisJournal does not maintain an infinite tape. At mathematically defined intervals (e.g., every 10,000 CPU cycles or when the tape buffer reaches 50MB), the Wasmtime JIT compiler pauses execution. It serializes the entire 32-bit linear memory state of the wasm32- wasi instance into a highly compressed binary snapshot.

The preceding hypercall tape is then flushed and permanently archived to the local BlobStore, and the active .mrb-dump journal resets to zero, anchored by the new snapshot hash. When a remote developer triggers a Time-Travel Debugging session, the IDE simply downloads the nearest snapshot prior to the crash and replays only the final megabytes of the hypercall tape, ensuring $O(1)$ memory overhead regardless of the workload's total uptime.

2.3 Kernel-Level Thermal Preemption (The 95°C Guillotine)

While WASM sandboxing protects against memory leaks and malicious host access, it is insufficient for protecting the Swarm from hardware-level starvation or deeply embedded denial-of-service vectors.

A malicious computational payload containing an infinite loop designed for intense floating- point operations will drive CPU utilization to 100%, risking thermal damage or triggering an emergency hardware shutdown.

The Marabunta Daemon ( marabunta-visor ) does not politely ask a runaway process to terminate in user-space. If it waited for the Linux OS scheduler to grant it CPU time to issue a SIGTERM , the node might already be dead.

Instead, Marabunta monitors the host's physical constraints from within the kernel itself.

The Implementation: src/bpf/thermal_guardian.bpf.c

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

#define MAX_TEMP 95000 // 95 Celsius in millidegrees
#define WASM_CGROUP_ID 0x1A4F

SEC("kprobe/thermal_zone_device_update") int BPF_KPROBE(thermal_guardian, struct thermal_zone_device *tz) { int temp; bpf_probe_read_kernel(&temp, sizeof(temp), &tz->temperature);

if (temp >= MAX_TEMP) { struct task_struct *task = bpf_get_current_task_btf(); u64 cgroup_id = bpf_get_current_cgroup_id();

if (cgroup_id == WASM_CGROUP_ID) { // Ruthless Ring-0 kernel-level preemption. Bypasses OS scheduler. bpf_send_signal(9); bpf_printk("MARABUNTA: Thermal Critical (%d). WASM Sandbox Terminated.", temp); } } return 0;

} char LICENSE[] SEC("license") = "GPL";

Architectural Analysis: Crash-Only Design

1. Ring 0 Execution: This C payload is compiled directly into the Rust binary as an eBPF (Extended Berkeley Packet Filter) object. The SEC("kprobe/ thermal_zone_device_update") macro attaches the function directly to the hardware thermal sensor interrupt. 
2. The Trigger: When the silicon hits 95°C ( MAX_TEMP ), the kernel instantly identifies the cgroup_id of the WASM sandbox executing the compute payload. 
3. The Preemption: It bypasses the OS scheduler entirely and executes bpf_send_signal(9) (SIGKILL) directly against the execution thread in microseconds.

The computational payload is physically destroyed. CPU load instantly drops to 0%. The node remains online to continue routing P2P Pings and participating in the Swarm consensus.

Marabunta implements crash-only deterministic design, fundamentally prioritizing the physical survival of the edge node over the survival of the executed payload.

