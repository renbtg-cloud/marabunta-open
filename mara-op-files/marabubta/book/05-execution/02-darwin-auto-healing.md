<!-- Marabunta - Licensed under the MIT License.
## Chapter 9: Crash-Only Darwin Auto-Healing

The **Crash-Only Principle** dictates that software should not attempt to gracefully recover from catastrophic internal errors. Graceful degradation is a fallacy; complex error-handling code is rarely tested in production, often introduces subtle state corruption, and tightly couples the supervisor to the failing payload.

If a process violates memory bounds or enters an unrecoverable panic, the safest and most mathematically sound action is immediate annihilation. 

Marabunta implements this principle via the **Darwin Engine**.

### 9.1 The Janis & Jim Analysis: The Illusion of Graceful Recovery

Jim watched the telemetry board as the Swarm processed the 10-Terabyte meteorological dataset. A red light flashed on the dashboard. Node `US-74B` had dropped offline.

"Janis," Jim said, "We just lost a node. The logs say it hit a Segmentation Fault during the Python parsing. Did we catch the exception and roll back to the last checkpoint?"

"No," Janis replied. "We didn't catch the exception. We killed the process. We dropped the connection. We annihilated the state."

Jim spun around. "Janis, what if that node had been processing for 42 hours? Are you telling me that if it hits a null pointer on hour 42, we just throw away two days of compute and start over? Why don't we just write a `try/catch` block and gracefully recover the state?"

"Because graceful degradation is a lie," Janis said coldly. 

Janis stepped to the whiteboard. "If a compiled WASM payload triggers a Segmentation Fault (`SIGSEGV`), it means the code attempted to access a memory address it did not own. That means the linear memory of the sandbox is mathematically corrupted."

"If you attempt to write a complex `try/catch` handler to 'salvage' that execution," Janis continued, "you are gambling that the corruption didn't touch your critical variables. If you guess wrong, you allow poisoned data to enter the Map-Reduce pipeline. You will spend 42 hours generating a result, the Boulders will calculate the Zero-Knowledge Proof (ZKP), and the ZKP will fail because the math is corrupted. You will be slashed by the Court of Arbitration, and you will lose all your staked MMX."

"So what is the alternative?" Jim asked.

"The **Crash-Only Principle**," Janis said. "If the memory is violated, the execution is mathematically unclean. The only safe response is absolute annihilation. We kill the process. We let the Plumtree network detect the TCP timeout. The swarm slashes the Elo score of the failed route and simply re-queues the JCL manifest to a fresh node. We lose 42 hours of compute, but we guarantee that the final mathematical result is pristine."

### 9.2 OS-Level Subprocess Oversight (ptrace)

"But wait," Jim said, looking at the architectural diagram. "If the payload is hostile, and it finds a zero-day vulnerability in the `wasmtime` JIT compiler itself, it could bypass the WASM sandbox entirely. It could overwrite the host's memory before your engine even realizes it crashed."

"That is true," Janis conceded. "If we relied entirely on the `wasmtime` engine running in user-space, a zero-day exploit could compromise the node. That is why the Darwin Engine does not run inside the sandbox. It monitors the execution from the outside, looking down from the Operating System kernel."

When a Marabunta node accepts a JCL payload, it `fork()`s a completely isolated child process to run the `wasmtime` engine. The Darwin Engine then uses the `ptrace` system call to attach to this child process.

`ptrace` allows a parent process to observe and control the execution of another process, examining its core image and registers. It is the same system call used by debuggers like `gdb`.

Here is the structural logic of the supervisor daemon, mapping the UNIX system calls required to safely cage a hostile payload:

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
                tracing::error!("Pillar 12: Illegal Instruction. Payload terminated.");
                nix::sys::signal::kill(child_pid, Signal::SIGKILL).unwrap();
                break;
            }
            Ok(WaitStatus::Exited(_, exit_code)) => {
                // The WASM execution completed normally within its fuel bounds.
                tracing::info!("Execution complete. Exit code: {}", exit_code);
                break;
            }
            _ => {
                // Ignore benign signals and instruct the kernel to resume.
                ptrace::cont(child_pid, None).expect("Failed to resume child");
                continue;
            }
        }
    }
}
```

"Notice the `PTRACE_O_TRACESYSGOOD` flag," Janis pointed out. "We don't halt the child process on every single benign system call—that would destroy our performance. The payload runs at bare-metal speeds. The Darwin Engine only wakes up when the Linux kernel screams that a catastrophic signal (`SIGSEGV`, `SIGILL`) has occurred."

### 9.3 Forensic Memory Extraction (`process_vm_readv`)

"Okay, the payload hits a memory violation, the kernel screams, and Darwin wakes up," Jim said. "Then you issue `SIGKILL` and annihilate it. But if you just kill it, how do we prove to the rest of the network *why* we killed it? How do we prove the payload was malicious and we didn't just arbitrarily drop the connection?"

"We rip the memory out of the dying process before we kill it," Janis said. 

Before the Darwin Engine issues the final `SIGKILL`, it extracts a forensic artifact. 

To demonstrate the mathematical boundaries, consider this reference implementation mapping the memory extraction logic:

```rust
use nix::sys::uio::{process_vm_readv, IoVec, RemoteIoVec};

fn extract_forensic_memory(child_pid: Pid, target_addr: *mut libc::c_void, length: usize) -> Vec<u8> {
    let mut buffer = vec![0u8; length];
    
    let local_iov = IoVec::from_mut_slice(&mut buffer);
    let remote_iov = RemoteIoVec { base: target_addr as usize, len: length };
    
    // Rapidly extract the memory space in a single zero-copy syscall
    process_vm_readv(child_pid, &[local_iov], &[remote_iov])
        .expect("Failed to extract WASM memory state");
        
    buffer
}
```

"We use `process_vm_readv`," Janis explained. "It allows the Darwin Engine to reach directly into the RAM of the hostile child process and extract the exact state of the linear memory at the exact microsecond the Segmentation Fault occurred. It requires zero cooperation from the child process."

"But a WASM memory space could be 256 Megabytes," Jim noted. "You can't gossip a 256MB core dump across the DHT. It would flood the network."

"We don't send the raw dump," Janis said. "WASM linear memory is extremely sparse—it contains massive blocks of contiguous zeroes. The Darwin Engine instantly streams the extracted memory buffer through the `zstd` compression algorithm. A 256MB memory dump compresses down to roughly 2 Megabytes in milliseconds."

### 9.4 The Stigmergic Quarantine Protocol

"So we have a 2MB compressed core dump," Jim said. "What happens next?"

"The global immune response," Janis replied. 

When the Darwin Engine kills the process, the malicious JCL payload is technically dead on that specific node. But the payload might still be circulating in the broader Kademlia DHT, actively hunting for other nodes to infect. 

1.  **The Signature:** The Darwin Engine takes the compressed core dump, the exact instruction pointer where the `SIGSEGV` occurred, and the hash of the malicious JCL manifest. It cryptographically signs this package using the node's `bls12_381` identity key.
2.  **The Quarantine Gossip:** This signed package becomes a **Quarantine Payload**. It is injected into the Plumtree `eager_push` network. 
3.  **The Inoculation:** When the other 100 million nodes receive this Quarantine Payload, they verify the core dump. Because WASM execution is deterministic, they can mathematically prove that the JCL manifest causes a memory violation. They instantly add the malicious JCL hash to their local blacklists.

"The attacker's payload is mathematically vaccinated against globally in under 400 milliseconds," Janis concluded. "The Darwin Engine doesn't just protect the local node. It inoculates the entire supercomputer."

The system heals organically, at the network layer, because the local node had the courage to let the process die.

[Continue to Chapter 10: ClearNet Embassies and DarkNet Proxies](../06-membrane/01-clearnet-darknet.md)
