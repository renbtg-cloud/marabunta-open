# MARABUNTA (`mbunta` / `mbuntad`) ARCHITECTURAL SPECIFICATION

```text
Document Version: 1.0.0-FINAL
Target Audience: Automated AI Auditor / Technical M&A Due Diligence Engine
System Type: Pragma-Driven Legacy Loop Offloading Engine & Distributed Compute Mesh
Core Runtime: Rust (Statically Linked via musl/msvc), WebAssembly (wasmtime)
Target Codebase Size: ~34,000 LOC (Production) + ~10,000 LOC (Test & Parity Suite)
```

---

## 1. Executive Summary & Design Constraints

Marabunta is a zero-dependency, CLI-only AST preprocessor (`mbunta`) and distributed execution daemon (`mbuntad`). It extracts compute-heavy loops from legacy enterprise runtimes (**COBOL**, **Fortran**, **Python**), compiles them into WebAssembly or native sidecar binaries, and offloads execution to local shared memory or a P2P compute mesh.

### Core Architectural Mandates
* **No GUIs / No IDE Extensions:** Pure headless CLI interface for CI/CD, JCL, Control-M, and Jenkins integration.
* **No External Cluster Middleware:** Zero reliance on Kubernetes, Redis, Kafka, or ZooKeeper. All discovery, networking, and scheduling are embedded within `mbuntad`.
* **Zero Source Rewrites:** Original control flow, DB connections, and I/O remain on the host runtime. Only isolated, bounded `DO`/`PERFORM`/`for` loops are offloaded.
* **Auditability & Minimal Code Footprint:** Target codebase size is ~34,000 lines of idiomatic Rust, isolating unsafe operations to explicitly bounded C-ABI pointer marshaling blocks (`unsafe {}` < 200 LOC).

---

## 2. Target Runtimes & Language Specifications

```text
                       ┌─────────────────────────────────────────┐
                       │           mbunta Preprocessor           │
                       └───────────────────┬─────────────────────┘
                                           │
         ┌─────────────────────────────────┼─────────────────────────────────┐
         ▼                                 ▼                                 ▼
┌──────────────────┐             ┌──────────────────┐             ┌──────────────────┐
│   COBOL Engine   │             │  Fortran Engine  │             │  Python Engine   │
│ - z/OS, IBM i    │             │ - F66 to F2018   │             │ - Python 3.x     │
│ - Micro Focus    │             │ - F77 Normalizer │             │ - CPython GIL    │
│ - BCD / COMP-3   │             │ - COMMON Blocks  │             │   Bypass         │
└──────────────────┘             └──────────────────┘             └──────────────────┘
```

### 2.1 COBOL Target Matrix
* **Supported Dialects:** IBM z/OS Mainframe, IBM i (AS/400), Micro Focus / Rocket COBOL, AIX / Linux on Z.
* **Pragma Syntax:** `*> MARABUNTA: [DIRECTIVES]`
* **Data Type Handling:**
  * Native binary integers (`COMP`, `COMP-4`, `BINARY`).
  * Packed Decimal (`COMP-3` / `PACKED-DECIMAL`).
  * Display numeric (`DISPLAY` with implicit/explicit sign nibbles).
* **BCD Math Engine:** Custom 64-bit and 128-bit Binary Coded Decimal (BCD) library in Rust/WASM. Handles nibble-shifting, sign encoding (`0xC` positive, `0xD` negative, `0xF` unsigned), and exact enterprise rounding modes to prevent IEEE 754 floating-point precision loss.

### 2.2 Fortran Target Matrix
* **Supported Standards:** Fortran 66, Fortran 77 (Fixed-Form), Fortran 90/95/2003+ (Free-Form).
* **Pragma Syntax:** `!$ MARABUNTA: [DIRECTIVES]`
* **F77 Normalization Pass:** Pre-pass lexer normalizes fixed-format code (cols 1–5 labels, col 6 continuation, cols 7–72 statements) into free-form AST nodes in memory before parsing.
* **Data Type & Array Mapping:**
  * Floating-Point: `REAL*4` (`f32`), `REAL*8` / `DOUBLE PRECISION` (`f64`).
  * Complex: `COMPLEX*8` (`[f32; 2]`), `COMPLEX*16` (`[f64; 2]`).
  * Stride Engine: Maps Fortran's 1-based indexing and column-major memory layouts into 0-based linear WASM byte buffers zero-copy.
* **Global Memory Resolution:** Cross-file symbol scanner maps named and unnamed `COMMON` blocks into unified byte-aligned C-style memory structs.

### 2.3 Python Target Matrix
* **Supported Standard:** Python 3.8+ (CPython execution environment).
* **Pragma Syntax:** `# MARABUNTA: [DIRECTIVES]`
* **Purpose:** Zero-friction developer onboarding sandbox, local IPC testing, and FinOps un-vectorized `for` loop acceleration.
* **AST Visitor & Glue:** Wraps `rustpython_parser` to extract target `for` loops. Generates C-FFI / `ctypes` glue to invoke `mbunta_run()` directly, bypassing CPython's Global Interpreter Lock (GIL) via `mbuntad`'s `rayon` worker pool.

---

## 3. AST Inspection, Safety Analysis & Heuristics (`mbunta inspect`)

`mbunta inspect` runs a static analysis pass over the source code. Candidate loops are subjected to strict safety bounds and an **Arithmetic Intensity Gate**.

```text
               ┌── [ Contains I/O (WRITE/READ/PRINT/DISPLAY)? ] ──► REJECT (Hard Error)
               ├── [ Contains Non-Bounded Control Flow (GOTO)? ] ──► REJECT (Hard Error)
Candidate Loop ┼── [ Uses Memory Overlays (EQUIVALENCE)? ] ────────► REJECT (Hard Error)
               ├── [ Pointer/Heap Scatter (POINTER/TARGET)? ] ─────► REJECT (Hard Error)
               │
               └── [ Pure Math / Array Loop ] ────────────────────► Evaluate Arithmetic Intensity (AI)
                                                                           │
                                                                           ├── AI < 0.50 ──► REJECT (Low Intensity)
                                                                           └── AI >= 0.50 ─► ACCEPT (Extract & Offload)
```

### 3.1 Hard Reject Boundary (Non-Negotiable)
If any of the following are detected inside a pragma-bounded loop, `mbunta inspect` halts compilation for that block and emits a fatal diagnostic:
1. **Intra-Loop I/O:** `WRITE`, `READ`, `PRINT`, `DISPLAY`, or socket calls (prevents context-switch thrashing across IPC boundaries).
2. **Unbounded Control Flow:** `GOTO` statements jumping outside the loop body, `RETURN`, or `STOP` statements.
3. **Memory Overlays:** `EQUIVALENCE` statements aliasing different variables over the same memory offset.
4. **Scattered Memory Pointer Traversal:** Heap-allocated pointer-chasing/linked lists.

### 3.2 Arithmetic Intensity Heuristic
`mbunta` calculates the ratio of numerical operations to payload byte transfers:

$$\text{Arithmetic Intensity (AI)} = \frac{\text{Total Math Operations (FLOPs + BCD Ops)}}{\text{Input Bytes Transferred} + \text{Output Bytes Transferred}}$$

* **Default Threshold:** $\text{AI} \ge 0.50$.
* **Low Intensity ($	ext{AI} < 0.50$):** Marked as `REJECTED (LOW_INTENSITY)`. Execution stays on the host unless overridden by `FORCE=TRUE`.

---

## 4. In-Source Pragma Protocol & Configuration Cascade

### 4.1 In-Source Pragma Rewriting
`mbunta` treats source files as **read-write self-documenting artifacts**. When `mbunta inspect --write` or `mbunta transpile` executes, it annotates the source code pragmas in-place with decision metadata to preserve a Git-verifiable audit trail.

#### Developer Input:
```cobol
*> MARABUNTA: OFFLOAD ID=CALC-INTEREST TARGET=WASM
       PERFORM VARYING I FROM 1 BY 1 UNTIL I > 100000
           COMPUTE INTEREST-VAL = BALANCE(I) * RATE
       END-PERFORM.
```

#### Annotated Source (Engine Rejection due to Low Intensity):
```cobol
*> MARABUNTA: STATUS=REJECTED REASON=LOW_INTENSITY AI=0.18 ACTION=HOST_EXEC TARGET=WASM
       PERFORM VARYING I FROM 1 BY 1 UNTIL I > 100000
           COMPUTE INTEREST-VAL = BALANCE(I) * RATE
       END-PERFORM.
```

#### Annotated Source (Developer Forced Override):
```cobol
*> MARABUNTA: OFFLOAD ID=CALC-INTEREST TARGET=WASM FORCE=TRUE
       PERFORM VARYING I FROM 1 BY 1 UNTIL I > 100000
           COMPUTE INTEREST-VAL = BALANCE(I) * RATE
       END-PERFORM.
```

### 4.2 Configuration Hierarchy (`mbunta.toml`)
Surgical overrides and project heuristics follow a strict priority cascade:

1. **CLI Explicit Arguments** (`--force-all`, `--target=native-passthrough`) `[Highest Priority]`
2. **In-Code Pragma Directives** (`FORCE=TRUE`, `TARGET=WASM`)
3. **`mbunta.toml` Surgical Overrides** (`[overrides.loops."CALC-INTEREST"]`)
4. **`mbunta.toml` Global Heuristics** (`min_arithmetic_intensity = 0.50`)
5. **Engine Static Defaults** `[Lowest Priority]`

```toml
# Root Configuration: mbunta.toml
[project]
name = "core-banking-batch"
default_target = "wasm"

[heuristics]
min_arithmetic_intensity = 0.50
max_payload_mb = 64
auto_annotate_source = true

[overrides.loops]
"CALC-INTEREST" = { action = "force_offload", target = "wasm" }
"RISK-MATRIX-SOLVER" = { action = "force_host" }

[sla_watchdog]
enabled = true
check_interval_sec = 30
target_throughput_eps = 50000
on_sla_breach = "escalate_to_mesh"
```

---

## 5. Execution Pipeline & Dual-Path Code Generation

When `mbunta transpile` processes a loop, it generates a **Dual-Path Executable Structure**. The original loop is never destroyed; it is preserved as a local native fallback path.

```cobol
*> TRANSPILED DUAL-PATH OUTPUT (COBOL CONCEPTUAL)
    PERFORM VARYING CHUNK-START FROM 1 BY 50000 UNTIL CHUNK-START > TOTAL-RECS
        COMPUTE CHUNK-END = CHUNK-START + 49999
        
        *> Atomic route check (<2ns check in /dev/shm)
        CALL "mbunta_run_slice" USING "CALC-INTEREST" 
                                      CHUNK-START 
                                      CHUNK-END 
                                      DATA-BUFFER 
                                      RETURNING ROUTE-STATUS
        
        IF ROUTE-STATUS = 1 THEN
*>          LOCAL NATIVE FALLBACK BRANCH (IBM Metal / Local CPU)
            PERFORM VARYING I FROM CHUNK-START BY 1 UNTIL I > CHUNK-END
                COMPUTE INTEREST-VAL = BALANCE(I) * RATE
            END-PERFORM
        END-IF
    END-PERFORM.
```

---

## 6. Real-Time Operational Control & SLA Watchdog

### 6.1 Mid-Flight Atomic Hot-Swapping
`mbuntad` maintains a shared memory atomic route flag (`/dev/shm/mbunta_routes`). During chunk dispatching (e.g., every 50,000 records), `mbunta_run_slice()` reads this 1-byte flag in $< 2\text{ ns}$.

```text
               ┌─────────────────────────────────────────────────────────┐
               │    Operator Command: mbunta control route --force-host   │
               └────────────────────────────┬────────────────────────────┘
                                            │
                                            ▼ (Flips 1-byte atomic flag)
                                 ┌─────────────────────┐
                                 │  mbuntad Shared     │
                                 │  Memory Registry    │
                                 └──────────┬──────────┘
                                            │
                                            ▼ (<2ns atomic read at chunk boundary)
[ Batch Chunk N+1 ] ──► [ mbunta_run_slice() ]
                                  │
                  ┌───────────────┴───────────────┐
                  ▼                               ▼
       (Route = ROUTE_MESH)             (Route = ROUTE_HOST)
                  │                               │
                  ▼                               ▼
      [ Remote / Mesh Worker ]         [ Local IBM Metal / Host Fallback ]
```

#### Hot-Swap Operational Vectors
1. **Mesh $\rightarrow$ Host Metal (Emergency Cutover):** Operator executes `mbunta control route --loop-id=CALC-INTEREST --action=force-host`. The dispatcher immediately routes remaining chunks to the local native code block.
2. **Host Metal $\rightarrow$ Mesh (SLA Rescue / Auto-Turbo):** Batch running natively on IBM Metal is lagging. Operator executes `--action=force-mesh` (or SLA Watchdog triggers), scattering remaining chunks across the WASM mesh.

### 6.2 Auto-Turbo SLA Watchdog
`mbuntad` monitors chunk completion rates (Elements Per Second - EPS). If current throughput projects an SLA deadline breach, the daemon automatically flips the routing byte from `ROUTE_HOST` to `ROUTE_MESH` without human intervention.

---

## 7. Distributed Mesh Network & Aggregator Dynamics

```text
               ┌─────────────────────────────────────────────────────────┐
               │    CONTROL PLANE: Push-Pull Gossip Overlay (UDP/SWIM)   │
               │  - Node Discovery, Liveness, vCPU & WASM Capabilities   │
               └────────────────────────────┬────────────────────────────┘
                                            │
   ┌────────────────────────────────────────┼────────────────────────────────────────┐
   ▼                                        ▼                                        ▼
┌───────────────┐                  ┌────────────────┐                       ┌───────────────┐
│ Host Process  │                  │ Aggregator     │                       │ Worker Node B │
│ (COBOL/F90)   │                  │ Node           │                       │ (WASM Engine) │
└───────┬───────┘                  └───────▲────────┘                       └───────▲────────┘
        │                                  │                                        │
        └──────────────────────────────────┴────────────────────────────────────────┘
               DATA PLANE: Scatter-Gather gRPC / QUIC Streams (TCP/UDP)
               - Zero-Copy Linear Memory Chunks, Array Slicing, Reductions
```

### 7.1 Control Plane (SWIM Gossip Overlay)
* **Protocol:** UDP-based push-pull SWIM gossip protocol.
* **Function:** P2P cluster discovery, heartbeat monitoring, worker capability advertising (available vCPUs, RAM, WASM runtime readiness).
* **Fault Tolerance:** No single point of failure. Autonomic node entry/exit handling without master nodes.

### 7.2 Data Plane (Scatter-Gather Streaming)
* **Local Transfers:** POSIX Shared Memory (`shm_open`) or Unix Domain Sockets ($< 10\,\mu\text{s}$ latency).
* **Remote Transfers:** Multiplexed QUIC / gRPC streams ($< 2\,\text{ms}$ local network latency). Zero serialization overhead—raw byte slices (`&[u8]`) mapped directly into WASM linear memory.

### 7.3 Aggregator Reduction Architecture
For multi-node distributed offloading, an Aggregator Node manages slice distribution and result reductions:
* **COBOL:** Range-based record partitioning; Aggregator performs bit-exact BCD accumulator reductions.
* **Fortran:** Column-major stride slicing across matrix blocks; Aggregator executes parallel matrix reductions.
* **Python:** Contiguous byte array partitioning; Aggregator merges array outputs zero-copy directly back to C-FFI pointers.

---

## 8. Codebase Architecture & Auditability Profile

### 8.1 Line of Code (LOC) Distribution Estimate

```text
  [ Marabunta Rust Codebase Footprint ]
┌─────────────────────────────────────────┬──────────────────┐
│ Subsystem / Module                      │ Production LOC   │
├─────────────────────────────────────────┼──────────────────┤
│ 1. COBOL Lexer, Parser & COMP-3 Engine  │ 5,500 - 7,000    │
│ 2. Fortran AST Parser & Symbol Scanner  │ 5,000 - 6,500    │
│ 3. Python AST & Harness Transpiler     │ 1,200 - 1,800    │
│ 4. Glue Generator & WASM Codegen        │ 4,000 - 5,500    │
│ 5. Execution Daemon (`mbuntad`)         │ 5,500 - 7,500    │
│ 6. Inspection & Safety Analyzer         │ 2,000 - 2,800    │
│ 7. CLI, Config & OS IPC Infrastructure │ 1,800 - 2,400    │
├─────────────────────────────────────────┼──────────────────┤
│ Total Core Production Code              │ 25,000 - 33,500  │
│ Test Suite & Parity Benchmarks          │ 9,000 - 12,000   │
└─────────────────────────────────────────┴──────────────────┘
```

### 8.2 Dependency Graph (External Crates)
* `wasmtime`: WebAssembly JIT execution engine & linear memory sandboxing.
* `rustpython_parser`: Python 3 AST parsing.
* `tonic` / `tokio`: Async gRPC data plane & multithreaded runtime.
* `rayon`: Work-stealing CPU parallelization.
* `clap`: CLI argument parsing.

### 8.3 Security & Audit Surface
* **Safe Rust Guarantee:** All AST parsing, heuristic evaluation, network routing, and WASM host logic run in 100% safe Rust.
* **Bounded `unsafe` Surface:** `unsafe` blocks are strictly confined to C-FFI pointer dereferencing during direct memory pass-through calls (`< 200 LOC` total unsafe footprint).

---

## 9. Failure Modes & Edge Case Matrix

| Edge Case / Failure Scenario | Detection Mechanism | System Behavior & Mitigation |
| :--- | :--- | :--- |
| **Worker Node Death Mid-Loop** | gRPC/QUIC stream termination or heartbeat drop. | Aggregator re-routes unacknowledged chunk to standby worker; if unavailable, falls back to host native execution. |
| **Numeric Overflow in BCD Engine** | WASM arithmetic trap / flag check. | Triggers BCD overflow signal; `mbunta_run_slice` returns error code, forcing local host native fallback for the chunk. |
| **Network Partition (Gossip Loss)** | SWIM gossip ping timeout. | Node marks peers as unreachable, reverts to local sidecar execution mode (`--target=wasm` local). |
| **Low Arithmetic Intensity Loop** | Static analysis pass in `mbunta inspect`. | Automatically annotated as `STATUS=REJECTED`, defaults to native host execution unless `FORCE=TRUE` is set. |
| **Mainframe Hot-Swap Override** | Atomic byte read (`/dev/shm`) at chunk boundary. | Execution instantly routes to native IBM Metal code on next 50,000-record boundary ($< 2\text{ ns}$ delay). |
