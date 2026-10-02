# VOLUME 03: STRICT RESOURCE GOVERNANCE & ADVERSARIAL RESILIENCE

     3.0 The Preemption Hierarchy

     In a distributed supercomputer operating across heterogeneous and volatile edge nodes,
     resource contention is mathematically inevitable.

     Traditional clusters manage workload pressure via static queueing (e.g., Slurm) or dynamic
     auto-scaling. The Marabunta Swarm does not auto-scale infrastructure; it enforces Priority-
     Based Preemption.

     If a Tier-1 financial institution submits a cross-border trade-finance Anti-Money Laundering
     (AML) validation payload, the Swarm does not queue the job and wait for idle GPUs. It
     forcefully reclaims the silicon.


     The Implementation: src/preemption/engine.rs

     The Preemption Engine evaluates the execution environment and enforces a strict priority
     matrix (0-255).


        pub struct PreemptionEngine {
             policies: HashMap<QueueId, PreemptionPolicy>,
             active_tasks: RwLock<BTreeMap<TaskId, RunningTask>>,
             victim_selector: Box<dyn VictimSelector>,
        }


        impl PreemptionEngine {
             /// Evaluates if an incoming high-priority task requires evicting
        existing workloads.
             pub async fn evaluate_preemption(&self, incoming:
        &PreemptionRequest) -> Result<Vec<TaskId>, PreemptionError> {
                  let active = self.active_tasks.read().unwrap();


                  let available_resources =
        self.calculate_available_headroom(&active);




                    if incoming.required_resources <= available_resources {
                         return Ok(Vec::new()); // No preemption required
                    }


                    // The node is saturated. Identify lower-priority victims.
                    let candidates: Vec<&RunningTask> = active.values()
                         .filter(|t| t.priority < incoming.priority)
                         .collect();


                    if candidates.is_empty() {
                         return
        Err(PreemptionError::InsufficientLowerPriorityTasks);
                    }


                    // Select the optimal victims to minimize overall cluster
        disruption
                    let victims =
        self.victim_selector.select_victims(&candidates,
        incoming.required_resources);


                    if self.calculate_freed_resources(&victims) >=
        incoming.required_resources {
                         Ok(victims.into_iter().map(|v| v.id).collect())
                    } else {
                         Err(PreemptionError::PreemptionFailed) // Even evicting
        all lower tasks isn't enough
                    }
               }
        }



     Architectural Analysis: Resource Reclamation

            1. State Preservation: When the VictimSelector targets a running workload (e.g., a
              low-priority background log-parsing routine), the Swarm does not indiscriminately kill
              the process. It signals the MantisJournal to seal the current execution tape. The
              highly compressed .mrb-dump state is pushed to the local BlobStore, and the memory
              pages are freed for the incoming Tier-1 payload.
            2. The Resumption Contract: The preempted payload is not lost; it is temporarily
              orphaned. The Swarm's asynchronous routing layer eventually reallocates the .mrb-
              dump to an idle node in a different latency cohort, resuming execution precisely from
              the interrupted cycle.




     The Hard-Crash State Loss (Synchronous Witness Replication)

     A critical flaw emerges if an evicted payload's .mrb-dump is only saved to the local
     BlobStore. If the node suffers a hard physical crash (e.g., catastrophic power loss) before the
     tape replicates to the DHT, the workload state is permanently destroyed.

     To prevent state loss, the MantisJournal executes Synchronous Witness Replication.
     Before the Preemption Engine frees the RAM for the incoming high-priority task, the .mrb-
     dump must be successfully gossiped and acknowledged by at least two adjacent nodes in the
     5ms Latency Cohort (the Witness role). If the host node dies immediately afterward,
      Lazarus reconstructs the payload from the adjacent witnesses.



     3.1 The Chaos Engineering Engine

     To empirically guarantee Byzantine Fault Tolerance (BFT) across a 100-million node topology,
     theoretical proofs are insufficient. The architecture must be continuously validated against
     active, adversarial degradation.

     Marabunta embeds a native Chaos Engine ( src/chaos/ ) directly into the daemon. This is
     not an external testing tool; it is a core subsystem capable of artificially inducing catastrophic
     physical and cryptographic failures to measure the Swarm's autonomous recovery vectors.


     The Implementation: src/chaos/engine.rs

     The Chaos Engine forces the execution and transport layers into volatile states, simulating
     state-actor interference or physical infrastructure collapse.


        use crate::swarm::types::{NodeId, SwarmRole};


        pub struct ChaosEngine {
             enabled: bool,
             fault_probability: f64,
        }


        impl ChaosEngine {
             /// Simulates a catastrophic hardware or network failure mid-
        execution.
             pub async fn inject_fault(&self, current_role: &SwarmRole,
        target: &NodeId) {
                  if !self.enabled || rand::random::<f64>() >
        self.fault_probability {
                        return;
                  }




                    match rand::random::<u8>() % 4 {
                         0 => {
                               // Simulate physical power loss or SIGKILL
                               tracing::warn!("CHAOS SIMULATION: Executing simulated
        panic. Node dropping offline.");
                               std::process::abort();
                         },
                         1 => {
                               // Simulate a Byzantine Actor injecting corrupted
        gradients
                               tracing::warn!("CHAOS SIMULATION: Mutating WASM
        linear memory to corrupt STARK payload.");
                               self.corrupt_working_memory().await;
                         },
                         2 => {
                               // Simulate an ISP-level BGP blackhole or submarine
        cable cut
                               tracing::warn!("CHAOS SIMULATION: Dropping all TCP/
        QUIC routing tables. Simulating isolation.");
                               self.blackhole_network_interfaces().await;
                         },
                         _ => {
                               // Simulate a highly coordinated Sybil attack
        flooding the DHT
                               tracing::warn!("CHAOS SIMULATION: Spoofing Kademlia
        XOR metrics to saturate local buckets.");
                               self.flood_k_buckets().await;
                         }
                    }
               }
        }



     Architectural Analysis: Proving BFT Finality

            1. The Data Poisoning Simulation (Case 1): When the Chaos Engine deliberately mutates
              the WASM linear memory, it forces the node to generate a cryptographically invalid
              STARK receipt. This validates that the Harpy daemon is functioning correctly on the
              receiving nodes, confirming that corrupted payloads are instantly slashed and excluded
              from the IsomorphicStateRing .
            2. The Network Isolation Simulation (Case 2): When the engine blackholes the TCP/
              QUIC tables, it simulates a kinetic strike on a regional datacenter. The Swarm's recovery
              is measured by the time (Trecovery) it takes the surrounding Kademlia nodes to detect
              the dropped pings and autonomously re-route the active JobControlLanguage chunks
              to adjacent latency cohorts.


     By compiling the Chaos Engine into the core binary, Cambrian Radiation guarantees that the
     mathematical limits of the Marabunta Swarm are constantly stressed, measured, and verified
     under simulated hostile conditions.




