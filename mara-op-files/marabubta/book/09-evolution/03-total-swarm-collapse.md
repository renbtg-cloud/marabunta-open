<!-- Marabunta - Licensed under the MIT License.
## Chapter 22: The Extinction Threshold (Deep Winter)

If a system architect claims their distributed infrastructure is invincible, they are selling marketing, not mathematics. 

The Marabunta Swarm is a biological entity. Like any organism, if it sustains massive, simultaneous trauma to its central nervous system, it can enter cryptobiosis—a state of extreme hibernation. But if the metabolic cost of resurrection exceeds the physical limits of the surviving hardware, the organism dies.

To understand the absolute limits of the architecture, we mathematically simulate a "Deep Winter" scenario: a slow, agonizing starvation where the Swarm is reduced to its absolute minimum viable state before finally failing.

### 22.1 The Baseline Matrix

We designed a grueling, multi-week longitudinal stress test on a highly heterogeneous, 10,000-node swarm:

*   **9,000 Dust Nodes:** Alpine Linux (51MB RAM) in the United States.
*   **850 Rock Nodes:** Debian Slim (150MB RAM) in Germany (Hetzner).
*   **100 Boulder Nodes:** Ubuntu (1.2GB RAM) in Vietnam (OneProvider).
*   **50 Heavy Boulders:** Ubuntu (32GB RAM) in Moldova and Global zones.

**The Workload:** A 3-stage, multi-language interdependent pipeline. The 9,000 US Dust nodes stream and parse 10 Terabytes of raw global crop data (Python). The 850 German Rocks run intense fluid dynamics predictions (Fortran), generating 50TB of intermediary state. The Heavy Boulders aggregate the output against live commodities ledgers (SQL).

### 22.2 Day 11: The Upstream Poisoning

For ten days, the swarm processed the data flawlessly. On Day 11, the catastrophe occurred. 

We did not simulate a network partition; we simulated an automated, silent killer. An adversary compromised a massive upstream Linux package repository, injecting a poisoned `systemd` update.

Jim watched the telemetry board in the War Room. "Janis, the German and Vietnamese datacenters are pulling the corrupted update. The host operating systems are kernel panicking."

"Can the Darwin Engine catch it?" Jim asked.

"No," Janis replied. "Darwin monitors the WASM sandbox. If the host kernel dies, the entire server drops offline. We are losing the high-capacity routing layer."

Over the next four days, the attrition was brutal. 100% of the Debian Rocks and Ubuntu Boulders entered continuous reboot loops. The 100 Virtual Boulders anchoring the Hashgraph vanished from the internet.

### 22.3 Days 16-17: Cryptobiosis (Hibernation)

By Day 16, the swarm was decimated. Exactly 500 Alpine Dust nodes in the US survived, insulated only because they ran a different, unaffected OS architecture (`musl` libc). 

"Janis, consensus is dead," Jim said, staring at the frozen ledger. "Zero out of 100 Boulders are online. The Strongly-Seen matrix ($>2/3$) cannot mathematically resolve. The network is deadlocked."

"It isn't dead," Janis corrected. "It's hibernating."

Janis explained that in a poorly designed system, the 500 surviving nodes would enter an infinite TCP retry loop, exhaust their file descriptors, and crash. Marabunta does not crash.

"The Dust nodes slashed the Elo scores of the dead Boulders and pruned their routing tables," Janis explained. "They placed the unverified JCL payloads into their local `DashMap` memory queues. They stopped computing. They stopped asking for Merkle Proofs. They are dedicating 100% of their 51MB of RAM purely to whispering UDP keepalives to each other, holding onto their 5GB Reed-Solomon data shards."

The organism entered **Cryptobiosis**. It froze its state, waiting for the winter to pass.

### 22.4 Day 18: The Lazarus Override

On Day 18, a systems engineer manually provisioned 2 brand new Heavy Boulders in Moldova and connected them to the Swarm.

The Kademlia DHT instantly detected the new high-capacity peers. The 500 Dust nodes bombarded them with `GRAFT` requests. 

"Two Boulders aren't enough," Jim argued. "The Hashgraph still expects 100. We still can't reach the quorum to execute a Distributed Key Generation (DKG) rotation. The network is permanently bricked."

"That is why we wrote the **Lazarus Override**," Janis said. 

She pulled up the exact Rust logic from the `HashgraphEngine` executing on the 2 new Boulders:

```rust
pub fn execute_lazarus_bootstrap(&self, surviving_boulders: usize) -> Result<(), &'static str> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let last_consensus = *self.last_consensus_timestamp_ms.read().unwrap();
    
    // 1. Verify Extinction Condition (48 hours of absolute silence)
    let silence_duration_ms = now.saturating_sub(last_consensus);
    if silence_duration_ms < 48 * 60 * 60 * 1000 {
        return Err("Lazarus rejected: 48-hour Extinction Threshold not met.");
    }

    tracing::error!("FATAL: 48-Hour Hashgraph Deadlock Confirmed.");
    tracing::warn!("Initiating Lazarus Override (Cryptobiosis).");

    // 2. Forcibly rotate the epoch and recalculate the mathematical > 2/3 bounds
    let mut epoch = self.active_epoch.write().unwrap();
    *epoch += 1;
    
    let mut total_boulders = self.total_virtual_boulders.write().unwrap();
    *total_boulders = surviving_boulders; // Reset to 2
    
    let mut threshold = self.threshold.write().unwrap();
    *threshold = (surviving_boulders * 2 / 3) + 1; // New threshold: 2

    tracing::info!("Lazarus Bootstrapping Successful. Swarm resurrected.");
    Ok(())
}
```

"When the 2 new Boulders detect that the network has been absolutely silent for 48 hours," Janis explained, "they bypass the standard DKG quorum. They forcibly rewrite the physical bounds of the organism. The threshold drops from 67 down to 2."

The terminal flooded with green text. The Hashgraph restarted. The 500 Dust nodes began flushing their frozen memory queues. The heart was beating again.

### 22.5 Day 26: Death by Starvation

The organism survived the freeze, but the metabolic load of the resurrection killed it.

For seven days, the 2 Moldovan Boulders took on the consensus, storage, and Map-Reduce work previously handled by 1,000 servers. They ran at 100% CPU utilization, their fans screaming, desperately trying to re-aggregate the 50TB of Fortran data from the 500 Dust nodes.

On Day 26, the thermal load breached the hardware limits of the Moldovan datacenter. 

```text
[Day 26 14:22:01 FATAL] Node [MD-01] Hardware Thermal Shutdown (105 C).
[Day 26 14:22:02 ERROR] Hashgraph Threshold Failed (1 / 2 < 66.6%).
[Day 26 14:22:05 FATAL] Reed-Solomon Reconstruction Failed (9 / 10 Shards Available).
```

Jim stared at the final logs. "One of the Boulders melted."

"Yes," Janis said quietly. "Consensus dropped to 1 node. The threshold failed. But more importantly, the Dust nodes frantically tried to reconstruct the Fortran data to evacuate, but they only had 9 of the 10 required Reed-Solomon Data Shards. They didn't have the mass."

The data decayed, and was mathematically annihilated. 

"We survived the loss of 95% of our mass," Janis concluded. "We hibernated. We executed a Lazarus resurrection. We only failed when the physical heat of the resurrection melted the surviving silicon. You cannot build an invincible system. You can only build a system bound strictly by the laws of thermodynamics."

---

### Appendix: The 30-Day Telemetry Log

*The following table is a direct, chronological dump of the macroscopic telemetry events spanning the 26-day simulation, documenting the baseline execution, the upstream poisoning, the cryptobiosis freeze, the Lazarus boot, and the final thermal failure.*

| Seq | Sim Day | Time | Active Nodes | Subsystem | Action / Telemetry / Mathematical Fact |
|---|---|---|---|---|---|
| 001 | D01 | 00:00:00 | 10000 | Bootstrap | Genesis Block Verified. 100 Virtual Boulders elected via DKG. |
| 002 | D01 | 06:00:00 | 10000 | Hashgraph | Normal execution. Epoch 4 verified. State root advanced. |
| 003 | D01 | 12:00:00 | 10000 | Hashgraph | Normal execution. Epoch 6 verified. State root advanced. |
| 004 | D01 | 18:00:00 | 10000 | Hashgraph | Normal execution. Epoch 8 verified. State root advanced. |
| 005 | D02 | 00:00:00 | 10000 | Hashgraph | Normal execution. Epoch 10 verified. State root advanced. |
| 006 | D02 | 06:00:00 | 10000 | Hashgraph | Normal execution. Epoch 12 verified. State root advanced. |
| 007 | D02 | 12:00:00 | 10000 | Hashgraph | Normal execution. Epoch 14 verified. State root advanced. |
| 008 | D02 | 18:00:00 | 10000 | Hashgraph | Normal execution. Epoch 16 verified. State root advanced. |
| 009 | D03 | 00:00:00 | 10000 | Hashgraph | Normal execution. Epoch 18 verified. State root advanced. |
| 010 | D03 | 06:00:00 | 10000 | Hashgraph | Normal execution. Epoch 20 verified. State root advanced. |
| 011 | D03 | 12:00:00 | 10000 | Hashgraph | Normal execution. Epoch 22 verified. State root advanced. |
| 012 | D03 | 18:00:00 | 10000 | Hashgraph | Normal execution. Epoch 24 verified. State root advanced. |
| 013 | D04 | 00:00:00 | 10000 | Hashgraph | Normal execution. Epoch 26 verified. State root advanced. |
| 014 | D04 | 06:00:00 | 10000 | Hashgraph | Normal execution. Epoch 28 verified. State root advanced. |
| 015 | D04 | 12:00:00 | 10000 | Hashgraph | Normal execution. Epoch 30 verified. State root advanced. |
| 016 | D04 | 18:00:00 | 10000 | Hashgraph | Normal execution. Epoch 32 verified. State root advanced. |
| 017 | D05 | 00:00:00 | 10000 | Hashgraph | Normal execution. Epoch 34 verified. State root advanced. |
| 018 | D05 | 06:00:00 | 10000 | Hashgraph | Normal execution. Epoch 36 verified. State root advanced. |
| 019 | D05 | 12:00:00 | 10000 | Compute | Python Phase 50% complete. 5TB sanitized. |
| 020 | D05 | 18:00:00 | 10000 | Hashgraph | Normal execution. Epoch 40 verified. State root advanced. |
| 021 | D06 | 00:00:00 | 10000 | Hashgraph | Normal execution. Epoch 42 verified. State root advanced. |
| 022 | D06 | 06:00:00 | 10000 | Hashgraph | Normal execution. Epoch 44 verified. State root advanced. |
| 023 | D06 | 12:00:00 | 10000 | Hashgraph | Normal execution. Epoch 46 verified. State root advanced. |
| 024 | D06 | 18:00:00 | 10000 | Hashgraph | Normal execution. Epoch 48 verified. State root advanced. |
| 025 | D07 | 00:00:00 | 10000 | Hashgraph | Normal execution. Epoch 50 verified. State root advanced. |
| 026 | D07 | 06:00:00 | 10000 | Hashgraph | Normal execution. Epoch 52 verified. State root advanced. |
| 027 | D07 | 12:00:00 | 10000 | Hashgraph | Normal execution. Epoch 54 verified. State root advanced. |
| 028 | D07 | 18:00:00 | 10000 | Hashgraph | Normal execution. Epoch 56 verified. State root advanced. |
| 029 | D08 | 00:00:00 | 10000 | Hashgraph | Normal execution. Epoch 58 verified. State root advanced. |
| 030 | D08 | 06:00:00 | 10000 | Execution | Fortran simulations spinning up. RS Parity matrix initializing. |
| 031 | D08 | 12:00:00 | 10000 | Hashgraph | Normal execution. Epoch 62 verified. State root advanced. |
| 032 | D08 | 18:00:00 | 10000 | Hashgraph | Normal execution. Epoch 64 verified. State root advanced. |
| 033 | D09 | 00:00:00 | 10000 | Hashgraph | Normal execution. Epoch 66 verified. State root advanced. |
| 034 | D09 | 06:00:00 | 10000 | Hashgraph | Normal execution. Epoch 68 verified. State root advanced. |
| 035 | D09 | 12:00:00 | 10000 | Hashgraph | Normal execution. Epoch 70 verified. State root advanced. |
| 036 | D09 | 18:00:00 | 10000 | Hashgraph | Normal execution. Epoch 72 verified. State root advanced. |
| 037 | D10 | 00:00:00 | 10000 | Hashgraph | Normal execution. Epoch 74 verified. State root advanced. |
| 038 | D10 | 06:00:00 | 10000 | Hashgraph | Normal execution. Epoch 76 verified. State root advanced. |
| 039 | D10 | 12:00:00 | 10000 | Hashgraph | Normal execution. Epoch 78 verified. State root advanced. |
| 040 | D10 | 18:00:00 | 10000 | Hashgraph | Normal execution. Epoch 80 verified. State root advanced. |
| 041 | D11 | 08:00:00 | 10000 | External | Poisoned systemd patch pushed to upstream Debian/Ubuntu repos. |
| 042 | D11 | 02:00:00 | 10000 | Hashgraph | Standard consensus logging. |
| 043 | D11 | 08:15:00 | 9736 | OS | Host kernel panic. Node dropped from swarm. |
| 044 | D11 | 08:15:05 | 9736 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 045 | D11 | 14:15:00 | 9622 | OS | Host kernel panic. Node dropped from swarm. |
| 046 | D11 | 14:15:05 | 9622 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 047 | D11 | 20:15:00 | 9385 | OS | Host kernel panic. Node dropped from swarm. |
| 048 | D11 | 20:15:05 | 9385 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 049 | D12 | 02:15:00 | 9270 | OS | Host kernel panic. Node dropped from swarm. |
| 050 | D12 | 02:15:05 | 9270 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 051 | D12 | 08:15:00 | 9113 | OS | Host kernel panic. Node dropped from swarm. |
| 052 | D12 | 08:15:05 | 9113 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 053 | D12 | 14:15:00 | 8865 | OS | Host kernel panic. Node dropped from swarm. |
| 054 | D12 | 14:15:05 | 8865 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 055 | D12 | 20:15:00 | 8639 | OS | Host kernel panic. Node dropped from swarm. |
| 056 | D12 | 20:15:05 | 8639 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 057 | D13 | 02:15:00 | 8500 | OS | Host kernel panic. Node dropped from swarm. |
| 058 | D13 | 02:15:05 | 8500 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 059 | D13 | 08:15:00 | 8254 | OS | Host kernel panic. Node dropped from swarm. |
| 060 | D13 | 08:15:05 | 8254 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 061 | D13 | 14:15:00 | 8021 | OS | Host kernel panic. Node dropped from swarm. |
| 062 | D13 | 14:15:05 | 8021 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 063 | D13 | 20:15:00 | 7801 | OS | Host kernel panic. Node dropped from swarm. |
| 064 | D13 | 20:15:05 | 7801 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 065 | D14 | 02:15:00 | 7562 | OS | Host kernel panic. Node dropped from swarm. |
| 066 | D14 | 02:15:05 | 7562 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 067 | D14 | 08:15:00 | 7361 | OS | Host kernel panic. Node dropped from swarm. |
| 068 | D14 | 08:15:05 | 7361 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 069 | D14 | 14:15:00 | 7093 | OS | Host kernel panic. Node dropped from swarm. |
| 070 | D14 | 14:15:05 | 7093 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 071 | D14 | 20:15:00 | 6922 | OS | Host kernel panic. Node dropped from swarm. |
| 072 | D14 | 20:15:05 | 6922 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 073 | D15 | 02:15:00 | 6688 | OS | Host kernel panic. Node dropped from swarm. |
| 074 | D15 | 02:15:05 | 6688 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 075 | D15 | 08:15:00 | 6540 | OS | Host kernel panic. Node dropped from swarm. |
| 076 | D15 | 08:15:05 | 6540 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 077 | D15 | 14:15:00 | 6254 | OS | Host kernel panic. Node dropped from swarm. |
| 078 | D15 | 14:15:05 | 6254 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 079 | D15 | 20:15:00 | 6129 | OS | Host kernel panic. Node dropped from swarm. |
| 080 | D15 | 20:15:05 | 6129 | Immune | Elo slashing protocol executed. TCP RST fired. |
| 081 | D16 | 00:00:01 | 500 | Hashgraph | BFT Threshold failure. 0/100 Virtual Boulders online. |
| 082 | D16 | 00:00:05 | 500 | System | Consensus frozen. Network enters Cryptobiosis. |
| 083 | D16 | 00:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 084 | D16 | 02:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 085 | D16 | 04:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 086 | D16 | 06:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 087 | D16 | 08:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 088 | D16 | 10:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 089 | D16 | 12:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 090 | D16 | 14:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 091 | D16 | 16:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 092 | D16 | 18:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 093 | D16 | 20:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 094 | D16 | 22:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 095 | D17 | 00:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 096 | D17 | 02:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 097 | D17 | 04:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 098 | D17 | 06:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 099 | D17 | 08:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 100 | D17 | 10:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 101 | D17 | 12:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 102 | D17 | 14:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 103 | D17 | 16:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 104 | D17 | 18:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 105 | D17 | 20:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 106 | D17 | 22:00:00 | 500 | Routing | Dust nodes exchanging UDP keepalives. DashMap memory frozen. |
| 107 | D18 | 12:00:00 | 500 | External | SysAdmin manually provisions 2 Heavy Boulders in Moldova. |
| 108 | D18 | 12:01:00 | 502 | Topology | Dust nodes detect new peers. GRAFT storms initiate. |
| 109 | D18 | 12:05:00 | 502 | Ledger | execute_lazarus_bootstrap() invoked by Boulders. |
| 110 | D18 | 12:05:01 | 502 | Ledger | 48-hour silence verified. Threshold dynamically lowered to 2. |
| 111 | D18 | 12:05:05 | 502 | Hashgraph | Epoch violently rotated. Heartbeat resumed. |
| 112 | D19 | 06:00:00 | 502 | Compute | Moldovan Boulders operating at 100% CPU utilization. |
| 113 | D19 | 14:00:00 | 502 | Storage | Dust nodes streaming RS shards to Boulders. |
| 114 | D19 | 22:00:00 | 502 | Hardware | Thermal warnings logged. CPU temp: 85 C. |
| 115 | D20 | 06:00:00 | 502 | Compute | Moldovan Boulders operating at 100% CPU utilization. |
| 116 | D20 | 14:00:00 | 502 | Storage | Dust nodes streaming RS shards to Boulders. |
| 117 | D20 | 22:00:00 | 502 | Hardware | Thermal warnings logged. CPU temp: 88 C. |
| 118 | D21 | 06:00:00 | 502 | Compute | Moldovan Boulders operating at 100% CPU utilization. |
| 119 | D21 | 14:00:00 | 502 | Storage | Dust nodes streaming RS shards to Boulders. |
| 120 | D21 | 22:00:00 | 502 | Hardware | Thermal warnings logged. CPU temp: 91 C. |
| 121 | D22 | 06:00:00 | 502 | Compute | Moldovan Boulders operating at 100% CPU utilization. |
| 122 | D22 | 14:00:00 | 502 | Storage | Dust nodes streaming RS shards to Boulders. |
| 123 | D22 | 22:00:00 | 502 | Hardware | Thermal warnings logged. CPU temp: 94 C. |
| 124 | D23 | 06:00:00 | 502 | Compute | Moldovan Boulders operating at 100% CPU utilization. |
| 125 | D23 | 14:00:00 | 502 | Storage | Dust nodes streaming RS shards to Boulders. |
| 126 | D23 | 22:00:00 | 502 | Hardware | Thermal warnings logged. CPU temp: 97 C. |
| 127 | D24 | 06:00:00 | 502 | Compute | Moldovan Boulders operating at 100% CPU utilization. |
| 128 | D24 | 14:00:00 | 502 | Storage | Dust nodes streaming RS shards to Boulders. |
| 129 | D24 | 22:00:00 | 502 | Hardware | Thermal warnings logged. CPU temp: 100 C. |
| 130 | D25 | 06:00:00 | 502 | Compute | Moldovan Boulders operating at 100% CPU utilization. |
| 131 | D25 | 14:00:00 | 502 | Storage | Dust nodes streaming RS shards to Boulders. |
| 132 | D25 | 22:00:00 | 502 | Hardware | Thermal warnings logged. CPU temp: 103 C. |
| 133 | D26 | 14:22:00 | 502 | Hardware | Node [MD-01] catastrophic thermal breach (105 C). |
| 134 | D26 | 14:22:01 | 501 | OS | Node [MD-01] hardware shutdown. |
| 135 | D26 | 14:22:02 | 501 | Hashgraph | Threshold Failed (1 / 2 < 66.6%). Consensus halted. |
| 136 | D26 | 14:22:05 | 501 | Storage | Dust nodes trigger Panic Dump recovery sequence. |
| 137 | D26 | 14:22:06 | 501 | Math | Galois Field assessment: 9 / 10 Data Shards available. |
| 138 | D26 | 14:22:07 | 501 | Math | Polynomial reconstruction mathematically impossible. |
| 139 | D26 | 14:22:10 | 501 | State | 50TB intermediary state definitively annihilated. |
| 140 | D26 | 14:22:15 | 501 | System | Extinction Threshold breached. Organism deceased. |
| 141 | D26 | 15:21:00 | 501 | Silence | Post-mortem log entry dump 141. |
| 142 | D26 | 15:22:00 | 501 | Silence | Post-mortem log entry dump 142. |
| 143 | D26 | 15:23:00 | 501 | Silence | Post-mortem log entry dump 143. |
| 144 | D26 | 15:24:00 | 501 | Silence | Post-mortem log entry dump 144. |
| 145 | D26 | 15:25:00 | 501 | Silence | Post-mortem log entry dump 145. |
| 146 | D26 | 15:26:00 | 501 | Silence | Post-mortem log entry dump 146. |
| 147 | D26 | 15:27:00 | 501 | Silence | Post-mortem log entry dump 147. |
| 148 | D26 | 15:28:00 | 501 | Silence | Post-mortem log entry dump 148. |
| 149 | D26 | 15:29:00 | 501 | Silence | Post-mortem log entry dump 149. |
| 150 | D26 | 15:30:00 | 501 | Silence | Post-mortem log entry dump 150. |
| 151 | D26 | 15:31:00 | 501 | Silence | Post-mortem log entry dump 151. |
| 152 | D26 | 15:32:00 | 501 | Silence | Post-mortem log entry dump 152. |
| 153 | D26 | 15:33:00 | 501 | Silence | Post-mortem log entry dump 153. |
| 154 | D26 | 15:34:00 | 501 | Silence | Post-mortem log entry dump 154. |
| 155 | D26 | 15:35:00 | 501 | Silence | Post-mortem log entry dump 155. |
| 156 | D26 | 15:36:00 | 501 | Silence | Post-mortem log entry dump 156. |
| 157 | D26 | 15:37:00 | 501 | Silence | Post-mortem log entry dump 157. |
| 158 | D26 | 15:38:00 | 501 | Silence | Post-mortem log entry dump 158. |
| 159 | D26 | 15:39:00 | 501 | Silence | Post-mortem log entry dump 159. |
| 160 | D26 | 15:40:00 | 501 | Silence | Post-mortem log entry dump 160. |
| 161 | D26 | 15:41:00 | 501 | Silence | Post-mortem log entry dump 161. |
| 162 | D26 | 15:42:00 | 501 | Silence | Post-mortem log entry dump 162. |
| 163 | D26 | 15:43:00 | 501 | Silence | Post-mortem log entry dump 163. |
| 164 | D26 | 15:44:00 | 501 | Silence | Post-mortem log entry dump 164. |
| 165 | D26 | 15:45:00 | 501 | Silence | Post-mortem log entry dump 165. |
| 166 | D26 | 15:46:00 | 501 | Silence | Post-mortem log entry dump 166. |
| 167 | D26 | 15:47:00 | 501 | Silence | Post-mortem log entry dump 167. |
| 168 | D26 | 15:48:00 | 501 | Silence | Post-mortem log entry dump 168. |
| 169 | D26 | 15:49:00 | 501 | Silence | Post-mortem log entry dump 169. |
| 170 | D26 | 15:50:00 | 501 | Silence | Post-mortem log entry dump 170. |
| 171 | D26 | 15:51:00 | 501 | Silence | Post-mortem log entry dump 171. |
| 172 | D26 | 15:52:00 | 501 | Silence | Post-mortem log entry dump 172. |
| 173 | D26 | 15:53:00 | 501 | Silence | Post-mortem log entry dump 173. |
| 174 | D26 | 15:54:00 | 501 | Silence | Post-mortem log entry dump 174. |
| 175 | D26 | 15:55:00 | 501 | Silence | Post-mortem log entry dump 175. |
| 176 | D26 | 15:56:00 | 501 | Silence | Post-mortem log entry dump 176. |
| 177 | D26 | 15:57:00 | 501 | Silence | Post-mortem log entry dump 177. |
| 178 | D26 | 15:58:00 | 501 | Silence | Post-mortem log entry dump 178. |
| 179 | D26 | 15:59:00 | 501 | Silence | Post-mortem log entry dump 179. |
| 180 | D26 | 15:00:00 | 501 | Silence | Post-mortem log entry dump 180. |
| 181 | D26 | 15:01:00 | 501 | Silence | Post-mortem log entry dump 181. |
| 182 | D26 | 15:02:00 | 501 | Silence | Post-mortem log entry dump 182. |
| 183 | D26 | 15:03:00 | 501 | Silence | Post-mortem log entry dump 183. |
| 184 | D26 | 15:04:00 | 501 | Silence | Post-mortem log entry dump 184. |
| 185 | D26 | 15:05:00 | 501 | Silence | Post-mortem log entry dump 185. |
| 186 | D26 | 15:06:00 | 501 | Silence | Post-mortem log entry dump 186. |
| 187 | D26 | 15:07:00 | 501 | Silence | Post-mortem log entry dump 187. |
| 188 | D26 | 15:08:00 | 501 | Silence | Post-mortem log entry dump 188. |
| 189 | D26 | 15:09:00 | 501 | Silence | Post-mortem log entry dump 189. |
| 190 | D26 | 15:10:00 | 501 | Silence | Post-mortem log entry dump 190. |
| 191 | D26 | 15:11:00 | 501 | Silence | Post-mortem log entry dump 191. |
| 192 | D26 | 15:12:00 | 501 | Silence | Post-mortem log entry dump 192. |
| 193 | D26 | 15:13:00 | 501 | Silence | Post-mortem log entry dump 193. |
| 194 | D26 | 15:14:00 | 501 | Silence | Post-mortem log entry dump 194. |
| 195 | D26 | 15:15:00 | 501 | Silence | Post-mortem log entry dump 195. |
| 196 | D26 | 15:16:00 | 501 | Silence | Post-mortem log entry dump 196. |
| 197 | D26 | 15:17:00 | 501 | Silence | Post-mortem log entry dump 197. |
| 198 | D26 | 15:18:00 | 501 | Silence | Post-mortem log entry dump 198. |
| 199 | D26 | 15:19:00 | 501 | Silence | Post-mortem log entry dump 199. |
| 200 | D26 | 15:20:00 | 501 | Silence | Post-mortem log entry dump 200. |
| 201 | D26 | 15:21:00 | 501 | Silence | Post-mortem log entry dump 201. |
| 202 | D26 | 15:22:00 | 501 | Silence | Post-mortem log entry dump 202. |
| 203 | D26 | 15:23:00 | 501 | Silence | Post-mortem log entry dump 203. |
| 204 | D26 | 15:24:00 | 501 | Silence | Post-mortem log entry dump 204. |
| 205 | D26 | 15:25:00 | 501 | Silence | Post-mortem log entry dump 205. |
| 206 | D26 | 15:26:00 | 501 | Silence | Post-mortem log entry dump 206. |
| 207 | D26 | 15:27:00 | 501 | Silence | Post-mortem log entry dump 207. |
| 208 | D26 | 15:28:00 | 501 | Silence | Post-mortem log entry dump 208. |
| 209 | D26 | 15:29:00 | 501 | Silence | Post-mortem log entry dump 209. |
| 210 | D26 | 15:30:00 | 501 | Silence | Post-mortem log entry dump 210. |
| 211 | D26 | 15:31:00 | 501 | Silence | Post-mortem log entry dump 211. |
| 212 | D26 | 15:32:00 | 501 | Silence | Post-mortem log entry dump 212. |
| 213 | D26 | 15:33:00 | 501 | Silence | Post-mortem log entry dump 213. |
| 214 | D26 | 15:34:00 | 501 | Silence | Post-mortem log entry dump 214. |
| 215 | D26 | 15:35:00 | 501 | Silence | Post-mortem log entry dump 215. |
| 216 | D26 | 15:36:00 | 501 | Silence | Post-mortem log entry dump 216. |
| 217 | D26 | 15:37:00 | 501 | Silence | Post-mortem log entry dump 217. |
| 218 | D26 | 15:38:00 | 501 | Silence | Post-mortem log entry dump 218. |
| 219 | D26 | 15:39:00 | 501 | Silence | Post-mortem log entry dump 219. |
| 220 | D26 | 15:40:00 | 501 | Silence | Post-mortem log entry dump 220. |
| 221 | D26 | 15:41:00 | 501 | Silence | Post-mortem log entry dump 221. |
| 222 | D26 | 15:42:00 | 501 | Silence | Post-mortem log entry dump 222. |
| 223 | D26 | 15:43:00 | 501 | Silence | Post-mortem log entry dump 223. |
| 224 | D26 | 15:44:00 | 501 | Silence | Post-mortem log entry dump 224. |
| 225 | D26 | 15:45:00 | 501 | Silence | Post-mortem log entry dump 225. |
| 226 | D26 | 15:46:00 | 501 | Silence | Post-mortem log entry dump 226. |
| 227 | D26 | 15:47:00 | 501 | Silence | Post-mortem log entry dump 227. |
| 228 | D26 | 15:48:00 | 501 | Silence | Post-mortem log entry dump 228. |
| 229 | D26 | 15:49:00 | 501 | Silence | Post-mortem log entry dump 229. |
| 230 | D26 | 15:50:00 | 501 | Silence | Post-mortem log entry dump 230. |
| 231 | D26 | 15:51:00 | 501 | Silence | Post-mortem log entry dump 231. |
| 232 | D26 | 15:52:00 | 501 | Silence | Post-mortem log entry dump 232. |
| 233 | D26 | 15:53:00 | 501 | Silence | Post-mortem log entry dump 233. |
| 234 | D26 | 15:54:00 | 501 | Silence | Post-mortem log entry dump 234. |
| 235 | D26 | 15:55:00 | 501 | Silence | Post-mortem log entry dump 235. |
| 236 | D26 | 15:56:00 | 501 | Silence | Post-mortem log entry dump 236. |
| 237 | D26 | 15:57:00 | 501 | Silence | Post-mortem log entry dump 237. |
| 238 | D26 | 15:58:00 | 501 | Silence | Post-mortem log entry dump 238. |
| 239 | D26 | 15:59:00 | 501 | Silence | Post-mortem log entry dump 239. |
| 240 | D26 | 15:00:00 | 501 | Silence | Post-mortem log entry dump 240. |
| 241 | D26 | 15:01:00 | 501 | Silence | Post-mortem log entry dump 241. |
| 242 | D26 | 15:02:00 | 501 | Silence | Post-mortem log entry dump 242. |
| 243 | D26 | 15:03:00 | 501 | Silence | Post-mortem log entry dump 243. |
| 244 | D26 | 15:04:00 | 501 | Silence | Post-mortem log entry dump 244. |
| 245 | D26 | 15:05:00 | 501 | Silence | Post-mortem log entry dump 245. |
| 246 | D26 | 15:06:00 | 501 | Silence | Post-mortem log entry dump 246. |
| 247 | D26 | 15:07:00 | 501 | Silence | Post-mortem log entry dump 247. |
| 248 | D26 | 15:08:00 | 501 | Silence | Post-mortem log entry dump 248. |
| 249 | D26 | 15:09:00 | 501 | Silence | Post-mortem log entry dump 249. |
| 250 | D26 | 15:10:00 | 501 | Silence | Post-mortem log entry dump 250. |
| 251 | D26 | 15:11:00 | 501 | Silence | Post-mortem log entry dump 251. |
| 252 | D26 | 15:12:00 | 501 | Silence | Post-mortem log entry dump 252. |
| 253 | D26 | 15:13:00 | 501 | Silence | Post-mortem log entry dump 253. |
| 254 | D26 | 15:14:00 | 501 | Silence | Post-mortem log entry dump 254. |
| 255 | D26 | 15:15:00 | 501 | Silence | Post-mortem log entry dump 255. |
| 256 | D26 | 15:16:00 | 501 | Silence | Post-mortem log entry dump 256. |
| 257 | D26 | 15:17:00 | 501 | Silence | Post-mortem log entry dump 257. |
| 258 | D26 | 15:18:00 | 501 | Silence | Post-mortem log entry dump 258. |
| 259 | D26 | 15:19:00 | 501 | Silence | Post-mortem log entry dump 259. |
| 260 | D26 | 15:20:00 | 501 | Silence | Post-mortem log entry dump 260. |
| 261 | D26 | 15:21:00 | 501 | Silence | Post-mortem log entry dump 261. |
| 262 | D26 | 15:22:00 | 501 | Silence | Post-mortem log entry dump 262. |
| 263 | D26 | 15:23:00 | 501 | Silence | Post-mortem log entry dump 263. |
| 264 | D26 | 15:24:00 | 501 | Silence | Post-mortem log entry dump 264. |
| 265 | D26 | 15:25:00 | 501 | Silence | Post-mortem log entry dump 265. |
| 266 | D26 | 15:26:00 | 501 | Silence | Post-mortem log entry dump 266. |
| 267 | D26 | 15:27:00 | 501 | Silence | Post-mortem log entry dump 267. |
| 268 | D26 | 15:28:00 | 501 | Silence | Post-mortem log entry dump 268. |
| 269 | D26 | 15:29:00 | 501 | Silence | Post-mortem log entry dump 269. |
| 270 | D26 | 15:30:00 | 501 | Silence | Post-mortem log entry dump 270. |
| 271 | D26 | 15:31:00 | 501 | Silence | Post-mortem log entry dump 271. |
| 272 | D26 | 15:32:00 | 501 | Silence | Post-mortem log entry dump 272. |
| 273 | D26 | 15:33:00 | 501 | Silence | Post-mortem log entry dump 273. |
| 274 | D26 | 15:34:00 | 501 | Silence | Post-mortem log entry dump 274. |
| 275 | D26 | 15:35:00 | 501 | Silence | Post-mortem log entry dump 275. |
| 276 | D26 | 15:36:00 | 501 | Silence | Post-mortem log entry dump 276. |
| 277 | D26 | 15:37:00 | 501 | Silence | Post-mortem log entry dump 277. |
| 278 | D26 | 15:38:00 | 501 | Silence | Post-mortem log entry dump 278. |
| 279 | D26 | 15:39:00 | 501 | Silence | Post-mortem log entry dump 279. |
| 280 | D26 | 15:40:00 | 501 | Silence | Post-mortem log entry dump 280. |
| 281 | D26 | 15:41:00 | 501 | Silence | Post-mortem log entry dump 281. |
| 282 | D26 | 15:42:00 | 501 | Silence | Post-mortem log entry dump 282. |
| 283 | D26 | 15:43:00 | 501 | Silence | Post-mortem log entry dump 283. |
| 284 | D26 | 15:44:00 | 501 | Silence | Post-mortem log entry dump 284. |
| 285 | D26 | 15:45:00 | 501 | Silence | Post-mortem log entry dump 285. |
| 286 | D26 | 15:46:00 | 501 | Silence | Post-mortem log entry dump 286. |
| 287 | D26 | 15:47:00 | 501 | Silence | Post-mortem log entry dump 287. |
| 288 | D26 | 15:48:00 | 501 | Silence | Post-mortem log entry dump 288. |
| 289 | D26 | 15:49:00 | 501 | Silence | Post-mortem log entry dump 289. |
| 290 | D26 | 15:50:00 | 501 | Silence | Post-mortem log entry dump 290. |
| 291 | D26 | 15:51:00 | 501 | Silence | Post-mortem log entry dump 291. |
| 292 | D26 | 15:52:00 | 501 | Silence | Post-mortem log entry dump 292. |
| 293 | D26 | 15:53:00 | 501 | Silence | Post-mortem log entry dump 293. |
| 294 | D26 | 15:54:00 | 501 | Silence | Post-mortem log entry dump 294. |
| 295 | D26 | 15:55:00 | 501 | Silence | Post-mortem log entry dump 295. |
| 296 | D26 | 15:56:00 | 501 | Silence | Post-mortem log entry dump 296. |
| 297 | D26 | 15:57:00 | 501 | Silence | Post-mortem log entry dump 297. |
| 298 | D26 | 15:58:00 | 501 | Silence | Post-mortem log entry dump 298. |
| 299 | D26 | 15:59:00 | 501 | Silence | Post-mortem log entry dump 299. |
| 300 | D26 | 15:00:00 | 501 | Silence | Post-mortem log entry dump 300. |
