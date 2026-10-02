<!-- Marabunta - Licensed under the MIT License.
## Chapter 13: Court of Arbitration & ZKP Slashing

The Spot Market operates on a fundamental contradiction: the system financially incentivizes nodes to execute workloads, but it relies on anonymous, untrusted hardware. 

If a Wall Street bank submits a Python Map-Reduce job to calculate the Value at Risk (VaR) of a $10 Billion portfolio, and the job is picked up by a gaming PC in a North Korean internet cafe, how does the bank know the PC didn't simply return a random number to steal the MMX reward without actually burning the electricity required to run the math?

This is known as the **Verifier's Dilemma**. 

If every node in the network has to re-run the calculation just to verify that the first node told the truth, the network does not scale. You are just repeating the same work 10 million times.

We must build a system where execution scales horizontally, but verification is absolute. We achieve this via **Optimistic Execution and The Court of Arbitration**.

### 13.1 The Janis & Jim Analysis: The Verifier's Dilemma

Jim pulled up the MMX payout ledger on the War Room terminal. 

"Janis," Jim said, "Node `RU-499` just claimed 500 MMX tokens for executing the VaR simulation. It submitted an output hash. How do we know the hash is real? Do we run a Zero-Knowledge Proof (ZKP) on every single transaction?"

"No," Janis said. "Generating a SNARK for every single WASM execution would add a 10x overhead to the network. We would lose all our thermodynamic efficiency. We use **Optimistic Execution**."

Janis explained that when a JCL manifest is submitted, the `MarketplaceEngine` does not assign it to one node, and it does not assign it to a thousand nodes. It assigns the payload to exactly **$N=3$** nodes.

"The three nodes run the WASM payload in complete isolation from each other," Janis said. "When they finish, they gossip their final 32-byte output hash to the Kademlia DHT."

*   Node A (Germany) reports: `0xA1B2...`
*   Node B (Brazil) reports: `0xA1B2...`
*   Node C (North Korea) reports: `0x9F88...`

"Look at the hashes," Janis said. "Germany and Brazil agree. North Korea disagrees."

"So North Korea is lying," Jim said. "They just guessed a random hash to save power and collect the reward."

"Probably," Janis nodded. "But in a zero-trust network, we don't operate on probability. We operate on mathematical certainty. Node C might have experienced a cosmic ray bit-flip in its RAM. Or, Node A and Node B might be colluding Sybil nodes trying to steal the reward, and Node C is actually the only honest one."

"If we don't know who is telling the truth," Jim asked, "how do we settle it?"

"We take them to Court."

### 13.2 The Deterministic Anchor (WASI Constraints)

Before a Court can rule on a dispute, the system must guarantee that a single "truth" actually exists. 

"If three different computers run the same Python script, they might naturally get slightly different answers," Jim argued. "Different CPU architectures handle floating-point math differently. What if Node A is an ARM Mac, and Node C is an Intel Xeon?"

"That is exactly why we do not run native Python," Janis corrected. "We run **WebAssembly**."

WebAssembly (`wasm32-wasip1`) is mathematically designed to be strictly deterministic. If you feed the exact same WASM binary the exact same input bytes, it will produce the exact same output bytes 100% of the time, regardless of the underlying hardware architecture.

To enforce this, the Marabunta `wasmtime` compiler explicitly disables non-deterministic features. Floating-point rounding operations (the classic IEEE 754 problem) are forced into strict deterministic modes. The WASI interface prevents the payload from reading the system clock or generating hardware-level entropy (random numbers). 

"Because the execution is perfectly blind," Janis concluded, "there is only one mathematically correct answer. Any deviation is proof of a lie."

### 13.3 The Court of Arbitration (The Jury)

When a hash collision occurs (Node A and B vs. Node C), the network automatically halts the MMX payout. The `MarketplaceEngine` invokes the **Court of Arbitration**.

The network autonomously selects 5 Heavy Boulders from the DHT to act as the Jury. These nodes must have an Elo score $> 2500$ (proving months of historical honesty) and must be geographically separated (enforced by their Zone Certificates).

To demonstrate the mathematical boundaries, consider this abstract architectural pattern defining the Arbitration Engine:

```rust
use wasmtime::{Engine, Module, Store};

pub struct ArbitrationEngine {
    pub jury_id: NodeId,
    pub engine: Engine,
}

impl ArbitrationEngine {
    /// The Jury node pulls the disputed payload and executes it locally 
    /// to determine the absolute, deterministic truth.
    pub fn execute_dispute(&self, wasm_bytes: &[u8], input_data: &[u8]) -> [u8; 32] {
        
        let module = Module::new(&self.engine, wasm_bytes)
            .expect("Jury failed to compile disputed module");
            
        let mut store = Store::new(&self.engine, ());
        
        // Execute the payload under strict deterministic bounds
        let instance = wasmtime::Instance::new(&mut store, &module, &[])
            .expect("Jury execution failed");
            
        let run_func = instance.get_typed_func::<&[u8], [u8; 32]>(&mut store, "execute")
            .expect("Invalid WASM ABI");
            
        // Return the definitive 32-byte hash
        run_func.call(&mut store, input_data).unwrap()
    }
}
```

"The 5 Jury nodes pull the WASM binary and the input data from the DHT," Janis explained. "They execute the payload locally. Because WASM is deterministic, the Jury will find the absolute truth."

The 5 Jury nodes calculate their hashes. They gossip them to the network.

*   Jury 1: `0xA1B2...`
*   Jury 2: `0xA1B2...`
*   Jury 3: `0xA1B2...`
*   Jury 4: `0xA1B2...`
*   Jury 5: `0xA1B2...`

"The Jury has ruled," Janis said. "The correct hash is `0xA1B2...`. Node C is mathematically proven to be a liar."

### 13.4 The Financial Guillotine (ZKP Slashing)

Jim stared at the terminal. "Okay. Node C lied to save $0.02 of electricity. We caught them. We don't pay them. But they can just spin up another node and try again. It costs them nothing to lie."

"In Marabunta," Janis said, her voice dropping, "you cannot compute without skin in the game. To accept a JCL payload, a node must cryptographically stake MMX tokens in a smart contract on the internal ledger."

Janis brought up the Court's final execution log.

```text
[15:42:01 INFO] Court of Arbitration: Consensus reached on URN:mrb:job:var-sim
[15:42:01 INFO] Verdict: Node [RU-499] is GUILTY of State Falsification.
[15:42:02 FATAL] Initiating Slashing Protocol against Node [RU-499].
[15:42:02 INFO] 5,000 MMX Stake Confiscated.
[15:42:03 INFO] 2,500 MMX Burned (Deflationary Event).
[15:42:03 INFO] 2,500 MMX Distributed to Arbitration Jury.
[15:42:04 FATAL] Node [RU-499] Elo slashed to 0. Public Key amputated from DHT.
```

"When Node C is found guilty," Janis explained, "the network mathematically confiscates 100% of Node C's staked MMX. We don't just withhold their reward; we seize their collateral."

Janis pointed to the distribution log. "50% of the slashed funds are permanently burned, removing them from circulation and increasing the value of all remaining MMX tokens. The other 50% is distributed to the 5 Jury nodes to compensate them for the electricity they burned running the arbitration compute."

"Finally," Janis concluded, "Node C's Elo score is slashed to 0. As we established in Chapter 12, the `TrafficShaper` drops all their packets, and their `bls12_381` public key is permanently blacklisted. They are amputated from the swarm."

Jim let out a slow breath. "They tried to save two cents on electricity, and we just burned five thousand dollars of their capital."

"Yes," Janis said. "We make lying financially ruinous. That is how a zero-trust network achieves absolute truth."

[Continue to Chapter 14: Federated ML Metacognition](./05-metacognition.md)