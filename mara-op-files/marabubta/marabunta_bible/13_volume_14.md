# VOLUME 14: THE SOVEREIGNTY OF CONTROL (BINARY SEGMENTATION)

     14.0 The Illusion of Decentralized Chaos

     A peer-to-peer swarm scaling to 100 million or more (e.g., 15 billion) nodes presents a profound optical risk to
     enterprise architects: the illusion of unmanageable chaos. If an infrastructure possesses no
     centralized AWS control plane to physically unplug, how does a Fortune 500 bank guarantee
     that a misconfigured algorithm won't permanently saturate the global network or leak
     proprietary state?

     Marabunta resolves this paradox through Asymmetric Binary Segmentation and
     cryptographic authority.

     The Swarm is biologically decentralized at the execution and routing layers, but it is strictly,
     mathematically authoritarian at the command layer.



     14.1 The Monopoly of the Gateway

     The Marabunta architecture distributes two fundamentally different binaries.

     The software available to the 100 million or more (e.g., 15 billion) public "Ephemeral" nodes—the marabunta-visor
     —is a neutered execution terminal. It contains the Chrysalis PoW grinder, the Kademlia DHT
     routing logic, and the wasm32-wasi sandbox. Crucially, it physically lacks the
      Assimilator and Neuromancer orchestration pipelines.


     A standard node cannot compile Abstract Syntax Trees (ASTs), it cannot parse Job Control
     Language (MCL) manifests, and it cannot inject new workloads into the Swarm.

     The acquiring enterprise possesses the proprietary Gateway Assimilator binary. This is the
     only software on Earth containing the ed25519 Root Keys capable of cryptographically
     signing and injecting a workload that the Harpy daemons will accept. The enterprise
     maintains an absolute, cryptographic monopoly over the computational capacity of the public
     mesh.




     14.2 Frictionless Infiltration (The Browser Node)

     Deploying a custom 14MB Rust binary requiring raw network sockets across a locked-down,
     50,000-seat corporate IT fleet is an insurmountable friction point. Enterprise workstations and
     zero-trust government laptops will actively block the installation.

     Marabunta bypasses OS-level deployment friction entirely.

     The marabunta-visor daemon is not just compiled for x86_64-linux or aarch64-
     apple-darwin . The entire Kademlia routing engine and WASM execution hypervisor
     compiles down to wasm32-unknown-unknown .


     Implementation: src/web_ui/static/worker-exec.js

     An enterprise employee simply opens an internal corporate portal in Google Chrome or
     Microsoft Edge. A background Web Worker instantly boots a full Marabunta node entirely
     within the browser's V8 JavaScript engine.


        // The Marabunta Web Worker: Bootstrapping a node without OS
        installation
        import init, { WasmNode } from './marabunta_wasm.js';


        async function bootstrapBrowserNode() {
             await init();


             // The browser node generates an ephemeral identity and connects
             // to the enterprise Swarm via Secure WebSockets (WSS) or WebRTC.
             const node = new WasmNode();
             await node.connect_to_gateway("wss://gateway.internal.corp");


             // The V8 engine now functions as a Tier-0 Worker, silently
        executing
             // distributed WASM MapReduce chunks in the background tab.
             node.start_execution_loop();
        }



     The corporation instantly possesses a 50,000-node private supercomputer aggregating data
     across their global offices. It requires zero administrative privileges, zero .msi or .pkg
     installers, and zero IT support tickets. The network simply exists wherever a browser tab is
     open.




     14.3 Cryptographic Kill-Switches (Epidemic
     Revocation)

     If an administrator accidentally deploys a critical financial payload containing an infinite loop
     or a catastrophic logic error, they cannot SSH into 100 million or more (e.g., 15 billion) edge nodes to kill the processes.

     Instead, they execute an Epidemic Revocation.

         1. The Poison Pill: The Gateway Administrator cryptographically signs a 256-byte
            Revocation Certificate with their Root Key, targeting the specific JobId .
         2. The Gossip: The certificate is injected into the Kademlia DHT. Utilizing the Plumtree
            protocol, the kill-order propagates to 100 million or more (e.g., 15 billion) nodes in $O(\log N)$ time (typically
            under 5 seconds for 15 billion devices).
         3. The Guillotine: Every marabunta-visor receives the gossip, cryptographically
            verifies the Root Key signature against the Genesis Block, and instantly drops the target
            workload. The daemon flushes the linear memory of the WASM sandbox and
            permanently erases the .mrb-dump journals from the local BlobStore.

     The Swarm is autonomous, but the Administrator's cryptographic signature holds absolute,
     instantaneous veto power over the physics of the network.




