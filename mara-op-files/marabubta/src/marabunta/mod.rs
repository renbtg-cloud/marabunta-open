// Marabunta - Licensed under the MIT License.
//! Marabunta Radiation Protocol
//!
//! Post-quantum resilient, adaptive communication layer for the Marabunta Compute swarm.
//! Implements fixed-frame transport, epidemic gossip, blind computation,
//! and hierarchical routing with FIPS 203/204 cryptographic primitives.

pub mod audit;
pub mod bpf_loader;
pub mod config;
pub mod crypto;
pub mod discovery;
pub mod federation;
pub mod frame;
pub mod gossip;
pub mod hierarchy;
pub mod identity;
pub mod neighborhood;
pub mod node;
pub mod sandbox;
pub mod traffic;
pub mod transition;
pub mod transport;
pub mod vault;
