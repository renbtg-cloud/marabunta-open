// Marabunta - Licensed under the MIT License.
//! WAN Bootstrap and NAT Traversal Module
//!
//! This module provides infrastructure for coordinator and node discovery
//! across the internet (WAN). It enables:
//!
//! - **Bootstrap Server**: Central registry where coordinators register and
//!   nodes discover available coordinators
//! - **NAT Traversal**: STUN-like functionality for nodes to discover their
//!   public IP and hole punching coordination for P2P connections
//! - **Relay Service**: Fallback for nodes behind symmetric NAT that cannot
//!   establish direct P2P connections
//!
//! # Architecture
//!
//! ```text
//!                    ┌─────────────────────┐
//!                    │  Bootstrap Server   │
//!                    │  (Public Internet)  │
//!                    └──────────┬──────────┘
//!                               │
//!          ┌────────────────────┼────────────────────┐
//!          │                    │                    │
//!    ┌─────▼─────┐        ┌─────▼─────┐        ┌─────▼─────┐
//!    │Coordinator│        │Coordinator│        │Coordinator│
//!    │  (Org A)  │        │  (Org B)  │        │  (Org C)  │
//!    └───────────┘        └───────────┘        └───────────┘
//!          │
//!    ┌─────┼─────┐
//!    │     │     │
//!  ┌─▼─┐ ┌─▼─┐ ┌─▼─┐
//!  │ N │ │ N │ │ N │  (Nodes)
//!  └───┘ └───┘ └───┘
//! ```
//!
//! # Usage
//!
//! ## Starting a Bootstrap Server
//!
//! ```rust,no_run
//! use marabunta_compute::wan::{BootstrapServer, BootstrapConfig};
//! use std::net::SocketAddr;
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = BootstrapConfig::default();
//!     let server = BootstrapServer::new(config);
//!     server.start().await.unwrap();
//! }
//! ```
//!
//! ## Registering a Coordinator
//!
//! ```rust,ignore
//! use marabunta_compute::wan::{BootstrapClient, CoordinatorRegistration, CapacityInfo, AuthToken};
//! use std::time::Duration;
//!
//! async fn register() {
//!     let mut client = BootstrapClient::new(vec!["bootstrap.example.com:9000".parse().unwrap()]);
//!     client.connect().await.unwrap();
//!
//!     let registration = CoordinatorRegistration {
//!         name: "my-coordinator".to_string(),
//!         public_addr: "192.168.1.100:8080".parse().unwrap(),
//!         internal_addr: None,
//!         organization: "MyOrg".to_string(),
//!         region: "us-west".to_string(),
//!         accepts_phantom: true,
//!         capacity: CapacityInfo::default(),
//!         auth_token: AuthToken::new("secret".to_string(), Duration::from_secs(3600)),
//!         metadata: None,
//!     };
//!     let id = client.register_coordinator(registration).await.unwrap();
//! }
//! ```
//!
//! ## Finding Coordinators (from a node)
//!
//! ```rust,no_run
//! use marabunta_compute::wan::{BootstrapClient, CoordinatorQuery};
//!
//! async fn find() {
//!     let mut client = BootstrapClient::new(vec!["bootstrap.example.com:9000".parse().unwrap()]);
//!     client.connect().await.unwrap();
//!
//!     let query = CoordinatorQuery {
//!         region: Some("us-west".to_string()),
//!         ..Default::default()
//!     };
//!     let coordinators = client.find_coordinators(query).await.unwrap();
//! }
//! ```

pub mod bootstrap_server;
pub mod client;
pub mod errors;
pub mod nat_traversal;
pub mod protocol;
pub mod relay;

// Re-export main types
pub use bootstrap_server::{BootstrapConfig, BootstrapServer, RegisteredCoordinator};
pub use client::{BootstrapClient, ClientConfig};
pub use errors::*;
pub use nat_traversal::{
    ConnectedPeer, ConnectionType, HolePunchCoordinator, HolePunchSession, NatTypeDetector,
    SessionState as HolePunchSessionState, StunService, connect_with_fallback,
};
pub use protocol::*;
pub use relay::{
    RelayConfig, RelayService, RelaySession, RelaySessionInfo, SessionState as RelaySessionState,
};
