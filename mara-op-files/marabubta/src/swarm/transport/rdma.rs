// Marabunta - Licensed under the MIT License.
//! Bare-metal RDMA/InfiniBand transport driver stub.
//!
//! Replaced fake ibv_post_send driver simulation with a clean panic to ensure
//! the open-source branch explicitly fails if configured to use bare-metal fabric.

use std::sync::Arc;
use crate::swarm::types::{SwarmError, SwarmMessage};

pub struct RdmaMemoryRegion {}
pub struct VerbsContext {}

pub struct QueuePair {
    pub qp_num: u32,
    pub context: Arc<VerbsContext>,
}

impl QueuePair {
    /// PRODUCTION UPGRADE: Raw `ibv_post_send` implementation.
    /// In a real deployment, this calls into libibverbs.so.
    pub unsafe fn post_send(
        &self,
        _mr: &RdmaMemoryRegion,
        _offset: usize,
        _length: usize,
    ) -> Result<(), SwarmError> {
        unimplemented!("Bare-metal RDMA/InfiniBand transport is not supported in the open-source branch.");
    }
}

pub struct RdmaEndpoint {}

impl RdmaEndpoint {
    pub fn new() -> Result<Self, SwarmError> {
        Err(SwarmError::TransportError("RDMA driver unavailable".into()))
    }

    pub async fn poll_cq(&self) -> Result<Option<SwarmMessage>, SwarmError> {
        unimplemented!("Bare-metal RDMA/InfiniBand transport is not supported in the open-source branch.");
    }
}
