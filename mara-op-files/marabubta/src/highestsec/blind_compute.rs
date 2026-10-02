// Marabunta - Licensed under the MIT License.
//! Stub for the Fully Homomorphic Encryption (FHE) / Sealed Computing layer.
//! 
//! Provides hollow structs and traits for the open-source branch so that
//! the highestsec sandbox can compile without the proprietary tfhe/mpc libraries.

use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A traced FHE operation for the ZK-STARK engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FheOp {
    Add(u32, u32, u32),
    Mul(u32, u32, u32),
    CmpGt(u32, u32, u32),
    Mux(u32, u32, u32, u32),
    Custom(String),
}

/// Trait defining the operations a WASM Sandbox expects the host FHE engine to support.
pub trait FheRegistry: Send + Sync {
    fn fhe_add(&self, a: u32, b: u32) -> Option<u32>;
    fn fhe_mul(&self, a: u32, b: u32) -> Option<u32>;
    fn fhe_cmp_gt(&self, a: u32, b: u32) -> Option<u32>;
    fn fhe_mux(&self, cond: u32, a: u32, b: u32) -> Option<u32>;
    fn get_trace(&self) -> Vec<FheOp>;
    fn get_serialized(&self, handle: u32) -> Option<Vec<u8>>;
}

/// A stubbed registry that implements nothing but returns None,
/// since FHE is removed from this branch.
#[derive(Clone)]
pub struct CiphertextRegistry {}

impl CiphertextRegistry {
    pub fn new(_server_key: ()) -> Self {
        Self {}
    }

    pub fn insert_serialized(&self, _data: &[u8]) -> Option<u32> {
        None
    }
}

impl FheRegistry for CiphertextRegistry {
    fn fhe_add(&self, _a: u32, _b: u32) -> Option<u32> { None }
    fn fhe_mul(&self, _a: u32, _b: u32) -> Option<u32> { None }
    fn fhe_cmp_gt(&self, _a: u32, _b: u32) -> Option<u32> { None }
    fn fhe_mux(&self, _cond: u32, _a: u32, _b: u32) -> Option<u32> { None }
    fn get_trace(&self) -> Vec<FheOp> { Vec::new() }
    fn get_serialized(&self, _handle: u32) -> Option<Vec<u8>> { None }
}