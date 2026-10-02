// Marabunta - Licensed under the MIT License.
//! WASM sandbox for deterministic blind computation.
//!
//! Provides a sandboxed execution environment with fuel metering, memory limits,
//! and deterministic RNG. Currently uses a lightweight mock executor;
//! wasmtime integration can be enabled via feature flag.

use crate::marabunta::config;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Sandbox configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxConfig {
    pub max_memory_mb: u32,
    pub max_execution_time_s: u64,
    pub max_fuel: u64,
    pub max_wasm_size_mb: u32,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: config::MAX_WASM_MEMORY_MB,
            max_execution_time_s: config::MAX_EXECUTION_TIME_S,
            max_fuel: config::DEFAULT_WASM_FUEL,
            max_wasm_size_mb: config::MAX_WASM_BINARY_MB,
        }
    }
}

/// Deterministic crash snapshot for time-travel debugging.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrashSnapshot {
    pub wasm_instruction_pointer: u64,
    pub memory_dump: Vec<u8>,
    pub registers: std::collections::HashMap<String, u64>,
    pub fuel_at_crash: u64,
    pub input_provided: Vec<u8>,
}

impl std::fmt::Display for CrashSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CrashSnapshot(ip: {}, fuel: {})", self.wasm_instruction_pointer, self.fuel_at_crash)
    }
}

/// Sandbox execution errors.
#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("WASM binary too large: {0} MB (max {1} MB)")]
    BinaryTooLarge(u32, u32),

    #[error("WASM validation failed: {0}")]
    ValidationFailed(String),

    #[error("execution timed out after {0}s")]
    Timeout(u64),

    #[error("fuel exhausted")]
    FuelExhausted,

    #[error("memory limit exceeded: {0} MB")]
    MemoryExceeded(u32),

    #[error("execution error: {0}")]
    ExecutionError(String),

    #[error("deterministic panic: snapshot captured")]
    PanicWithSnapshot(CrashSnapshot),
}

/// Result of sandbox execution.
#[derive(Debug, Clone)]
pub struct SandboxResult {
    pub output: Vec<u8>,
    pub fuel_consumed: u64,
    pub memory_peak_mb: u32,
    pub execution_time_ms: u64,
}

/// WASM sandbox executor.
pub struct WasmSandbox {
    config: SandboxConfig,
}

impl WasmSandbox {
    pub fn new(config: SandboxConfig) -> Self {
        Self { config }
    }

    /// Validate a WASM binary before execution.
    pub fn validate_wasm(&self, wasm_bytes: &[u8]) -> Result<(), SandboxError> {
        let size_mb = (wasm_bytes.len() / (1024 * 1024)) as u32;
        if size_mb > self.config.max_wasm_size_mb {
            return Err(SandboxError::BinaryTooLarge(
                size_mb,
                self.config.max_wasm_size_mb,
            ));
        }

        // Basic WASM magic number validation
        if wasm_bytes.len() < 8 {
            return Err(SandboxError::ValidationFailed("too short".into()));
        }
        if &wasm_bytes[0..4] != b"\0asm" {
            return Err(SandboxError::ValidationFailed(
                "invalid WASM magic number".into(),
            ));
        }

        Ok(())
    }

    /// Execute WASM code deterministically.
    pub fn execute(
        &self,
        wasm_bytes: &[u8],
        input: &[u8],
        seed: u64,
        params: &[u8],
    ) -> Result<SandboxResult, SandboxError> {
        self.validate_wasm(wasm_bytes)?;

        // Deterministic execution using real Wasmtime engine
        use crate::highestsec::sandbox::{HighestsecSandbox, HighestsecSandboxConfig};
        
        let hs_config = HighestsecSandboxConfig {
            max_memory_pages: self.config.max_memory_mb * 16,
            max_fuel: self.config.max_fuel,
            max_execution_ms: self.config.max_execution_time_s * 1000,
            max_output_bytes: self.config.max_memory_mb as u64 * 1024 * 1024,
        };
        
        let sandbox = HighestsecSandbox::new(hs_config).map_err(|e| SandboxError::ExecutionError(e.to_string()))?;
        
        let mut combined_params = params.to_vec();
        combined_params.extend_from_slice(&seed.to_le_bytes());
        
        let hs_result = sandbox.execute(wasm_bytes, input, &combined_params, false, None, None, None, None)
            .map_err(|e| match e {
                crate::highestsec::sandbox::HighestsecSandboxError::FuelExhausted => SandboxError::FuelExhausted,
                crate::highestsec::sandbox::HighestsecSandboxError::Timeout => SandboxError::Timeout(self.config.max_execution_time_s),
                other => SandboxError::ExecutionError(other.to_string()),
            })?;

        Ok(SandboxResult {
            output: hs_result.output,
            fuel_consumed: hs_result.fuel_consumed,
            memory_peak_mb: hs_result.peak_memory_pages / 16,
            execution_time_ms: hs_result.wall_clock_ms,
        })
    }
}

impl Default for WasmSandbox {
    fn default() -> Self {
        Self::new(SandboxConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_wasm() -> Vec<u8> {
        // Minimal valid WASM header + some content
        let mut wasm = b"\0asm\x01\x00\x00\x00".to_vec();
        wasm.extend_from_slice(&[0u8; 100]); // padding
        wasm
    }

    #[test]
    fn test_validate_valid_wasm() {
        let sandbox = WasmSandbox::default();
        assert!(sandbox.validate_wasm(&make_wasm()).is_ok());
    }

    #[test]
    fn test_validate_invalid_magic() {
        let sandbox = WasmSandbox::default();
        assert!(sandbox.validate_wasm(b"not-wasm-data").is_err());
    }

    #[test]
    fn test_validate_too_short() {
        let sandbox = WasmSandbox::default();
        assert!(sandbox.validate_wasm(b"tiny").is_err());
    }

    #[test]
    fn test_sandbox_config_defaults() {
        let cfg = SandboxConfig::default();
        assert_eq!(cfg.max_memory_mb, config::MAX_WASM_MEMORY_MB);
        assert_eq!(cfg.max_execution_time_s, config::MAX_EXECUTION_TIME_S);
        assert_eq!(cfg.max_fuel, config::DEFAULT_WASM_FUEL);
    }
}
