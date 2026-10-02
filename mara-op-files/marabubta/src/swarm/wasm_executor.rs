// Marabunta - Licensed under the MIT License.
//! Pillar 5.3: Queen Ant AOT Pipeline
//!
//! Provides background compilation of incoming WASM payloads into native machine code.
//! Ensures near bare-metal execution speed for compute modules across the swarm.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use wasmtime::{Engine, Module, Store, Linker, Caller, Config as WasmtimeConfig};
use anyhow::Result as AnyhowResult;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};


// ============================================================================
// Configuration
// ============================================================================

/// Maximum WASM module size (64 MB).
const MAX_MODULE_SIZE_BYTES: u64 = 64 * 1024 * 1024;

/// Maximum execution time for a single WASM invocation.
const MAX_EXECUTION_DURATION: Duration = Duration::from_secs(300);

/// Maximum memory a single WASM instance may use (256 MB).
const MAX_INSTANCE_MEMORY_BYTES: u64 = 256 * 1024 * 1024;

/// Maximum number of cached modules.
const MAX_CACHED_MODULES: usize = 256;

/// Default fuel limit (instruction count proxy) for metered execution.
const DEFAULT_FUEL_LIMIT: u64 = 1_000_000_000;

// ============================================================================
// WasmConfig
// ============================================================================

/// Configuration for the WASM executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmConfig {
    pub max_module_size_bytes: u64,
    pub max_execution_secs: u64,
    pub max_memory_bytes: u64,
    pub max_cached_modules: usize,
    pub fuel_limit: u64,
    pub enable_wasi: bool,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            max_module_size_bytes: MAX_MODULE_SIZE_BYTES,
            max_execution_secs: MAX_EXECUTION_DURATION.as_secs(),
            max_memory_bytes: MAX_INSTANCE_MEMORY_BYTES,
            max_cached_modules: MAX_CACHED_MODULES,
            fuel_limit: DEFAULT_FUEL_LIMIT,
            enable_wasi: false,
        }
    }
}

// ============================================================================
// CachedModule
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedModule {
    pub module_id: String,
    pub size_bytes: u64,
    pub cached_at: DateTime<Utc>,
    pub execution_count: u64,
    pub last_executed_at: Option<DateTime<Utc>>,
    pub content_hash: String,
    pub requires_wasi: bool,
    #[serde(skip)]
    pub wasm_bytes: Vec<u8>,
    #[serde(skip)]
    pub aot_blob: Option<Vec<u8>>,
}

// ============================================================================
// WasmExecutionResult
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmExecutionResult {
    pub module_id: String,
    pub success: bool,
    pub duration_ms: u64,
    pub fuel_consumed: u64,
    pub peak_memory_bytes: u64,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
    pub error: Option<String>,
}

// ============================================================================
// WasmStats
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmStats {
    pub modules_cached: u64,
    pub total_executions: u64,
    pub total_errors: u64,
    pub avg_execution_ms: f64,
    pub memory_usage_bytes: u64,
}

struct SandboxState {
    max_memory_bytes: usize,
    stdout: Vec<u8>,
}

impl wasmtime::ResourceLimiter for SandboxState {
    fn memory_growing(&mut self, _cur: usize, desired: usize, _max: Option<usize>) -> AnyhowResult<bool> {
        Ok(desired <= self.max_memory_bytes)
    }
    fn table_growing(&mut self, _cur: u32, desired: u32, _max: Option<u32>) -> AnyhowResult<bool> {
        Ok(desired <= 10_000)
    }
}

// ============================================================================
// WasmExecutor
// ============================================================================

pub struct WasmExecutor {
    engine: Engine,
    pub hardware_monitor: Option<std::sync::Arc<crate::swarm::thermal::HardwareMonitor>>,
    pub dmz_tx: Option<tokio::sync::mpsc::Sender<(String, tokio::sync::oneshot::Sender<Vec<u8>>)>>,
    module_cache: DashMap<String, CachedModule>,
    config: WasmConfig,
    total_executions: AtomicU64,
    total_errors: AtomicU64,
    total_duration_ms: AtomicU64,
    memory_usage_bytes: AtomicU64,
}

impl Default for WasmExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl WasmExecutor {
    pub fn new() -> Self { Self::with_config(WasmConfig::default()) }

    pub fn with_hardware_monitor(mut self, hm: std::sync::Arc<crate::swarm::thermal::HardwareMonitor>) -> Self {
        self.hardware_monitor = Some(hm);
        self
    }

    pub fn with_config(config: WasmConfig) -> Self {
        let mut engine_config = WasmtimeConfig::new();

        // Pillar 5.3: Queen Ant AOT Pipeline
        // Configure Cranelift for optimal native machine code compilation.
        engine_config.cranelift_opt_level(wasmtime::OptLevel::SpeedAndSize);

        // 4.1 Strict IEEE 754 Canonicalization for Deterministic FPU
        // Essential for 15-Billion Node consensus on floating point arithmetic.
        engine_config.wasm_relaxed_simd(false);
        engine_config.cranelift_nan_canonicalization(true);

        // 4.2 Deterministic Fuel Metering
        engine_config.consume_fuel(true);
        engine_config.epoch_interruption(true); // REQUIRED FOR THERMAL PANIC
        engine_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);

        let engine = Engine::new(&engine_config).expect("Failed to initialize Hyperscale Wasmtime engine");

        Self {
            engine,
            hardware_monitor: None,
            dmz_tx: None,
            module_cache: DashMap::new(),
            config,
            total_executions: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            total_duration_ms: AtomicU64::new(0),
            memory_usage_bytes: AtomicU64::new(0),
        }
    }

    pub fn get_stats(&self) -> WasmStats {
        let execs = self.total_executions.load(Ordering::Relaxed);
        let ms = self.total_duration_ms.load(Ordering::Relaxed);
        WasmStats {
            modules_cached: self.module_cache.len() as u64,
            total_executions: execs,
            total_errors: self.total_errors.load(Ordering::Relaxed),
            avg_execution_ms: if execs > 0 { ms as f64 / execs as f64 } else { 0.0 },
            memory_usage_bytes: self.memory_usage_bytes.load(Ordering::Relaxed),
        }
    }

    pub fn cached_modules(&self) -> Vec<CachedModule> {
        self.module_cache.iter().map(|r| r.value().clone()).collect()
    }

    pub fn cache_size(&self) -> usize { self.module_cache.len() }
    pub fn is_cached(&self, id: &str) -> bool { self.module_cache.contains_key(id) }

    fn evict_oldest(&self) {
        if let Some(id) = self.module_cache.iter().next().map(|r| r.key().clone()) {
            if let Some((_, removed)) = self.module_cache.remove(&id) {
                self.memory_usage_bytes.fetch_sub(removed.size_bytes, Ordering::Relaxed);
            }
        }
    }

    pub fn load_module(&self, wasm_bytes: &[u8]) -> Result<String, WasmError> {
        if wasm_bytes.len() as u64 > self.config.max_module_size_bytes {
            return Err(WasmError::ModuleTooLarge { size: wasm_bytes.len() as u64, max: self.config.max_module_size_bytes });
        }
        let hash = { use sha2::{Digest, Sha256}; hex::encode(Sha256::digest(wasm_bytes)) };
        let module_id = format!("wasm-{}", &hash[..16]);
        if self.module_cache.contains_key(&module_id) { return Ok(module_id); }
        if self.module_cache.len() >= self.config.max_cached_modules { self.evict_oldest(); }

        let size = wasm_bytes.len() as u64;
        let cached = CachedModule {
            module_id: module_id.clone(),
            size_bytes: size,
            cached_at: Utc::now(),
            execution_count: 0,
            last_executed_at: None,
            content_hash: hash,
            requires_wasi: false,
            wasm_bytes: wasm_bytes.to_vec(),
            aot_blob: None, // Will be populated asynchronously
        };
        self.module_cache.insert(module_id.clone(), cached);
        self.memory_usage_bytes.fetch_add(size, Ordering::Relaxed);

        // --- Phase 1: THE QUEEN ANT AOT PIPELINE (Asynchronous Pre-Compilation) ---
        let engine = self.engine.clone();
        let bytes = wasm_bytes.to_vec();
        let m_id = module_id.clone();
        let cache = self.module_cache.clone();

        tokio::task::spawn_blocking(move || {
            info!(%m_id, "Queen Ant Pipeline: Background AOT compilation initiated...");
            match engine.precompile_module(&bytes) {
                Ok(aot_blob) => {
                    if let Some(mut entry) = cache.get_mut(&m_id) {
                        entry.aot_blob = Some(aot_blob);
                        info!(%m_id, "Queen Ant Pipeline: AOT digestion complete. Zero-latency execution ready.");
                    }
                }
                Err(e) => warn!(%m_id, "Queen Ant Pipeline: AOT compilation failed: {}", e),
            }
        });

        Ok(module_id)
    }

    pub fn unload_module(&self, module_id: &str) -> bool {
        if let Some((_, removed)) = self.module_cache.remove(module_id) {
            self.memory_usage_bytes.fetch_sub(removed.size_bytes, Ordering::Relaxed);
            true
        } else { false }
    }

    /// Executes the pre-compiled WASM module.
    /// Incorporates Phase 3 structural limits: Enforces `max_memory_mb` to mathematically
    /// prevent Silent OOM Killer assassinations. The sandbox will violently Trap if this
    /// limit is breached, saving the Rust host process.
    pub async fn execute(
        &self, 
        module_id: &str, 
        input: &[u8], 
        _env: &[(&str, &str)], 
        max_fuel: u64,
        max_memory_mb: Option<u64>
    ) -> Result<WasmExecutionResult, WasmError> {
        let mut entry = self.module_cache.get_mut(module_id).ok_or_else(|| WasmError::ModuleNotFound(module_id.to_string()))?;
        entry.execution_count += 1;
        entry.last_executed_at = Some(Utc::now());
        let aot_blob = entry.aot_blob.clone();
        let wasm_bytes = entry.wasm_bytes.clone();
        drop(entry);

        self.total_executions.fetch_add(1, Ordering::Relaxed);
        let module = if let Some(blob) = aot_blob {
            unsafe { Module::deserialize(&self.engine, &blob).map_err(|e| WasmError::InvalidModule(e.to_string()))? }
        } else {
            Module::new(&self.engine, &wasm_bytes).map_err(|e| WasmError::InvalidModule(e.to_string()))?
        };

        // Enforce dynamic job memory limits or fallback to the config default
        let active_max_memory_bytes = max_memory_mb
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(self.config.max_memory_bytes) as usize;

        let mut store = Store::new(&self.engine, SandboxState { max_memory_bytes: active_max_memory_bytes, stdout: Vec::new() });
        store.limiter(|state| state);

        // Pillar 11.1: Fuel Metering
        let target_fuel = if max_fuel > 0 { max_fuel } else { self.config.fuel_limit };
        let fuel_meter = crate::swarm::fuel_metering::FuelMeter::new(target_fuel);
        fuel_meter.inject_into_store(&mut store);

        // Set epoch deadline for Thermal Panic interrupts
        store.set_epoch_deadline(1);

        let mut linker = Linker::new(&self.engine);
        linker.func_wrap("env", "__marabunta_audit_emit", |mut c: Caller<'_, SandboxState>, p: u32, l: u32| {
            if let Some(m) = c.get_export("memory").and_then(|e| e.into_memory()) {
                if let Some(s) = m.data(&c).get(p as usize..(p+l) as usize) {
                    let b = s.to_vec();
                    c.data_mut().stdout.extend_from_slice(&b);
                }
            }
        }).map_err(|e| WasmError::ExecutionFailed(e.to_string()))?;

        let inst = linker.instantiate(&mut store, &module).map_err(|e| WasmError::ExecutionFailed(e.to_string()))?;
        let run = inst.get_typed_func::<u32, i32>(&mut store, "main").or_else(|_| inst.get_typed_func::<u32, i32>(&mut store, "_start")).map_err(|_| WasmError::ExecutionFailed("No main".into()))?;
        let mem = inst.get_memory(&mut store, "memory").ok_or(WasmError::ExecutionFailed("No memory".into()))?;
        mem.write(&mut store, 0, input).map_err(|e| WasmError::ExecutionFailed(e.to_string()))?;

        let start = std::time::Instant::now();
        
        // Spawn an async task to poll the HardwareMonitor and tick the Engine's epoch
        // if a ThermalPanic is detected. This violently interrupts the WASM thread.
        let engine_clone = self.engine.clone();
        let hm_clone = self.hardware_monitor.clone();
        let interrupt_handle = tokio::spawn(async move {
            if let Some(hm) = hm_clone {
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
                loop {
                    interval.tick().await;
                    if hm.is_panicking.load(std::sync::atomic::Ordering::SeqCst) {
                        tracing::error!("THERMAL PANIC: Violently terminating active WASM sandbox to save silicon.");
                        engine_clone.increment_epoch();
                        break;
                    }
                }
            }
        });

        let res = run.call(&mut store, input.len() as u32);
        interrupt_handle.abort(); // Cancel the watcher if execution finishes naturally
        
        let _ = res.map_err(|e| {
            self.total_errors.fetch_add(1, Ordering::Relaxed);
            if e.to_string().contains("wasm trap: interrupt") {
                WasmError::ExecutionFailed("THERMAL_PANIC_TERMINATION".to_string())
            } else {
                WasmError::ExecutionFailed(e.to_string())
            }
        })?;
        let dur = start.elapsed();
        self.total_duration_ms.fetch_add(dur.as_millis() as u64, Ordering::Relaxed);

        Ok(WasmExecutionResult {
            module_id: module_id.to_string(),
            success: true,
            duration_ms: dur.as_millis() as u64,
            fuel_consumed: store.get_fuel().unwrap_or(0),
            peak_memory_bytes: mem.data_size(&store) as u64,
            stdout: store.data().stdout.clone(),
            stderr: Vec::new(),
            exit_code: 0,
            error: None,
        })
    }

    fn detect_wasi_imports(bytes: &[u8]) -> bool { bytes.windows(22).any(|w| w == b"wasi_snapshot_preview1") }
}

mod hex {
    pub fn encode(b: impl AsRef<[u8]>) -> String {
        b.as_ref().iter().map(|x| format!("{:02x}", x)).collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WasmError {
    #[error("Not found: {0}")] ModuleNotFound(String),
    #[error("Too large: {size}")] ModuleTooLarge { size: u64, max: u64 },
    #[error("Invalid: {0}")] InvalidModule(String),
    #[error("Failed: {0}")] ExecutionFailed(String),
    #[error("Timeout: {0}")] Timeout(u64),
    #[error("Memory: {used}")] MemoryLimitExceeded { used: u64, max: u64 },
    #[error("Fuel: {consumed}")] FuelExhausted { consumed: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_new() { let _ = WasmExecutor::new(); }
}
