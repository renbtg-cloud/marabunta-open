// Marabunta - Licensed under the MIT License.
//! eBPF Loader for Marabunta kernel-level guardians.
//!
//! In this open-source branch, hardware-accelerated eBPF thermal monitoring and
//! cgroup preemption are disabled. The system gracefully falls back to the
//! user-space `sysinfo` polling loop defined in `src/swarm/thermal.rs`.

use tracing::debug;

pub struct BpfLoader {}

impl Default for BpfLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl BpfLoader {
    pub fn new() -> Self {
        Self {}
    }

    /// Stub: Hardware eBPF thermal monitoring is disabled.
    pub fn load_thermal_guardian(&self) -> Result<(), Box<dyn std::error::Error>> {
        debug!("eBPF Thermal Guardian disabled. Using user-space sysinfo polling.");
        Ok(())
    }

    /// Stub: Hardware eBPF cgroup preemption is disabled.
    pub fn update_active_cgroup(&self, _cgroup_id: u64) -> Result<(), Box<dyn std::error::Error>> {
        debug!("eBPF Cgroup mapping disabled. Using standard OS scheduler.");
        Ok(())
    }
}
