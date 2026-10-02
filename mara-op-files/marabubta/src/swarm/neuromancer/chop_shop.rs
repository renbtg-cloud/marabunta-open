// Marabunta - Licensed under the MIT License.
//! Chop Shop -- Capability detection & grafting for the Neuromancer subsystem.
//!
//! Detects local hardware capabilities (CPU, memory, storage, GPU, network)
//! via `sysinfo` and optional external probes (e.g. `nvidia-smi`).  Tracks
//! remote node capabilities received via gossip.  Emits `CapabilityDetected`
//! and `CapabilityLost` events when the local capability set changes.

use std::collections::HashMap;
use std::process::Command;
use std::sync::Arc;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use sysinfo::{Disks, Networks, System};
use tracing::{debug, info, warn};

use super::bus::NeuromancerBus;
use crate::swarm::neuromancer::config::ChopShopConfig;
use super::types::{MarabuntaEvent, Capability, NodeId};

// ============================================================================
// Public types
// ============================================================================

/// Details about a detected hardware capability.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CapabilityDetails {
    Cpu {
        arch: String,
        cores: u32,
        model: String,
    },
    Gpu {
        vendor: String,
        model: String,
        vram_mb: u64,
    },
    Memory {
        total_mb: u64,
        available_mb: u64,
    },
    Storage {
        total_mb: u64,
        available_mb: u64,
        disk_type: String,
    },
    Network {
        bandwidth_mbps: u64,
        interface: String,
    },
    Accelerator {
        kind: String,
        model: String,
    },
}

/// A single detected capability with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectedCapability {
    pub capability: Capability,
    pub details: CapabilityDetails,
    pub detected_at: SystemTime,
}

// ============================================================================
// ChopShop
// ============================================================================

/// Hardware capability detection and tracking engine.
///
/// Scans local hardware on demand and maintains a registry of remote node
/// capabilities received via gossip.  When the local capability set changes
/// between scans, the appropriate `CapabilityDetected` / `CapabilityLost`
/// events are emitted on the [`NeuromancerBus`].
pub struct ChopShop {
    config: ChopShopConfig,
    bus: Arc<NeuromancerBus>,
    local_node: NodeId,
    local_capabilities: Vec<DetectedCapability>,
    remote_capabilities: HashMap<NodeId, Vec<DetectedCapability>>,
}

impl ChopShop {
    /// Create a new ChopShop with the given configuration.
    pub fn new(config: ChopShopConfig, bus: Arc<NeuromancerBus>, local_node: NodeId) -> Self {
        Self {
            config,
            bus,
            local_node,
            local_capabilities: Vec::new(),
            remote_capabilities: HashMap::new(),
        }
    }

    /// Perform a full hardware scan and emit events for changes.
    ///
    /// Detects CPU, memory, storage, network, and optionally GPU capabilities.
    /// Compares with the previous scan and emits `CapabilityDetected` for new
    /// capabilities and `CapabilityLost` for removed ones.
    pub fn full_scan(&mut self) {
        let mut new_capabilities = Vec::new();

        // CPU
        new_capabilities.push(Self::detect_cpu());

        // Memory
        new_capabilities.push(Self::detect_memory());

        // Storage (may produce multiple entries)
        new_capabilities.extend(Self::detect_storage());

        // GPU (optional -- graceful failure)
        if self.config.enable_hotplug {
            if let Some(gpu_cap) = Self::detect_gpu() {
                new_capabilities.push(gpu_cap);
            }
        }

        // Network
        new_capabilities.extend(Self::detect_network());

        // Diff against previous capabilities
        let now = SystemTime::now();

        // Find newly appeared capabilities (present in new but not in old)
        for new_cap in &new_capabilities {
            let existed = self
                .local_capabilities
                .iter()
                .any(|old| old.capability == new_cap.capability);
            if !existed {
                info!(
                    node = %self.local_node,
                    capability = ?new_cap.capability,
                    "capability detected"
                );
                self.bus.emit(MarabuntaEvent::CapabilityDetected {
                    node: self.local_node,
                    capability: new_cap.capability.clone(),
                    timestamp: now,
                });
            }
        }

        // Find lost capabilities (present in old but not in new)
        for old_cap in &self.local_capabilities {
            let still_present = new_capabilities
                .iter()
                .any(|new| new.capability == old_cap.capability);
            if !still_present {
                warn!(
                    node = %self.local_node,
                    capability = ?old_cap.capability,
                    "capability lost"
                );
                self.bus.emit(MarabuntaEvent::CapabilityLost {
                    node: self.local_node,
                    capability: old_cap.capability.clone(),
                    timestamp: now,
                });
            }
        }

        self.local_capabilities = new_capabilities;
    }

    /// Detect CPU capabilities using sysinfo.
    pub fn detect_cpu() -> DetectedCapability {
        let mut sys = System::new();
        sys.refresh_cpu_usage();
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        sys.refresh_cpu_usage();

        let arch = std::env::consts::ARCH.to_string();
        let cores = sys.cpus().len() as u32;
        let model = sys
            .cpus()
            .first()
            .map(|c| c.brand().to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        let capability = match arch.as_str() {
            "x86_64" | "x86" => Capability::CpuX86_64,
            "aarch64" | "arm" => Capability::CpuArm64,
            other => Capability::Custom(format!("cpu_{}", other)),
        };

        DetectedCapability {
            capability,
            details: CapabilityDetails::Cpu { arch, cores, model },
            detected_at: SystemTime::now(),
        }
    }

    /// Detect memory capabilities using sysinfo.
    pub fn detect_memory() -> DetectedCapability {
        let mut sys = System::new();
        sys.refresh_memory();

        let total_mb = sys.total_memory() / (1024 * 1024);
        let available_mb = sys.available_memory() / (1024 * 1024);

        DetectedCapability {
            capability: Capability::HighMemory { mb: total_mb },
            details: CapabilityDetails::Memory {
                total_mb,
                available_mb,
            },
            detected_at: SystemTime::now(),
        }
    }

    /// Detect storage capabilities using sysinfo Disks.
    pub fn detect_storage() -> Vec<DetectedCapability> {
        let disks = Disks::new_with_refreshed_list();
        let mut capabilities = Vec::new();

        for disk in disks.list() {
            let total_mb = disk.total_space() / (1024 * 1024);
            let available_mb = disk.available_space() / (1024 * 1024);
            let disk_type = if disk.is_removable() {
                "removable".to_string()
            } else {
                format!("{:?}", disk.kind())
            };

            let mount_point = disk.mount_point().to_string_lossy().to_string();

            capabilities.push(DetectedCapability {
                capability: Capability::HighStorage { mb: total_mb },
                details: CapabilityDetails::Storage {
                    total_mb,
                    available_mb,
                    disk_type: format!("{} ({})", disk_type, mount_point),
                },
                detected_at: SystemTime::now(),
            });
        }

        capabilities
    }

    /// Attempt to detect GPU capabilities via nvidia-smi.
    ///
    /// Runs `nvidia-smi --query-gpu=name,memory.total --format=csv,noheader`
    /// and parses the output.  Returns `None` if nvidia-smi is not available
    /// or produces no parseable output.
    pub fn detect_gpu() -> Option<DetectedCapability> {
        let output = Command::new("nvidia-smi")
            .args(["--query-gpu=name,memory.total", "--format=csv,noheader"])
            .output();

        match output {
            Ok(out) if out.status.success() => {
                let stdout = String::from_utf8_lossy(&out.stdout);
                let line = stdout.lines().next()?;
                let parts: Vec<&str> = line.splitn(2, ',').collect();
                if parts.len() < 2 {
                    debug!("nvidia-smi output has unexpected format: {:?}", line);
                    return None;
                }

                let model = parts[0].trim().to_string();
                let vram_str = parts[1].trim();
                // nvidia-smi reports memory like "8192 MiB" or "8192 MB"
                let vram_mb = vram_str
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                info!(model = %model, vram_mb = vram_mb, "NVIDIA GPU detected");

                Some(DetectedCapability {
                    capability: Capability::GpuCuda { vram_mb },
                    details: CapabilityDetails::Gpu {
                        vendor: "NVIDIA".to_string(),
                        model,
                        vram_mb,
                    },
                    detected_at: SystemTime::now(),
                })
            }
            Ok(out) => {
                debug!(
                    status = ?out.status,
                    "nvidia-smi returned non-zero exit code"
                );
                None
            }
            Err(e) => {
                debug!(error = %e, "nvidia-smi not available");
                None
            }
        }
    }

    /// Detect basic network interfaces using sysinfo.
    fn detect_network() -> Vec<DetectedCapability> {
        let networks = Networks::new_with_refreshed_list();
        let mut capabilities = Vec::new();

        for (name, data) in networks.list() {
            // Estimate bandwidth from received/transmitted data rates
            // This is a rough heuristic -- the actual bandwidth is unknown
            // from sysinfo alone.  We use total bytes as a proxy.
            let total_bytes = data.total_received() + data.total_transmitted();
            // Estimate at least 100 Mbps for any active interface
            let bandwidth_mbps = if total_bytes > 0 { 100 } else { 0 };

            if bandwidth_mbps > 0 {
                capabilities.push(DetectedCapability {
                    capability: Capability::HighBandwidth {
                        mbps: bandwidth_mbps,
                    },
                    details: CapabilityDetails::Network {
                        bandwidth_mbps,
                        interface: name.to_string(),
                    },
                    detected_at: SystemTime::now(),
                });
            }
        }

        capabilities
    }

    /// Record capabilities reported by a remote node.
    pub fn handle_remote_capabilities(&mut self, node: NodeId, caps: Vec<DetectedCapability>) {
        debug!(
            node = %node,
            count = caps.len(),
            "received remote capabilities"
        );
        self.remote_capabilities.insert(node, caps);
    }

    /// Remove all tracked capabilities for a node that has left the swarm.
    pub fn handle_node_left(&mut self, node: &NodeId) {
        if self.remote_capabilities.remove(node).is_some() {
            debug!(node = %node, "removed capabilities for departed node");
        }
    }

    /// Get the local node's detected capabilities.
    pub fn local_capabilities(&self) -> &[DetectedCapability] {
        &self.local_capabilities
    }

    /// Get all known capabilities indexed by node ID.
    ///
    /// Includes both the local node and all remote nodes.
    pub fn all_capabilities(&self) -> HashMap<NodeId, &[DetectedCapability]> {
        let mut result = HashMap::new();
        result.insert(self.local_node, self.local_capabilities.as_slice());
        for (node, caps) in &self.remote_capabilities {
            result.insert(*node, caps.as_slice());
        }
        result
    }
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::neuromancer::bus::NeuromancerBus;
    use crate::swarm::neuromancer::config::ChopShopConfig;
    use std::time::Duration;

    fn test_bus() -> Arc<NeuromancerBus> {
        Arc::new(NeuromancerBus::new(256))
    }

    fn test_config() -> ChopShopConfig {
        ChopShopConfig {
            scan_interval: Duration::from_secs(10),
            enable_hotplug: false, // disable GPU probing in tests
        }
    }

    #[test]
    fn test_cpu_detection_produces_valid_capability() {
        let cap = ChopShop::detect_cpu();

        match &cap.details {
            CapabilityDetails::Cpu { arch, cores, model } => {
                assert!(!arch.is_empty(), "arch should not be empty");
                assert!(*cores > 0, "should detect at least one core");
                assert!(!model.is_empty(), "model should not be empty");
            }
            other => panic!("expected Cpu details, got {:?}", other),
        }

        // Capability should be a CPU variant
        match &cap.capability {
            Capability::CpuX86_64 | Capability::CpuArm64 | Capability::Custom(_) => {}
            other => panic!("expected CPU capability, got {:?}", other),
        }
    }

    #[test]
    fn test_memory_detection_produces_valid_capability() {
        let cap = ChopShop::detect_memory();

        match &cap.details {
            CapabilityDetails::Memory {
                total_mb,
                available_mb,
            } => {
                assert!(*total_mb > 0, "total memory should be > 0");
                assert!(
                    *available_mb <= *total_mb,
                    "available should be <= total"
                );
            }
            other => panic!("expected Memory details, got {:?}", other),
        }

        match &cap.capability {
            Capability::HighMemory { mb } => {
                assert!(*mb > 0, "memory capability should report > 0 MB");
            }
            other => panic!("expected HighMemory capability, got {:?}", other),
        }
    }

    #[test]
    fn test_new_capability_emits_detected_event() {
        let bus = test_bus();
        let mut rx = bus.subscribe();
        let node = NodeId::new();
        let mut shop = ChopShop::new(test_config(), bus.clone(), node);

        // First scan -- everything is new
        shop.full_scan();

        // Should have emitted at least one CapabilityDetected (CPU at minimum)
        let mut detected_count = 0;
        while let Ok(event) = rx.try_recv() {
            if matches!(event, MarabuntaEvent::CapabilityDetected { .. }) {
                detected_count += 1;
            }
        }
        assert!(
            detected_count > 0,
            "first scan should emit at least one CapabilityDetected"
        );
    }

    #[test]
    fn test_lost_capability_emits_lost_event() {
        let bus = test_bus();
        let node = NodeId::new();
        let mut shop = ChopShop::new(test_config(), bus.clone(), node);

        // Inject a fake capability that won't appear in a real scan
        shop.local_capabilities.push(DetectedCapability {
            capability: Capability::Fpga,
            details: CapabilityDetails::Accelerator {
                kind: "FPGA".to_string(),
                model: "Fake FPGA".to_string(),
            },
            detected_at: SystemTime::now(),
        });

        // Subscribe after injecting so we only see the scan events
        let mut rx = bus.subscribe();

        // Scan -- the fake FPGA should disappear
        shop.full_scan();

        let mut lost_count = 0;
        while let Ok(event) = rx.try_recv() {
            if let MarabuntaEvent::CapabilityLost { capability, .. } = &event {
                if *capability == Capability::Fpga {
                    lost_count += 1;
                }
            }
        }
        assert_eq!(lost_count, 1, "should emit CapabilityLost for removed FPGA");
    }

    #[test]
    fn test_remote_capability_tracking() {
        let bus = test_bus();
        let node = NodeId::new();
        let mut shop = ChopShop::new(test_config(), bus, node);

        let remote_a = NodeId::new();
        let remote_b = NodeId::new();

        let caps_a = vec![DetectedCapability {
            capability: Capability::CpuX86_64,
            details: CapabilityDetails::Cpu {
                arch: "x86_64".to_string(),
                cores: 8,
                model: "Test CPU A".to_string(),
            },
            detected_at: SystemTime::now(),
        }];

        let caps_b = vec![
            DetectedCapability {
                capability: Capability::CpuArm64,
                details: CapabilityDetails::Cpu {
                    arch: "aarch64".to_string(),
                    cores: 4,
                    model: "Test CPU B".to_string(),
                },
                detected_at: SystemTime::now(),
            },
            DetectedCapability {
                capability: Capability::GpuCuda { vram_mb: 8192 },
                details: CapabilityDetails::Gpu {
                    vendor: "NVIDIA".to_string(),
                    model: "RTX 3080".to_string(),
                    vram_mb: 8192,
                },
                detected_at: SystemTime::now(),
            },
        ];

        shop.handle_remote_capabilities(remote_a, caps_a);
        shop.handle_remote_capabilities(remote_b, caps_b);

        // Both remotes should be tracked
        let all = shop.all_capabilities();
        assert!(all.contains_key(&remote_a));
        assert!(all.contains_key(&remote_b));
        assert_eq!(all[&remote_a].len(), 1);
        assert_eq!(all[&remote_b].len(), 2);

        // Remove one
        shop.handle_node_left(&remote_a);
        let all = shop.all_capabilities();
        assert!(!all.contains_key(&remote_a));
        assert!(all.contains_key(&remote_b));
    }

    #[test]
    fn test_storage_detection_produces_capabilities() {
        let caps = ChopShop::detect_storage();
        // On most systems there's at least one disk
        // But we won't assert > 0 since CI might be weird;
        // just verify the structure is correct for any returned.
        for cap in &caps {
            match &cap.details {
                CapabilityDetails::Storage {
                    total_mb,
                    available_mb,
                    disk_type,
                } => {
                    assert!(
                        *available_mb <= *total_mb,
                        "available should be <= total"
                    );
                    assert!(!disk_type.is_empty());
                }
                other => panic!("expected Storage details, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_gpu_detection_graceful_failure() {
        // On most CI/test machines nvidia-smi is not available.
        // This test verifies detect_gpu() returns None gracefully.
        let result = ChopShop::detect_gpu();
        // We don't assert None -- on a machine with an NVIDIA GPU this
        // would succeed.  We just verify it doesn't panic.
        if let Some(cap) = result {
            match &cap.details {
                CapabilityDetails::Gpu {
                    vendor,
                    model,
                    vram_mb,
                } => {
                    assert_eq!(vendor, "NVIDIA");
                    assert!(!model.is_empty());
                    assert!(*vram_mb > 0);
                }
                other => panic!("expected Gpu details, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_second_scan_no_duplicate_events() {
        let bus = test_bus();
        let node = NodeId::new();
        let mut shop = ChopShop::new(test_config(), bus.clone(), node);

        // First scan
        shop.full_scan();

        // Subscribe after first scan so we only see second scan events
        let mut rx = bus.subscribe();

        // Second scan with same hardware -- should emit no new events
        shop.full_scan();

        let mut detected_count = 0;
        let mut lost_count = 0;
        while let Ok(event) = rx.try_recv() {
            match event {
                MarabuntaEvent::CapabilityDetected { .. } => detected_count += 1,
                MarabuntaEvent::CapabilityLost { .. } => lost_count += 1,
                _ => {}
            }
        }

        // No change in hardware between scans
        assert_eq!(
            detected_count, 0,
            "second scan of same hardware should emit no CapabilityDetected"
        );
        assert_eq!(
            lost_count, 0,
            "second scan of same hardware should emit no CapabilityLost"
        );
    }
}
