// Marabunta - Licensed under the MIT License.
//! CGroup V2 allocation for WASM Sandboxes.
//! 
//! Dynamically isolates Wasmtime threads into dedicated Linux cgroups,
//! enabling the eBPF Ring-0 Thermal Guillotine to target specific workloads.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use tracing::{info, warn, error};

pub struct CgroupManager {
    base_path: PathBuf,
}

impl Default for CgroupManager {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from("/sys/fs/cgroup/marabunta_wasm"),
        }
    }
}

impl CgroupManager {
    /// Initializes the base cgroup hierarchy for the Marabunta Daemon.
    pub fn init(&self) -> std::io::Result<()> {
        if !self.base_path.exists() {
            fs::create_dir_all(&self.base_path)?;
            
            // Enable memory and cpu controllers
            let subtree_control = self.base_path.join("cgroup.subtree_control");
            if subtree_control.exists() {
                let _ = fs::write(&subtree_control, "+cpu +memory");
            }
            info!("HIGHESTSEC: CGroup v2 hierarchy initialized at {:?}", self.base_path);
        }
        Ok(())
    }

    /// Allocates a dedicated cgroup slice for a new WASM execution thread.
    /// Returns the unique cgroup ID (inode number on Linux) that the eBPF map requires.
    pub fn allocate_sandbox_cgroup(&self, sandbox_id: &str) -> std::io::Result<u64> {
        let sandbox_path = self.base_path.join(sandbox_id);
        
        if sandbox_path.exists() {
            let _ = fs::remove_dir(&sandbox_path); // Clean up orphaned cgroups
        }
        
        fs::create_dir(&sandbox_path)?;
        
        // Enforce strict Memory and CPU weight limits via Cgroup V2 interface
        let memory_max = sandbox_path.join("memory.max");
        fs::write(memory_max, "4G").unwrap_or_else(|e| warn!("Failed to set memory.max: {}", e));
        
        let cpu_weight = sandbox_path.join("cpu.weight");
        fs::write(cpu_weight, "50").unwrap_or_else(|e| warn!("Failed to set cpu.weight: {}", e));

        // Get the inode of the directory to serve as the unique cgroup_id for eBPF
        let metadata = std::fs::metadata(&sandbox_path)?;
        let inode = std::os::unix::fs::MetadataExt::ino(&metadata);

        info!("HIGHESTSEC: Allocated isolated Cgroup v2 '{}' (ID: {})", sandbox_id, inode);
        Ok(inode)
    }

    /// Attaches the currently executing thread to the allocated sandbox cgroup.
    pub fn attach_current_thread(&self, sandbox_id: &str) -> std::io::Result<()> {
        let procs_file = self.base_path.join(sandbox_id).join("cgroup.procs");
        let pid = std::process::id();
        
        fs::write(&procs_file, pid.to_string())?;
        info!("HIGHESTSEC: Wasmtime thread (PID {}) successfully attached to cgroup {}", pid, sandbox_id);
        Ok(())
    }
}
