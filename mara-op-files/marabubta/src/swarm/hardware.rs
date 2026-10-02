// Marabunta - Licensed under the MIT License.

use parking_lot::RwLock;
use std::time::{Instant, Duration};

#[derive(Debug, Clone)]
pub struct HardwareBounds {
    pub max_known_nodes: usize,
    pub max_known_jobs: usize,
    pub max_known_assignments: usize,
    pub max_pending_chunks: usize,
    pub max_concurrent_chunks: usize,
    pub max_outbound_connections: usize,
    pub blob_max_storage_bytes: u64,
    pub sandbox_default_memory_mb: u64,
    pub sandbox_default_disk_mb: u64,
    /// Pillar 5.2: Bare-metal GPU capability
    pub has_sovereign_gpu: bool,
}

static BOUNDS: RwLock<Option<(HardwareBounds, Instant)>> = RwLock::new(None);

/// Dynamically evaluates the host's physical hardware capabilities.
/// Uses an epoch-based TTL cache (60 seconds) to ensure the node can adapt 
/// to high-availability RAM hot-swaps (vertical autoscaling) without restarting,
/// while avoiding system-call latency on the async hot-path.
pub fn get_bounds() -> HardwareBounds {
    // Fast path: Acquire a read lock and check the cache TTL
    {
        let read_guard = BOUNDS.read();
        if let Some((bounds, last_updated)) = &*read_guard {
            if last_updated.elapsed() < Duration::from_secs(60) {
                return bounds.clone();
            }
        }
    }

    // Slow path: Cache expired or empty. Acquire write lock to query the OS kernel.
    let mut write_guard = BOUNDS.write();
    
    // Double-check pattern (another thread might have updated it while we waited for the write lock)
    if let Some((bounds, last_updated)) = &*write_guard {
        if last_updated.elapsed() < Duration::from_secs(60) {
            return bounds.clone();
        }
    }

    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let ram_mb = sys.total_memory() / (1024 * 1024);
    
    let disks = sysinfo::Disks::new_with_refreshed_list();
    let total_disk_mb = std::cmp::max(1024, disks.iter().map(|d| d.total_space()).sum::<u64>() / (1024 * 1024));

    // Pillar 5.2: Detect NVIDIA/AMD GPUs.
    // In production, this uses NVML or rocm-smi. For the architectural proof:
    let has_sovereign_gpu = std::path::Path::new("/dev/nvidia0").exists() || std::path::Path::new("/dev/kfd").exists();

    let new_bounds = HardwareBounds {
        max_known_nodes: std::cmp::max(1_000, std::cmp::min((ram_mb * 10) as usize, 100_000)),
        max_known_jobs: std::cmp::max(500, std::cmp::min((ram_mb * 5) as usize, 50_000)),
        max_known_assignments: std::cmp::max(2_000, std::cmp::min((ram_mb * 20) as usize, 200_000)),
        max_pending_chunks: std::cmp::max(1_000, std::cmp::min((ram_mb * 10) as usize, 100_000)),
        max_concurrent_chunks: std::cmp::max(2, std::cmp::min((ram_mb / 200) as usize, 256)),
        max_outbound_connections: std::cmp::max(64, std::cmp::min((ram_mb / 2) as usize, 10_000)),
        blob_max_storage_bytes: std::cmp::min(10 * 1024 * 1024 * 1024, (total_disk_mb * 1024 * 1024 / 5) as u64), // up to 20% of disk
        sandbox_default_memory_mb: std::cmp::max(64, std::cmp::min(ram_mb / 10, 4096) as u64), // 10% RAM
        sandbox_default_disk_mb: std::cmp::max(128, std::cmp::min(total_disk_mb / 20, 10240) as u64), // 5% disk
        has_sovereign_gpu,
    };


    *write_guard = Some((new_bounds.clone(), Instant::now()));
    new_bounds
}
