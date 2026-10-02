// Marabunta - Licensed under the MIT License.
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>

#define MAX_TEMP 95000 // 95 Celsius in millidegrees

// BPF Map to hold the active Wasmtime/WASI cgroup ID. 
// Populated dynamically from Rust user-space (`marabunta-visor`) when a high-priority task boots.
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 1);
    __type(key, u32);
    __type(value, u64);
} active_wasm_cgroup SEC(".maps");

SEC("kprobe/thermal_zone_device_update")
int BPF_KPROBE(thermal_guardian, struct thermal_zone_device *tz) {
    int temp;
    
    // Read the physical hardware temperature directly from the kernel struct
    bpf_probe_read_kernel(&temp, sizeof(temp), &tz->temperature);
    
    if (temp >= MAX_TEMP) {
        u32 key = 0;
        u64 *target_cgroup_id = bpf_map_lookup_elem(&active_wasm_cgroup, &key);
        
        if (target_cgroup_id) {
            u64 current_cgroup_id = bpf_get_current_cgroup_id();
            
            // If the CPU is melting AND the current executing thread belongs to our WASM payload
            if (current_cgroup_id == *target_cgroup_id) {
                // Execute ruthless Ring-0 preemption. Bypass OS scheduler.
                bpf_send_signal(9);
                bpf_printk("MARABUNTA EBPF: Thermal Critical (%dC). WASM Sandbox Terminated.", temp / 1000);
            }
        }
    }
    return 0;
}

char LICENSE[] SEC("license") = "Dual MIT/GPL";
