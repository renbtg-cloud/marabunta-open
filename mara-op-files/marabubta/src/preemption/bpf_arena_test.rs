// Marabunta - Licensed under the MIT License.
// BPF Arena Unit Tests
// This tests the thermodynamic decision making of the BpfMarketArena
// without requiring full network or message-passing integration.

use crate::preemption::bpf_arena::{BpfMarketArena, ThermodynamicTelemetry};
use crate::preemption::task_state::{RunningTask, CheckpointInfo};
use crate::preemption::types::{JobId, TaskId, NodeId};
use chrono::{Duration, Utc};

#[test]
fn test_bpf_market_arena_thermodynamics() {
    let node_id = "node-frankfurt-gpu-01".to_string();
    
    let telemetry = ThermodynamicTelemetry {
        thermal_celsius: 65.0,
        available_memory_mb: 8192,
        current_spot_price_mmx: 1.5,
        network_latency_ms: 12,
    };

    let arena = BpfMarketArena::new(node_id.clone(), telemetry);

    let victim_task = RunningTask::new(
        "task-frame-4402",
        "job-university-render",
        &node_id,
        2,
        crate::preemption::types::ResourceUsage::new(4.0, 8.0, 1, 10.0)
    ).with_started_at(Utc::now() - Duration::hours(1));

    // Test 1: Weak Preemptor
    let weak_payload = vec![0x01];
    let result = arena.conduct_trial(&weak_payload, &victim_task);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), false, "Weak preemptor should not evict");

    // Test 2: Strong Preemptor (HFT)
    let strong_payload = vec![0x02, 0x02];
    let result2 = arena.conduct_trial(&strong_payload, &victim_task);
    assert!(result2.is_ok());
    assert_eq!(result2.unwrap(), true, "Strong preemptor should evict");

    // Test 3: Thermal Emergency
    let hot_telemetry = ThermodynamicTelemetry {
        thermal_celsius: 95.0,
        available_memory_mb: 8192,
        current_spot_price_mmx: 1.5,
        network_latency_ms: 12,
    };
    let hot_arena = BpfMarketArena::new(node_id.clone(), hot_telemetry);
    let cooling_payload = vec![0x03];
    let result3 = hot_arena.conduct_trial(&cooling_payload, &victim_task);
    assert!(result3.is_ok());
    assert_eq!(result3.unwrap(), true, "Cooling preemptor should evict during thermal event");
}
