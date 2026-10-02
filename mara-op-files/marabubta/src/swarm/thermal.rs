// Marabunta - Licensed under the MIT License.
use sysinfo::{System, Components, CpuRefreshKind, RefreshKind};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use tokio::sync::RwLock;
use std::time::Duration;
use tracing::{info, warn, error};
use chrono::Utc;
use crate::swarm::events::{EventBus, SwarmEvent};
use crate::swarm::complexity::{ConcernDomain, EventSeverity, ComplexityHint};

pub const PANIC_TEMP_C: f32 = 90.0;
pub const WARNING_TEMP_C: f32 = 80.0;
pub const PANIC_LOAD: f32 = 0.95;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThermalState {
    Normal,
    Warning,
    Panic, // Node should shed all non-critical load
}

pub struct HardwareMonitor {
    sys: RwLock<System>,
    state: RwLock<ThermalState>,
    event_bus: Arc<EventBus>,
    node_id: crate::swarm::types::NodeId,
    pub is_panicking: AtomicBool,
    work_engine: tokio::sync::RwLock<Option<std::sync::Weak<crate::swarm::work::WorkEngine>>>,
}

impl HardwareMonitor {
    pub fn new(event_bus: Arc<EventBus>, node_id: crate::swarm::types::NodeId) -> Arc<Self> {
        let sys = System::new_with_specifics(
            RefreshKind::new().with_cpu(CpuRefreshKind::everything())
        );
        
        Arc::new(Self {
            sys: RwLock::new(sys),
            state: RwLock::new(ThermalState::Normal),
            event_bus,
            node_id,
            is_panicking: AtomicBool::new(false),
            work_engine: tokio::sync::RwLock::new(None),
        })
    }

    pub async fn set_work_engine(&self, engine: std::sync::Weak<crate::swarm::work::WorkEngine>) {
        *self.work_engine.write().await = Some(engine);
    }

    pub async fn run_daemon(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        
        loop {
            interval.tick().await;
            
            let mut sys = self.sys.write().await;
            sys.refresh_cpu();
            
            let cpus = sys.cpus();
            let avg_load = if !cpus.is_empty() {
                cpus.iter().map(|c| c.cpu_usage()).sum::<f32>() / (cpus.len() as f32) / 100.0
            } else {
                0.0
            };
            
            let components = Components::new_with_refreshed_list();
            let mut max_temp = 0.0_f32;
            for comp in components.list() {
                let t = comp.temperature();
                if t > max_temp {
                    max_temp = t;
                }
            }

            // BLIND GUILLOTINE FIX: Cloud Sensor Masking
            // In cloud environments (AWS, Docker), `max_temp` often returns 0.0 because the 
            // hypervisor masks hardware sensors from the guest OS. We must use sustained CPU 
            // load as a fallback heuristic to prevent runway melting/billing.
            let is_blind_cloud = max_temp < 1.0;
            
            let new_state = if max_temp >= PANIC_TEMP_C || (is_blind_cloud && avg_load >= PANIC_LOAD) {
                ThermalState::Panic
            } else if max_temp >= WARNING_TEMP_C || (is_blind_cloud && avg_load >= 0.85) {
                ThermalState::Warning
            } else {
                ThermalState::Normal
            };

            let mut current_state = self.state.write().await;
            if *current_state != new_state {
                self.transition_state(&current_state, &new_state, max_temp, avg_load).await;
                *current_state = new_state;
                self.is_panicking.store(new_state == ThermalState::Panic, Ordering::SeqCst);
            }
        }
    }

    async fn transition_state(&self, old: &ThermalState, new: &ThermalState, temp: f32, load: f32) {
        let severity = match new {
            ThermalState::Panic => EventSeverity::Critical,
            ThermalState::Warning => EventSeverity::Warning,
            ThermalState::Normal => EventSeverity::Info,
        };

        let msg = match new {
            ThermalState::Panic => "THERMAL PANIC: Node shedding load to prevent silicon meltdown",
            ThermalState::Warning => "THERMAL WARNING: Node running hot",
            ThermalState::Normal => "THERMAL RECOVERY: Node returning to normal operation",
        };

        if *new == ThermalState::Panic {
            error!(temp = temp, load = load, "{}", msg);
            // 🔥 THERMAL GUILLOTINE: Signal the WorkEngine to abort all active tasks.
            if let Some(weak_engine) = &*self.work_engine.read().await {
                if let Some(engine) = weak_engine.upgrade() {
                    let engine_arc: Arc<crate::swarm::work::WorkEngine> = engine.clone();
                    engine_arc.shed_active_load();
                }
            }
        } else if *new == ThermalState::Warning {
            warn!(temp = temp, load = load, "{}", msg);
            // ❄️ THERMODYNAMIC ARBITRAGE: Gracefully yield active chunks before we hit panic thresholds.
            if let Some(weak_engine) = &*self.work_engine.read().await {
                if let Some(engine) = weak_engine.upgrade() {
                    let engine_clone: Arc<crate::swarm::work::WorkEngine> = engine.clone();
                    tokio::spawn(async move {
                        engine_clone.yield_active_load().await;
                    });
                }
            }
        } else {
            info!(temp = temp, load = load, "{}", msg);
        }

        self.event_bus.emit(SwarmEvent {
            id: 0,
            timestamp: Utc::now(),
            domain: ConcernDomain::Health,
            severity,
            complexity: ComplexityHint::Moderate,
            summary: msg.to_string(),
            details: serde_json::json!({
                "temperature_c": temp,
                "cpu_load": load,
                "previous_state": format!("{:?}", old),
                "new_state": format!("{:?}", new),
            }),
            related_entities: vec![],
            suggested_actions: vec!["Decrease global job admission rate".to_string()],
            source_node: Some(self.node_id),
            correlation_id: None,
            supersedes: None,
        });
    }
}
