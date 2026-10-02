// Marabunta - Licensed under the MIT License.
use tracing::{error, info, warn, debug};
use rusqlite::{params, Connection};
use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use chrono::Utc;

use crate::swarm::quantum_accelerator::{QuantumAccelerator, QuantumJobState};
use crate::swarm::events::{EventBus, SwarmEvent};
use crate::swarm::complexity::{ConcernDomain, EventSeverity, ComplexityHint};

/// Manages long-running (weeks) quantum computation tasks.
/// Uses a local SQLite database to persist state across node reboots,
/// treating the quantum backend as a high-latency asynchronous service.
pub struct QuantumJobManager {
    db_path: String,
    accelerator: Arc<QuantumAccelerator>,
    event_bus: Arc<EventBus>,
}

impl QuantumJobManager {
    /// Creates a new manager and initializes the local persistence schema.
    pub fn new(db_path: &str, accelerator: Arc<QuantumAccelerator>, event_bus: Arc<EventBus>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let manager = Self {
            db_path: db_path.to_string(),
            accelerator,
            event_bus,
        };
        
        manager.init_db_sync()?;
        
        Ok(manager)
    }

    /// Initializes the SQLite database synchronously.
    fn init_db_sync(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = Connection::open(&self.db_path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS quantum_jobs (
                local_task_id TEXT PRIMARY KEY,
                external_job_id TEXT,
                state TEXT NOT NULL,
                qasm_payload TEXT NOT NULL,
                result TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_polled_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;
        Ok(())
    }

    /// Accepts high-level FHE constraints, offloads QASM generation to a CPU thread,
    /// persists the intent to disk, and submits it to the quantum provider.
    pub async fn submit_fhe_task(&self, local_task_id: String, fhe_logic: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Initiating submission for local FHE task: {}", local_task_id);
        
        // 1. CPU-Bound compilation (simulated)
        let qasm_payload = task::spawn_blocking(move || {
            debug!("Compiling FHE constraints to OpenQASM on CPU thread...");
            std::thread::sleep(Duration::from_millis(50));
            format!("// QASM compiled from FHE logic: {}\nOPENQASM 3.0;", fhe_logic)
        }).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // 2. Persist intent to database BEFORE submission
        let path = self.db_path.clone();
        let tid = local_task_id.clone();
        let payload = qasm_payload.clone();
        task::spawn_blocking(move || -> Result<(), rusqlite::Error> {
            let conn = Connection::open(&path)?;
            conn.execute(
                "INSERT INTO quantum_jobs (local_task_id, state, qasm_payload) 
                 VALUES (?1, 'PENDING_SUBMISSION', ?2)",
                params![tid, payload],
            )?;
            Ok(())
        }).await?.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // 3. Submit to external, high-latency Quantum Provider
        let circuit = crate::swarm::quantum_accelerator::QuantumCircuit::new_fhe_accelerator(127, &qasm_payload);
        let external_job_id = self.accelerator.submit_circuit(&circuit).await?;
        let ext_id_for_db = external_job_id.clone();

        // 4. Update local DB with the external reference
        let path_update = self.db_path.clone();
        let tid_update = local_task_id.clone();
        task::spawn_blocking(move || -> Result<(), rusqlite::Error> {
            let conn = Connection::open(&path_update)?;
            conn.execute(
                "UPDATE quantum_jobs SET external_job_id = ?1, state = 'QUEUED' 
                 WHERE local_task_id = ?2",
                params![ext_id_for_db, tid_update],
            )?;
            Ok(())
        }).await?.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        info!("Task {} successfully enqueued at provider with external ID: {}", local_task_id, external_job_id);
        Ok(())
    }

    /// A background daemon designed to run indefinitely in a spawned task.
    pub async fn run_polling_daemon(self: Arc<Self>, poll_interval: Duration) {
        info!("Quantum Manager polling daemon started. Interval: {:?}", poll_interval);
        loop {
            tokio::time::sleep(poll_interval).await;
            debug!("Polling daemon waking up to check pending quantum jobs...");

            let path = self.db_path.clone();
            let poll_result = task::spawn_blocking(move || -> Result<Vec<(String, String)>, rusqlite::Error> {
                let conn = Connection::open(&path)?;
                let mut stmt = conn.prepare("SELECT local_task_id, external_job_id FROM quantum_jobs WHERE state IN ('QUEUED', 'RUNNING')")?;
                let rows = stmt.query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?;
                
                let mut jobs = Vec::new();
                for val in rows.flatten() { jobs.push(val); }
                Ok(jobs)
            }).await;

            let jobs_to_poll = match poll_result {
                Ok(Ok(jobs)) => jobs,
                _ => {
                    error!("Failed to read pending jobs from database");
                    continue;
                }
            };

            for (local_id, external_id) in jobs_to_poll {
                match self.accelerator.check_job_status(&external_id).await {
                    Ok(QuantumJobState::Completed(result)) => {
                        info!("Quantum Job {} (Local: {}) COMPLETED.", external_id, local_id);
                        let _ = self.update_job_state(&local_id, "COMPLETED", Some(&result)).await;
                        
                        let event = SwarmEvent {
                            id: 0,
                            timestamp: Utc::now(),
                            domain: ConcernDomain::Work,
                            severity: EventSeverity::Info,
                            complexity: ComplexityHint::Detailed,
                            summary: format!("High-latency Quantum FHE job {} completed", local_id),
                            details: serde_json::json!({
                                "local_task_id": local_id,
                                "external_job_id": external_id,
                                "result": result,
                            }),
                            related_entities: Vec::new(),
                            suggested_actions: Vec::new(),
                            source_node: None,
                            correlation_id: Some(local_id.clone()),
                            supersedes: None,
                        };
                        self.event_bus.emit(event);
                    },
                    Ok(QuantumJobState::Failed(err)) => {
                        warn!("Quantum Job {} (Local: {}) FAILED: {}", external_id, local_id, err);
                        let _ = self.update_job_state(&local_id, "FAILED", Some(&err)).await;
                        
                        let event = SwarmEvent {
                            id: 0,
                            timestamp: Utc::now(),
                            domain: ConcernDomain::Work,
                            severity: EventSeverity::Error,
                            complexity: ComplexityHint::Detailed,
                            summary: format!("High-latency Quantum FHE job {} failed", local_id),
                            details: serde_json::json!({
                                "local_task_id": local_id,
                                "external_job_id": external_id,
                                "error": err,
                            }),
                            related_entities: Vec::new(),
                            suggested_actions: vec!["Check external quantum provider quota".to_string()],
                            source_node: None,
                            correlation_id: Some(local_id.clone()),
                            supersedes: None,
                        };
                        self.event_bus.emit(event);
                    },
                    Ok(QuantumJobState::Running { .. }) => {
                        let _ = self.update_job_state(&local_id, "RUNNING", None).await;
                    },
                    Ok(_) => {}, 
                    Err(e) => {
                        error!("Failed to poll quantum provider for job {}: {}", external_id, e);
                    }
                }
            }
        }
    }

    async fn update_job_state(&self, local_task_id: &str, new_state: &str, result: Option<&str>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path = self.db_path.clone();
        let tid = local_task_id.to_string();
        let state = new_state.to_string();
        let res = result.map(|s| s.to_string());
        
        task::spawn_blocking(move || -> Result<(), rusqlite::Error> {
            let conn = Connection::open(&path)?;
            conn.execute(
                "UPDATE quantum_jobs SET state = ?1, result = ?2, last_polled_at = CURRENT_TIMESTAMP WHERE local_task_id = ?3",
                params![state, res, tid],
            )?;
            Ok(())
        }).await?.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        Ok(())
    }
}
