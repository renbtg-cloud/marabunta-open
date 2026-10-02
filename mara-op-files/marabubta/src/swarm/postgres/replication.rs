// Marabunta - Licensed under the MIT License.
//! Streaming replication management for the swarm-managed PostgreSQL cluster.
//!
//! Handles:
//! - **Primary election**: deterministic selection among eligible PG nodes
//! - **Replica setup**: `pg_basebackup` + `standby.signal` + connection info
//! - **Promotion**: `pg_ctl promote` when a replica must become primary
//! - **Slot management**: create/drop replication slots per replica
//! - **Lag monitoring**: parse `pg_stat_replication` for replication health
//!
//! The election algorithm is **deterministic given the same inputs**: all nodes
//! evaluating the same set of candidates will reach the same conclusion,
//! avoiding split-brain without requiring a distributed consensus protocol.

use std::path::Path;
use std::process::Stdio;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tracing::{debug, info, warn};

use super::config::PgConfig;
use super::types::{PgError, PgInstance, PgNodeInfo, PgStatus};
use crate::swarm::types::NodeId;

// ============================================================================
// Replication slot info
// ============================================================================

/// Information about a PG replication slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotInfo {
    pub slot_name: String,
    pub slot_type: String,
    pub active: bool,
    pub restart_lsn: Option<String>,
}

// ============================================================================
// Replication lag report
// ============================================================================

/// Parsed replication lag information from `pg_stat_replication`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationLagReport {
    pub replica_node_id: NodeId,
    pub sent_lsn: Option<String>,
    pub write_lsn: Option<String>,
    pub flush_lsn: Option<String>,
    pub replay_lsn: Option<String>,
    pub write_lag_ms: Option<u64>,
    pub flush_lag_ms: Option<u64>,
    pub replay_lag_ms: Option<u64>,
    pub state: String,
}

// ============================================================================
// Election candidate
// ============================================================================

/// A candidate for primary election, carrying the criteria needed for
/// deterministic ranking.
#[derive(Debug, Clone)]
pub struct ElectionCandidate {
    pub node_id: NodeId,
    pub wal_position: Option<String>,
    pub replication_lag_ms: Option<u64>,
    pub last_health_check: Option<DateTime<Utc>>,
    pub status: PgStatus,
}

impl ElectionCandidate {
    /// Build a candidate from PgNodeInfo.
    pub fn from_node_info(info: &PgNodeInfo) -> Self {
        Self {
            node_id: info.node_id,
            wal_position: info.wal_position.clone(),
            replication_lag_ms: info.replication_lag_ms,
            last_health_check: info.last_health_check,
            status: info.status.clone(),
        }
    }
}

// ============================================================================
// ReplicationManager
// ============================================================================

/// Manages streaming replication for the PG cluster.
pub struct ReplicationManager {
    config: PgConfig,
}

impl ReplicationManager {
    pub fn new(config: PgConfig) -> Self {
        Self { config }
    }

    // ========================================================================
    // Primary election
    // ========================================================================

    /// Elect a primary from a set of PG-capable nodes.
    ///
    /// The algorithm is deterministic: given the same set of candidates,
    /// every node in the swarm will choose the same winner.
    ///
    /// **Election criteria** (in priority order):
    /// 1. Only nodes with `PgStatus::Running` are eligible
    /// 2. Highest WAL position (most up-to-date data)
    /// 3. Lowest replication lag
    /// 4. Most recent health check (proof of liveness)
    /// 5. Tie-breaker: lexicographically lowest NodeId (deterministic)
    pub fn elect_primary(&self, candidates: &[ElectionCandidate]) -> Option<NodeId> {
        let mut eligible: Vec<&ElectionCandidate> = candidates
            .iter()
            .filter(|c| c.status.is_running())
            .collect();

        if eligible.is_empty() {
            warn!("No eligible candidates for PG primary election");
            return None;
        }

        // Sort by election criteria
        eligible.sort_by(|a, b| {
            // 1. WAL position: higher is better (reverse order)
            let wal_cmp = b
                .wal_position
                .as_deref()
                .unwrap_or("")
                .cmp(a.wal_position.as_deref().unwrap_or(""));

            if wal_cmp != std::cmp::Ordering::Equal {
                return wal_cmp;
            }

            // 2. Replication lag: lower is better
            let lag_a = a.replication_lag_ms.unwrap_or(u64::MAX);
            let lag_b = b.replication_lag_ms.unwrap_or(u64::MAX);
            let lag_cmp = lag_a.cmp(&lag_b);

            if lag_cmp != std::cmp::Ordering::Equal {
                return lag_cmp;
            }

            // 3. Health check: more recent is better (reverse order)
            let hc_cmp = b
                .last_health_check
                .cmp(&a.last_health_check);

            if hc_cmp != std::cmp::Ordering::Equal {
                return hc_cmp;
            }

            // 4. Tie-breaker: lexicographic NodeId
            a.node_id.0.cmp(&b.node_id.0)
        });

        let winner = &eligible[0];
        info!(
            winner = %winner.node_id,
            wal = ?winner.wal_position,
            candidates = eligible.len(),
            "Primary election complete"
        );

        Some(winner.node_id)
    }

    // ========================================================================
    // Replica setup
    // ========================================================================

    /// Set up this node as a streaming replica of the given primary.
    ///
    /// Runs `pg_basebackup` to clone the primary's data, then writes
    /// `standby.signal` and `postgresql.auto.conf` to configure replication.
    pub async fn setup_replica(
        &self,
        instance: &PgInstance,
        primary_info: &PgNodeInfo,
    ) -> Result<(), PgError> {
        let primary_addr = primary_info
            .listen_addr
            .ok_or_else(|| PgError::ReplicationFailed("primary has no listen address".into()))?;

        let pg_basebackup = instance.bin_dir.join("pg_basebackup");
        if !pg_basebackup.exists() {
            return Err(PgError::ReplicationFailed(format!(
                "pg_basebackup not found at {}",
                pg_basebackup.display()
            )));
        }

        let slot_name = self.slot_name_for_node(&instance.data_dir);

        info!(
            primary = %primary_addr,
            slot = %slot_name,
            "Setting up streaming replication via pg_basebackup"
        );

        let output = Command::new(&pg_basebackup)
            .arg("-h")
            .arg(primary_addr.ip().to_string())
            .arg("-p")
            .arg(primary_info.pg_port.to_string())
            .arg("-U")
            .arg("marabunta")
            .arg("-D")
            .arg(&instance.data_dir)
            .arg("-Fp") // Plain format
            .arg("-Xs") // Stream WAL during backup
            .arg("-R") // Write recovery settings (standby.signal + primary_conninfo)
            .arg("-S")
            .arg(&slot_name)
            .arg("--checkpoint=fast")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .map_err(|e| PgError::ReplicationFailed(format!("exec pg_basebackup: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(PgError::ReplicationFailed(format!(
                "pg_basebackup failed: {}",
                stderr
            )));
        }

        info!("Replica setup complete via pg_basebackup");
        Ok(())
    }

    // ========================================================================
    // Promotion
    // ========================================================================

    /// Promote this replica to primary.
    pub async fn promote_to_primary(&self, instance: &PgInstance) -> Result<(), PgError> {
        let pg_ctl = instance.bin_dir.join("pg_ctl");

        info!(
            data_dir = %instance.data_dir.display(),
            "Promoting replica to primary"
        );

        let output = Command::new(&pg_ctl)
            .arg("promote")
            .arg("-D")
            .arg(&instance.data_dir)
            .arg("-w") // Wait for promotion to complete
            .arg("-t")
            .arg("30") // Timeout 30 seconds
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .map_err(|e| PgError::FailoverFailed(format!("exec pg_ctl promote: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(PgError::FailoverFailed(format!(
                "pg_ctl promote failed: {}",
                stderr
            )));
        }

        // Remove standby.signal if it exists (PG should do this, but be safe)
        let standby_signal = instance.data_dir.join("standby.signal");
        if standby_signal.exists() {
            let _ = tokio::fs::remove_file(&standby_signal).await;
        }

        info!("Replica promoted to primary successfully");
        Ok(())
    }

    // ========================================================================
    // Replication slot management
    // ========================================================================

    /// Create a replication slot on the primary for a given replica.
    ///
    /// Slot names are derived from the data directory path hash to be
    /// deterministic and unique.
    pub async fn create_replication_slot(
        &self,
        client: &tokio_postgres::Client,
        slot_name: &str,
    ) -> Result<(), PgError> {
        // Check if slot already exists
        let existing = client
            .query_opt(
                "SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot_name],
            )
            .await
            .map_err(|e| PgError::ReplicationFailed(format!("check slot: {e}")))?;

        if existing.is_some() {
            debug!(slot = slot_name, "Replication slot already exists");
            return Ok(());
        }

        client
            .execute(
                "SELECT pg_create_physical_replication_slot($1)",
                &[&slot_name],
            )
            .await
            .map_err(|e| PgError::ReplicationFailed(format!("create slot: {e}")))?;

        info!(slot = slot_name, "Created replication slot");
        Ok(())
    }

    /// Drop a replication slot.
    pub async fn drop_replication_slot(
        &self,
        client: &tokio_postgres::Client,
        slot_name: &str,
    ) -> Result<(), PgError> {
        client
            .execute(
                "SELECT pg_drop_replication_slot($1)",
                &[&slot_name],
            )
            .await
            .map_err(|e| PgError::ReplicationFailed(format!("drop slot: {e}")))?;

        info!(slot = slot_name, "Dropped replication slot");
        Ok(())
    }

    /// List all replication slots.
    pub async fn list_replication_slots(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<Vec<SlotInfo>, PgError> {
        let rows = client
            .query(
                "SELECT slot_name, slot_type, active, restart_lsn::text
                 FROM pg_replication_slots
                 ORDER BY slot_name",
                &[],
            )
            .await
            .map_err(|e| PgError::QueryFailed(format!("list slots: {e}")))?;

        Ok(rows
            .iter()
            .map(|row| SlotInfo {
                slot_name: row.get("slot_name"),
                slot_type: row.get("slot_type"),
                active: row.get("active"),
                restart_lsn: row.get("restart_lsn"),
            })
            .collect())
    }

    // ========================================================================
    // Lag monitoring
    // ========================================================================

    /// Get the current WAL position of this PG instance.
    ///
    /// On a primary, returns `pg_current_wal_lsn()`.
    /// On a replica, returns `pg_last_wal_replay_lsn()`.
    pub async fn current_wal_position(
        &self,
        client: &tokio_postgres::Client,
        is_primary: bool,
    ) -> Result<Option<String>, PgError> {
        let query = if is_primary {
            "SELECT pg_current_wal_lsn()::text as lsn"
        } else {
            "SELECT pg_last_wal_replay_lsn()::text as lsn"
        };

        let row = client
            .query_opt(query, &[])
            .await
            .map_err(|e| PgError::QueryFailed(format!("get WAL position: {e}")))?;

        Ok(row.and_then(|r| r.get("lsn")))
    }

    /// Check replication lag from the primary's perspective.
    ///
    /// Queries `pg_stat_replication` on the primary to get lag info
    /// for all connected replicas.
    pub async fn check_replication_lag(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<Vec<ReplicationLagReport>, PgError> {
        let rows = client
            .query(
                "SELECT
                    application_name,
                    sent_lsn::text,
                    write_lsn::text,
                    flush_lsn::text,
                    replay_lsn::text,
                    EXTRACT(EPOCH FROM write_lag)::bigint * 1000 as write_lag_ms,
                    EXTRACT(EPOCH FROM flush_lag)::bigint * 1000 as flush_lag_ms,
                    EXTRACT(EPOCH FROM replay_lag)::bigint * 1000 as replay_lag_ms,
                    state
                 FROM pg_stat_replication",
                &[],
            )
            .await
            .map_err(|e| PgError::QueryFailed(format!("check replication lag: {e}")))?;

        Ok(rows
            .iter()
            .map(|row| {
                let _app_name: String = row.get("application_name");
                ReplicationLagReport {
                    // application_name is set to the slot name, which contains
                    // the node identity; use a placeholder NodeId here since
                    // we can't parse a UUID from an arbitrary app name
                    replica_node_id: NodeId::default(),
                    sent_lsn: row.get("sent_lsn"),
                    write_lsn: row.get("write_lsn"),
                    flush_lsn: row.get("flush_lsn"),
                    replay_lsn: row.get("replay_lsn"),
                    write_lag_ms: row
                        .get::<_, Option<i64>>("write_lag_ms")
                        .map(|v| v.max(0) as u64),
                    flush_lag_ms: row
                        .get::<_, Option<i64>>("flush_lag_ms")
                        .map(|v| v.max(0) as u64),
                    replay_lag_ms: row
                        .get::<_, Option<i64>>("replay_lag_ms")
                        .map(|v| v.max(0) as u64),
                    state: row.get("state"),
                }
            })
            .collect())
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /// Generate a deterministic replication slot name from a data directory path.
    fn slot_name_for_node(&self, data_dir: &Path) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(data_dir.to_string_lossy().as_bytes());
        let hash = hex::encode(hasher.finalize());
        // Slot names must be <= 63 chars, start with letter
        format!("marabunta_{}", &hash[..16])
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use crate::swarm::postgres::types::{PgNodeRole, PgStatus};

    fn test_config() -> PgConfig {
        PgConfig::default()
    }

    fn test_node_id(suffix: &str) -> NodeId {
        NodeId(Uuid::new_v5(&Uuid::NAMESPACE_DNS, suffix.as_bytes()))
    }

    fn make_candidate(
        suffix: &str,
        wal: Option<&str>,
        lag_ms: Option<u64>,
        running: bool,
    ) -> ElectionCandidate {
        ElectionCandidate {
            node_id: test_node_id(suffix),
            wal_position: wal.map(|s| s.to_string()),
            replication_lag_ms: lag_ms,
            last_health_check: Some(Utc::now()),
            status: if running {
                PgStatus::Running
            } else {
                PgStatus::Stopped
            },
        }
    }

    #[test]
    fn test_elect_primary_single_candidate() {
        let mgr = ReplicationManager::new(test_config());
        let candidates = vec![make_candidate("node-1", Some("0/1000"), None, true)];

        let winner = mgr.elect_primary(&candidates);
        assert_eq!(winner, Some(test_node_id("node-1")));
    }

    #[test]
    fn test_elect_primary_no_running_candidates() {
        let mgr = ReplicationManager::new(test_config());
        let candidates = vec![
            make_candidate("node-1", Some("0/1000"), None, false),
            make_candidate("node-2", Some("0/2000"), None, false),
        ];

        assert!(mgr.elect_primary(&candidates).is_none());
    }

    #[test]
    fn test_elect_primary_empty_candidates() {
        let mgr = ReplicationManager::new(test_config());
        assert!(mgr.elect_primary(&[]).is_none());
    }

    #[test]
    fn test_elect_primary_highest_wal_wins() {
        let mgr = ReplicationManager::new(test_config());
        let candidates = vec![
            make_candidate("node-1", Some("0/1000"), None, true),
            make_candidate("node-2", Some("0/3000"), None, true),
            make_candidate("node-3", Some("0/2000"), None, true),
        ];

        let winner = mgr.elect_primary(&candidates);
        assert_eq!(winner, Some(test_node_id("node-2")));
    }

    #[test]
    fn test_elect_primary_lowest_lag_breaks_wal_tie() {
        let mgr = ReplicationManager::new(test_config());
        let candidates = vec![
            make_candidate("node-1", Some("0/1000"), Some(100), true),
            make_candidate("node-2", Some("0/1000"), Some(10), true),
        ];

        let winner = mgr.elect_primary(&candidates);
        assert_eq!(winner, Some(test_node_id("node-2")));
    }

    #[test]
    fn test_elect_primary_deterministic_tiebreak() {
        let mgr = ReplicationManager::new(test_config());
        // Same WAL, same lag, same health — NodeId breaks tie
        let candidates = vec![
            make_candidate("node-b", Some("0/1000"), Some(10), true),
            make_candidate("node-a", Some("0/1000"), Some(10), true),
        ];

        let winner1 = mgr.elect_primary(&candidates);

        // Reverse input order — should get same result
        let reversed = vec![candidates[1].clone(), candidates[0].clone()];
        let winner2 = mgr.elect_primary(&reversed);

        assert_eq!(winner1, winner2, "Election must be deterministic");
    }

    #[test]
    fn test_elect_primary_filters_non_running() {
        let mgr = ReplicationManager::new(test_config());
        let candidates = vec![
            make_candidate("node-1", Some("0/3000"), None, false), // Stopped — best WAL but ineligible
            make_candidate("node-2", Some("0/1000"), None, true),  // Running
        ];

        let winner = mgr.elect_primary(&candidates);
        assert_eq!(winner, Some(test_node_id("node-2")));
    }

    #[test]
    fn test_slot_name_deterministic() {
        let mgr = ReplicationManager::new(test_config());
        let path = std::path::PathBuf::from("/data/marabunta/pgdata");

        let name1 = mgr.slot_name_for_node(&path);
        let name2 = mgr.slot_name_for_node(&path);
        assert_eq!(name1, name2);
        assert!(name1.starts_with("marabunta_"));
        assert!(name1.len() <= 63);
    }

    #[test]
    fn test_slot_name_unique_per_path() {
        let mgr = ReplicationManager::new(test_config());
        let name1 = mgr.slot_name_for_node(&std::path::PathBuf::from("/data/node1/pgdata"));
        let name2 = mgr.slot_name_for_node(&std::path::PathBuf::from("/data/node2/pgdata"));
        assert_ne!(name1, name2);
    }

    #[test]
    fn test_election_candidate_from_node_info() {
        let info = PgNodeInfo {
            node_id: test_node_id("test"),
            role: PgNodeRole::Replica,
            status: PgStatus::Running,
            pg_version: Some("16.2".into()),
            listen_addr: Some("10.0.0.1:5433".parse().unwrap()),
            pg_port: 5433,
            wal_position: Some("0/A000".into()),
            last_health_check: Some(Utc::now()),
            replication_lag_ms: Some(25),
        };

        let candidate = ElectionCandidate::from_node_info(&info);
        assert_eq!(candidate.node_id, info.node_id);
        assert_eq!(candidate.wal_position, Some("0/A000".into()));
        assert_eq!(candidate.replication_lag_ms, Some(25));
        assert!(candidate.status.is_running());
    }
}
