// Marabunta - Licensed under the MIT License.
//! PostgreSQL detection, deployment, and lifecycle management.
//!
//! The deployer follows a detection-first approach:
//!
//! 1. Probe for an existing system-installed PG (`pg_isready`, path scan)
//! 2. Check for a previously-deployed bundled PG binary
//! 3. If `allow_auto_deploy` is true and resources are sufficient, run
//!    `initdb` + generate config + start
//! 4. Generate `postgresql.conf` auto-tuned to node resources
//! 5. Generate `pg_hba.conf` scoped to the swarm's internal network
//!
//! The deployer does **not** download PG binaries from the internet (yet).
//! It relies on system-installed PG or a bundled binary placed at
//! `<data_dir>/../pg/bin/postgres`.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::process::Command;
use tracing::{debug, info, warn};

use super::config::PgConfig;
use super::types::{PgError, PgInstance, PgResourceSnapshot, PgStatus};

// ============================================================================
// Well-known PG binary locations
// ============================================================================

/// Common paths where PostgreSQL binaries are installed on various systems.
const PG_BIN_SEARCH_PATHS: &[&str] = &[
    // Debian/Ubuntu
    "/usr/lib/postgresql/17/bin",
    "/usr/lib/postgresql/16/bin",
    "/usr/lib/postgresql/15/bin",
    "/usr/lib/postgresql/14/bin",
    // RHEL/Fedora/CentOS
    "/usr/pgsql-17/bin",
    "/usr/pgsql-16/bin",
    "/usr/pgsql-15/bin",
    "/usr/pgsql-14/bin",
    // macOS Homebrew
    "/opt/homebrew/opt/postgresql@17/bin",
    "/opt/homebrew/opt/postgresql@16/bin",
    "/opt/homebrew/opt/postgresql@15/bin",
    "/usr/local/opt/postgresql@16/bin",
    "/usr/local/opt/postgresql@15/bin",
    // Generic / PATH fallback
    "/usr/local/bin",
    "/usr/bin",
];

// ============================================================================
// PgDeployer
// ============================================================================

/// Handles PG detection, deployment, configuration, and lifecycle.
pub struct PgDeployer {
    config: PgConfig,
}

impl PgDeployer {
    pub fn new(config: PgConfig) -> Self {
        Self { config }
    }

    // ========================================================================
    // Resource eligibility
    // ========================================================================

    /// Check whether this node's resources meet the minimum requirements
    /// to host a PostgreSQL instance.
    pub fn should_host_pg(&self, resources: &PgResourceSnapshot) -> bool {
        if resources.memory_total_mb < self.config.min_ram_for_pg_mb {
            debug!(
                total_mb = resources.memory_total_mb,
                min_mb = self.config.min_ram_for_pg_mb,
                "Insufficient RAM for PG hosting"
            );
            return false;
        }

        if resources.disk_available_mb < self.config.min_disk_for_pg_mb {
            debug!(
                available_mb = resources.disk_available_mb,
                min_mb = self.config.min_disk_for_pg_mb,
                "Insufficient disk space for PG hosting"
            );
            return false;
        }

        true
    }

    // ========================================================================
    // Detection
    // ========================================================================

    /// Detect an existing PostgreSQL installation.
    ///
    /// Searches well-known paths for the `postgres` binary and probes it
    /// with `--version`. Returns the first valid instance found.
    pub async fn detect_existing(&self) -> Option<PgInstance> {
        // First check the bundled location
        let bundled_bin_dir = self.bundled_bin_dir();
        if let Some(instance) = self.probe_pg_at(&bundled_bin_dir).await {
            info!(
                bin_dir = %bundled_bin_dir.display(),
                version = ?instance.version,
                "Found bundled PG installation"
            );
            return Some(instance);
        }

        // Then check system paths
        for path in PG_BIN_SEARCH_PATHS {
            let bin_dir = PathBuf::from(path);
            if let Some(instance) = self.probe_pg_at(&bin_dir).await {
                info!(
                    bin_dir = %bin_dir.display(),
                    version = ?instance.version,
                    "Found system PG installation"
                );
                return Some(instance);
            }
        }

        debug!("No PostgreSQL installation found");
        None
    }

    /// Probe a specific directory for a valid `postgres` binary.
    async fn probe_pg_at(&self, bin_dir: &Path) -> Option<PgInstance> {
        let postgres_path = bin_dir.join("postgres");
        if !postgres_path.exists() {
            return None;
        }

        // Get version
        let version = match Command::new(&postgres_path)
            .arg("--version")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .await
        {
            Ok(output) if output.status.success() => {
                let stdout = String::from_utf8_lossy(&output.stdout);
                Self::parse_version_string(&stdout)
            }
            _ => None,
        };

        let is_system_pg = !bin_dir.starts_with(&self.config.data_dir);

        Some(PgInstance {
            bin_dir: bin_dir.to_path_buf(),
            data_dir: self.config.data_dir.clone(),
            port: self.config.pg_port,
            version,
            is_system_pg,
        })
    }

    /// Parse version from `postgres --version` output.
    ///
    /// Example: "postgres (PostgreSQL) 16.2" -> "16.2"
    fn parse_version_string(output: &str) -> Option<String> {
        // Pattern: "postgres (PostgreSQL) X.Y.Z" or "postgres (PostgreSQL) X.Y"
        output
            .split_whitespace()
            .last()
            .map(|v| v.trim().to_string())
            .filter(|v| {
                v.chars()
                    .next()
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false)
            })
    }

    /// Path where a bundled PG binary would be located.
    fn bundled_bin_dir(&self) -> PathBuf {
        self.config
            .data_dir
            .parent()
            .unwrap_or(Path::new("."))
            .join("pg")
            .join("bin")
    }

    // ========================================================================
    // Initialization
    // ========================================================================

    /// Run `initdb` to create a fresh PG data directory.
    ///
    /// Only runs if the data directory does not already contain a valid
    /// PG cluster (checks for `PG_VERSION` file).
    pub async fn initdb(&self, instance: &PgInstance) -> Result<(), PgError> {
        let data_dir = &instance.data_dir;

        // Check if already initialized
        if data_dir.join("PG_VERSION").exists() {
            info!(data_dir = %data_dir.display(), "PG data directory already initialized");
            return Ok(());
        }

        // Create data directory
        tokio::fs::create_dir_all(data_dir)
            .await
            .map_err(|e| PgError::InitDbFailed(format!("create data dir: {e}")))?;

        let initdb_path = instance.bin_dir.join("initdb");
        if !initdb_path.exists() {
            return Err(PgError::InitDbFailed(format!(
                "initdb not found at {}",
                initdb_path.display()
            )));
        }

        info!(
            initdb = %initdb_path.display(),
            data_dir = %data_dir.display(),
            "Running initdb"
        );

        let output = Command::new(&initdb_path)
            .arg("-D")
            .arg(data_dir)
            .arg("--encoding=UTF8")
            .arg("--locale=C")
            .arg("--auth=trust") // Internal auth; pg_hba.conf restricts network access
            .arg("--username=marabunta")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .map_err(|e| PgError::InitDbFailed(format!("exec initdb: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(PgError::InitDbFailed(format!(
                "initdb failed (exit {}): {}",
                output.status, stderr
            )));
        }

        info!("initdb completed successfully");
        Ok(())
    }

    // ========================================================================
    // Configuration generation
    // ========================================================================

    /// Generate `postgresql.conf` tuned to this node's resources.
    pub fn generate_postgresql_conf(&self, resources: &PgResourceSnapshot) -> String {
        let total_ram_mb = resources.memory_total_mb;

        // shared_buffers: fraction of RAM, capped at 8GB
        let shared_buffers_mb = ((total_ram_mb as f32 * self.config.shared_buffers_fraction) as u64)
            .min(8192);

        // effective_cache_size: fraction of RAM
        let effective_cache_mb =
            (total_ram_mb as f32 * self.config.effective_cache_fraction) as u64;

        // work_mem: conservative — available_ram / max_connections / 4
        let work_mem_mb = (resources.memory_available_mb / self.config.max_connections as u64 / 4)
            .max(4)
            .min(64);

        // maintenance_work_mem: 5% of RAM, capped at 2GB
        let maintenance_work_mem_mb = ((total_ram_mb as f32 * 0.05) as u64).min(2048).max(64);

        // max_wal_senders: enough for replication + extra
        let max_wal_senders = self.config.replication_factor + 2;

        // wal_keep_size: scaled to disk
        let wal_keep_mb = (resources.disk_available_mb / 20).max(256).min(4096);

        format!(
            r#"# ============================================================================
# Marabunta Swarm — auto-generated postgresql.conf
# Generated for: {total_ram_mb} MB RAM, {cpu_cores} CPU cores, {disk_mb} MB disk
# ============================================================================

# Connection settings
listen_addresses = '*'
port = {port}
max_connections = {max_connections}

# Memory settings
shared_buffers = {shared_buffers_mb}MB
effective_cache_size = {effective_cache_mb}MB
work_mem = {work_mem_mb}MB
maintenance_work_mem = {maintenance_work_mem_mb}MB

# WAL settings (always replica-ready)
wal_level = replica
max_wal_senders = {max_wal_senders}
wal_keep_size = {wal_keep_mb}MB
synchronous_commit = on
checkpoint_completion_target = 0.9

# Replication slots (auto-created by replication manager)
max_replication_slots = {max_wal_senders}

# Logging
log_destination = 'stderr'
logging_collector = on
log_directory = 'log'
log_filename = 'postgresql-%Y-%m-%d.log'
log_min_duration_statement = 1000
log_line_prefix = '%m [%p] %q%u@%d '

# Performance
random_page_cost = 1.1
effective_io_concurrency = 200
default_statistics_target = 100

# Autovacuum (tuned for append-mostly workload)
autovacuum = on
autovacuum_naptime = 60
autovacuum_vacuum_threshold = 1000
autovacuum_analyze_threshold = 500

# Timezone
timezone = 'UTC'
"#,
            total_ram_mb = total_ram_mb,
            cpu_cores = resources.cpu_cores,
            disk_mb = resources.disk_available_mb,
            port = self.config.pg_port,
            max_connections = self.config.max_connections,
            shared_buffers_mb = shared_buffers_mb,
            effective_cache_mb = effective_cache_mb,
            work_mem_mb = work_mem_mb,
            maintenance_work_mem_mb = maintenance_work_mem_mb,
            max_wal_senders = max_wal_senders,
            wal_keep_mb = wal_keep_mb,
        )
    }

    /// Generate `pg_hba.conf` restricting access to the swarm's internal
    /// network and the local node.
    ///
    /// `swarm_cidrs` should contain the CIDR ranges of known swarm nodes
    /// (e.g., "10.0.0.0/24", "192.168.1.0/24").
    pub fn generate_pg_hba_conf(&self, swarm_cidrs: &[String]) -> String {
        let mut lines = vec![
            "# ============================================================================".to_string(),
            "# Marabunta Swarm — auto-generated pg_hba.conf".to_string(),
            "# ============================================================================".to_string(),
            String::new(),
            "# TYPE  DATABASE  USER       ADDRESS    METHOD".to_string(),
            String::new(),
            "# Local connections (Unix socket)".to_string(),
            "local   all       marabunta                trust".to_string(),
            "local   all       all                     reject".to_string(),
            String::new(),
            "# Loopback (IPv4 + IPv6)".to_string(),
            "host    all       marabunta   127.0.0.1/32   trust".to_string(),
            "host    all       marabunta   ::1/128        trust".to_string(),
            String::new(),
            "# Replication connections (same trust as data connections)".to_string(),
            "local   replication  marabunta               trust".to_string(),
            "host    replication  marabunta  127.0.0.1/32  trust".to_string(),
            "host    replication  marabunta  ::1/128       trust".to_string(),
        ];

        if !swarm_cidrs.is_empty() {
            lines.push(String::new());
            lines.push("# Swarm internal network".to_string());
            for cidr in swarm_cidrs {
                lines.push(format!(
                    "host    all          marabunta  {cidr}  trust"
                ));
                lines.push(format!(
                    "host    replication  marabunta  {cidr}  trust"
                ));
            }
        }

        lines.push(String::new());
        lines.push("# Reject everything else".to_string());
        lines.push("host    all       all        0.0.0.0/0      reject".to_string());
        lines.push("host    all       all        ::/0           reject".to_string());
        lines.push(String::new());

        lines.join("\n")
    }

    /// Write the generated configuration files to the PG data directory.
    pub async fn write_config_files(
        &self,
        instance: &PgInstance,
        resources: &PgResourceSnapshot,
        swarm_cidrs: &[String],
    ) -> Result<(), PgError> {
        let pg_conf = self.generate_postgresql_conf(resources);
        let hba_conf = self.generate_pg_hba_conf(swarm_cidrs);

        let pg_conf_path = instance.data_dir.join("postgresql.conf");
        let hba_path = instance.data_dir.join("pg_hba.conf");

        tokio::fs::write(&pg_conf_path, &pg_conf)
            .await
            .map_err(PgError::Io)?;

        tokio::fs::write(&hba_path, &hba_conf)
            .await
            .map_err(PgError::Io)?;

        info!(
            pg_conf = %pg_conf_path.display(),
            hba_conf = %hba_path.display(),
            "Wrote PG configuration files"
        );

        Ok(())
    }

    // ========================================================================
    // Lifecycle management
    // ========================================================================

    /// Start the PostgreSQL instance using `pg_ctl start`.
    pub async fn start(&self, instance: &PgInstance) -> Result<(), PgError> {
        let pg_ctl = instance.bin_dir.join("pg_ctl");
        if !pg_ctl.exists() {
            return Err(PgError::StartFailed(format!(
                "pg_ctl not found at {}",
                pg_ctl.display()
            )));
        }

        info!(
            data_dir = %instance.data_dir.display(),
            port = instance.port,
            "Starting PostgreSQL"
        );

        let output = Command::new(&pg_ctl)
            .arg("start")
            .arg("-D")
            .arg(&instance.data_dir)
            .arg("-w") // Wait for startup to complete
            .arg("-t")
            .arg("30") // Timeout 30 seconds
            .arg("-l")
            .arg(instance.data_dir.join("log").join("startup.log"))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .map_err(|e| PgError::StartFailed(format!("exec pg_ctl: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(PgError::StartFailed(format!(
                "pg_ctl start failed (exit {}): {}",
                output.status, stderr
            )));
        }

        info!("PostgreSQL started successfully");
        Ok(())
    }

    /// Stop the PostgreSQL instance gracefully using `pg_ctl stop`.
    pub async fn stop(&self, instance: &PgInstance) -> Result<(), PgError> {
        let pg_ctl = instance.bin_dir.join("pg_ctl");

        info!(
            data_dir = %instance.data_dir.display(),
            "Stopping PostgreSQL"
        );

        let output = Command::new(&pg_ctl)
            .arg("stop")
            .arg("-D")
            .arg(&instance.data_dir)
            .arg("-m")
            .arg("fast") // Fast shutdown (no new connections, finish in-progress)
            .arg("-w") // Wait for shutdown
            .arg("-t")
            .arg("60") // Timeout 60 seconds
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .map_err(|e| PgError::Io(std::io::Error::other(e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(stderr = %stderr, "pg_ctl stop returned non-zero");
        } else {
            info!("PostgreSQL stopped successfully");
        }

        Ok(())
    }

    /// Check if PostgreSQL is currently running (via `pg_ctl status`).
    pub async fn is_running(&self, instance: &PgInstance) -> bool {
        let pg_ctl = instance.bin_dir.join("pg_ctl");
        if !pg_ctl.exists() {
            return false;
        }

        match Command::new(&pg_ctl)
            .arg("status")
            .arg("-D")
            .arg(&instance.data_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
        {
            Ok(output) => output.status.success(),
            Err(_) => false,
        }
    }

    /// Reload PostgreSQL configuration without restart (`pg_ctl reload`).
    pub async fn reload_config(&self, instance: &PgInstance) -> Result<(), PgError> {
        let pg_ctl = instance.bin_dir.join("pg_ctl");

        let output = Command::new(&pg_ctl)
            .arg("reload")
            .arg("-D")
            .arg(&instance.data_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .map_err(|e| PgError::Io(std::io::Error::other(e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(stderr = %stderr, "pg_ctl reload returned non-zero");
        } else {
            debug!("PostgreSQL configuration reloaded");
        }

        Ok(())
    }

    /// Get the current PG status by combining file checks and `pg_ctl status`.
    pub async fn current_status(&self, instance: &PgInstance) -> PgStatus {
        if !instance.data_dir.join("PG_VERSION").exists() {
            return PgStatus::NotInstalled;
        }

        if self.is_running(instance).await {
            PgStatus::Running
        } else {
            PgStatus::Stopped
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> PgConfig {
        PgConfig::default()
    }

    fn test_resources_8gb() -> PgResourceSnapshot {
        PgResourceSnapshot {
            memory_total_mb: 8192,
            memory_available_mb: 6144,
            disk_total_mb: 256_000,
            disk_available_mb: 200_000,
            cpu_cores: 4,
        }
    }

    fn test_resources_16gb() -> PgResourceSnapshot {
        PgResourceSnapshot {
            memory_total_mb: 16384,
            memory_available_mb: 12288,
            disk_total_mb: 512_000,
            disk_available_mb: 400_000,
            cpu_cores: 8,
        }
    }

    fn test_resources_32gb() -> PgResourceSnapshot {
        PgResourceSnapshot {
            memory_total_mb: 32768,
            memory_available_mb: 24576,
            disk_total_mb: 1_024_000,
            disk_available_mb: 800_000,
            cpu_cores: 16,
        }
    }

    fn test_resources_dust() -> PgResourceSnapshot {
        PgResourceSnapshot {
            memory_total_mb: 1024,
            memory_available_mb: 512,
            disk_total_mb: 16_000,
            disk_available_mb: 8_000,
            cpu_cores: 1,
        }
    }

    fn test_resources_low_disk() -> PgResourceSnapshot {
        PgResourceSnapshot {
            memory_total_mb: 8192,
            memory_available_mb: 6144,
            disk_total_mb: 16_000,
            disk_available_mb: 1024, // Only 1GB free
            cpu_cores: 4,
        }
    }

    #[test]
    fn test_should_host_pg_8gb() {
        let deployer = PgDeployer::new(test_config());
        assert!(deployer.should_host_pg(&test_resources_8gb()));
    }

    #[test]
    fn test_should_host_pg_dust_rejected() {
        let deployer = PgDeployer::new(test_config());
        assert!(!deployer.should_host_pg(&test_resources_dust()));
    }

    #[test]
    fn test_should_host_pg_low_disk_rejected() {
        let deployer = PgDeployer::new(test_config());
        assert!(!deployer.should_host_pg(&test_resources_low_disk()));
    }

    #[test]
    fn test_postgresql_conf_8gb() {
        let deployer = PgDeployer::new(test_config());
        let conf = deployer.generate_postgresql_conf(&test_resources_8gb());

        assert!(conf.contains("port = 5433"));
        assert!(conf.contains("max_connections = 100"));
        // shared_buffers = 8192 * 0.25 = 2048MB
        assert!(conf.contains("shared_buffers = 2048MB"));
        // effective_cache_size = 8192 * 0.5 = 4096MB
        assert!(conf.contains("effective_cache_size = 4096MB"));
        assert!(conf.contains("wal_level = replica"));
        assert!(conf.contains("listen_addresses = '*'"));
    }

    #[test]
    fn test_postgresql_conf_16gb() {
        let deployer = PgDeployer::new(test_config());
        let conf = deployer.generate_postgresql_conf(&test_resources_16gb());

        // shared_buffers = 16384 * 0.25 = 4096MB
        assert!(conf.contains("shared_buffers = 4096MB"));
        // effective_cache_size = 16384 * 0.5 = 8192MB
        assert!(conf.contains("effective_cache_size = 8192MB"));
    }

    #[test]
    fn test_postgresql_conf_32gb() {
        let deployer = PgDeployer::new(test_config());
        let conf = deployer.generate_postgresql_conf(&test_resources_32gb());

        // shared_buffers = 32768 * 0.25 = 8192MB, capped at 8192
        assert!(conf.contains("shared_buffers = 8192MB"));
    }

    #[test]
    fn test_postgresql_conf_custom_port() {
        let mut config = test_config();
        config.pg_port = 5434;
        let deployer = PgDeployer::new(config);
        let conf = deployer.generate_postgresql_conf(&test_resources_8gb());

        assert!(conf.contains("port = 5434"));
    }

    #[test]
    fn test_pg_hba_conf_no_cidrs() {
        let deployer = PgDeployer::new(test_config());
        let hba = deployer.generate_pg_hba_conf(&[]);

        assert!(hba.contains("local   all       marabunta                trust"));
        assert!(hba.contains("host    all       marabunta   127.0.0.1/32   trust"));
        assert!(hba.contains("host    all       all        0.0.0.0/0      reject"));
        // No swarm network section
        assert!(!hba.contains("Swarm internal network"));
    }

    #[test]
    fn test_pg_hba_conf_with_cidrs() {
        let deployer = PgDeployer::new(test_config());
        let cidrs = vec!["10.0.0.0/24".to_string(), "192.168.1.0/24".to_string()];
        let hba = deployer.generate_pg_hba_conf(&cidrs);

        assert!(hba.contains("Swarm internal network"));
        assert!(hba.contains("host    all          marabunta  10.0.0.0/24  trust"));
        assert!(hba.contains("host    replication  marabunta  10.0.0.0/24  trust"));
        assert!(hba.contains("host    all          marabunta  192.168.1.0/24  trust"));
        assert!(hba.contains("host    replication  marabunta  192.168.1.0/24  trust"));
    }

    #[test]
    fn test_pg_hba_conf_rejects_non_marabunta() {
        let deployer = PgDeployer::new(test_config());
        let hba = deployer.generate_pg_hba_conf(&[]);

        // Non-marabunta local users rejected
        assert!(hba.contains("local   all       all                     reject"));
        // All remote users rejected
        assert!(hba.contains("host    all       all        0.0.0.0/0      reject"));
    }

    #[test]
    fn test_parse_version_string() {
        assert_eq!(
            PgDeployer::parse_version_string("postgres (PostgreSQL) 16.2"),
            Some("16.2".to_string())
        );
        assert_eq!(
            PgDeployer::parse_version_string("postgres (PostgreSQL) 15.4.1"),
            Some("15.4.1".to_string())
        );
        assert_eq!(
            PgDeployer::parse_version_string("not a version"),
            None
        );
        assert_eq!(
            PgDeployer::parse_version_string(""),
            None
        );
    }

    #[test]
    fn test_replication_factor_in_conf() {
        let mut config = test_config();
        config.replication_factor = 3;
        let deployer = PgDeployer::new(config);
        let conf = deployer.generate_postgresql_conf(&test_resources_8gb());

        // max_wal_senders = replication_factor + 2 = 5
        assert!(conf.contains("max_wal_senders = 5"));
    }

    #[test]
    fn test_bundled_bin_dir() {
        let deployer = PgDeployer::new(test_config());
        let bin_dir = deployer.bundled_bin_dir();
        // Default data_dir is ./data/marabunta/pgdata, so bundled is ./data/marabunta/pg/bin
        assert!(bin_dir.ends_with("pg/bin"));
    }

    #[test]
    fn test_work_mem_bounds() {
        let deployer = PgDeployer::new(test_config());

        // 8GB machine: 6144 available / 100 conns / 4 = 15MB
        let conf = deployer.generate_postgresql_conf(&test_resources_8gb());
        assert!(conf.contains("work_mem = 15MB"));

        // Edge machine (if it somehow got through): should floor at 4MB
        let edge = PgResourceSnapshot {
            memory_total_mb: 2048,
            memory_available_mb: 256,
            disk_total_mb: 16_000,
            disk_available_mb: 8_000,
            cpu_cores: 1,
        };
        let conf = deployer.generate_postgresql_conf(&edge);
        assert!(conf.contains("work_mem = 4MB"));
    }
}
