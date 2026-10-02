// Marabunta - Licensed under the MIT License.
//! Out-of-process plugin manager.
//!
//! Spawns, monitors, and restarts plugin processes. Each plugin runs as a
//! separate OS process communicating with the swarm host over a Unix socket
//! at `{socket_dir}/{plugin_name}.sock`.
//!
//! The manager owns the full lifecycle: spawn -> monitor -> stop/restart -> cleanup.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tracing::{debug, info, warn};

use crate::plugin::config::{
    PluginConfig, PLUGIN_MAX_RESTART_ATTEMPTS, PLUGIN_RESTART_BACKOFF, PLUGIN_STOP_TIMEOUT,
};
use crate::plugin::types::{PluginError, PluginId, PluginResult, PluginState};

// ============================================================================
// ProcessInfo -- public summary of a managed process
// ============================================================================

/// Read-only snapshot of a managed plugin process.
#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub plugin_id: PluginId,
    pub plugin_name: String,
    pub pid: Option<u32>,
    pub state: PluginState,
    pub restart_count: u32,
    pub socket_path: PathBuf,
    pub uptime_secs: Option<u64>,
}

// ============================================================================
// ManagedProcess -- internal bookkeeping per plugin
// ============================================================================

struct ManagedProcess {
    plugin_name: String,
    config: PluginConfig,
    child: Option<tokio::process::Child>,
    pid: Option<u32>,
    socket_path: PathBuf,
    state: PluginState,
    restart_count: u32,
    started_at: Option<Instant>,
    last_restart_at: Option<Instant>,
}

// ============================================================================
// ProcessManager
// ============================================================================

/// Manages out-of-process plugin lifecycles.
///
/// Each plugin is spawned as a child process with environment variables
/// pointing it to its Unix socket. The manager tracks state, handles
/// graceful and forced shutdown, and supports automatic restart with
/// exponential-ish backoff (constant for now, matching config).
pub struct ProcessManager {
    processes: DashMap<String, ManagedProcess>,
    socket_dir: PathBuf,
}

impl ProcessManager {
    /// Create a new process manager that places sockets in `socket_dir`.
    pub fn new(socket_dir: PathBuf) -> Self {
        Self {
            processes: DashMap::new(),
            socket_dir,
        }
    }

    // ========================================================================
    // Spawn
    // ========================================================================

    /// Spawn a plugin as a child process.
    ///
    /// 1. Validates that the binary path exists (first token of the binary string).
    /// 2. Creates the socket path `{socket_dir}/{plugin_name}.sock`.
    /// 3. Sets env vars: `SWARM_SOCKET`, `PLUGIN_NAME`, plus any from `config.env`.
    /// 4. Sets `working_dir` if configured.
    /// 5. Parses the binary command string (handles `"python3 ./script.py"` style).
    /// 6. Spawns the child with `kill_on_drop(true)`.
    /// 7. Stores the `ManagedProcess` and returns the OS pid.
    pub async fn spawn_plugin(
        &self,
        plugin_id: &PluginId,
        config: &PluginConfig,
    ) -> PluginResult<u32> {
        let binary_str = config.binary.as_deref().ok_or_else(|| {
            PluginError::Process("plugin config has no binary path".into())
        })?;

        // Parse the binary command string.
        let (program, args) = parse_binary_command(binary_str);

        // Validate that the program exists on disk (or is resolvable via PATH).
        validate_program_exists(&program)?;

        // Ensure socket directory exists.
        tokio::fs::create_dir_all(&self.socket_dir).await.map_err(|e| {
            PluginError::Process(format!(
                "failed to create socket directory {}: {}",
                self.socket_dir.display(),
                e
            ))
        })?;

        let socket_path = self.socket_dir.join(format!("{}.sock", config.name));

        // Remove stale socket file if it exists.
        if socket_path.exists() {
            let _ = tokio::fs::remove_file(&socket_path).await;
        }

        // Build the command.
        let mut cmd = tokio::process::Command::new(&program);
        for arg in &args {
            cmd.arg(arg);
        }

        // Environment variables.
        cmd.env("SWARM_SOCKET", &socket_path);
        cmd.env("PLUGIN_NAME", &config.name);
        for (key, value) in &config.env {
            cmd.env(key, value);
        }

        // Working directory.
        if let Some(ref working_dir) = config.working_dir {
            cmd.current_dir(working_dir);
        }

        // Stdio: pipe stdout/stderr so the child doesn't inherit our terminal,
        // but we don't actively read them here (a future log-forwarder can).
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        cmd.kill_on_drop(true);

        // On Unix, start a new process group and apply resource limits.
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;

            // Capture resource limit values before the closure (which must be 'static).
            let rlimit_as = config.max_memory_mb.map(|mb| mb * 1024 * 1024); // MB → bytes
            let rlimit_cpu = config.max_cpu_seconds;
            let rlimit_nofile = config.max_open_files;
            let rlimit_nproc = config.max_processes;

            if rlimit_as.is_some() || rlimit_cpu.is_some() || rlimit_nofile.is_some() || rlimit_nproc.is_some() {
                info!(
                    plugin = %config.name,
                    max_memory_mb = ?config.max_memory_mb,
                    max_cpu_seconds = ?config.max_cpu_seconds,
                    max_open_files = ?config.max_open_files,
                    max_processes = ?config.max_processes,
                    "applying resource limits to plugin process"
                );
            }

            // SAFETY: pre_exec runs in the child between fork and exec.
            // setsid() and setrlimit() are async-signal-safe.
            unsafe {
                cmd.pre_exec(move || {
                    libc::setsid();

                    if let Some(bytes) = rlimit_as {
                        let rlim = libc::rlimit { rlim_cur: bytes, rlim_max: bytes };
                        if libc::setrlimit(libc::RLIMIT_AS, &rlim) != 0 {
                            return Err(std::io::Error::last_os_error());
                        }
                    }
                    if let Some(secs) = rlimit_cpu {
                        let rlim = libc::rlimit { rlim_cur: secs, rlim_max: secs };
                        if libc::setrlimit(libc::RLIMIT_CPU, &rlim) != 0 {
                            return Err(std::io::Error::last_os_error());
                        }
                    }
                    if let Some(files) = rlimit_nofile {
                        let rlim = libc::rlimit { rlim_cur: files, rlim_max: files };
                        if libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) != 0 {
                            return Err(std::io::Error::last_os_error());
                        }
                    }
                    if let Some(nproc) = rlimit_nproc {
                        let rlim = libc::rlimit { rlim_cur: nproc, rlim_max: nproc };
                        if libc::setrlimit(libc::RLIMIT_NPROC, &rlim) != 0 {
                            return Err(std::io::Error::last_os_error());
                        }
                    }

                    Ok(())
                });
            }
        }

        let child = cmd.spawn().map_err(|e| {
            PluginError::Process(format!("failed to spawn plugin '{}': {}", config.name, e))
        })?;

        let pid = child.id().ok_or_else(|| {
            PluginError::Process(format!(
                "plugin '{}' exited immediately after spawn",
                config.name
            ))
        })?;

        info!(
            plugin = %config.name,
            pid = pid,
            socket = %socket_path.display(),
            "spawned plugin process"
        );

        let managed = ManagedProcess {
            plugin_name: config.name.clone(),
            config: config.clone(),
            child: Some(child),
            pid: Some(pid),
            socket_path,
            state: PluginState::Spawned,
            restart_count: 0,
            started_at: Some(Instant::now()),
            last_restart_at: None,
        };

        self.processes.insert(plugin_id.0.clone(), managed);

        Ok(pid)
    }

    // ========================================================================
    // Stop
    // ========================================================================

    /// Stop a running plugin process.
    ///
    /// On Unix, sends SIGTERM and waits up to `timeout` for the process to
    /// exit. If it does not exit in time, sends SIGKILL. On non-Unix
    /// platforms, calls `kill()` directly.
    ///
    /// Cleans up the socket file on disk.
    ///
    /// Returns `true` if the process exited cleanly (SIGTERM was sufficient),
    /// `false` if a force kill was required.
    pub async fn stop_plugin(
        &self,
        plugin_id: &PluginId,
        timeout: Duration,
    ) -> PluginResult<bool> {
        let mut entry = self.processes.get_mut(&plugin_id.0).ok_or_else(|| {
            PluginError::NotFound(format!("plugin '{}' not found in process manager", plugin_id))
        })?;
        let managed = entry.value_mut();
        managed.state = PluginState::Stopping;

        let socket_path = managed.socket_path.clone();
        let plugin_name = managed.plugin_name.clone();

        let clean = if let Some(ref mut child) = managed.child {
            let pid = managed.pid;

            // Attempt graceful shutdown via SIGTERM.
            #[cfg(unix)]
            {
                if let Some(raw_pid) = pid {
                    // Send SIGTERM to the process group (negative pid).
                    unsafe {
                        libc::kill(-(raw_pid as i32), libc::SIGTERM);
                    }
                    debug!(plugin = %plugin_name, pid = raw_pid, "sent SIGTERM");
                }
            }

            #[cfg(not(unix))]
            {
                let _ = child.start_kill();
                debug!(plugin = %plugin_name, "sent kill (non-unix)");
            }

            // Wait for exit with timeout.
            let wait_result = tokio::time::timeout(timeout, child.wait()).await;

            match wait_result {
                Ok(Ok(status)) => {
                    debug!(
                        plugin = %plugin_name,
                        status = %status,
                        "plugin exited after SIGTERM"
                    );
                    true
                }
                Ok(Err(e)) => {
                    warn!(
                        plugin = %plugin_name,
                        error = %e,
                        "error waiting for plugin exit, force killing"
                    );
                    let _ = child.kill().await;
                    false
                }
                Err(_) => {
                    // Timeout expired. Force kill.
                    warn!(
                        plugin = %plugin_name,
                        timeout_secs = timeout.as_secs(),
                        "plugin did not exit within timeout, force killing"
                    );
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    false
                }
            }
        } else {
            // No child process -- nothing to kill.
            true
        };

        managed.child = None;
        managed.pid = None;
        managed.state = PluginState::Stopped;

        // Drop the DashMap guard before async filesystem I/O.
        drop(entry);

        // Clean up socket file.
        if socket_path.exists() {
            if let Err(e) = tokio::fs::remove_file(&socket_path).await {
                warn!(
                    plugin = %plugin_name,
                    socket = %socket_path.display(),
                    error = %e,
                    "failed to remove socket file"
                );
            }
        }

        info!(
            plugin = %plugin_name,
            clean = clean,
            "plugin process stopped"
        );

        Ok(clean)
    }

    // ========================================================================
    // Restart
    // ========================================================================

    /// Restart a plugin process.
    ///
    /// 1. Stops the running process (with the default stop timeout).
    /// 2. Checks the restart count against the configured maximum.
    /// 3. Sleeps for the backoff duration.
    /// 4. Re-spawns the plugin.
    /// 5. Returns the new OS pid.
    pub async fn restart_plugin(&self, plugin_id: &PluginId) -> PluginResult<u32> {
        // Grab the config before stopping so we can re-spawn.
        let config = {
            let entry = self.processes.get(&plugin_id.0).ok_or_else(|| {
                PluginError::NotFound(format!(
                    "plugin '{}' not found in process manager",
                    plugin_id
                ))
            })?;
            entry.value().config.clone()
        };

        let max_restarts = config
            .max_restart_attempts
            .unwrap_or(PLUGIN_MAX_RESTART_ATTEMPTS);

        // Stop the current process.
        let _ = self.stop_plugin(plugin_id, PLUGIN_STOP_TIMEOUT).await;

        // Check restart budget.
        let current_count = self.get_restart_count(plugin_id);
        if current_count >= max_restarts {
            // Mark as failed.
            if let Some(mut entry) = self.processes.get_mut(&plugin_id.0) {
                entry.value_mut().state = PluginState::Failed;
            }
            return Err(PluginError::Lifecycle(format!(
                "plugin '{}' exceeded maximum restart attempts ({}/{})",
                plugin_id, current_count, max_restarts
            )));
        }

        // Backoff.
        tokio::time::sleep(PLUGIN_RESTART_BACKOFF).await;

        // Remove old entry so spawn_plugin can insert a fresh one.
        // Preserve the restart count.
        let previous_count = {
            if let Some((_, old)) = self.processes.remove(&plugin_id.0) {
                old.restart_count
            } else {
                0
            }
        };

        // Re-spawn.
        let new_pid = self.spawn_plugin(plugin_id, &config).await?;

        // Restore and increment restart bookkeeping.
        if let Some(mut entry) = self.processes.get_mut(&plugin_id.0) {
            let managed = entry.value_mut();
            managed.restart_count = previous_count + 1;
            managed.last_restart_at = Some(Instant::now());
        }

        info!(
            plugin = %config.name,
            new_pid = new_pid,
            restart_count = previous_count + 1,
            "plugin restarted"
        );

        Ok(new_pid)
    }

    // ========================================================================
    // Queries
    // ========================================================================

    /// Get the socket path for a plugin.
    pub fn get_socket_path(&self, plugin_id: &PluginId) -> Option<PathBuf> {
        self.processes
            .get(&plugin_id.0)
            .map(|entry| entry.value().socket_path.clone())
    }

    /// Check if a plugin's child process is still alive.
    ///
    /// Attempts a non-blocking wait on the child. If the child has exited
    /// the process is no longer running.
    pub fn is_running(&self, plugin_id: &PluginId) -> bool {
        let mut entry = match self.processes.get_mut(&plugin_id.0) {
            Some(e) => e,
            None => return false,
        };
        let managed = entry.value_mut();
        match managed.child {
            Some(ref mut child) => {
                // try_wait returns Ok(Some(status)) if exited, Ok(None) if still running.
                match child.try_wait() {
                    Ok(Some(_)) => {
                        // Process has exited.
                        false
                    }
                    Ok(None) => true,
                    Err(_) => false,
                }
            }
            None => false,
        }
    }

    /// Get the OS pid for a plugin.
    pub fn get_pid(&self, plugin_id: &PluginId) -> Option<u32> {
        self.processes
            .get(&plugin_id.0)
            .and_then(|entry| entry.value().pid)
    }

    /// Get the current state of a plugin.
    pub fn get_state(&self, plugin_id: &PluginId) -> Option<PluginState> {
        self.processes
            .get(&plugin_id.0)
            .map(|entry| entry.value().state)
    }

    /// Set the state of a plugin.
    pub fn set_state(&self, plugin_id: &PluginId, state: PluginState) {
        if let Some(mut entry) = self.processes.get_mut(&plugin_id.0) {
            entry.value_mut().state = state;
        }
    }

    /// Get the number of times a plugin has been restarted.
    pub fn get_restart_count(&self, plugin_id: &PluginId) -> u32 {
        self.processes
            .get(&plugin_id.0)
            .map(|entry| entry.value().restart_count)
            .unwrap_or(0)
    }

    /// Increment the restart counter and return the new value.
    pub fn increment_restart_count(&self, plugin_id: &PluginId) -> u32 {
        if let Some(mut entry) = self.processes.get_mut(&plugin_id.0) {
            let managed = entry.value_mut();
            managed.restart_count += 1;
            managed.restart_count
        } else {
            0
        }
    }

    /// Remove all state for a plugin. Does NOT stop the process -- call
    /// [`stop_plugin`] first if the process may still be running.
    pub fn remove_plugin(&self, plugin_id: &PluginId) {
        self.processes.remove(&plugin_id.0);
    }

    /// Number of plugins currently in a running-like state (have a child process).
    pub fn active_count(&self) -> usize {
        self.processes
            .iter()
            .filter(|entry| entry.value().child.is_some())
            .count()
    }

    /// Return a summary snapshot of every managed process.
    pub fn list_processes(&self) -> Vec<ProcessInfo> {
        self.processes
            .iter()
            .map(|entry| {
                let key = entry.key().clone();
                let managed = entry.value();
                let uptime_secs = managed.started_at.map(|s| s.elapsed().as_secs());
                ProcessInfo {
                    plugin_id: PluginId(key),
                    plugin_name: managed.plugin_name.clone(),
                    pid: managed.pid,
                    state: managed.state,
                    restart_count: managed.restart_count,
                    socket_path: managed.socket_path.clone(),
                    uptime_secs,
                }
            })
            .collect()
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Parse a binary command string into (program, args).
///
/// If the string contains spaces, split on the first space to get the program,
/// then split the remainder into individual arguments. For example:
///
/// - `"python3 ./script.py"` -> `("python3", ["./script.py"])`
/// - `"python3 -u ./script.py --flag"` -> `("python3", ["-u", "./script.py", "--flag"])`
/// - `"./plugins/my-plugin"` -> `("./plugins/my-plugin", [])`
fn parse_binary_command(binary: &str) -> (String, Vec<String>) {
    let trimmed = binary.trim();
    let parts: Vec<&str> = trimmed.splitn(2, char::is_whitespace).collect();
    let program = parts[0].to_string();
    let args = if parts.len() > 1 {
        parts[1]
            .split_whitespace()
            .map(String::from)
            .collect()
    } else {
        Vec::new()
    };
    (program, args)
}

/// Validate that a program exists on disk or is findable in PATH.
fn validate_program_exists(program: &str) -> PluginResult<()> {
    let path = Path::new(program);

    // If it looks like a path (contains a separator), check the filesystem.
    if program.contains('/') || program.contains('\\') {
        if !path.exists() {
            return Err(PluginError::Process(format!(
                "binary not found at path: {}",
                program
            )));
        }
        return Ok(());
    }

    // Otherwise it should be a bare command name resolvable via PATH.
    // Use `which`-style lookup.
    if let Ok(path_var) = std::env::var("PATH") {
        for dir in path_var.split(':') {
            let candidate = Path::new(dir).join(program);
            if candidate.exists() {
                return Ok(());
            }
        }
    }

    Err(PluginError::Process(format!(
        "binary '{}' not found in PATH",
        program
    )))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // ====================================================================
    // parse_binary_command
    // ====================================================================

    #[test]
    fn parse_simple_binary() {
        let (program, args) = parse_binary_command("./plugins/my-plugin");
        assert_eq!(program, "./plugins/my-plugin");
        assert!(args.is_empty());
    }

    #[test]
    fn parse_binary_with_single_arg() {
        let (program, args) = parse_binary_command("python3 ./script.py");
        assert_eq!(program, "python3");
        assert_eq!(args, vec!["./script.py"]);
    }

    #[test]
    fn parse_binary_with_multiple_args() {
        let (program, args) = parse_binary_command("python3 -u ./script.py --flag");
        assert_eq!(program, "python3");
        assert_eq!(args, vec!["-u", "./script.py", "--flag"]);
    }

    #[test]
    fn parse_binary_with_leading_trailing_whitespace() {
        let (program, args) = parse_binary_command("  python3  ./script.py  ");
        assert_eq!(program, "python3");
        assert_eq!(args, vec!["./script.py"]);
    }

    #[test]
    fn parse_binary_with_tabs() {
        let (program, args) = parse_binary_command("python3\t./script.py");
        assert_eq!(program, "python3");
        assert_eq!(args, vec!["./script.py"]);
    }

    // ====================================================================
    // validate_program_exists
    // ====================================================================

    #[test]
    fn validate_existing_path_binary() {
        // /bin/sh should exist on any unix system.
        #[cfg(unix)]
        {
            assert!(validate_program_exists("/bin/sh").is_ok());
        }
    }

    #[test]
    fn validate_nonexistent_path_binary() {
        let result = validate_program_exists("/nonexistent/path/binary_xyz_12345");
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::Process(msg) => {
                assert!(msg.contains("not found"), "unexpected message: {}", msg);
            }
            other => panic!("unexpected error variant: {:?}", other),
        }
    }

    #[test]
    fn validate_bare_command_in_path() {
        // "sh" should be findable via PATH on any unix system.
        #[cfg(unix)]
        {
            assert!(validate_program_exists("sh").is_ok());
        }
    }

    #[test]
    fn validate_nonexistent_bare_command() {
        let result = validate_program_exists("nonexistent_binary_xyz_12345");
        assert!(result.is_err());
    }

    // ====================================================================
    // ProcessManager -- state management
    // ====================================================================

    fn test_config(name: &str) -> PluginConfig {
        PluginConfig {
            name: name.into(),
            binary: Some("/bin/sh".into()),
            library: None,
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: true,
            health_check_interval: None,
            max_restart_attempts: None,
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        }
    }

    #[test]
    fn new_manager_has_no_processes() {
        let mgr = ProcessManager::new(PathBuf::from("/tmp/test-plugins"));
        assert_eq!(mgr.active_count(), 0);
        assert!(mgr.list_processes().is_empty());
    }

    #[test]
    fn get_state_returns_none_for_unknown() {
        let mgr = ProcessManager::new(PathBuf::from("/tmp/test-plugins"));
        let id = PluginId("unknown".into());
        assert!(mgr.get_state(&id).is_none());
    }

    #[test]
    fn get_pid_returns_none_for_unknown() {
        let mgr = ProcessManager::new(PathBuf::from("/tmp/test-plugins"));
        let id = PluginId("unknown".into());
        assert!(mgr.get_pid(&id).is_none());
    }

    #[test]
    fn get_socket_path_returns_none_for_unknown() {
        let mgr = ProcessManager::new(PathBuf::from("/tmp/test-plugins"));
        let id = PluginId("unknown".into());
        assert!(mgr.get_socket_path(&id).is_none());
    }

    #[test]
    fn is_running_returns_false_for_unknown() {
        let mgr = ProcessManager::new(PathBuf::from("/tmp/test-plugins"));
        let id = PluginId("unknown".into());
        assert!(!mgr.is_running(&id));
    }

    #[test]
    fn restart_count_starts_at_zero() {
        let mgr = ProcessManager::new(PathBuf::from("/tmp/test-plugins"));
        let id = PluginId("test".into());
        assert_eq!(mgr.get_restart_count(&id), 0);
    }

    #[test]
    fn increment_restart_count_on_unknown_returns_zero() {
        let mgr = ProcessManager::new(PathBuf::from("/tmp/test-plugins"));
        let id = PluginId("unknown".into());
        assert_eq!(mgr.increment_restart_count(&id), 0);
    }

    #[test]
    fn set_and_get_state_on_managed_process() {
        let mgr = ProcessManager::new(PathBuf::from("/tmp/test-plugins"));
        let id = PluginId("test-plugin".into());

        // Manually insert a ManagedProcess to test state management
        // without spawning a real process.
        let config = test_config("test-plugin");
        mgr.processes.insert(
            id.0.clone(),
            ManagedProcess {
                plugin_name: "test-plugin".into(),
                config,
                child: None,
                pid: None,
                socket_path: PathBuf::from("/tmp/test-plugins/test-plugin.sock"),
                state: PluginState::Spawned,
                restart_count: 0,
                started_at: Some(Instant::now()),
                last_restart_at: None,
            },
        );

        assert_eq!(mgr.get_state(&id), Some(PluginState::Spawned));

        mgr.set_state(&id, PluginState::Running);
        assert_eq!(mgr.get_state(&id), Some(PluginState::Running));

        mgr.set_state(&id, PluginState::Failed);
        assert_eq!(mgr.get_state(&id), Some(PluginState::Failed));
    }

    #[test]
    fn increment_restart_count_works() {
        let mgr = ProcessManager::new(PathBuf::from("/tmp/test-plugins"));
        let id = PluginId("counter-test".into());
        let config = test_config("counter-test");

        mgr.processes.insert(
            id.0.clone(),
            ManagedProcess {
                plugin_name: "counter-test".into(),
                config,
                child: None,
                pid: None,
                socket_path: PathBuf::from("/tmp/test-plugins/counter-test.sock"),
                state: PluginState::Spawned,
                restart_count: 0,
                started_at: None,
                last_restart_at: None,
            },
        );

        assert_eq!(mgr.get_restart_count(&id), 0);
        assert_eq!(mgr.increment_restart_count(&id), 1);
        assert_eq!(mgr.increment_restart_count(&id), 2);
        assert_eq!(mgr.increment_restart_count(&id), 3);
        assert_eq!(mgr.get_restart_count(&id), 3);
    }

    #[test]
    fn remove_plugin_cleans_up_state() {
        let mgr = ProcessManager::new(PathBuf::from("/tmp/test-plugins"));
        let id = PluginId("remove-me".into());
        let config = test_config("remove-me");

        mgr.processes.insert(
            id.0.clone(),
            ManagedProcess {
                plugin_name: "remove-me".into(),
                config,
                child: None,
                pid: None,
                socket_path: PathBuf::from("/tmp/test-plugins/remove-me.sock"),
                state: PluginState::Running,
                restart_count: 3,
                started_at: Some(Instant::now()),
                last_restart_at: None,
            },
        );

        assert!(mgr.get_state(&id).is_some());
        mgr.remove_plugin(&id);
        assert!(mgr.get_state(&id).is_none());
        assert!(mgr.get_pid(&id).is_none());
        assert_eq!(mgr.get_restart_count(&id), 0);
    }

    #[test]
    fn active_count_reflects_children() {
        let mgr = ProcessManager::new(PathBuf::from("/tmp/test-plugins"));
        let config = test_config("active-test");

        // Insert two entries: one with a child (simulated via None, since we
        // can't create a real Child in a unit test), one without.
        mgr.processes.insert(
            "a".into(),
            ManagedProcess {
                plugin_name: "a".into(),
                config: config.clone(),
                child: None,
                pid: None,
                socket_path: PathBuf::from("/tmp/test-plugins/a.sock"),
                state: PluginState::Stopped,
                restart_count: 0,
                started_at: None,
                last_restart_at: None,
            },
        );
        mgr.processes.insert(
            "b".into(),
            ManagedProcess {
                plugin_name: "b".into(),
                config,
                child: None,
                pid: None,
                socket_path: PathBuf::from("/tmp/test-plugins/b.sock"),
                state: PluginState::Stopped,
                restart_count: 0,
                started_at: None,
                last_restart_at: None,
            },
        );

        // Both have child = None, so active_count should be 0.
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn list_processes_returns_all_entries() {
        let mgr = ProcessManager::new(PathBuf::from("/tmp/test-plugins"));
        let config = test_config("list-test");

        mgr.processes.insert(
            "p1".into(),
            ManagedProcess {
                plugin_name: "plugin-one".into(),
                config: config.clone(),
                child: None,
                pid: Some(1234),
                socket_path: PathBuf::from("/tmp/test-plugins/plugin-one.sock"),
                state: PluginState::Running,
                restart_count: 2,
                started_at: Some(Instant::now()),
                last_restart_at: None,
            },
        );
        mgr.processes.insert(
            "p2".into(),
            ManagedProcess {
                plugin_name: "plugin-two".into(),
                config,
                child: None,
                pid: None,
                socket_path: PathBuf::from("/tmp/test-plugins/plugin-two.sock"),
                state: PluginState::Stopped,
                restart_count: 0,
                started_at: None,
                last_restart_at: None,
            },
        );

        let list = mgr.list_processes();
        assert_eq!(list.len(), 2);

        // Find the running one.
        let running = list.iter().find(|p| p.plugin_name == "plugin-one").unwrap();
        assert_eq!(running.pid, Some(1234));
        assert_eq!(running.state, PluginState::Running);
        assert_eq!(running.restart_count, 2);
        assert!(running.uptime_secs.is_some());

        // Find the stopped one.
        let stopped = list.iter().find(|p| p.plugin_name == "plugin-two").unwrap();
        assert_eq!(stopped.pid, None);
        assert_eq!(stopped.state, PluginState::Stopped);
        assert!(stopped.uptime_secs.is_none());
    }

    #[test]
    fn get_socket_path_returns_correct_path() {
        let mgr = ProcessManager::new(PathBuf::from("/tmp/test-plugins"));
        let id = PluginId("socket-test".into());
        let config = test_config("socket-test");

        mgr.processes.insert(
            id.0.clone(),
            ManagedProcess {
                plugin_name: "socket-test".into(),
                config,
                child: None,
                pid: None,
                socket_path: PathBuf::from("/tmp/test-plugins/socket-test.sock"),
                state: PluginState::Spawned,
                restart_count: 0,
                started_at: None,
                last_restart_at: None,
            },
        );

        let path = mgr.get_socket_path(&id).unwrap();
        assert_eq!(path, PathBuf::from("/tmp/test-plugins/socket-test.sock"));
    }

    // ====================================================================
    // Async tests -- spawn / stop / restart with real processes
    // ====================================================================

    #[tokio::test]
    #[ignore] // Requires a real filesystem; run manually or in integration CI.
    async fn spawn_and_stop_real_process() {
        let dir = tempfile::tempdir().unwrap();
        let socket_dir = dir.path().join("sockets");
        let mgr = ProcessManager::new(socket_dir.clone());

        let id = PluginId("sleep-test".into());
        let config = PluginConfig {
            name: "sleep-test".into(),
            binary: Some("sleep 3600".into()),
            library: None,
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: true,
            health_check_interval: None,
            max_restart_attempts: Some(3),
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };

        // Spawn.
        let pid = mgr.spawn_plugin(&id, &config).await.unwrap();
        assert!(pid > 0);
        assert_eq!(mgr.get_pid(&id), Some(pid));
        assert_eq!(mgr.get_state(&id), Some(PluginState::Spawned));
        assert!(mgr.is_running(&id));
        assert_eq!(mgr.active_count(), 1);

        // Socket path should be set.
        let socket_path = mgr.get_socket_path(&id).unwrap();
        assert_eq!(socket_path, socket_dir.join("sleep-test.sock"));

        // Stop.
        let clean = mgr
            .stop_plugin(&id, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(clean);
        assert_eq!(mgr.get_state(&id), Some(PluginState::Stopped));
        assert!(!mgr.is_running(&id));
    }

    #[tokio::test]
    #[ignore]
    async fn spawn_nonexistent_binary_fails() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = ProcessManager::new(dir.path().join("sockets"));

        let id = PluginId("bad-binary".into());
        let config = PluginConfig {
            name: "bad-binary".into(),
            binary: Some("/nonexistent/path/binary_xyz_12345".into()),
            library: None,
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: false,
            health_check_interval: None,
            max_restart_attempts: None,
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };

        let result = mgr.spawn_plugin(&id, &config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[ignore]
    async fn spawn_with_no_binary_config_fails() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = ProcessManager::new(dir.path().join("sockets"));

        let id = PluginId("no-bin".into());
        let config = PluginConfig {
            name: "no-bin".into(),
            binary: None,
            library: Some("libfoo.so".into()),
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: false,
            health_check_interval: None,
            max_restart_attempts: None,
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };

        let result = mgr.spawn_plugin(&id, &config).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::Process(msg) => {
                assert!(msg.contains("no binary path"), "unexpected: {}", msg);
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[tokio::test]
    #[ignore]
    async fn stop_unknown_plugin_fails() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = ProcessManager::new(dir.path().join("sockets"));

        let id = PluginId("ghost".into());
        let result = mgr.stop_plugin(&id, Duration::from_secs(1)).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::NotFound(msg) => {
                assert!(msg.contains("ghost"), "unexpected: {}", msg);
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[tokio::test]
    #[ignore]
    async fn force_kill_on_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let socket_dir = dir.path().join("sockets");
        let mgr = ProcessManager::new(socket_dir);

        let id = PluginId("trap-test".into());
        // Spawn a process that ignores SIGTERM (traps it).
        let config = PluginConfig {
            name: "trap-test".into(),
            binary: Some("sh -c trap '' TERM; sleep 3600".into()),
            library: None,
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: false,
            health_check_interval: None,
            max_restart_attempts: None,
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };

        let pid = mgr.spawn_plugin(&id, &config).await.unwrap();
        assert!(pid > 0);

        // Stop with a very short timeout -- SIGTERM will be ignored, so we
        // should fall back to SIGKILL.
        let clean = mgr
            .stop_plugin(&id, Duration::from_millis(500))
            .await
            .unwrap();
        assert!(!clean, "expected forced kill, but got clean shutdown");
        assert_eq!(mgr.get_state(&id), Some(PluginState::Stopped));
    }

    #[tokio::test]
    #[ignore]
    async fn restart_increments_count() {
        let dir = tempfile::tempdir().unwrap();
        let socket_dir = dir.path().join("sockets");
        let mgr = ProcessManager::new(socket_dir);

        let id = PluginId("restart-test".into());
        let config = PluginConfig {
            name: "restart-test".into(),
            binary: Some("sleep 3600".into()),
            library: None,
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: true,
            health_check_interval: None,
            max_restart_attempts: Some(10),
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };

        let pid1 = mgr.spawn_plugin(&id, &config).await.unwrap();
        assert_eq!(mgr.get_restart_count(&id), 0);

        let pid2 = mgr.restart_plugin(&id).await.unwrap();
        assert_ne!(pid1, pid2);
        assert_eq!(mgr.get_restart_count(&id), 1);
        assert!(mgr.is_running(&id));

        // Clean up.
        let _ = mgr.stop_plugin(&id, Duration::from_secs(2)).await;
    }

    #[tokio::test]
    #[ignore]
    async fn restart_fails_after_max_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let socket_dir = dir.path().join("sockets");
        let mgr = ProcessManager::new(socket_dir);

        let id = PluginId("max-restart".into());
        let config = PluginConfig {
            name: "max-restart".into(),
            binary: Some("sleep 3600".into()),
            library: None,
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: true,
            health_check_interval: None,
            max_restart_attempts: Some(0), // zero means no restarts allowed
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };

        let _pid = mgr.spawn_plugin(&id, &config).await.unwrap();

        let result = mgr.restart_plugin(&id).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::Lifecycle(msg) => {
                assert!(msg.contains("exceeded"), "unexpected: {}", msg);
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[tokio::test]
    #[ignore]
    async fn spawn_with_custom_env_and_working_dir() {
        let dir = tempfile::tempdir().unwrap();
        let work_dir = dir.path().join("workdir");
        std::fs::create_dir_all(&work_dir).unwrap();
        let socket_dir = dir.path().join("sockets");
        let mgr = ProcessManager::new(socket_dir);

        let id = PluginId("env-test".into());
        let mut env = HashMap::new();
        env.insert("MY_CUSTOM_VAR".into(), "hello".into());

        let config = PluginConfig {
            name: "env-test".into(),
            binary: Some("sleep 3600".into()),
            library: None,
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: false,
            health_check_interval: None,
            max_restart_attempts: None,
            env,
            working_dir: Some(work_dir),
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };

        let pid = mgr.spawn_plugin(&id, &config).await.unwrap();
        assert!(pid > 0);
        assert!(mgr.is_running(&id));

        let _ = mgr.stop_plugin(&id, Duration::from_secs(2)).await;
    }

    #[tokio::test]
    #[ignore]
    async fn list_processes_after_spawn() {
        let dir = tempfile::tempdir().unwrap();
        let socket_dir = dir.path().join("sockets");
        let mgr = ProcessManager::new(socket_dir);

        let id = PluginId("list-spawn".into());
        let config = PluginConfig {
            name: "list-spawn".into(),
            binary: Some("sleep 3600".into()),
            library: None,
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: false,
            health_check_interval: None,
            max_restart_attempts: None,
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };

        mgr.spawn_plugin(&id, &config).await.unwrap();

        let list = mgr.list_processes();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].plugin_name, "list-spawn");
        assert!(list[0].pid.is_some());
        assert_eq!(list[0].state, PluginState::Spawned);
        assert!(list[0].uptime_secs.is_some());

        let _ = mgr.stop_plugin(&id, Duration::from_secs(2)).await;
    }
}
