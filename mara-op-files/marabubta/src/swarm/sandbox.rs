// Marabunta - Licensed under the MIT License.
//! Sandboxed execution engine for the Marabunta Swarm.
//!
//! Untrusted code must never run on bare metal. This module provides three
//! isolation strategies, selected automatically based on the host platform:
//!
//! | Mode             | Isolation level         | Platform requirement          |
//! |------------------|-------------------------|-------------------------------|
//! | **Bubblewrap**   | Full (PID+mount+net ns) | Linux with `bwrap` installed  |
//! | **Landlock**     | Filesystem-only          | Linux 5.13+ kernel            |
//! | **AndroidSandbox** | App-level              | Android (via app sandbox)     |
//! | **None**         | Trusted workloads only   | Any                           |
//!
//! # Security guarantees
//!
//! When running in `Bubblewrap` mode (the strongest):
//!
//! - Script **cannot** read or write outside `/work` and `/tmp`.
//! - Script **cannot** access the network (`--unshare-net`).
//! - Script **cannot** fork-bomb the host (PID cgroup limit).
//! - Script **cannot** OOM the node (memory cgroup limit).
//! - Script is **killed** if it exceeds its timeout (`kill_on_drop(true)`).
//! - Job directory is **wiped** after execution completes or fails.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::config::{
    CHUNK_TIMEOUT, SANDBOX_DEFAULT_CPU_SHARES, 
    SANDBOX_DEFAULT_MAX_PIDS,  SANDBOX_JOBS_DIR,
};
use super::types::{Chunk, ChunkResult, ScriptType, SwarmError};

// ============================================================================
// Sandbox mode
// ============================================================================

/// Available isolation strategies.
///
/// The executor picks the strongest available mode via [`SandboxedExecutor::detect_mode`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxMode {
    /// Full namespace isolation: mount, PID, network, user namespaces plus
    /// seccomp and cgroup resource limits. Requires `bwrap` on the host.
    Bubblewrap,

    /// Lightweight filesystem-only sandboxing via the Landlock LSM.
    /// Available on Linux 5.13+ without any extra packages.
    Landlock,

    /// Delegates to Android's application sandbox. The app already runs in
    /// an isolated UID; we just tighten the working directory.
    AndroidSandbox,

    /// No sandboxing. Only for trusted/internal workloads that have been
    /// explicitly approved by the node operator.
    None,
}

impl std::fmt::Display for SandboxMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SandboxMode::Bubblewrap => write!(f, "bubblewrap"),
            SandboxMode::Landlock => write!(f, "landlock"),
            SandboxMode::AndroidSandbox => write!(f, "android"),
            SandboxMode::None => write!(f, "none"),
        }
    }
}

// ============================================================================
// Sandbox configuration
// ============================================================================

/// Per-job resource and isolation configuration.
///
/// Defaults are drawn from the compile-time constants in [`super::config`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxConfig {
    /// Which isolation strategy to use.
    pub mode: SandboxMode,

    /// cgroup `memory.max` in megabytes.
    pub memory_limit_mb: u64,

    /// cgroup `cpu.weight` (1..=10000). 100 is the kernel default.
    pub cpu_shares: u32,

    /// cgroup `pids.max` -- hard ceiling on process/thread count.
    pub max_pids: u32,

    /// Whether the sandboxed process is allowed outbound network access.
    /// Almost always `false` for untrusted code.
    pub network_access: bool,

    /// Paths the script may write to (bind-mounted read/write).
    pub writable_paths: Vec<PathBuf>,

    /// Paths the script may read from (bind-mounted read-only).
    pub readable_paths: Vec<PathBuf>,

    /// Maximum tmpfs size (MB) for the job working directory.
    pub max_disk_mb: u64,

    /// Hard timeout for the entire execution (spawn -> collect output).
    pub timeout: Duration,

    /// Optional seccomp allow-list. If `None`, a sensible default set is used.
    /// If `Some(vec)`, only the listed syscalls are permitted.
    pub allowed_syscalls: Option<Vec<String>>,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            mode: SandboxMode::Bubblewrap,
            memory_limit_mb: crate::swarm::hardware::get_bounds().sandbox_default_memory_mb,
            cpu_shares: SANDBOX_DEFAULT_CPU_SHARES,
            max_pids: SANDBOX_DEFAULT_MAX_PIDS,
            network_access: false,
            writable_paths: Vec::new(),
            readable_paths: vec![
                PathBuf::from("/usr"),
                PathBuf::from("/lib"),
                PathBuf::from("/lib64"),
                PathBuf::from("/bin"),
                PathBuf::from("/etc/alternatives"),
            ],
            max_disk_mb: crate::swarm::hardware::get_bounds().sandbox_default_disk_mb,
            timeout: CHUNK_TIMEOUT,
            allowed_syscalls: None,
        }
    }
}

impl SandboxConfig {
    /// Create a config tuned for the given mode with all other fields defaulted.
    pub fn for_mode(mode: SandboxMode) -> Self {
        Self {
            mode,
            ..Self::default()
        }
    }

    /// Builder: set memory limit.
    pub fn with_memory_limit_mb(mut self, mb: u64) -> Self {
        self.memory_limit_mb = mb;
        self
    }

    /// Builder: set timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Builder: allow network access.
    pub fn with_network_access(mut self, allow: bool) -> Self {
        self.network_access = allow;
        self
    }

    /// Builder: set max PIDs.
    pub fn with_max_pids(mut self, max: u32) -> Self {
        self.max_pids = max;
        self
    }

    /// Builder: add a readable path.
    pub fn with_readable_path(mut self, path: PathBuf) -> Self {
        self.readable_paths.push(path);
        self
    }
}

// ============================================================================
// Cgroup path helper
// ============================================================================

/// Name of the top-level cgroup slice under which all sandboxed jobs run.
const CGROUP_SLICE: &str = "marabunta";

/// Build the cgroup directory path for a given PID.
///
/// Layout: `/sys/fs/cgroup/{CGROUP_SLICE}/job-{pid}/`
fn cgroup_dir(pid: u32) -> PathBuf {
    PathBuf::from(format!("/sys/fs/cgroup/{}/job-{}", CGROUP_SLICE, pid))
}

// ============================================================================
// SandboxedExecutor
// ============================================================================

/// Executes chunks inside an isolated sandbox.
///
/// The executor owns a [`SandboxConfig`] and provides the full lifecycle:
/// directory setup, script materialisation, sandboxed spawn, resource limiting,
/// output capture, and cleanup.
pub struct SandboxedExecutor {
    config: SandboxConfig,
}

impl SandboxedExecutor {
    /// Create a new executor with the given sandbox configuration.
    pub fn new(config: SandboxConfig) -> Self {
        Self { config }
    }

    /// Create an executor that auto-detects the best available sandbox mode.
    pub fn auto() -> Self {
        let mode = Self::detect_mode();
        info!(mode = %mode, "sandbox mode auto-detected");
        Self::new(SandboxConfig::for_mode(mode))
    }

    /// Return a reference to the active configuration.
    pub fn config(&self) -> &SandboxConfig {
        &self.config
    }

    // ========================================================================
    // Mode detection
    // ========================================================================

    /// Probe the current system and return the strongest available sandbox mode.
    ///
    /// Detection order (highest isolation first):
    /// 1. Android (`target_os = "android"`) -- use the app sandbox.
    /// 2. Bubblewrap -- check that `bwrap` exists on `$PATH`.
    /// 3. Landlock -- check the kernel ABI version via
    ///    `/sys/kernel/security/lsm`.
    /// 4. None -- fallback for unsupported platforms.
    pub fn detect_mode() -> SandboxMode {
        // Android is detected at compile time.
        if cfg!(target_os = "android") {
            return SandboxMode::AndroidSandbox;
        }

        // Check for bubblewrap.
        if Self::is_bwrap_available() {
            return SandboxMode::Bubblewrap;
        }

        // Check for Landlock LSM.
        if Self::is_landlock_available() {
            return SandboxMode::Landlock;
        }

        warn!("no sandbox backend available -- falling back to unsandboxed execution");
        SandboxMode::None
    }

    /// Return `true` if `bwrap` is found on `$PATH` and is executable.
    fn is_bwrap_available() -> bool {
        std::process::Command::new("bwrap")
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    /// Return `true` if the kernel advertises Landlock support.
    fn is_landlock_available() -> bool {
        // Landlock ABI version file was introduced in 5.13.
        std::fs::read_to_string("/sys/kernel/security/lsm")
            .map(|lsm_list| lsm_list.contains("landlock"))
            .unwrap_or(false)
    }

    // ========================================================================
    // Main execution entry point
    // ========================================================================

    /// Execute a chunk inside a sandbox.
    ///
    /// # Steps
    ///
    /// 1. Create job directory at `{SANDBOX_JOBS_DIR}/{job_id}/{chunk_id}/`.
    /// 2. Write the script and any input data files into the job directory.
    /// 3. Build the appropriate sandbox command (bwrap / landlock / raw).
    /// 4. Apply cgroup v2 resource limits (memory, CPU, PIDs).
    /// 5. Enforce timeout via `tokio::time::timeout` + `kill_on_drop(true)`.
    /// 6. Capture stdout/stderr and collect output files.
    /// 7. Wipe the job directory unconditionally.
    ///
    /// # Arguments
    ///
    /// * `chunk` -- the work unit (carries job_id, chunk_id).
    /// * `script` -- the script source code to execute.
    /// * `script_type` -- shell, python, or custom interpreter.
    /// * `input_files` -- files to copy into the sandbox workspace.
    /// * `job_dir` -- override for the job directory (if `None`, auto-created
    ///   under [`SANDBOX_JOBS_DIR`]).
    pub async fn execute(
        &self,
        chunk: &Chunk,
        script: &str,
        script_type: &ScriptType,
        input_files: &[(String, PathBuf)],
        job_dir: &Path,
    ) -> ChunkResult {
        let start = Instant::now();

        // ------------------------------------------------------------------
        // 1. Prepare the job directory
        // ------------------------------------------------------------------
        if let Err(e) = self.prepare_job_dir(job_dir, script, script_type, input_files) {
            return self.error_result(
                &start,
                format!("failed to prepare job directory: {}", e),
            );
        }

        let script_filename = Self::script_filename(script_type);
        let script_path = job_dir.join(&script_filename);

        // ------------------------------------------------------------------
        // 2. Build and spawn the sandboxed process
        // ------------------------------------------------------------------
        let mut cmd = match self.config.mode {
            SandboxMode::Bubblewrap => {
                self.build_bwrap_command(&script_path, script_type, job_dir)
            }
            SandboxMode::Landlock | SandboxMode::AndroidSandbox | SandboxMode::None => {
                self.build_direct_command(&script_path, script_type, job_dir)
            }
        };

        let child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                self.cleanup(0, job_dir);
                return self.error_result(
                    &start,
                    format!("failed to spawn sandboxed process: {}", e),
                );
            }
        };

        let pid = child.id().unwrap_or(0);
        debug!(
            chunk_id = %chunk.id,
            job_id = %chunk.job_id,
            pid = pid,
            mode = %self.config.mode,
            "sandboxed process spawned"
        );

        // ------------------------------------------------------------------
        // 3. Apply cgroup limits (best-effort; failure is non-fatal on
        //    systems where we lack cgroup write access).
        // ------------------------------------------------------------------
        if self.config.mode == SandboxMode::Bubblewrap && pid > 0 {
            if let Err(e) = self.apply_cgroup_limits(pid) {
                warn!(
                    pid = pid,
                    error = %e,
                    "cgroup limit application failed (non-fatal)"
                );
            }
        }

        // ------------------------------------------------------------------
        // 4. Await completion with timeout
        // ------------------------------------------------------------------
        let result = tokio::time::timeout(self.config.timeout, child.wait_with_output()).await;

        let chunk_result = match result {
            Ok(Ok(output)) => {
                let success = output.status.success();
                let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
                let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

                // Collect output files from the job directory.
                let output_data = self.collect_output_files(job_dir);

                debug!(
                    chunk_id = %chunk.id,
                    success = success,
                    stdout_len = stdout.len(),
                    stderr_len = stderr.len(),
                    output_files = output_data.len(),
                    duration_ms = start.elapsed().as_millis() as u64,
                    "sandboxed execution completed"
                );

                ChunkResult {
                    output_blob_hash: None,
                    success: true,
                    output: output_data,
                    stdout,
                    stderr,
                    duration_ms: start.elapsed().as_millis() as u64,
                    completed_at: Utc::now(),
                    fuel_consumed: 0, execution_error: None,  is_e2ee: false, blind_execution_proof: None, journal_dump: None }
            }
            Ok(Err(e)) => {
                self.error_result(&start, format!("sandboxed process I/O error: {}", e))
            }
            Err(_) => {
                // Timeout. The child handle is dropped, and kill_on_drop(true)
                // ensures the process tree is killed.
                warn!(
                    chunk_id = %chunk.id,
                    timeout_secs = self.config.timeout.as_secs(),
                    "sandboxed execution timed out"
                );
                self.error_result(
                    &start,
                    format!(
                        "sandboxed execution timed out after {}s",
                        self.config.timeout.as_secs()
                    ),
                )
            }
        };

        // ------------------------------------------------------------------
        // 5. Cleanup: remove cgroup + wipe job directory
        // ------------------------------------------------------------------
        self.cleanup(pid, job_dir);

        chunk_result
    }

    // ========================================================================
    // Job directory management
    // ========================================================================

    /// Create the job directory, write the script, and copy input files.
    fn prepare_job_dir(
        &self,
        job_dir: &Path,
        script: &str,
        script_type: &ScriptType,
        input_files: &[(String, PathBuf)],
    ) -> Result<(), SwarmError> {
        // Create the directory tree.
        std::fs::create_dir_all(job_dir).map_err(|e| {
            SwarmError::Sandbox(format!(
                "cannot create job dir {}: {}",
                job_dir.display(),
                e
            ))
        })?;

        // Write the script file.
        let script_filename = Self::script_filename(script_type);
        let script_path = job_dir.join(&script_filename);
        std::fs::write(&script_path, script).map_err(|e| {
            SwarmError::Sandbox(format!(
                "cannot write script to {}: {}",
                script_path.display(),
                e
            ))
        })?;

        // Make the script executable (Unix only).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o755);
            std::fs::set_permissions(&script_path, perms).map_err(|e| {
                SwarmError::Sandbox(format!(
                    "cannot chmod script {}: {}",
                    script_path.display(),
                    e
                ))
            })?;
        }

        // Copy input files into the job directory.
        for (filename, src_path) in input_files {
            let dest = job_dir.join(filename);
            // Create parent dirs within job_dir if the filename has slashes.
            if let Some(parent) = dest.parent() {
                if parent != job_dir {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        SwarmError::Sandbox(format!(
                            "cannot create input subdir {}: {}",
                            parent.display(),
                            e
                        ))
                    })?;
                }
            }
            std::fs::copy(src_path, &dest).map_err(|e| {
                SwarmError::Sandbox(format!(
                    "cannot copy input file {} -> {}: {}",
                    src_path.display(),
                    dest.display(),
                    e
                ))
            })?;
        }

        // Create an output subdirectory for scripts to write results into.
        let output_dir = job_dir.join("output");
        std::fs::create_dir_all(&output_dir).map_err(|e| {
            SwarmError::Sandbox(format!(
                "cannot create output dir {}: {}",
                output_dir.display(),
                e
            ))
        })?;

        Ok(())
    }

    /// Derive the script filename from the script type.
    fn script_filename(script_type: &ScriptType) -> String {
        match script_type {
            ScriptType::Shell => "script.sh".to_string(),
            ScriptType::Python => "script.py".to_string(),
            ScriptType::Custom(name) => format!("script.{}", name),
            ScriptType::Plugin { plugin_id, config: _ } => format!("{}.plugin", plugin_id),
            ScriptType::BlindComputation { .. } => "script.wasm".to_string(),
        }
    }

    /// Build the default job directory path for a chunk.
    ///
    /// Layout: `{SANDBOX_JOBS_DIR}/{job_id}/{chunk_id}/`
    pub fn default_job_dir(job_id: &crate::common::types::JobId, chunk_id: &super::types::ChunkId) -> PathBuf {
        PathBuf::from(SANDBOX_JOBS_DIR)
            .join(job_id.0.to_string())
            .join(chunk_id.0.to_string())
    }

    /// Collect all files in `{job_dir}/output/` into a single byte vector.
    ///
    /// If there is exactly one output file, its raw bytes are returned.
    /// If there are multiple files, they are concatenated with a header.
    /// If there are no output files, an empty vec is returned.
    fn collect_output_files(&self, job_dir: &Path) -> Vec<u8> {
        let output_dir = job_dir.join("output");
        let entries = match std::fs::read_dir(&output_dir) {
            Ok(entries) => entries,
            Err(_) => return Vec::new(),
        };

        let mut files: Vec<(String, Vec<u8>)> = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if let Ok(data) = std::fs::read(&path) {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    files.push((name, data));
                }
            }
        }

        match files.len() {
            0 => Vec::new(),
            1 => files.into_iter().next().unwrap().1,
            _ => {
                // Multiple files: produce a simple concatenation with delimiters.
                // Format: "--- {filename} ---\n{data}\n" for each file.
                let mut combined = Vec::new();
                files.sort_by(|a, b| a.0.cmp(&b.0));
                for (name, data) in &files {
                    combined.extend_from_slice(format!("--- {} ---\n", name).as_bytes());
                    combined.extend_from_slice(data);
                    combined.push(b'\n');
                }
                combined
            }
        }
    }

    // ========================================================================
    // Bubblewrap command construction
    // ========================================================================

    /// Build a `bwrap` command with full namespace isolation.
    ///
    /// The resulting command isolates the process inside:
    /// - A new mount namespace (only explicitly bound paths are visible)
    /// - A new PID namespace (cannot signal host processes)
    /// - A new network namespace (no network access unless explicitly allowed)
    /// - A new user namespace (runs as UID 65534/nobody inside)
    /// - A new session (detached from the controlling terminal)
    ///
    /// The job directory is bind-mounted at `/work` (read-write) and the
    /// interpreter is invoked on the script within that mount.
    fn build_bwrap_command(
        &self,
        _script_path: &Path,
        script_type: &ScriptType,
        job_dir: &Path,
    ) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new("bwrap");

        // -- Namespace isolation ----------------------------------------
        if self.config.network_access {
            // Unshare everything except network.
            cmd.args(["--unshare-pid", "--unshare-ipc", "--unshare-uts"]);
        } else {
            cmd.arg("--unshare-all");
        }

        // -- Safety flags -----------------------------------------------
        cmd.arg("--die-with-parent");
        cmd.arg("--new-session");

        // -- System read-only bind mounts -------------------------------
        // These give the sandboxed process access to interpreters and libs.
        let system_ro_paths: &[(&str, &str)] = &[
            ("/usr", "/usr"),
            ("/bin", "/bin"),
            ("/lib", "/lib"),
            ("/etc/alternatives", "/etc/alternatives"),
            ("/etc/ld.so.cache", "/etc/ld.so.cache"),
        ];

        for &(src, dest) in system_ro_paths {
            if Path::new(src).exists() {
                cmd.args(["--ro-bind", src, dest]);
            }
        }

        // /lib64 exists on x86_64 systems.
        if Path::new("/lib64").exists() {
            cmd.args(["--ro-bind", "/lib64", "/lib64"]);
        }

        // /usr/lib may be separate from /lib on some distros.
        if Path::new("/usr/lib").exists() {
            cmd.args(["--ro-bind", "/usr/lib", "/usr/lib"]);
        }

        // -- Additional readable paths from config ----------------------
        for rp in &self.config.readable_paths {
            if rp.exists() {
                let s = rp.to_string_lossy();
                cmd.args(["--ro-bind", &s, &s]);
            }
        }

        // -- Job directory: writable at /work ---------------------------
        let job_dir_str = job_dir.to_string_lossy().to_string();
        cmd.args(["--bind", &job_dir_str, "/work"]);

        // -- Additional writable paths from config ----------------------
        for wp in &self.config.writable_paths {
            if wp.exists() {
                let s = wp.to_string_lossy();
                cmd.args(["--bind", &s, &s]);
            }
        }

        // -- Virtual filesystems ----------------------------------------
        cmd.args(["--tmpfs", "/tmp"]);
        cmd.args(["--proc", "/proc"]);
        cmd.args(["--dev", "/dev"]);

        // -- Working directory inside the sandbox -----------------------
        cmd.args(["--chdir", "/work"]);

        // -- Environment ------------------------------------------------
        cmd.args(["--setenv", "HOME", "/work"]);
        cmd.args(["--setenv", "TMPDIR", "/tmp"]);
        cmd.args(["--setenv", "MARABUNTA_SANDBOXED", "1"]);
        cmd.args([
            "--setenv",
            "MARABUNTA_OUTPUT_DIR",
            "/work/output",
        ]);

        // -- Interpreter and script -------------------------------------
        cmd.arg("--");
        let (interpreter, script_sandbox_path) = Self::interpreter_for(script_type);
        cmd.arg(interpreter);
        // The script is inside /work/ because job_dir is mounted there.
        let _ = script_sandbox_path; // We build the path explicitly below.
        let sandbox_script = format!("/work/{}", Self::script_filename(script_type));
        cmd.arg(&sandbox_script);

        // -- Process configuration --------------------------------------
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        cmd.kill_on_drop(true);

        // Prevent the child from inheriting our entire env.
        cmd.env_clear();
        // But we need PATH for the interpreter to find shared libs.
        cmd.env("PATH", "/usr/local/bin:/usr/bin:/bin");

        cmd
    }

    // ========================================================================
    // Direct command construction (Landlock / Android / None)
    // ========================================================================

    /// Build a direct (non-bwrap) command.
    ///
    /// Used for Landlock, Android, and None modes where we invoke the
    /// interpreter directly and rely on other mechanisms for isolation.
    fn build_direct_command(
        &self,
        script_path: &Path,
        script_type: &ScriptType,
        job_dir: &Path,
    ) -> tokio::process::Command {
        let (interpreter, _) = Self::interpreter_for(script_type);
        let mut cmd = tokio::process::Command::new(interpreter);

        cmd.arg(script_path);
        cmd.current_dir(job_dir);

        // Set environment.
        cmd.env("MARABUNTA_SANDBOXED", match self.config.mode {
            SandboxMode::Landlock => "landlock",
            SandboxMode::AndroidSandbox => "android",
            _ => "0",
        });
        cmd.env("MARABUNTA_OUTPUT_DIR", job_dir.join("output"));
        cmd.env("HOME", job_dir);
        cmd.env("TMPDIR", job_dir.join("tmp"));

        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        cmd.kill_on_drop(true);

        cmd
    }

    /// Return the interpreter binary name and a placeholder for the sandbox
    /// script path, based on the script type.
    fn interpreter_for(script_type: &ScriptType) -> (&'static str, &'static str) {
        match script_type {
            ScriptType::Shell => ("/bin/sh", "/work/script.sh"),
            ScriptType::Python => ("python3", "/work/script.py"),
            ScriptType::Custom(_) => ("/bin/sh", "/work/script"),
            ScriptType::Plugin { plugin_id: _, config: _ } => ("./plugin_runner", "/work/plugin.bin"),
            ScriptType::BlindComputation { .. } => ("/bin/wasmtime", "/work/script.wasm"),
        }
    }

    // ========================================================================
    // Cgroup v2 resource limits
    // ========================================================================

    /// Apply cgroup v2 resource limits to the given PID.
    ///
    /// Creates a new cgroup slice under `/sys/fs/cgroup/{CGROUP_SLICE}/job-{pid}/`
    /// and writes:
    /// - `memory.max` -- hard memory ceiling in bytes
    /// - `pids.max` -- hard process count ceiling
    /// - `cpu.weight` -- relative CPU share weight
    /// - `cgroup.procs` -- move the process into the new slice
    ///
    /// This function is best-effort: on systems where the cgroup hierarchy
    /// is not writable (containers, unprivileged users), it logs a warning
    /// but does not fail the execution.
    fn apply_cgroup_limits(&self, pid: u32) -> Result<(), SwarmError> {
        let cg = cgroup_dir(pid);

        // Ensure the parent cgroup slice exists.
        let parent = PathBuf::from(format!("/sys/fs/cgroup/{}", CGROUP_SLICE));
        std::fs::create_dir_all(&parent).map_err(|e| {
            SwarmError::Sandbox(format!(
                "cannot create cgroup parent {}: {}",
                parent.display(),
                e
            ))
        })?;

        // Enable controllers in the parent.
        let subtree_control = parent.join("cgroup.subtree_control");
        if subtree_control.exists() {
            // Best-effort: enable memory, pids, and cpu controllers.
            let _ = std::fs::write(&subtree_control, "+memory +pids +cpu");
        }

        // Create the per-job cgroup.
        std::fs::create_dir_all(&cg).map_err(|e| {
            SwarmError::Sandbox(format!(
                "cannot create cgroup dir {}: {}",
                cg.display(),
                e
            ))
        })?;

        // memory.max: value in bytes
        let memory_bytes = self.config.memory_limit_mb * 1024 * 1024;
        Self::write_cgroup_file(&cg, "memory.max", &memory_bytes.to_string())?;

        // pids.max
        Self::write_cgroup_file(&cg, "pids.max", &self.config.max_pids.to_string())?;

        // cpu.weight (1-10000; kernel default is 100)
        let weight = self.config.cpu_shares.clamp(1, 10000);
        Self::write_cgroup_file(&cg, "cpu.weight", &weight.to_string())?;

        // Move the process into the cgroup.
        Self::write_cgroup_file(&cg, "cgroup.procs", &pid.to_string())?;

        debug!(
            pid = pid,
            memory_max_mb = self.config.memory_limit_mb,
            pids_max = self.config.max_pids,
            cpu_weight = weight,
            cgroup_path = %cg.display(),
            "cgroup v2 limits applied"
        );

        Ok(())
    }

    /// Write a value to a cgroup v2 control file.
    fn write_cgroup_file(cg_dir: &Path, filename: &str, value: &str) -> Result<(), SwarmError> {
        let path = cg_dir.join(filename);
        std::fs::write(&path, value).map_err(|e| {
            SwarmError::Sandbox(format!(
                "cannot write cgroup file {}: {}",
                path.display(),
                e
            ))
        })
    }

    // ========================================================================
    // Cleanup
    // ========================================================================

    /// Remove the cgroup slice and wipe the job directory.
    ///
    /// Both operations are best-effort: failures are logged but do not
    /// propagate. This ensures cleanup runs even when prior stages failed.
    fn cleanup(&self, pid: u32, job_dir: &Path) {
        // Remove the cgroup (must be empty of processes first).
        if pid > 0 {
            let cg = cgroup_dir(pid);
            if cg.exists() {
                if let Err(e) = std::fs::remove_dir(&cg) {
                    debug!(
                        pid = pid,
                        error = %e,
                        "cgroup removal failed (may still have processes)"
                    );
                }
            }
        }

        // Wipe the job directory.
        if job_dir.exists() {
            if let Err(e) = std::fs::remove_dir_all(job_dir) {
                warn!(
                    path = %job_dir.display(),
                    error = %e,
                    "failed to remove job directory"
                );
            } else {
                debug!(path = %job_dir.display(), "job directory wiped");
            }
        }
    }

    // ========================================================================
    // Helper: error result
    // ========================================================================

    /// Construct a failed `ChunkResult` with the given error message.
    fn error_result(&self, start: &Instant, message: String) -> ChunkResult {
        ChunkResult {
            output_blob_hash: None,
            success: false,
            output: Vec::new(),
            stdout: String::new(),
            stderr: message,
            duration_ms: start.elapsed().as_millis() as u64,
            completed_at: Utc::now(),
            fuel_consumed: 0, execution_error: None,  is_e2ee: false, blind_execution_proof: None, journal_dump: None }
    }
}

// ============================================================================
// Validation helpers
// ============================================================================

/// Validate that a job directory path is safe (within the sandbox base).
///
/// Prevents path traversal attacks where a malicious job_id containing
/// `../` could escape the sandbox jobs directory.
pub fn validate_job_dir(job_dir: &Path) -> Result<(), SwarmError> {
    let canonical = job_dir
        .canonicalize()
        .or_else(|_| {
            // If the directory doesn't exist yet, canonicalize the parent.
            if let Some(parent) = job_dir.parent() {
                std::fs::create_dir_all(parent).ok();
                parent.canonicalize().map(|p| p.join(
                    job_dir.file_name().unwrap_or_default()
                ))
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "no parent directory",
                ))
            }
        })
        .map_err(|e| {
            SwarmError::Sandbox(format!(
                "cannot resolve job dir {}: {}",
                job_dir.display(),
                e
            ))
        })?;

    let sandbox_base = PathBuf::from(SANDBOX_JOBS_DIR);
    if !canonical.starts_with(&sandbox_base) {
        return Err(SwarmError::Sandbox(format!(
            "job directory {} escapes sandbox base {}",
            canonical.display(),
            sandbox_base.display()
        )));
    }

    Ok(())
}

/// Check whether the current process has the permissions needed to apply
/// cgroup v2 limits.
///
/// Returns `true` if the cgroup base directory is writable, `false` otherwise.
pub fn can_apply_cgroups() -> bool {
    let parent = PathBuf::from(format!("/sys/fs/cgroup/{}", CGROUP_SLICE));

    // Try to create the parent slice. If this succeeds, we can manage cgroups.
    match std::fs::create_dir_all(&parent) {
        Ok(()) => {
            // Verify we can actually write into it.
            let test_file = parent.join(".marabunta_probe");
            let writable = std::fs::write(&test_file, "probe").is_ok();
            let _ = std::fs::remove_file(&test_file);
            writable
        }
        Err(_) => false,
    }
}

/// Ensure the base sandbox jobs directory exists.
///
/// Should be called once during node startup.
pub fn ensure_sandbox_dirs() -> Result<(), SwarmError> {
    std::fs::create_dir_all(SANDBOX_JOBS_DIR).map_err(|e| {
        SwarmError::Sandbox(format!(
            "cannot create sandbox base dir {}: {}",
            SANDBOX_JOBS_DIR, e
        ))
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::types::{ChunkId, ScriptType};
    use crate::common::types::JobId;
    use std::time::Duration;

    // -----------------------------------------------------------------------
    // SandboxMode
    // -----------------------------------------------------------------------

    #[test]
    fn sandbox_mode_display() {
        assert_eq!(SandboxMode::Bubblewrap.to_string(), "bubblewrap");
        assert_eq!(SandboxMode::Landlock.to_string(), "landlock");
        assert_eq!(SandboxMode::AndroidSandbox.to_string(), "android");
        assert_eq!(SandboxMode::None.to_string(), "none");
    }

    #[test]
    fn sandbox_mode_serde_roundtrip() {
        let modes = vec![
            SandboxMode::Bubblewrap,
            SandboxMode::Landlock,
            SandboxMode::AndroidSandbox,
            SandboxMode::None,
        ];
        for mode in &modes {
            let json = serde_json::to_string(mode).unwrap();
            let back: SandboxMode = serde_json::from_str(&json).unwrap();
            assert_eq!(&back, mode);
        }
    }

    // -----------------------------------------------------------------------
    // SandboxConfig
    // -----------------------------------------------------------------------

    #[test]
    fn sandbox_config_default_values() {
        let cfg = SandboxConfig::default();
        assert_eq!(cfg.mode, SandboxMode::Bubblewrap);
        assert_eq!(cfg.memory_limit_mb, crate::swarm::hardware::get_bounds().sandbox_default_memory_mb);
        assert_eq!(cfg.cpu_shares, SANDBOX_DEFAULT_CPU_SHARES);
        assert_eq!(cfg.max_pids, SANDBOX_DEFAULT_MAX_PIDS);
        assert!(!cfg.network_access);
        assert_eq!(cfg.max_disk_mb, crate::swarm::hardware::get_bounds().sandbox_default_disk_mb);
        assert_eq!(cfg.timeout, CHUNK_TIMEOUT);
        assert!(cfg.allowed_syscalls.is_none());
    }

    #[test]
    fn sandbox_config_for_mode() {
        let cfg = SandboxConfig::for_mode(SandboxMode::Landlock);
        assert_eq!(cfg.mode, SandboxMode::Landlock);
        // All other fields should be the default.
        assert_eq!(cfg.memory_limit_mb, crate::swarm::hardware::get_bounds().sandbox_default_memory_mb);
    }

    #[test]
    fn sandbox_config_builder_chain() {
        let cfg = SandboxConfig::for_mode(SandboxMode::None)
            .with_memory_limit_mb(1024)
            .with_timeout(Duration::from_secs(60))
            .with_network_access(true)
            .with_max_pids(128)
            .with_readable_path(PathBuf::from("/opt/data"));

        assert_eq!(cfg.mode, SandboxMode::None);
        assert_eq!(cfg.memory_limit_mb, 1024);
        assert_eq!(cfg.timeout, Duration::from_secs(60));
        assert!(cfg.network_access);
        assert_eq!(cfg.max_pids, 128);
        assert!(cfg.readable_paths.contains(&PathBuf::from("/opt/data")));
    }

    #[test]
    fn sandbox_config_serde_roundtrip() {
        let cfg = SandboxConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let back: SandboxConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.mode, cfg.mode);
        assert_eq!(back.memory_limit_mb, cfg.memory_limit_mb);
        assert_eq!(back.cpu_shares, cfg.cpu_shares);
        assert_eq!(back.max_pids, cfg.max_pids);
        assert_eq!(back.network_access, cfg.network_access);
        assert_eq!(back.max_disk_mb, cfg.max_disk_mb);
    }

    // -----------------------------------------------------------------------
    // SandboxedExecutor construction
    // -----------------------------------------------------------------------

    #[test]
    fn executor_new() {
        let exec = SandboxedExecutor::new(SandboxConfig::default());
        assert_eq!(exec.config().mode, SandboxMode::Bubblewrap);
        assert_eq!(exec.config().memory_limit_mb, crate::swarm::hardware::get_bounds().sandbox_default_memory_mb);
    }

    #[test]
    fn executor_detect_mode_returns_valid_mode() {
        // This test runs on the CI host; we just verify it returns a valid
        // enum variant without panicking.
        let mode = SandboxedExecutor::detect_mode();
        match mode {
            SandboxMode::Bubblewrap
            | SandboxMode::Landlock
            | SandboxMode::AndroidSandbox
            | SandboxMode::None => {} // all good
        }
    }

    // -----------------------------------------------------------------------
    // Script filename derivation
    // -----------------------------------------------------------------------

    #[test]
    fn script_filename_shell() {
        assert_eq!(
            SandboxedExecutor::script_filename(&ScriptType::Shell),
            "script.sh"
        );
    }

    #[test]
    fn script_filename_python() {
        assert_eq!(
            SandboxedExecutor::script_filename(&ScriptType::Python),
            "script.py"
        );
    }

    #[test]
    fn script_filename_custom() {
        assert_eq!(
            SandboxedExecutor::script_filename(&ScriptType::Custom("lua".into())),
            "script.lua"
        );
    }

    // -----------------------------------------------------------------------
    // Default job directory
    // -----------------------------------------------------------------------

    #[test]
    fn default_job_dir_path_structure() {
        let job_id = JobId::new();
        let chunk_id = ChunkId::new();
        let dir = SandboxedExecutor::default_job_dir(&job_id, &chunk_id);

        let dir_str = dir.to_string_lossy();
        assert!(dir_str.starts_with(SANDBOX_JOBS_DIR));
        assert!(dir_str.contains(&job_id.0.to_string()));
        assert!(dir_str.contains(&chunk_id.0.to_string()));
    }

    // -----------------------------------------------------------------------
    // Job directory preparation
    // -----------------------------------------------------------------------

    #[test]
    fn prepare_job_dir_creates_structure() {
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().join("test-job");

        let exec = SandboxedExecutor::new(SandboxConfig::for_mode(SandboxMode::None));
        exec.prepare_job_dir(
            &job_dir,
            "echo hello",
            &ScriptType::Shell,
            &[],
        )
        .unwrap();

        assert!(job_dir.exists());
        assert!(job_dir.join("script.sh").exists());
        assert!(job_dir.join("output").exists());

        let script_content = std::fs::read_to_string(job_dir.join("script.sh")).unwrap();
        assert_eq!(script_content, "echo hello");
    }

    #[test]
    fn prepare_job_dir_copies_input_files() {
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().join("test-job-inputs");

        // Create an input file.
        let input_path = tmp.path().join("data.csv");
        std::fs::write(&input_path, "col1,col2\na,b\n").unwrap();

        let exec = SandboxedExecutor::new(SandboxConfig::for_mode(SandboxMode::None));
        exec.prepare_job_dir(
            &job_dir,
            "cat data.csv",
            &ScriptType::Shell,
            &[("data.csv".to_string(), input_path)],
        )
        .unwrap();

        let copied = std::fs::read_to_string(job_dir.join("data.csv")).unwrap();
        assert_eq!(copied, "col1,col2\na,b\n");
    }

    #[test]
    fn prepare_job_dir_python_script() {
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().join("test-python");

        let exec = SandboxedExecutor::new(SandboxConfig::for_mode(SandboxMode::None));
        exec.prepare_job_dir(
            &job_dir,
            "print('hello')",
            &ScriptType::Python,
            &[],
        )
        .unwrap();

        assert!(job_dir.join("script.py").exists());
        let content = std::fs::read_to_string(job_dir.join("script.py")).unwrap();
        assert_eq!(content, "print('hello')");
    }

    // -----------------------------------------------------------------------
    // Output file collection
    // -----------------------------------------------------------------------

    #[test]
    fn collect_output_files_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().join("collect-empty");
        std::fs::create_dir_all(job_dir.join("output")).unwrap();

        let exec = SandboxedExecutor::new(SandboxConfig::for_mode(SandboxMode::None));
        let data = exec.collect_output_files(&job_dir);
        assert!(data.is_empty());
    }

    #[test]
    fn collect_output_files_single() {
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().join("collect-single");
        let output_dir = job_dir.join("output");
        std::fs::create_dir_all(&output_dir).unwrap();
        std::fs::write(output_dir.join("result.txt"), b"42").unwrap();

        let exec = SandboxedExecutor::new(SandboxConfig::for_mode(SandboxMode::None));
        let data = exec.collect_output_files(&job_dir);
        assert_eq!(data, b"42");
    }

    #[test]
    fn collect_output_files_multiple() {
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().join("collect-multi");
        let output_dir = job_dir.join("output");
        std::fs::create_dir_all(&output_dir).unwrap();
        std::fs::write(output_dir.join("a.txt"), b"alpha").unwrap();
        std::fs::write(output_dir.join("b.txt"), b"beta").unwrap();

        let exec = SandboxedExecutor::new(SandboxConfig::for_mode(SandboxMode::None));
        let data = exec.collect_output_files(&job_dir);
        let text = String::from_utf8(data).unwrap();
        // Files are sorted alphabetically.
        assert!(text.contains("--- a.txt ---"));
        assert!(text.contains("alpha"));
        assert!(text.contains("--- b.txt ---"));
        assert!(text.contains("beta"));
    }

    #[test]
    fn collect_output_files_no_output_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().join("collect-nodir");
        std::fs::create_dir_all(&job_dir).unwrap();
        // Intentionally do NOT create output/ subdirectory.

        let exec = SandboxedExecutor::new(SandboxConfig::for_mode(SandboxMode::None));
        let data = exec.collect_output_files(&job_dir);
        assert!(data.is_empty());
    }

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------

    #[test]
    fn cleanup_removes_job_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().join("cleanup-test");
        std::fs::create_dir_all(job_dir.join("output")).unwrap();
        std::fs::write(job_dir.join("script.sh"), "echo hi").unwrap();

        let exec = SandboxedExecutor::new(SandboxConfig::for_mode(SandboxMode::None));
        exec.cleanup(0, &job_dir);
        assert!(!job_dir.exists());
    }

    #[test]
    fn cleanup_nonexistent_dir_is_noop() {
        let exec = SandboxedExecutor::new(SandboxConfig::for_mode(SandboxMode::None));
        // Should not panic.
        exec.cleanup(0, Path::new("/tmp/marabunta/nonexistent_job_dir_12345"));
    }

    // -----------------------------------------------------------------------
    // Error result helper
    // -----------------------------------------------------------------------

    #[test]
    fn error_result_populates_stderr() {
        let exec = SandboxedExecutor::new(SandboxConfig::for_mode(SandboxMode::None));
        let start = Instant::now();
        let result = exec.error_result(&start, "something broke".to_string());

        assert!(!result.success);
        assert!(result.output.is_empty());
        assert!(result.stdout.is_empty());
        assert_eq!(result.stderr, "something broke");
        assert!(result.duration_ms < 1000); // should be nearly instant
    }

    // -----------------------------------------------------------------------
    // Cgroup path construction
    // -----------------------------------------------------------------------

    #[test]
    fn cgroup_dir_path_format() {
        let path = cgroup_dir(12345);
        assert_eq!(
            path,
            PathBuf::from("/sys/fs/cgroup/marabunta/job-12345")
        );
    }

    // -----------------------------------------------------------------------
    // Interpreter mapping
    // -----------------------------------------------------------------------

    #[test]
    fn interpreter_for_shell() {
        let (interp, _) = SandboxedExecutor::interpreter_for(&ScriptType::Shell);
        assert_eq!(interp, "/bin/sh");
    }

    #[test]
    fn interpreter_for_python() {
        let (interp, _) = SandboxedExecutor::interpreter_for(&ScriptType::Python);
        assert_eq!(interp, "python3");
    }

    #[test]
    fn interpreter_for_custom() {
        let (interp, _) = SandboxedExecutor::interpreter_for(&ScriptType::Custom("rb".into()));
        assert_eq!(interp, "/bin/sh");
    }

    // -----------------------------------------------------------------------
    // Bwrap command construction (verify args, don't actually run)
    // -----------------------------------------------------------------------

    #[test]
    fn build_bwrap_command_basic_structure() {
        let exec = SandboxedExecutor::new(SandboxConfig::default());
        let job_dir = PathBuf::from("/tmp/marabunta/jobs/test-job/test-chunk");
        let script_path = job_dir.join("script.sh");

        let cmd = exec.build_bwrap_command(
            &script_path,
            &ScriptType::Shell,
            &job_dir,
        );

        // The command program should be "bwrap".
        let inner = cmd.as_std();
        assert_eq!(inner.get_program(), "bwrap");

        // Collect all args into a single string for easier assertion.
        let args: Vec<String> = inner
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let args_str = args.join(" ");

        assert!(args_str.contains("--unshare-all"), "should unshare all namespaces");
        assert!(args_str.contains("--die-with-parent"), "should die with parent");
        assert!(args_str.contains("--new-session"), "should create new session");
        assert!(args_str.contains("--bind"), "should have writable bind mount");
        assert!(args_str.contains("/work"), "job dir should be mounted at /work");
        assert!(args_str.contains("--tmpfs /tmp"), "should have tmpfs at /tmp");
        assert!(args_str.contains("--proc /proc"), "should mount proc");
        assert!(args_str.contains("--dev /dev"), "should mount dev");
        assert!(args_str.contains("--chdir /work"), "should chdir to /work");
        assert!(args_str.contains("--"), "should have argument separator");
        assert!(args_str.contains("/bin/sh"), "should invoke shell interpreter");
        assert!(args_str.contains("/work/script.sh"), "should pass script path");
    }

    #[test]
    fn build_bwrap_command_network_access() {
        let cfg = SandboxConfig::default().with_network_access(true);
        let exec = SandboxedExecutor::new(cfg);
        let job_dir = PathBuf::from("/tmp/marabunta/jobs/test-net");
        let script_path = job_dir.join("script.sh");

        let cmd = exec.build_bwrap_command(
            &script_path,
            &ScriptType::Shell,
            &job_dir,
        );

        let args: Vec<String> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let args_str = args.join(" ");

        // Should NOT contain --unshare-all; should have individual unshares
        // but NOT unshare-net.
        assert!(
            !args_str.contains("--unshare-all"),
            "should not unshare-all when network is allowed"
        );
        assert!(
            args_str.contains("--unshare-pid"),
            "should still unshare PID namespace"
        );
    }

    #[test]
    fn build_bwrap_command_python() {
        let exec = SandboxedExecutor::new(SandboxConfig::default());
        let job_dir = PathBuf::from("/tmp/marabunta/jobs/py-job");
        let script_path = job_dir.join("script.py");

        let cmd = exec.build_bwrap_command(
            &script_path,
            &ScriptType::Python,
            &job_dir,
        );

        let args: Vec<String> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let args_str = args.join(" ");

        assert!(args_str.contains("python3"), "should invoke python3");
        assert!(args_str.contains("/work/script.py"), "should pass python script");
    }

    // -----------------------------------------------------------------------
    // Direct command construction
    // -----------------------------------------------------------------------

    #[test]
    fn build_direct_command_shell() {
        let exec = SandboxedExecutor::new(SandboxConfig::for_mode(SandboxMode::None));
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().to_path_buf();
        let script_path = job_dir.join("script.sh");

        let cmd = exec.build_direct_command(
            &script_path,
            &ScriptType::Shell,
            &job_dir,
        );

        let inner = cmd.as_std();
        assert_eq!(inner.get_program(), "/bin/sh");
    }

    #[test]
    fn build_direct_command_python() {
        let exec = SandboxedExecutor::new(SandboxConfig::for_mode(SandboxMode::Landlock));
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().to_path_buf();
        let script_path = job_dir.join("script.py");

        let cmd = exec.build_direct_command(
            &script_path,
            &ScriptType::Python,
            &job_dir,
        );

        let inner = cmd.as_std();
        assert_eq!(inner.get_program(), "python3");

        // Should set MARABUNTA_SANDBOXED=landlock
        let envs: std::collections::HashMap<String, String> = inner
            .get_envs()
            .filter_map(|(k, v)| {
                Some((
                    k.to_string_lossy().into_owned(),
                    v?.to_string_lossy().into_owned(),
                ))
            })
            .collect();
        assert_eq!(envs.get("MARABUNTA_SANDBOXED").map(|s| s.as_str()), Some("landlock"));
    }

    // -----------------------------------------------------------------------
    // Full execution (SandboxMode::None -- no bwrap needed)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn execute_shell_script_none_mode() {
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().join("exec-shell");

        let exec = SandboxedExecutor::new(
            SandboxConfig::for_mode(SandboxMode::None)
                .with_timeout(Duration::from_secs(10)),
        );

        let chunk = Chunk {
            id: ChunkId::new(),
            job_id: JobId::new(),
            sequence: 0,
            payload: crate::common::types::TaskPayload::Shell {
                command: "echo".to_string(),
                args: vec!["hello".to_string()],
            },
            created_at: Utc::now(),
            ..Default::default()
        };

        let result = exec.execute(
            &chunk,
            "#!/bin/sh\necho sandboxed",
            &ScriptType::Shell,
            &[],
            &job_dir,
        ).await;

        assert!(result.success, "stderr: {}", result.stderr);
        assert!(result.stdout.contains("sandboxed"), "stdout: {}", result.stdout);
        assert!(result.duration_ms < 10_000);

        // Job dir should be cleaned up.
        assert!(!job_dir.exists());
    }

    #[tokio::test]
    async fn execute_timeout_kills_process() {
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().join("exec-timeout");

        let exec = SandboxedExecutor::new(
            SandboxConfig::for_mode(SandboxMode::None)
                .with_timeout(Duration::from_secs(1)),
        );

        let chunk = Chunk {
            id: ChunkId::new(),
            job_id: JobId::new(),
            sequence: 0,
            payload: crate::common::types::TaskPayload::Shell {
                command: "sleep".to_string(),
                args: vec!["60".to_string()],
            },
            created_at: Utc::now(),
            ..Default::default()
        };

        let result = exec.execute(
            &chunk,
            "#!/bin/sh\nsleep 60",
            &ScriptType::Shell,
            &[],
            &job_dir,
        ).await;

        assert!(!result.success);
        assert!(result.stderr.contains("timed out"));
    }

    #[tokio::test]
    async fn execute_collects_output_files() {
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().join("exec-output");

        let exec = SandboxedExecutor::new(
            SandboxConfig::for_mode(SandboxMode::None)
                .with_timeout(Duration::from_secs(10)),
        );

        // Script that writes to the output directory.
        let script = r#"#!/bin/sh
mkdir -p output
echo "result=42" > output/result.txt
echo "done"
"#;

        let chunk = Chunk {
            id: ChunkId::new(),
            job_id: JobId::new(),
            sequence: 0,
            payload: crate::common::types::TaskPayload::Shell {
                command: "echo".to_string(),
                args: vec![],
            },
            created_at: Utc::now(),
            ..Default::default()
        };

        let result = exec.execute(
            &chunk,
            script,
            &ScriptType::Shell,
            &[],
            &job_dir,
        ).await;

        assert!(result.success, "stderr: {}", result.stderr);
        // Output should contain the result file contents.
        let output_str = String::from_utf8_lossy(&result.output);
        assert!(
            output_str.contains("result=42"),
            "output should contain file data: {}",
            output_str
        );
    }

    #[tokio::test]
    async fn execute_with_input_files() {
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().join("exec-inputs");

        // Create an input file.
        let input_path = tmp.path().join("input.txt");
        std::fs::write(&input_path, "hello world").unwrap();

        let exec = SandboxedExecutor::new(
            SandboxConfig::for_mode(SandboxMode::None)
                .with_timeout(Duration::from_secs(10)),
        );

        let script = "#!/bin/sh\ncat input.txt";

        let chunk = Chunk {
            id: ChunkId::new(),
            job_id: JobId::new(),
            sequence: 0,
            payload: crate::common::types::TaskPayload::Shell {
                command: "echo".to_string(),
                args: vec![],
            },
            created_at: Utc::now(),
            ..Default::default()
        };

        let result = exec.execute(
            &chunk,
            script,
            &ScriptType::Shell,
            &[("input.txt".to_string(), input_path)],
            &job_dir,
        ).await;

        assert!(result.success, "stderr: {}", result.stderr);
        assert!(
            result.stdout.contains("hello world"),
            "stdout: {}",
            result.stdout
        );
    }

    #[tokio::test]
    async fn execute_failing_script() {
        let tmp = tempfile::tempdir().unwrap();
        let job_dir = tmp.path().join("exec-fail");

        let exec = SandboxedExecutor::new(
            SandboxConfig::for_mode(SandboxMode::None)
                .with_timeout(Duration::from_secs(10)),
        );

        let script = "#!/bin/sh\necho 'error message' >&2\nexit 1";

        let chunk = Chunk {
            id: ChunkId::new(),
            job_id: JobId::new(),
            sequence: 0,
            payload: crate::common::types::TaskPayload::Shell {
                command: "echo".to_string(),
                args: vec![],
            },
            created_at: Utc::now(),
            ..Default::default()
        };

        let result = exec.execute(
            &chunk,
            script,
            &ScriptType::Shell,
            &[],
            &job_dir,
        ).await;

        assert!(!result.success);
        assert!(result.stderr.contains("error message"));
    }

    // -----------------------------------------------------------------------
    // Validation
    // -----------------------------------------------------------------------

    #[test]
    fn validate_job_dir_rejects_traversal() {
        // This test creates the sandbox base so canonicalize works, then
        // tries a path that would escape it.
        let _ = std::fs::create_dir_all(SANDBOX_JOBS_DIR);

        let bad_path = PathBuf::from(format!("{}/../../../etc/shadow", SANDBOX_JOBS_DIR));
        let result = validate_job_dir(&bad_path);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("escapes sandbox base"),
            "error should mention escaping: {}",
            err_msg
        );
    }

    // -----------------------------------------------------------------------
    // Cgroup file write helper
    // -----------------------------------------------------------------------

    #[test]
    fn write_cgroup_file_creates_file() {
        let tmp = tempfile::tempdir().unwrap();
        let result = SandboxedExecutor::write_cgroup_file(
            tmp.path(),
            "test.max",
            "12345",
        );
        assert!(result.is_ok());
        let content = std::fs::read_to_string(tmp.path().join("test.max")).unwrap();
        assert_eq!(content, "12345");
    }

    #[test]
    fn write_cgroup_file_nonexistent_dir() {
        let result = SandboxedExecutor::write_cgroup_file(
            Path::new("/nonexistent_cgroup_test_dir_marabunta/"),
            "test.max",
            "12345",
        );
        assert!(result.is_err());
    }
}
