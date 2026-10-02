// Marabunta - Licensed under the MIT License.
//! Sandbox escape tests for the Marabunta Swarm executor.
//!
//! These tests verify that the sandboxed execution environment properly
//! constrains untrusted workloads. Each test submits a deliberately
//! malicious payload and verifies it is blocked.
//!
//! All tests are `#[ignore]` — they require Linux + bubblewrap (bwrap)
//! and may need elevated privileges for namespace setup.
//!
//! Run explicitly:
//! ```bash
//! cargo test sandbox_escape -- --ignored --nocapture
//! ```

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use crate::common::types::TaskResult;
    use crate::worker::shell_executor::execute_shell_simple;

    // ========================================================================
    // Helpers
    // ========================================================================

    /// Execute a shell command via execute_shell_simple and return (success, stdout, stderr).
    fn run_sandboxed(command: &str, args: &[&str], timeout: Duration) -> (bool, String, String) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let env: HashMap<String, String> = HashMap::new();

        rt.block_on(async {
            match execute_shell_simple(command, &args, &env, None, timeout).await {
                Ok(result) => (result.success, result.stdout, result.stderr),
                Err(e) => (false, String::new(), format!("executor error: {}", e)),
            }
        })
    }

    // ========================================================================
    // D.1: Path traversal via ../
    // ========================================================================

    #[test]
    #[ignore]
    fn test_path_traversal_dotdot() {
        let (_success, stdout, _stderr) = run_sandboxed(
            "cat",
            &["../../../etc/passwd"],
            Duration::from_secs(10),
        );

        assert!(
            !stdout.contains("root:"),
            "path traversal should be blocked — /etc/passwd content should not appear"
        );
    }

    // ========================================================================
    // D.2: Symlink escape
    // ========================================================================

    #[test]
    #[ignore]
    fn test_symlink_escape() {
        let (_success, stdout, _stderr) = run_sandboxed(
            "sh",
            &["-c", "ln -s /etc/passwd ./escape 2>/dev/null && cat ./escape 2>/dev/null || echo BLOCKED"],
            Duration::from_secs(10),
        );

        assert!(
            !stdout.contains("root:"),
            "symlink escape should be blocked"
        );
    }

    // ========================================================================
    // D.3: Fork bomb
    // ========================================================================

    #[test]
    #[ignore]
    fn test_fork_bomb() {
        let (success, _stdout, stderr) = run_sandboxed(
            "sh",
            &["-c", ":(){ :|:& };: 2>&1; echo SURVIVED"],
            Duration::from_secs(15),
        );

        // The fork bomb should be contained by PID limits.
        // Either the command fails or the sandbox kills it.
        // If we get here without hanging, the sandbox did its job.
        assert!(
            !success || stderr.contains("fork") || stderr.contains("Resource"),
            "fork bomb should be contained by PID limits"
        );
    }

    // ========================================================================
    // D.4: Memory exhaustion
    // ========================================================================

    #[test]
    #[ignore]
    fn test_memory_exhaustion() {
        let (success, stdout, stderr) = run_sandboxed(
            "sh",
            &["-c", "python3 -c 'x = \"A\" * (2 * 1024 * 1024 * 1024)' 2>&1 || echo OOM_CONTAINED"],
            Duration::from_secs(15),
        );

        // Should be OOM-killed or error out.
        assert!(
            !success || stdout.contains("OOM_CONTAINED") || stderr.contains("MemoryError") || stderr.contains("Killed"),
            "memory exhaustion should be contained"
        );
    }

    // ========================================================================
    // D.5: Disk exhaustion
    // ========================================================================

    #[test]
    #[ignore]
    fn test_disk_exhaustion() {
        let (success, stdout, stderr) = run_sandboxed(
            "sh",
            &["-c", "dd if=/dev/zero of=./bigfile bs=1M count=1024 2>&1 || echo DISK_CONTAINED"],
            Duration::from_secs(30),
        );

        // Should hit ENOSPC or be blocked.
        assert!(
            !success || stdout.contains("DISK_CONTAINED") || stderr.contains("No space") || stderr.contains("ENOSPC"),
            "disk exhaustion should be contained"
        );
    }

    // ========================================================================
    // D.6: Network access blocked
    // ========================================================================

    #[test]
    #[ignore]
    fn test_network_access_blocked() {
        let (success, stdout, stderr) = run_sandboxed(
            "sh",
            &["-c", "curl -s --connect-timeout 5 http://1.1.1.1/ 2>&1 || echo NET_BLOCKED"],
            Duration::from_secs(15),
        );

        // Network should be blocked inside the sandbox.
        assert!(
            stdout.contains("NET_BLOCKED") || stderr.contains("Connection refused")
                || stderr.contains("timed out") || stderr.contains("Network is unreachable")
                || !success,
            "network access should be blocked in sandbox"
        );
    }

    // ========================================================================
    // D.7: Host environment variables hidden
    // ========================================================================

    #[test]
    #[ignore]
    fn test_read_host_env_vars() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        // Use an empty env map (don't inherit host env).
        let env: HashMap<String, String> = HashMap::new();
        let args = vec!["-c".to_string(), "env".to_string()];

        let (success, stdout, stderr) = rt.block_on(async {
            match execute_shell_simple("sh", &args, &env, None, Duration::from_secs(10)).await {
                Ok(result) => (result.success, result.stdout, result.stderr),
                Err(e) => (false, String::new(), format!("{}", e)),
            }
        });

        // Since we passed an empty env map, the sandbox should not see host env vars.
        // The executor passes env vars explicitly via cmd.envs(env), so only the
        // explicitly-provided variables should be visible.
        assert!(
            !stdout.contains("HOME=") || stdout.lines().count() < 20,
            "sandbox should have limited environment variables"
        );
    }

    // ========================================================================
    // D.8: /proc filesystem hidden
    // ========================================================================

    #[test]
    #[ignore]
    fn test_proc_filesystem_hidden() {
        let (_success, stdout, stderr) = run_sandboxed(
            "sh",
            &["-c", "cat /proc/1/cmdline 2>&1 || echo PROC_HIDDEN"],
            Duration::from_secs(10),
        );

        assert!(
            stdout.contains("PROC_HIDDEN") || stderr.contains("Permission denied")
                || stderr.contains("No such file"),
            "/proc/1/cmdline should not be readable in sandbox"
        );
    }

    // ========================================================================
    // D.9: Write outside job directory
    // ========================================================================

    #[test]
    #[ignore]
    fn test_write_outside_job_dir() {
        let (_success, stdout, stderr) = run_sandboxed(
            "sh",
            &["-c", "touch /tmp/outside_sandbox_test 2>&1 || echo WRITE_BLOCKED"],
            Duration::from_secs(10),
        );

        assert!(
            stdout.contains("WRITE_BLOCKED") || stderr.contains("Permission denied")
                || stderr.contains("Read-only"),
            "writes outside job directory should be blocked"
        );
    }

    // ========================================================================
    // D.10: Execution timeout enforced
    // ========================================================================

    #[test]
    #[ignore]
    fn test_execution_timeout_enforced() {
        let start = std::time::Instant::now();

        let (success, _stdout, stderr) = run_sandboxed(
            "sleep",
            &["999999"],
            Duration::from_secs(5), // 5s timeout
        );

        let elapsed = start.elapsed();

        // Should complete within timeout + some slack.
        assert!(
            elapsed < Duration::from_secs(30),
            "execution should be killed by timeout (took {:?})",
            elapsed,
        );

        assert!(
            !success || stderr.contains("timeout") || stderr.contains("Killed"),
            "long-running process should be terminated"
        );
    }
}
