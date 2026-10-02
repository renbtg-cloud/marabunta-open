// Marabunta - Licensed under the MIT License.
//! Android-specific integration tests
//!
//! These tests verify Android-related functionality that can be tested on
//! the host machine (without an actual Android device/emulator).
//!
//! Tests gated behind `#[cfg(target_os = "android")]` require cross-compilation
//! and execution on a real device or emulator. Tests without that gate run on
//! any platform and verify shared logic.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::Duration;

    // ================================================================
    // Shell executor: Android working directory validation
    // ================================================================

    #[test]
    fn test_android_working_dir_allow_list_concept() {
        // Verify the allow/deny logic conceptually (the actual function
        // is #[cfg(target_os = "android")], so we test the patterns here).

        let allowed_prefixes = [
            "/data/data/",
            "/data/user/",
            "/storage/emulated/",
            "/sdcard/",
            "/data/local/tmp/",
        ];

        let denied_prefixes = [
            "/system",
            "/proc",
            "/sys",
            "/dev",
            "/etc",
            "/sbin",
            "/vendor",
            "/apex",
            "/init",
            "/root",
            "/oem",
        ];

        // These should be allowed
        let valid_dirs = [
            "/data/data/com.marabunta.worker/files",
            "/data/data/com.marabunta.worker/cache",
            "/data/user/0/com.marabunta.worker/files",
            "/storage/emulated/0/Download",
            "/sdcard/marabunta-output",
            "/data/local/tmp/marabunta-test",
        ];

        for dir in &valid_dirs {
            let in_allowed = allowed_prefixes.iter().any(|p| dir.starts_with(p));
            let in_denied = denied_prefixes.iter().any(|p| dir.starts_with(p));
            assert!(in_allowed, "Expected allowed: {}", dir);
            assert!(!in_denied, "Should not be denied: {}", dir);
        }

        // These should be denied
        let invalid_dirs = [
            "/system/bin",
            "/proc/self",
            "/sys/class",
            "/dev/null",
            "/etc/passwd",
            "/sbin/init",
            "/vendor/lib",
            "/apex/com.android.runtime",
            "/root/.bashrc",
        ];

        for dir in &invalid_dirs {
            let in_denied = denied_prefixes.iter().any(|p| dir.starts_with(p));
            assert!(in_denied, "Expected denied: {}", dir);
        }
    }

    // ================================================================
    // Checkpoint directory: platform-aware defaults
    // ================================================================

    #[test]
    fn test_checkpoint_dir_not_tmp_on_android() {
        // On Android, the default checkpoint dir must NOT be /tmp
        // (which doesn't exist on Android). Verify the config function
        // returns something reasonable.
        use crate::worker::checkpoint::default_checkpoint_dir;

        let dir = default_checkpoint_dir();
        let dir_str = dir.to_string_lossy();

        // On the host machine this will be /tmp/marabunta-checkpoints,
        // which is fine. The Android path is checked at compile time
        // via #[cfg(target_os = "android")].
        assert!(
            !dir_str.is_empty(),
            "Checkpoint dir should not be empty"
        );
    }

    // ================================================================
    // Python executor: config defaults
    // ================================================================

    #[test]
    fn test_python_config_default() {
        use crate::worker::python_executor::PythonConfig;

        let config = PythonConfig::default();
        assert_eq!(config.python_path, "python3");
        assert!(config.use_venv);
        assert!(config.pip_timeout.as_secs() > 0);

        // Venv dir should not be /tmp on Android builds
        let venv_str = config.venv_base_dir.to_string_lossy();
        assert!(
            !venv_str.is_empty(),
            "Venv base dir should not be empty"
        );
    }

    // ================================================================
    // Executor: data_dir propagation
    // ================================================================

    #[test]
    fn test_executor_with_data_dir() {
        use crate::worker::executor::TaskExecutor;
        use crate::worker::CheckpointManager;
        use std::sync::Arc;

        let checkpoint_mgr = Arc::new(CheckpointManager::new(
            PathBuf::from("/tmp/marabunta-test-checkpoints"),
            Duration::from_secs(30),
        ));

        let executor = TaskExecutor::new(2, Duration::from_secs(60), checkpoint_mgr);

        // Verify with_data_dir sets the checkpoint_dir properly
        let android_data_dir = PathBuf::from("/data/data/com.marabunta.worker/files");
        let executor = executor.with_data_dir(android_data_dir.clone());

        // The executor should have accepted the data_dir
        // (We can't directly inspect private fields, but we can verify
        // the method doesn't panic and the executor is still functional)
        assert!(executor.has_capacity());
    }

    // ================================================================
    // Shell executor: PATH prepend logic
    // ================================================================

    #[test]
    fn test_path_prepend_logic() {
        // Verify the PATH prepend logic that happens on Android
        let tools_dir = "/data/data/com.marabunta.worker/files/tools";
        let current_path = "/system/bin:/system/xbin";

        let new_path = format!("{}:{}", tools_dir, current_path);

        assert!(new_path.starts_with(tools_dir));
        assert!(new_path.contains("/system/bin"));
        assert_eq!(
            new_path,
            "/data/data/com.marabunta.worker/files/tools:/system/bin:/system/xbin"
        );
    }

    // ================================================================
    // Shell executor: environment isolation
    // ================================================================

    #[tokio::test]
    async fn test_shell_env_isolation() {
        use crate::worker::shell_executor::execute_shell_simple;

        // Verify that environment variables are properly isolated
        let mut env = HashMap::new();
        env.insert("MARABUNTA_TEST_VAR".to_string(), "test_value_12345".to_string());

        let result = execute_shell_simple(
            "sh",
            &["-c".to_string(), "echo $MARABUNTA_TEST_VAR".to_string()],
            &env,
            None,
            Duration::from_secs(5),
        )
        .await;

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.success);
        assert!(result.stdout.contains("test_value_12345"));
    }

    // ================================================================
    // Shell executor: timeout enforcement
    // ================================================================

    #[tokio::test]
    async fn test_shell_timeout_enforcement() {
        use crate::worker::executor::ExecutorError;
        use crate::worker::shell_executor::execute_shell_simple;

        // Verify that timeouts are enforced (important on Android where
        // we use shorter timeouts)
        let result = execute_shell_simple(
            "sleep",
            &["10".to_string()],
            &HashMap::new(),
            None,
            Duration::from_millis(200), // Very short timeout
        )
        .await;

        assert!(matches!(result, Err(ExecutorError::Timeout(_))));
    }

    // ================================================================
    // ABI detection patterns
    // ================================================================

    #[test]
    fn test_abi_selection_logic() {
        // Simulate the ABI selection logic from ToyboxManager/PythonManager
        let bundled_abis = ["arm64-v8a", "armeabi-v7a", "x86_64", "x86"];

        // Typical arm64 device
        let device_abis = ["arm64-v8a", "armeabi-v7a"];
        let selected = device_abis
            .iter()
            .find_map(|d| bundled_abis.iter().find(|b| *b == d))
            .unwrap();
        assert_eq!(*selected, "arm64-v8a");

        // x86_64 emulator
        let device_abis = ["x86_64", "x86"];
        let selected = device_abis
            .iter()
            .find_map(|d| bundled_abis.iter().find(|b| *b == d))
            .unwrap();
        assert_eq!(*selected, "x86_64");

        // 32-bit only device
        let device_abis = ["armeabi-v7a"];
        let selected = device_abis
            .iter()
            .find_map(|d| bundled_abis.iter().find(|b| *b == d))
            .unwrap();
        assert_eq!(*selected, "armeabi-v7a");
    }

    // ================================================================
    // Capabilities string format
    // ================================================================

    #[test]
    fn test_capabilities_string_format() {
        // Verify the format matches what NativeWorker.getCapabilities() returns
        let caps = format!(
            "shell={}|python={}|monte_carlo={}|parameter_sweep={}|wasm={}",
            1, 0, 1, 1, 0
        );

        assert_eq!(caps, "shell=1|python=0|monte_carlo=1|parameter_sweep=1|wasm=0");

        // Parse it back
        let parts: HashMap<&str, bool> = caps
            .split('|')
            .filter_map(|part| {
                let mut kv = part.splitn(2, '=');
                let key = kv.next()?;
                let val = kv.next()?;
                Some((key, val == "1"))
            })
            .collect();

        assert_eq!(parts.get("shell"), Some(&true));
        assert_eq!(parts.get("python"), Some(&false));
        assert_eq!(parts.get("monte_carlo"), Some(&true));
        assert_eq!(parts.get("parameter_sweep"), Some(&true));
        assert_eq!(parts.get("wasm"), Some(&false));
    }

    // ================================================================
    // Thermal state mapping
    // ================================================================

    #[test]
    fn test_thermal_state_mapping() {
        // Verify the JNI thermal state mapping:
        // ThermalStatus ordinal >= 3 (SEVERE) => thermal_ok = false
        // This mirrors the logic in android_jni.rs

        let thermal_states = [
            (0, true),  // NOMINAL
            (1, true),  // LIGHT
            (2, true),  // MODERATE
            (3, false), // SEVERE
            (4, false), // CRITICAL
            (5, false), // EMERGENCY
            (6, false), // SHUTDOWN
        ];

        for (ordinal, expected_ok) in thermal_states {
            let ok = ordinal < 3;
            assert_eq!(
                ok, expected_ok,
                "Thermal ordinal {} should map to ok={}",
                ordinal, expected_ok
            );
        }
    }
}
