// Marabunta - Licensed under the MIT License.
/// Seccomp-BPF syscall filter for sandbox security_domain-in-depth.
///
/// When built on Linux with the `highestsec-sandbox` feature (which includes
/// `seccompiler`), this installs a real BPF filter that kills the process
/// if a disallowed syscall is attempted. On other platforms or without the
/// feature, a stub that logs a warning is used.

#[derive(Debug)]
pub enum SeccompError {
    FilterCreation(String),
    FilterApplication(String),
    NotAvailable,
}

impl std::fmt::Display for SeccompError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FilterCreation(s) => write!(f, "seccomp filter creation failed: {}", s),
            Self::FilterApplication(s) => write!(f, "seccomp filter application failed: {}", s),
            Self::NotAvailable => write!(f, "seccomp not available on this platform"),
        }
    }
}

/// Allowed syscalls for the WASM sandbox — 16 syscalls required by Wasmtime.
///
/// Notably excluded: `socket/*` (no network), `execve/fork/clone` (no
/// processes), `open/openat` (no direct file access).
pub const ALLOWED_SYSCALLS: &[&str] = &[
    "read", "write", "close",                              // File descriptor I/O
    "mmap", "munmap", "mprotect", "brk", "madvise",        // JIT memory mapping
    "futex", "sched_yield",                                 // Internal synchronization
    "rt_sigaction", "rt_sigprocmask", "sigaltstack",        // Signal handling for traps
    "clock_gettime",                                        // WASI clock
    "exit", "exit_group",                                   // Process termination
];

/// Seccomp filter configuration.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SeccompConfig {
    pub enabled: bool,
    /// Use SECCOMP_RET_LOG instead of KILL (for debugging).
    pub log_only: bool,
    /// Additional syscalls to allow beyond the base set.
    pub additional_allowed: Vec<String>,
}

impl Default for SeccompConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            log_only: false,
            additional_allowed: vec![],
        }
    }
}

impl SeccompConfig {
    pub fn validate_for_blind_compute(&self) -> Result<(), String> {
        const FORBIDDEN_SYSCALLS: &[&str] = &[
            "socket", "connect", "accept", "sendto", "recvfrom", "open", "openat", "execve", "fork", "clone", "ptrace",
        ];

        for syscall in &self.additional_allowed {
            if FORBIDDEN_SYSCALLS.contains(&syscall.as_str()) {
                return Err(format!(
                    "forbidden syscall '{}' in additional_allowed for blind execution",
                    syscall
                ));
            }
        }
        Ok(())
    }
}

// ==========================================================================
// Real seccomp implementation (Linux + highestsec-sandbox feature)
// ==========================================================================

#[cfg(all(target_os = "linux", feature = "highestsec-sandbox"))]
pub fn install_seccomp_filter() -> Result<(), SeccompError> {
    install_seccomp_filter_with_config(&SeccompConfig::default())
}

#[cfg(all(target_os = "linux", feature = "highestsec-sandbox"))]
pub fn install_seccomp_filter_with_config(config: &SeccompConfig) -> Result<(), SeccompError> {
    use seccompiler::{SeccompAction, SeccompFilter, SeccompRule};
    use std::collections::BTreeMap;

    if !config.enabled {
        return Ok(());
    }

    let default_action = if config.log_only {
        SeccompAction::Log
    } else {
        SeccompAction::KillProcess
    };

    let mut rules: BTreeMap<i64, Vec<SeccompRule>> = BTreeMap::new();

    // Base allowed syscalls (Wasmtime requirements)
    let base_allowed: &[i64] = &[
        libc::SYS_read,
        libc::SYS_write,
        libc::SYS_close,
        libc::SYS_mmap,
        libc::SYS_munmap,
        libc::SYS_mprotect,
        libc::SYS_brk,
        libc::SYS_madvise,
        libc::SYS_futex,
        libc::SYS_sched_yield,
        libc::SYS_rt_sigaction,
        libc::SYS_rt_sigprocmask,
        libc::SYS_sigaltstack,
        libc::SYS_clock_gettime,
        libc::SYS_exit,
        libc::SYS_exit_group,
    ];

    for &syscall in base_allowed {
        rules.insert(syscall, vec![]);
    }

    // Additional allowed syscalls from config
    for name in &config.additional_allowed {
        if let Some(nr) = syscall_name_to_number(name) {
            rules.insert(nr, vec![]);
        }
    }

    let filter = SeccompFilter::new(
        rules,
        default_action,
        SeccompAction::Allow,
        std::env::consts::ARCH.try_into().map_err(|_| {
            SeccompError::FilterCreation("unsupported architecture".to_string())
        })?,
    )
    .map_err(|e| SeccompError::FilterCreation(format!("{}", e)))?;

    let bpf_prog: seccompiler::BpfProgram = filter.try_into().map_err(|e| {
        SeccompError::FilterCreation(format!("BPF compilation: {}", e))
    })?;

    seccompiler::apply_filter(&bpf_prog)
        .map_err(|e| SeccompError::FilterApplication(format!("{}", e)))?;

    Ok(())
}

#[cfg(all(target_os = "linux", feature = "highestsec-sandbox"))]
fn syscall_name_to_number(name: &str) -> Option<i64> {
    match name {
        "read" => Some(libc::SYS_read),
        "write" => Some(libc::SYS_write),
        "close" => Some(libc::SYS_close),
        "open" => Some(libc::SYS_open),
        "openat" => Some(libc::SYS_openat),
        "mmap" => Some(libc::SYS_mmap),
        "munmap" => Some(libc::SYS_munmap),
        "mprotect" => Some(libc::SYS_mprotect),
        "brk" => Some(libc::SYS_brk),
        "madvise" => Some(libc::SYS_madvise),
        "futex" => Some(libc::SYS_futex),
        "sched_yield" => Some(libc::SYS_sched_yield),
        "rt_sigaction" => Some(libc::SYS_rt_sigaction),
        "rt_sigprocmask" => Some(libc::SYS_rt_sigprocmask),
        "sigaltstack" => Some(libc::SYS_sigaltstack),
        "clock_gettime" => Some(libc::SYS_clock_gettime),
        "exit" => Some(libc::SYS_exit),
        "exit_group" => Some(libc::SYS_exit_group),
        "socket" => Some(libc::SYS_socket),
        "connect" => Some(libc::SYS_connect),
        "accept" => Some(libc::SYS_accept),
        "sendto" => Some(libc::SYS_sendto),
        "recvfrom" => Some(libc::SYS_recvfrom),
        _ => None,
    }
}

// ==========================================================================
// Stub implementation (non-Linux or without highestsec-sandbox feature)
// ==========================================================================

#[cfg(not(all(target_os = "linux", feature = "highestsec-sandbox")))]
pub fn install_seccomp_filter() -> Result<(), SeccompError> {
    Err(SeccompError::NotAvailable)
}

#[cfg(not(all(target_os = "linux", feature = "highestsec-sandbox")))]
pub fn install_seccomp_filter_with_config(config: &SeccompConfig) -> Result<(), SeccompError> {
    if !config.enabled {
        return Ok(());
    }
    Err(SeccompError::NotAvailable)
}

/// Check if seccomp is available on this platform.
pub fn is_seccomp_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        std::path::Path::new("/proc/sys/kernel/seccomp").exists()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

mod tests {
    use super::*;

    #[test]
    #[cfg(not(all(target_os = "linux", feature = "highestsec-sandbox")))]
    fn test_stub_returns_error_without_feature() {
        let result = install_seccomp_filter();
        assert!(result.is_err());
    }

    #[test]
    #[cfg(not(all(target_os = "linux", feature = "highestsec-sandbox")))]
    fn test_stub_with_config_returns_error() {
        let result = install_seccomp_filter_with_config(&SeccompConfig::default());
        assert!(result.is_err());
    }

    #[test]
    #[cfg(all(target_os = "linux", feature = "highestsec-sandbox"))]
    fn test_seccomp_install_returns_ok() {
        assert!(install_seccomp_filter().is_ok());
    }

    #[test]
    fn test_seccomp_config_default() {
        let config = SeccompConfig::default();
        assert!(config.enabled);
        assert!(!config.log_only);
        assert!(config.additional_allowed.is_empty());
    }

    #[test]
    fn test_seccomp_disabled_config() {
        let config = SeccompConfig {
            enabled: false,
            ..Default::default()
        };
        assert!(install_seccomp_filter_with_config(&config).is_ok());
    }

    #[test]
    fn test_allowed_syscalls_list() {
        assert!(ALLOWED_SYSCALLS.contains(&"read"));
        assert!(ALLOWED_SYSCALLS.contains(&"write"));
        assert!(ALLOWED_SYSCALLS.contains(&"mmap"));
        assert!(ALLOWED_SYSCALLS.contains(&"exit"));
        assert!(!ALLOWED_SYSCALLS.contains(&"socket"));
        assert!(!ALLOWED_SYSCALLS.contains(&"execve"));
        assert!(!ALLOWED_SYSCALLS.contains(&"fork"));
        assert!(!ALLOWED_SYSCALLS.contains(&"openat"));
    }

    #[test]
    fn test_seccomp_error_display() {
        let e = SeccompError::NotAvailable;
        assert!(format!("{}", e).contains("not available"));

        let e2 = SeccompError::FilterCreation("test".to_string());
        assert!(format!("{}", e2).contains("creation"));

        let e3 = SeccompError::FilterApplication("test".to_string());
        assert!(format!("{}", e3).contains("application"));
    }

    #[test]
    fn test_is_seccomp_available() {
        let _ = is_seccomp_available();
    }

    #[test]
    fn test_seccomp_config_with_additional_allowed() {
        let config = SeccompConfig {
            enabled: true,
            log_only: true,
            additional_allowed: vec!["openat".to_string()],
        };
        assert!(config.log_only);
        assert_eq!(config.additional_allowed.len(), 1);
    }

    #[test]
    fn test_seccomp_allowed_syscalls_count() {
        assert_eq!(ALLOWED_SYSCALLS.len(), 16);
    }

    #[test]
    fn test_seccomp_excludes_dangerous_syscalls() {
        // Verify the base allowlist does NOT include dangerous syscalls
        const DANGEROUS: &[&str] = &[
            "socket", "connect", "accept", "sendto", "recvfrom",
            "open", "openat", "execve", "fork", "clone", "ptrace",
        ];
        for dangerous in DANGEROUS {
            assert!(
                !ALLOWED_SYSCALLS.contains(dangerous),
                "ALLOWED_SYSCALLS must not contain dangerous syscall '{}'",
                dangerous
            );
        }
    }

    #[test]
    #[cfg(all(target_os = "linux", feature = "highestsec-sandbox"))]
    fn test_seccomp_default_action_is_kill() {
        // Verify default config uses KillProcess (not Log)
        let config = SeccompConfig::default();
        assert!(!config.log_only, "default seccomp must use KillProcess, not Log");
        assert!(config.enabled, "default seccomp must be enabled");
    }


}
