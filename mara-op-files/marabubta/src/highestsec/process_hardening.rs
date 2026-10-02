// Marabunta - Licensed under the MIT License.
//! Process hardening for blind computation.
//!
//! Applies OS-level protections to prevent memory dumping, core dumps,
//! and ptrace attachment when handling blind (classified) data.

use std::fmt;

/// Errors from process hardening operations.
#[derive(Debug)]
pub enum HardeningError {
    /// prctl(PR_SET_DUMPABLE, 0) failed.
    PrctlFailed(std::io::Error),
    /// setrlimit(RLIMIT_CORE, 0) failed.
    RlimitFailed(std::io::Error),
    /// Process hardening not available on this platform.
    NotAvailable(String),
}

impl fmt::Display for HardeningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrctlFailed(e) => write!(f, "prctl(PR_SET_DUMPABLE, 0) failed: {e}"),
            Self::RlimitFailed(e) => write!(f, "setrlimit(RLIMIT_CORE, 0) failed: {e}"),
            Self::NotAvailable(msg) => write!(f, "process hardening not available: {msg}"),
        }
    }
}

impl std::error::Error for HardeningError {}

/// Apply OS-level hardening:
/// 1. Disable core dumps via PR_SET_DUMPABLE
/// 2. Set RLIMIT_CORE to 0
///
/// Must be called before any blind data is decrypted.
#[cfg(target_os = "linux")]
pub fn harden_process() -> Result<(), HardeningError> {
    // 1. Disable core dumps (also prevents ptrace from non-root)
    let ret = unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) };
    if ret != 0 {
        return Err(HardeningError::PrctlFailed(std::io::Error::last_os_error()));
    }

    // 2. Set RLIMIT_CORE to 0 (belt-and-suspenders with PR_SET_DUMPABLE)
    let rlimit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    let ret = unsafe { libc::setrlimit(libc::RLIMIT_CORE, &rlimit) };
    if ret != 0 {
        return Err(HardeningError::RlimitFailed(std::io::Error::last_os_error()));
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn harden_process() -> Result<(), HardeningError> {
    Err(HardeningError::NotAvailable(
        "process hardening requires Linux".into(),
    ))
}

/// Lock a buffer in physical memory (prevent swapping to disk).
#[cfg(target_os = "linux")]
pub fn mlock_buffer(buf: &[u8]) -> Result<(), HardeningError> {
    let ret = unsafe { libc::mlock(buf.as_ptr() as *const libc::c_void, buf.len()) };
    if ret != 0 {
        Err(HardeningError::NotAvailable(format!("mlock failed: {}", std::io::Error::last_os_error())))
    } else {
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
pub fn mlock_buffer(_buf: &[u8]) -> Result<(), HardeningError> {
    Err(HardeningError::NotAvailable("process hardening requires Linux".into()))
}

/// Unlock a previously locked buffer.
#[cfg(target_os = "linux")]
pub fn munlock_buffer(buf: &[u8]) -> Result<(), HardeningError> {
    let ret = unsafe { libc::munlock(buf.as_ptr() as *const libc::c_void, buf.len()) };
    if ret != 0 {
        Err(HardeningError::NotAvailable(format!("munlock failed: {}", std::io::Error::last_os_error())))
    } else {
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
pub fn munlock_buffer(_buf: &[u8]) -> Result<(), HardeningError> {
    Err(HardeningError::NotAvailable("process hardening requires Linux".into()))
}

/// Mark a buffer as excluded from core dumps via madvise(MADV_DONTDUMP).
/// SecurityDomain-in-depth: even if PR_SET_DUMPABLE is bypassed, these pages are excluded.
#[cfg(target_os = "linux")]
pub fn madvise_dontdump(data: &[u8]) -> Result<(), HardeningError> {
    if data.is_empty() { return Ok(()); }
    let ret = unsafe {
        libc::madvise(
            data.as_ptr() as *mut libc::c_void,
            data.len(),
            libc::MADV_DONTDUMP,
        )
    };
    if ret != 0 {
        Err(HardeningError::NotAvailable(format!("madvise failed: {}", std::io::Error::last_os_error())))
    } else {
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
pub fn madvise_dontdump(_data: &[u8]) -> Result<(), HardeningError> {
    Err(HardeningError::NotAvailable("process hardening requires Linux".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_os = "linux")]
    fn test_harden_process_succeeds_on_linux() {
        // May fail in sandboxed CI, so accept both outcomes
        let result = harden_process();
        // On regular Linux, this should succeed
        if std::env::var("CI").is_err() {
            assert!(result.is_ok(), "harden_process failed: {:?}", result.err());
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_dumpable_is_zero_after_hardening() {
        if harden_process().is_ok() {
            let dumpable = unsafe { libc::prctl(libc::PR_GET_DUMPABLE, 0, 0, 0, 0) };
            assert_eq!(dumpable, 0, "PR_GET_DUMPABLE should be 0 after hardening");
        }
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn test_harden_process_fails_on_non_linux() {
        let result = harden_process();
        assert!(result.is_err());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_mlock_munlock_no_panic() {
        let buf = vec![0u8; 4096];
        // Allow failure in unprivileged environments (like CI), but don't panic or ignore completely
        let _ = mlock_buffer(&buf);
        let _ = munlock_buffer(&buf);
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn test_mlock_munlock_fails() {
        let buf = vec![0u8; 4096];
        assert!(mlock_buffer(&buf).is_err());
        assert!(munlock_buffer(&buf).is_err());
    }

    #[test]
    fn test_hardening_error_display() {
        let e = HardeningError::NotAvailable("test".into());
        assert!(format!("{}", e).contains("not available"));
    }
}
