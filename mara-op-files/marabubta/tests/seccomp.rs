// Marabunta - Licensed under the MIT License.

#[cfg(all(target_os = "linux", feature = "highestsec-sandbox"))]
#[test]
fn test_disallowed_syscall_kills_process() {
    use std::process::Command;
    use std::os::unix::process::ExitStatusExt; // For status.signal()

    // The main process installs the seccomp filter.
    marabunta_compute::highestsec::seccomp::install_seccomp_filter().expect("failed to install seccomp filter in test");

    let forbidden_syscall_test_path = std::env::var("FORBIDDEN_SYSCALL_TEST_PATH")
        .expect("FORBIDDEN_SYSCALL_TEST_PATH env var not set by build.rs");

    // Spawn the child process that attempts the forbidden syscall.
    let mut child = Command::new(&forbidden_syscall_test_path)
        .spawn()
        .expect("failed to spawn forbidden syscall test executable");

    let status = child.wait().expect("child process wait failed");

    // Expect the child process to be killed by a signal (SIGSYS from seccomp).
    assert!(!status.success(), "child process should not exit successfully");
    assert!(status.signal().is_some(), "child process should be killed by a signal");
    assert_eq!(status.signal().unwrap(), libc::SIGSYS, "child process should be killed by SIGSYS");
}
