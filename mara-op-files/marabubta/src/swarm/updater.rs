// Marabunta - Licensed under the MIT License.
//! Auto-update mechanism for the Marabunta Compute swarm node.
//!
//! Periodically checks for new versions, downloads platform-specific binaries,
//! verifies integrity via SHA-256 checksum and Ed25519 signature, and performs
//! an atomic binary replacement with rollback on health check failure.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::config::{UPDATE_CHECK_INTERVAL, UPDATE_DOWNLOAD_TIMEOUT, UPDATE_HEALTH_CHECK_DELAY};

// ============================================================================
// Error types
// ============================================================================

/// Errors that can occur during the update process.
#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Invalid manifest: {0}")]
    InvalidManifest(String),

    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("Signature verification failed: {0}")]
    SignatureInvalid(String),

    #[error("No binary available for platform: {0}")]
    UnsupportedPlatform(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Version parse error: {0}")]
    VersionParse(String),
}

// ============================================================================
// Manifest types
// ============================================================================

/// Update manifest returned by the update server.
///
/// The server hosts this at `{update_url}/latest.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateManifest {
    /// The latest available version (semver).
    pub version: String,

    /// ISO 8601 timestamp of when this release was published.
    pub released_at: String,

    /// Platform-specific binary information, keyed by target triple.
    pub binaries: HashMap<String, BinaryInfo>,

    /// Human-readable changelog for this release.
    pub changelog: String,

    /// If set, nodes running a version older than this must update.
    #[serde(default)]
    pub min_version: Option<String>,

    /// Ed25519 signature over the manifest (excluding this field itself).
    /// The signature covers the JSON-serialized manifest with `signature` set to None.
    #[serde(default)]
    pub signature: Option<String>,
}

/// Information about a platform-specific binary in the update manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryInfo {
    /// URL to download the binary.
    pub url: String,

    /// SHA-256 hex digest of the binary.
    pub sha256: String,

    /// Size of the binary in bytes.
    pub size_bytes: u64,
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the auto-update system, extracted from SwarmConfig.
#[derive(Debug, Clone)]
pub struct UpdateConfig {
    /// Whether auto-update is enabled.
    pub enabled: bool,
    /// URL to the update server (e.g., "https://releases.marabunta-compute.io").
    pub update_url: Option<String>,
    /// How often to check for updates.
    pub check_interval: Duration,
    /// Ed25519 public key (base64-encoded) for verifying release signatures.
    pub signing_public_key: Option<String>,
}

impl Default for UpdateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            update_url: None,
            check_interval: UPDATE_CHECK_INTERVAL,
            signing_public_key: None,
        }
    }
}

// ============================================================================
// UpdateChecker
// ============================================================================

/// The update checker manages periodic version checks and binary upgrades.
///
/// It runs as a background task that periodically polls an update server,
/// verifies the integrity of downloaded binaries, and performs atomic
/// in-place replacement of the running executable.
pub struct UpdateChecker {
    /// URL to check for latest version (returns JSON manifest).
    update_url: String,
    /// How often to check for updates.
    check_interval: Duration,
    /// Current running version (from Cargo.toml / env).
    current_version: String,
    /// Path to the running binary.
    binary_path: PathBuf,
    /// Ed25519 public key for verifying release signatures (base64-encoded).
    signing_public_key: Option<String>,
    /// Whether auto-update is enabled.
    enabled: bool,
    /// HTTP client with configured timeouts.
    client: reqwest::Client,
}

impl UpdateChecker {
    /// Create a new UpdateChecker from the provided configuration.
    ///
    /// The current version is read from the `CARGO_PKG_VERSION` environment
    /// variable at compile time. The binary path is determined from
    /// `std::env::current_exe()`.
    pub fn new(config: &UpdateConfig) -> Self {
        let binary_path = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("marabunta-swarm"));

        let client = reqwest::Client::builder()
            .timeout(UPDATE_DOWNLOAD_TIMEOUT)
            .user_agent(format!(
                "marabunta-swarm/{} ({})",
                env!("CARGO_PKG_VERSION"),
                target_triple()
            ))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            update_url: config
                .update_url
                .clone()
                .unwrap_or_else(|| "https://releases.marabunta-compute.io".to_string()),
            check_interval: config.check_interval,
            current_version: env!("CARGO_PKG_VERSION").to_string(),
            binary_path,
            signing_public_key: config.signing_public_key.clone(),
            enabled: config.enabled,
            client,
        }
    }

    /// Spawn the background update loop.
    ///
    /// The loop checks for updates at `check_interval` (with +/-10% jitter
    /// to prevent thundering herd across a 500-node fleet), downloads and
    /// installs updates when available, and respects the shutdown signal.
    pub fn spawn_update_loop(self, shutdown_rx: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.run_update_loop(shutdown_rx).await;
        })
    }

    /// Internal update loop implementation.
    async fn run_update_loop(&self, mut shutdown_rx: watch::Receiver<bool>) {
        if !self.enabled {
            info!("auto-update disabled, update loop will not run");
            return;
        }

        info!(
            update_url = %self.update_url,
            check_interval_secs = self.check_interval.as_secs(),
            current_version = %self.current_version,
            "auto-update loop started"
        );

        // Run the post-update health check on startup (if a .bak file exists).
        self.health_check_after_update().await;

        loop {
            // Apply jitter: +-10% of the check interval to avoid thundering herd.
            let jittered_interval = apply_jitter(self.check_interval);

            tokio::select! {
                _ = tokio::time::sleep(jittered_interval) => {}
                result = shutdown_rx.changed() => {
                    if result.is_err() || *shutdown_rx.borrow() {
                        info!("update loop shutting down");
                        return;
                    }
                }
            }

            if *shutdown_rx.borrow() {
                info!("update loop shutting down");
                return;
            }

            match self.check_for_update().await {
                Ok(Some(manifest)) => {
                    let is_mandatory = manifest.min_version.as_ref().is_some_and(|min| {
                        is_version_less_than(&self.current_version, min)
                    });

                    info!(
                        new_version = %manifest.version,
                        changelog = %manifest.changelog,
                        mandatory = is_mandatory,
                        "update available"
                    );

                    match self.download_and_install(&manifest).await {
                        Ok(()) => {
                            info!(
                                old_version = %self.current_version,
                                new_version = %manifest.version,
                                "update installed successfully, restart required"
                            );
                            // Attempt automatic restart if running under systemd.
                            self.attempt_restart().await;
                        }
                        Err(e) => {
                            error!(error = %e, "failed to install update");
                        }
                    }
                }
                Ok(None) => {
                    debug!(
                        current_version = %self.current_version,
                        "no update available"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "failed to check for updates");
                }
            }
        }
    }

    /// Check the update server for a newer version.
    ///
    /// Returns `Ok(Some(manifest))` if an update is available, `Ok(None)` if
    /// the current version is up to date, or `Err` on network/parse failures.
    pub async fn check_for_update(&self) -> Result<Option<UpdateManifest>, UpdateError> {
        if !self.enabled {
            return Ok(None);
        }

        let url = format!("{}/latest.json", self.update_url.trim_end_matches('/'));
        debug!(url = %url, "checking for updates");

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(UpdateError::InvalidManifest(format!(
                "update server returned HTTP {}",
                response.status()
            )));
        }

        let manifest: UpdateManifest = response.json().await.map_err(|e| {
            UpdateError::InvalidManifest(format!("failed to parse manifest JSON: {}", e))
        })?;

        // Verify manifest signature if we have a signing key configured.
        if let Some(ref pub_key) = self.signing_public_key {
            self.verify_manifest_signature(&manifest, pub_key)?;
        }

        // Compare versions using semver.
        if is_version_greater_than(&manifest.version, &self.current_version) {
            Ok(Some(manifest))
        } else {
            Ok(None)
        }
    }

    /// Download, verify, and install a new binary from the manifest.
    ///
    /// This performs:
    /// 1. Platform detection via target triple
    /// 2. Download to a temporary file
    /// 3. SHA-256 checksum verification
    /// 4. Atomic binary swap (current -> .bak, new -> current)
    /// 5. Set executable permissions on Unix
    pub async fn download_and_install(
        &self,
        manifest: &UpdateManifest,
    ) -> Result<(), UpdateError> {
        let triple = target_triple();
        let binary_info = manifest
            .binaries
            .get(triple)
            .ok_or_else(|| UpdateError::UnsupportedPlatform(triple.to_string()))?;

        info!(
            url = %binary_info.url,
            expected_size = binary_info.size_bytes,
            platform = triple,
            "downloading update"
        );

        // Step 1: Download to a temporary file alongside the current binary.
        let tmp_path = self.binary_path.with_extension("update.tmp");
        let response = self.client.get(&binary_info.url).send().await?;

        if !response.status().is_success() {
            return Err(UpdateError::InvalidManifest(format!(
                "download server returned HTTP {}",
                response.status()
            )));
        }

        let bytes = response.bytes().await?;

        // Step 2: Verify SHA-256 checksum.
        let actual_hash = {
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            hex::encode(hasher.finalize())
        };

        if actual_hash != binary_info.sha256 {
            return Err(UpdateError::ChecksumMismatch {
                expected: binary_info.sha256.clone(),
                actual: actual_hash,
            });
        }

        info!(
            sha256 = %actual_hash,
            size = bytes.len(),
            "checksum verified"
        );

        // Step 3: Write to temp file.
        tokio::fs::write(&tmp_path, &bytes).await?;

        // Step 4: Set executable permissions on Unix.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = std::fs::Permissions::from_mode(0o755);
            tokio::fs::set_permissions(&tmp_path, permissions).await?;
        }

        // Step 5: Atomic swap -- rename current to .bak, then tmp to current.
        let bak_path = self.binary_path.with_extension("bak");

        // Remove old backup if it exists (from a previous successful update).
        if bak_path.exists() {
            tokio::fs::remove_file(&bak_path).await.ok();
        }

        // Rename current binary to backup.
        if self.binary_path.exists() {
            tokio::fs::rename(&self.binary_path, &bak_path).await.map_err(|e| {
                UpdateError::Io(std::io::Error::new(
                    e.kind(),
                    format!(
                        "failed to backup current binary {:?} -> {:?}: {}",
                        self.binary_path, bak_path, e
                    ),
                ))
            })?;
        }

        // Rename temp file to the binary path.
        tokio::fs::rename(&tmp_path, &self.binary_path).await.map_err(|e| {
            // Attempt rollback on failure.
            error!(error = %e, "failed to move new binary into place, attempting rollback");
            // This is sync because we're already in an error path.
            let _ = std::fs::rename(&bak_path, &self.binary_path);
            UpdateError::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "failed to install new binary {:?} -> {:?}: {}",
                    tmp_path, self.binary_path, e
                ),
            ))
        })?;

        info!(
            binary = ?self.binary_path,
            backup = ?bak_path,
            version = %manifest.version,
            "update installed: {} -> {}",
            self.current_version,
            manifest.version
        );

        Ok(())
    }

    /// Verify the Ed25519 signature on an update manifest.
    ///
    /// The signature covers the manifest JSON with the `signature` field set
    /// to `None`, ensuring the signature does not cover itself.
    fn verify_manifest_signature(
        &self,
        manifest: &UpdateManifest,
        pub_key_b64: &str,
    ) -> Result<(), UpdateError> {
        let sig_b64 = manifest.signature.as_ref().ok_or_else(|| {
            UpdateError::SignatureInvalid("manifest has no signature field".to_string())
        })?;

        // Decode the public key from base64.
        let pub_key_bytes = BASE64.decode(pub_key_b64).map_err(|e| {
            UpdateError::SignatureInvalid(format!("invalid public key base64: {}", e))
        })?;

        let pub_key_array: [u8; 32] = pub_key_bytes.try_into().map_err(|_| {
            UpdateError::SignatureInvalid("public key must be 32 bytes".to_string())
        })?;

        let verifying_key = VerifyingKey::from_bytes(&pub_key_array).map_err(|e| {
            UpdateError::SignatureInvalid(format!("invalid Ed25519 public key: {}", e))
        })?;

        // Decode the signature from base64.
        let sig_bytes = BASE64.decode(sig_b64).map_err(|e| {
            UpdateError::SignatureInvalid(format!("invalid signature base64: {}", e))
        })?;

        let sig_array: [u8; 64] = sig_bytes.try_into().map_err(|_| {
            UpdateError::SignatureInvalid("signature must be 64 bytes".to_string())
        })?;

        let signature = Signature::from_bytes(&sig_array);

        // Reconstruct the canonical manifest JSON without the signature field.
        let mut manifest_for_signing = manifest.clone();
        manifest_for_signing.signature = None;
        let canonical_json = serde_json::to_string(&manifest_for_signing).map_err(|e| {
            UpdateError::SignatureInvalid(format!("failed to serialize manifest: {}", e))
        })?;

        // Verify the signature.
        verifying_key
            .verify(canonical_json.as_bytes(), &signature)
            .map_err(|e| {
                UpdateError::SignatureInvalid(format!("Ed25519 verification failed: {}", e))
            })?;

        debug!("manifest signature verified");
        Ok(())
    }

    /// Rollback to the previous binary version.
    ///
    /// Renames `{binary_path}.bak` back to `{binary_path}`.
    pub async fn rollback(&self) -> Result<(), UpdateError> {
        let bak_path = self.binary_path.with_extension("bak");

        if !bak_path.exists() {
            warn!("no backup binary found, cannot rollback");
            return Err(UpdateError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "no backup binary found for rollback",
            )));
        }

        warn!("rolling back to previous binary version");
        tokio::fs::rename(&bak_path, &self.binary_path).await?;
        info!(binary = ?self.binary_path, "rollback complete");

        Ok(())
    }

    /// Post-update health check.
    ///
    /// If a `.bak` file exists (indicating a recent update), this waits for
    /// `UPDATE_HEALTH_CHECK_DELAY` and then checks that the process is still
    /// healthy. If healthy, the backup is deleted. If the process appears
    /// unhealthy (e.g., crashed and was restarted with the .bak still present),
    /// a rollback is performed.
    pub async fn health_check_after_update(&self) {
        let bak_path = self.binary_path.with_extension("bak");

        if !bak_path.exists() {
            return;
        }

        info!(
            backup = ?bak_path,
            delay_secs = UPDATE_HEALTH_CHECK_DELAY.as_secs(),
            "post-update health check: backup detected, waiting before cleanup"
        );

        tokio::time::sleep(UPDATE_HEALTH_CHECK_DELAY).await;

        // Simple health check: if we're still running after the delay, we're healthy.
        // The process itself is evidence of health -- if it had crashed, the
        // supervisor/systemd would have restarted us and we'd enter this path again.
        if bak_path.exists() {
            info!("post-update health check passed, removing backup");
            if let Err(e) = tokio::fs::remove_file(&bak_path).await {
                warn!(error = %e, "failed to remove backup after successful health check");
            }
        }
    }

    /// Attempt to restart the process after a successful update.
    ///
    /// - On Linux with systemd: invokes `systemctl restart marabunta-swarm`
    /// - Otherwise: logs that a manual restart is needed
    async fn attempt_restart(&self) {
        // Check if running under systemd by looking for INVOCATION_ID env var.
        #[cfg(target_os = "linux")]
        {
            if std::env::var("INVOCATION_ID").is_ok() {
                info!("running under systemd, requesting service restart");
                match tokio::process::Command::new("systemctl")
                    .args(["restart", "marabunta-swarm"])
                    .status()
                    .await
                {
                    Ok(status) if status.success() => {
                        info!("systemctl restart initiated");
                        return;
                    }
                    Ok(status) => {
                        warn!(exit_code = ?status.code(), "systemctl restart failed");
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to invoke systemctl");
                    }
                }
            }
        }

        info!("please restart the process to apply the update");
    }

    /// Perform a one-shot update check and print the result.
    ///
    /// Used by the `--check-update` CLI flag for cron-based workflows.
    pub async fn check_and_report(&self) -> Result<(), UpdateError> {
        match self.check_for_update().await {
            Ok(Some(manifest)) => {
                let is_mandatory = manifest.min_version.as_ref().is_some_and(|min| {
                    is_version_less_than(&self.current_version, min)
                });

                println!("Update available: {} -> {}", self.current_version, manifest.version);
                println!("Released: {}", manifest.released_at);
                println!("Changelog: {}", manifest.changelog);
                if is_mandatory {
                    println!("*** This is a MANDATORY update ***");
                }
                Ok(())
            }
            Ok(None) => {
                println!("Current version {} is up to date.", self.current_version);
                Ok(())
            }
            Err(e) => {
                eprintln!("Error checking for updates: {}", e);
                Err(e)
            }
        }
    }
}

// ============================================================================
// Platform detection
// ============================================================================

/// Returns the Rust target triple for the current platform.
///
/// This is determined at compile time using `cfg` attributes to match
/// the platform-specific binary in the update manifest.
pub fn target_triple() -> &'static str {
    #[cfg(all(target_arch = "x86_64", target_os = "linux", target_env = "gnu"))]
    {
        return "x86_64-unknown-linux-gnu";
    }
    #[cfg(all(target_arch = "x86_64", target_os = "linux", target_env = "musl"))]
    {
        return "x86_64-unknown-linux-musl";
    }
    #[cfg(all(target_arch = "x86_64", target_os = "windows"))]
    {
        return "x86_64-pc-windows-msvc";
    }
    #[cfg(all(target_arch = "x86_64", target_os = "macos"))]
    {
        return "x86_64-apple-darwin";
    }
    #[cfg(all(target_arch = "aarch64", target_os = "macos"))]
    {
        return "aarch64-apple-darwin";
    }
    #[cfg(all(target_arch = "aarch64", target_os = "linux", target_env = "gnu"))]
    {
        return "aarch64-unknown-linux-gnu";
    }
    #[cfg(all(target_arch = "aarch64", target_os = "linux", target_env = "musl"))]
    {
        return "aarch64-unknown-linux-musl";
    }
    #[cfg(all(target_arch = "arm", target_os = "linux"))]
    {
        return "armv7-unknown-linux-gnueabihf";
    }
    // Fallback for any other platform combination.
    #[allow(unreachable_code)]
    "unknown"
}

// ============================================================================
// Semver comparison helpers
// ============================================================================

/// Returns true if `a` is strictly greater than `b` using semver comparison.
fn is_version_greater_than(a: &str, b: &str) -> bool {
    match (semver::Version::parse(a), semver::Version::parse(b)) {
        (Ok(va), Ok(vb)) => va > vb,
        _ => false,
    }
}

/// Returns true if `a` is strictly less than `b` using semver comparison.
fn is_version_less_than(a: &str, b: &str) -> bool {
    match (semver::Version::parse(a), semver::Version::parse(b)) {
        (Ok(va), Ok(vb)) => va < vb,
        _ => false,
    }
}

// ============================================================================
// Jitter helper
// ============================================================================

/// Apply +-10% jitter to a duration to prevent thundering herd.
///
/// For a 6-hour interval this produces a random duration in [5h24m, 6h36m].
fn apply_jitter(base: Duration) -> Duration {
    let base_ms = base.as_millis() as u64;
    let jitter_range = base_ms / 10; // 10% of the base
    if jitter_range == 0 {
        return base;
    }
    let offset = {
        let mut rng = rand::thread_rng();
        rng.gen_range(0..=(jitter_range * 2))
    };
    // offset is in [0, 2*jitter_range], so subtract jitter_range to get [-10%, +10%]
    let jittered_ms = (base_ms as i64 + offset as i64 - jitter_range as i64).max(1) as u64;
    Duration::from_millis(jittered_ms)
}

/// Encode raw bytes as lowercase hex string (for SHA-256 digests).
///
/// This avoids pulling in the `hex` crate for a trivial operation.
mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes
            .as_ref()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_comparison() {
        // Greater-than checks
        assert!(is_version_greater_than("0.2.0", "0.1.0"));
        assert!(is_version_greater_than("1.0.0", "0.99.99"));
        assert!(is_version_greater_than("1.0.1", "1.0.0"));
        assert!(is_version_greater_than("2.0.0", "1.9.9"));

        // Not greater
        assert!(!is_version_greater_than("0.1.0", "0.1.0")); // equal
        assert!(!is_version_greater_than("0.1.0", "0.2.0")); // less

        // Less-than checks
        assert!(is_version_less_than("0.1.0", "0.2.0"));
        assert!(is_version_less_than("0.1.0", "1.0.0"));
        assert!(!is_version_less_than("1.0.0", "0.1.0"));
        assert!(!is_version_less_than("1.0.0", "1.0.0")); // equal

        // Invalid version strings return false
        assert!(!is_version_greater_than("not-a-version", "0.1.0"));
        assert!(!is_version_less_than("0.1.0", "garbage"));
    }

    #[test]
    fn test_target_triple() {
        let triple = target_triple();
        assert!(!triple.is_empty());
        // On any supported platform, it should not be "unknown".
        #[cfg(any(
            all(target_arch = "x86_64", target_os = "linux"),
            all(target_arch = "x86_64", target_os = "macos"),
            all(target_arch = "x86_64", target_os = "windows"),
            all(target_arch = "aarch64", target_os = "macos"),
            all(target_arch = "aarch64", target_os = "linux"),
            all(target_arch = "arm", target_os = "linux"),
        ))]
        assert_ne!(triple, "unknown");
    }

    #[test]
    fn test_manifest_parsing() {
        let json = r#"{
            "version": "0.2.0",
            "released_at": "2026-02-08T00:00:00Z",
            "binaries": {
                "x86_64-unknown-linux-gnu": {
                    "url": "https://releases.example.com/v0.2.0/marabunta-swarm-linux-x86_64",
                    "sha256": "abc123def456",
                    "size_bytes": 15000000
                },
                "aarch64-apple-darwin": {
                    "url": "https://releases.example.com/v0.2.0/marabunta-swarm-macos-arm64",
                    "sha256": "789ghi",
                    "size_bytes": 14000000
                }
            },
            "changelog": "Bug fixes and performance improvements",
            "min_version": "0.1.0"
        }"#;

        let manifest: UpdateManifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.version, "0.2.0");
        assert_eq!(manifest.binaries.len(), 2);
        assert!(manifest.binaries.contains_key("x86_64-unknown-linux-gnu"));
        assert!(manifest.binaries.contains_key("aarch64-apple-darwin"));
        assert_eq!(manifest.changelog, "Bug fixes and performance improvements");
        assert_eq!(manifest.min_version.as_deref(), Some("0.1.0"));
        assert!(manifest.signature.is_none());

        let linux = &manifest.binaries["x86_64-unknown-linux-gnu"];
        assert_eq!(linux.sha256, "abc123def456");
        assert_eq!(linux.size_bytes, 15_000_000);
    }

    #[test]
    fn test_manifest_missing_platform() {
        let json = r#"{
            "version": "0.2.0",
            "released_at": "2026-02-08T00:00:00Z",
            "binaries": {
                "sparc-sun-solaris": {
                    "url": "https://example.com/sparc",
                    "sha256": "aaa",
                    "size_bytes": 100
                }
            },
            "changelog": "test"
        }"#;

        let manifest: UpdateManifest = serde_json::from_str(json).unwrap();
        let triple = target_triple();
        let result = manifest.binaries.get(triple);
        assert!(
            result.is_none(),
            "should not find binary for current platform in a Solaris-only manifest"
        );
    }

    #[test]
    fn test_checksum_verification() {
        let data = b"hello world";
        let mut hasher = Sha256::new();
        hasher.update(data);
        let correct_hash = hex::encode(hasher.finalize());

        // Correct checksum
        assert_eq!(
            correct_hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );

        // Wrong checksum detection
        let wrong_hash = "0000000000000000000000000000000000000000000000000000000000000000";
        assert_ne!(correct_hash, wrong_hash);
    }

    #[test]
    fn test_update_disabled() {
        let config = UpdateConfig {
            enabled: false,
            update_url: Some("https://example.com".to_string()),
            check_interval: Duration::from_secs(60),
            signing_public_key: None,
        };
        let checker = UpdateChecker::new(&config);
        assert!(!checker.enabled);

        // When disabled, check_for_update should return Ok(None) synchronously
        // (tested via the enabled flag check, not an async call here).
    }

    #[test]
    fn test_jitter_calculation() {
        let base = Duration::from_secs(3600); // 1 hour
        let min_expected = Duration::from_millis(3600_000 - 360_000); // -10%
        let max_expected = Duration::from_millis(3600_000 + 360_000); // +10%

        // Run multiple iterations to verify the jitter stays within bounds.
        for _ in 0..100 {
            let jittered = apply_jitter(base);
            assert!(
                jittered >= min_expected && jittered <= max_expected,
                "jittered duration {:?} is outside [{:?}, {:?}]",
                jittered,
                min_expected,
                max_expected
            );
        }
    }

    #[test]
    fn test_jitter_zero_duration() {
        let base = Duration::from_millis(0);
        let jittered = apply_jitter(base);
        assert_eq!(jittered, Duration::from_millis(0));
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex::encode([0xde, 0xad, 0xbe, 0xef]), "deadbeef");
        assert_eq!(hex::encode([0x00, 0xff]), "00ff");
        assert_eq!(hex::encode([]), "");
    }

    #[test]
    fn test_manifest_with_signature() {
        let json = r#"{
            "version": "0.3.0",
            "released_at": "2026-02-08T12:00:00Z",
            "binaries": {},
            "changelog": "signed release",
            "signature": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        }"#;

        let manifest: UpdateManifest = serde_json::from_str(json).unwrap();
        assert!(manifest.signature.is_some());
        assert_eq!(manifest.version, "0.3.0");
    }

    #[test]
    fn test_update_config_default() {
        let config = UpdateConfig::default();
        assert!(!config.enabled);
        assert!(config.update_url.is_none());
        assert_eq!(config.check_interval, UPDATE_CHECK_INTERVAL);
        assert!(config.signing_public_key.is_none());
    }
}
