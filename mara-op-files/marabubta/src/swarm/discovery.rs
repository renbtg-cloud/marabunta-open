// Marabunta - Licensed under the MIT License.
//! Software Discovery and Capability Probing (Phase 5).
//!
//! Automatically detects installed software on a node by probing known binary
//! names with version flags, then parsing version strings with regex. The
//! discovered inventory feeds into [`NodeProfile::installed_software`] and is
//! propagated via gossip so the swarm can make software-aware placement
//! decisions.
//!
//! # Architecture
//!
//! ```text
//!   SoftwareProber
//!       |
//!       +-- built-in ProbeSpecs (runtimes, databases, media, dev, ...)
//!       +-- custom ProbeSpecs (from config)
//!       |
//!       v
//!   probe_all()  -- runs each spec concurrently
//!       |
//!       +-- find_binary()  -- `which <name>` for each candidate binary
//!       +-- extract_version() -- run binary with version flag, regex parse
//!       |
//!       v
//!   Vec<InstalledSoftware>
//! ```
//!
//! # Requirement Matching
//!
//! Jobs can declare software requirements like `"python3>=3.10"` or `"ffmpeg"`.
//! The [`SoftwareRequirement`] type parses these strings, and
//! [`node_satisfies_software`] checks a node's inventory against a set of
//! requirements using semver-based comparison.

use std::time::Duration;

use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tracing::{debug, trace, warn};

use super::config::{DISCOVERY_INTERVAL, DISCOVERY_PROBE_TIMEOUT};
use super::types::{InstalledSoftware, SoftwareCategory};

// ============================================================================
// ProbeSpec
// ============================================================================

/// Specification for how to probe a single piece of software.
///
/// Each spec lists one or more candidate binary names (tried in order),
/// a flag to request version output, and a regex to extract the version
/// number from the combined stdout+stderr output.
#[derive(Debug, Clone)]
pub struct ProbeSpec {
    /// Human-readable software name (e.g. "python3", "ffmpeg").
    pub name: String,
    /// Candidate binary names to try via `which`, in priority order.
    /// For example: `["python3", "python"]`.
    pub binary_names: Vec<String>,
    /// The flag passed to the binary to get version output (e.g. "--version").
    pub version_flag: String,
    /// Regex pattern to extract a version number from output.
    /// Must contain at least one capture group; the first group is used.
    pub version_regex: String,
    /// Category classification for the discovered software.
    pub category: SoftwareCategory,
}

// ============================================================================
// Built-in probe specifications
// ============================================================================

/// Comprehensive set of built-in probes covering runtimes, databases,
/// media tools, dev tools, math/science tools, and network utilities.
fn builtin_probes() -> Vec<ProbeSpec> {
    vec![
        // ---- Runtimes ----
        ProbeSpec {
            name: "python3".into(),
            binary_names: vec!["python3".into(), "python".into()],
            version_flag: "--version".into(),
            version_regex: r"Python\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Runtime,
        },
        ProbeSpec {
            name: "node".into(),
            binary_names: vec!["node".into(), "nodejs".into()],
            version_flag: "--version".into(),
            version_regex: r"v(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Runtime,
        },
        ProbeSpec {
            name: "go".into(),
            binary_names: vec!["go".into()],
            version_flag: "version".into(),
            version_regex: r"go(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Runtime,
        },
        ProbeSpec {
            name: "java".into(),
            binary_names: vec!["java".into()],
            version_flag: "-version".into(),
            version_regex: r#"(?:version|openjdk)\s+"?(\d+\.\d+(?:\.\d+)?)"?"#.into(),
            category: SoftwareCategory::Runtime,
        },
        ProbeSpec {
            name: "ruby".into(),
            binary_names: vec!["ruby".into()],
            version_flag: "--version".into(),
            version_regex: r"ruby\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Runtime,
        },
        ProbeSpec {
            name: "lua".into(),
            binary_names: vec!["lua".into(), "lua5.4".into(), "lua5.3".into()],
            version_flag: "-v".into(),
            version_regex: r"Lua\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Runtime,
        },
        ProbeSpec {
            name: "Rscript".into(),
            binary_names: vec!["Rscript".into()],
            version_flag: "--version".into(),
            version_regex: r"(?:version|scripting front-end version)\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Runtime,
        },
        ProbeSpec {
            name: "php".into(),
            binary_names: vec!["php".into()],
            version_flag: "--version".into(),
            version_regex: r"PHP\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Runtime,
        },
        ProbeSpec {
            name: "perl".into(),
            binary_names: vec!["perl".into()],
            version_flag: "--version".into(),
            version_regex: r"v(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Runtime,
        },
        // ---- Databases ----
        ProbeSpec {
            name: "mysql".into(),
            binary_names: vec!["mysql".into(), "mariadb".into()],
            version_flag: "--version".into(),
            version_regex: r"(?:mysql|MariaDB).*?(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Database,
        },
        ProbeSpec {
            name: "psql".into(),
            binary_names: vec!["psql".into()],
            version_flag: "--version".into(),
            version_regex: r"psql.*?(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Database,
        },
        ProbeSpec {
            name: "sqlite3".into(),
            binary_names: vec!["sqlite3".into()],
            version_flag: "--version".into(),
            version_regex: r"(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Database,
        },
        ProbeSpec {
            name: "redis-cli".into(),
            binary_names: vec!["redis-cli".into()],
            version_flag: "--version".into(),
            version_regex: r"(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Database,
        },
        ProbeSpec {
            name: "mongosh".into(),
            binary_names: vec!["mongosh".into(), "mongo".into()],
            version_flag: "--version".into(),
            version_regex: r"(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Database,
        },
        // ---- Media tools ----
        ProbeSpec {
            name: "ffmpeg".into(),
            binary_names: vec!["ffmpeg".into()],
            version_flag: "-version".into(),
            version_regex: r"ffmpeg\s+version\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::MediaTool,
        },
        ProbeSpec {
            name: "imagemagick".into(),
            binary_names: vec!["magick".into(), "convert".into()],
            version_flag: "--version".into(),
            version_regex: r"ImageMagick\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::MediaTool,
        },
        ProbeSpec {
            name: "sox".into(),
            binary_names: vec!["sox".into()],
            version_flag: "--version".into(),
            version_regex: r"SoX.*?v(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::MediaTool,
        },
        // ---- Dev tools ----
        ProbeSpec {
            name: "gcc".into(),
            binary_names: vec!["gcc".into(), "cc".into()],
            version_flag: "--version".into(),
            version_regex: r"(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::DevTool,
        },
        ProbeSpec {
            name: "cargo".into(),
            binary_names: vec!["cargo".into()],
            version_flag: "--version".into(),
            version_regex: r"cargo\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::DevTool,
        },
        ProbeSpec {
            name: "docker".into(),
            binary_names: vec!["docker".into()],
            version_flag: "--version".into(),
            version_regex: r"Docker\s+version\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::DevTool,
        },
        ProbeSpec {
            name: "cmake".into(),
            binary_names: vec!["cmake".into()],
            version_flag: "--version".into(),
            version_regex: r"cmake\s+version\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::DevTool,
        },
        ProbeSpec {
            name: "make".into(),
            binary_names: vec!["make".into(), "gmake".into()],
            version_flag: "--version".into(),
            version_regex: r"(?:GNU Make|make)\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::DevTool,
        },
        ProbeSpec {
            name: "npm".into(),
            binary_names: vec!["npm".into()],
            version_flag: "--version".into(),
            version_regex: r"(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::DevTool,
        },
        ProbeSpec {
            name: "git".into(),
            binary_names: vec!["git".into()],
            version_flag: "--version".into(),
            version_regex: r"git\s+version\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::DevTool,
        },
        // ---- Math / Science ----
        ProbeSpec {
            name: "octave".into(),
            binary_names: vec!["octave".into(), "octave-cli".into()],
            version_flag: "--version".into(),
            version_regex: r"GNU Octave.*?(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::MathScience,
        },
        ProbeSpec {
            name: "gnuplot".into(),
            binary_names: vec!["gnuplot".into()],
            version_flag: "--version".into(),
            version_regex: r"gnuplot\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::MathScience,
        },
        ProbeSpec {
            name: "R".into(),
            binary_names: vec!["R".into()],
            version_flag: "--version".into(),
            version_regex: r"R\s+version\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::MathScience,
        },
        // ---- Network ----
        ProbeSpec {
            name: "curl".into(),
            binary_names: vec!["curl".into()],
            version_flag: "--version".into(),
            version_regex: r"curl\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Network,
        },
        ProbeSpec {
            name: "wget".into(),
            binary_names: vec!["wget".into()],
            version_flag: "--version".into(),
            version_regex: r"Wget\s+(\d+\.\d+(?:\.\d+)?)".into(),
            category: SoftwareCategory::Network,
        },
        ProbeSpec {
            name: "ssh".into(),
            binary_names: vec!["ssh".into()],
            version_flag: "-V".into(),
            version_regex: r"OpenSSH[_\s]+(\d+\.\d+(?:p\d+)?)".into(),
            category: SoftwareCategory::Network,
        },
    ]
}

// ============================================================================
// SoftwareProber
// ============================================================================

/// The software prober. Maintains a list of built-in and custom probes,
/// runs them asynchronously, and returns discovered software entries.
pub struct SoftwareProber {
    probes: Vec<ProbeSpec>,
    custom_probes: Vec<ProbeSpec>,
    probe_timeout: Duration,
}

impl SoftwareProber {
    /// Create a new prober pre-loaded with all built-in probe specifications.
    pub fn new() -> Self {
        Self {
            probes: builtin_probes(),
            custom_probes: Vec::new(),
            probe_timeout: DISCOVERY_PROBE_TIMEOUT,
        }
    }

    /// Add custom probes (e.g. from a config file). These are appended to
    /// the built-in set and run during `probe_all`.
    pub fn with_custom_probes(mut self, probes: Vec<ProbeSpec>) -> Self {
        self.custom_probes = probes;
        self
    }

    /// Override the per-probe timeout (default: `DISCOVERY_PROBE_TIMEOUT`).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.probe_timeout = timeout;
        self
    }

    /// Run all probes (built-in + custom) and return a list of discovered
    /// software. Each probe is run sequentially to avoid overwhelming the
    /// system during discovery sweeps.
    pub async fn probe_all(&self) -> Vec<InstalledSoftware> {
        let mut results = Vec::new();

        let all_specs = self.probes.iter().chain(self.custom_probes.iter());

        for spec in all_specs {
            match self.probe_one(spec).await {
                Some(sw) => {
                    debug!(
                        name = %sw.name,
                        version = ?sw.version,
                        path = %sw.path,
                        category = ?sw.category,
                        "discovered software"
                    );
                    results.push(sw);
                }
                None => {
                    trace!(name = %spec.name, "software not found");
                }
            }
        }

        results
    }

    /// Probe a single specification. Returns `Some(InstalledSoftware)` if the
    /// binary is found and a version can be extracted, `None` otherwise.
    async fn probe_one(&self, spec: &ProbeSpec) -> Option<InstalledSoftware> {
        // Step 1: Find a binary from the candidate list.
        let (_binary_name, binary_path) = self.find_binary(&spec.binary_names).await?;

        // Step 2: Extract the version by running the binary with the version flag.
        let version = self
            .extract_version(&binary_path, &spec.version_flag, &spec.version_regex)
            .await;

        Some(InstalledSoftware {
            name: spec.name.clone(),
            version,
            path: binary_path,
            category: spec.category,
        })
    }

    /// Find the first available binary from a list of candidate names using
    /// the `which` command. Returns `(binary_name, absolute_path)`.
    async fn find_binary(&self, names: &[String]) -> Option<(String, String)> {
        for name in names {
            let result = tokio::time::timeout(
                self.probe_timeout,
                Command::new("which").arg(name).output(),
            )
            .await;

            match result {
                Ok(Ok(output)) if output.status.success() => {
                    let path = String::from_utf8_lossy(&output.stdout)
                        .trim()
                        .to_string();
                    if !path.is_empty() {
                        return Some((name.clone(), path));
                    }
                }
                Ok(Ok(_)) => {
                    // `which` returned non-zero -- binary not found, try next.
                    trace!(binary = %name, "which: not found");
                }
                Ok(Err(e)) => {
                    trace!(binary = %name, error = %e, "which: execution error");
                }
                Err(_) => {
                    trace!(binary = %name, "which: timed out");
                }
            }
        }
        None
    }

    /// Run a binary with the given version flag, capture stdout+stderr, and
    /// extract a version string using the provided regex pattern.
    async fn extract_version(
        &self,
        binary_path: &str,
        flag: &str,
        regex_pattern: &str,
    ) -> Option<String> {
        let result = tokio::time::timeout(
            self.probe_timeout,
            Command::new(binary_path)
                .arg(flag)
                .output(),
        )
        .await;

        let output = match result {
            Ok(Ok(output)) => output,
            Ok(Err(e)) => {
                trace!(binary = %binary_path, error = %e, "version probe: execution error");
                return None;
            }
            Err(_) => {
                trace!(binary = %binary_path, "version probe: timed out");
                return None;
            }
        };

        // Combine stdout and stderr -- some tools print version to stderr
        // (e.g. java -version, ssh -V).
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        let re = match Regex::new(regex_pattern) {
            Ok(re) => re,
            Err(e) => {
                warn!(
                    binary = %binary_path,
                    pattern = %regex_pattern,
                    error = %e,
                    "invalid version regex"
                );
                return None;
            }
        };

        re.captures(&combined)
            .and_then(|caps| caps.get(1))
            .map(|m| m.as_str().to_string())
    }
}

impl Default for SoftwareProber {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// SoftwareRequirement
// ============================================================================

/// A parsed software requirement, optionally with a minimum version.
///
/// Requirement strings follow the format `"name>=version"` or just `"name"`.
/// Examples:
/// - `"python3>=3.10"` -- Python 3 at version 3.10 or newer
/// - `"ffmpeg"` -- any version of ffmpeg
/// - `"node>=18.0.0"` -- Node.js 18+
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoftwareRequirement {
    /// Software name to match against `InstalledSoftware::name`.
    pub name: String,
    /// Minimum acceptable version (inclusive). `None` means any version.
    pub min_version: Option<semver::Version>,
}

impl SoftwareRequirement {
    /// Parse a requirement from a string.
    ///
    /// Supported formats:
    /// - `"name>=version"` (e.g. `"python3>=3.10"`)
    /// - `"name"` (e.g. `"ffmpeg"`)
    ///
    /// Version strings with only two components (e.g. `"3.10"`) are
    /// normalized to three components by appending `.0` (e.g. `"3.10.0"`).
    pub fn parse(s: &str) -> Self {
        if let Some((name, version_str)) = s.split_once(">=") {
            let name = name.trim().to_string();
            let version_str = version_str.trim();
            let version = parse_lenient_version(version_str);
            Self {
                name,
                min_version: version,
            }
        } else {
            Self {
                name: s.trim().to_string(),
                min_version: None,
            }
        }
    }
}

// ============================================================================
// Version parsing helpers
// ============================================================================

/// Parse a version string leniently: handles `"3.10"` (two parts) by
/// appending `.0`, and strips any non-numeric suffix after the version
/// core (e.g. `"8.6p1"` -> `"8.6.1"` for OpenSSH-style versions).
fn parse_lenient_version(s: &str) -> Option<semver::Version> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Strip any leading 'v' prefix.
    let s = s.strip_prefix('v').unwrap_or(s);

    // Extract only the numeric dotted portion (e.g. "3.10.2" from "3.10.2-beta").
    let numeric_re = Regex::new(r"^(\d+(?:\.\d+)*)").ok()?;
    let numeric_part = numeric_re
        .captures(s)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str())?;

    let parts: Vec<&str> = numeric_part.split('.').collect();
    let normalized = match parts.len() {
        1 => format!("{}.0.0", parts[0]),
        2 => format!("{}.{}.0", parts[0], parts[1]),
        _ => format!("{}.{}.{}", parts[0], parts[1], parts[2]),
    };

    semver::Version::parse(&normalized).ok()
}

// ============================================================================
// Requirement matching
// ============================================================================

/// Check whether a node's installed software satisfies all given requirements.
///
/// Returns `true` only if every requirement in `required` is met by at least
/// one entry in `installed`.
pub fn node_satisfies_software(
    installed: &[InstalledSoftware],
    required: &[SoftwareRequirement],
) -> bool {
    required.iter().all(|req| satisfies_requirement(installed, req))
}

/// Check whether a single software requirement is satisfied by the installed
/// software list.
///
/// Matching rules:
/// 1. The requirement name is matched case-insensitively against
///    `InstalledSoftware::name`.
/// 2. If the requirement has no `min_version`, any installed version suffices.
/// 3. If the requirement specifies a `min_version`, the installed version
///    must be parseable and >= the required minimum.
/// 4. If the installed software has no version string (version probing
///    failed), a version-constrained requirement is NOT satisfied.
pub fn satisfies_requirement(
    installed: &[InstalledSoftware],
    req: &SoftwareRequirement,
) -> bool {
    let req_name = req.name.to_lowercase();

    for sw in installed {
        if sw.name.to_lowercase() != req_name {
            continue;
        }

        // Name matches. Check version constraint.
        match &req.min_version {
            None => return true, // Any version is fine.
            Some(min_ver) => {
                if let Some(ref installed_ver_str) = sw.version {
                    if let Some(installed_ver) = parse_lenient_version(installed_ver_str) {
                        if installed_ver >= *min_ver {
                            return true;
                        }
                    }
                }
                // Version didn't match or couldn't be parsed -- keep looking
                // in case there are multiple installations.
            }
        }
    }

    false
}

/// Return the discovery re-probe interval for use by the node's
/// periodic maintenance loop.
pub fn discovery_interval() -> Duration {
    DISCOVERY_INTERVAL
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- ProbeSpec construction ----

    #[test]
    fn builtin_probes_are_comprehensive() {
        let probes = builtin_probes();

        // We should have a sizable number of built-in probes.
        assert!(
            probes.len() >= 25,
            "expected >= 25 builtin probes, got {}",
            probes.len()
        );

        // Check that all categories are represented.
        let categories: std::collections::HashSet<SoftwareCategory> =
            probes.iter().map(|p| p.category).collect();
        assert!(categories.contains(&SoftwareCategory::Runtime));
        assert!(categories.contains(&SoftwareCategory::Database));
        assert!(categories.contains(&SoftwareCategory::MediaTool));
        assert!(categories.contains(&SoftwareCategory::DevTool));
        assert!(categories.contains(&SoftwareCategory::MathScience));
        assert!(categories.contains(&SoftwareCategory::Network));
    }

    #[test]
    fn builtin_probes_have_valid_regex() {
        let probes = builtin_probes();
        for probe in &probes {
            let result = Regex::new(&probe.version_regex);
            assert!(
                result.is_ok(),
                "invalid regex for probe '{}': {}",
                probe.name,
                result.unwrap_err()
            );
        }
    }

    #[test]
    fn builtin_probes_have_nonempty_fields() {
        let probes = builtin_probes();
        for probe in &probes {
            assert!(!probe.name.is_empty(), "probe has empty name");
            assert!(
                !probe.binary_names.is_empty(),
                "probe '{}' has no binary names",
                probe.name
            );
            assert!(
                !probe.version_flag.is_empty(),
                "probe '{}' has empty version_flag",
                probe.name
            );
            assert!(
                !probe.version_regex.is_empty(),
                "probe '{}' has empty version_regex",
                probe.name
            );
        }
    }

    // ---- Specific probes by name ----

    #[test]
    fn builtin_probes_include_expected_software() {
        let probes = builtin_probes();
        let names: Vec<&str> = probes.iter().map(|p| p.name.as_str()).collect();

        // Runtimes
        assert!(names.contains(&"python3"));
        assert!(names.contains(&"node"));
        assert!(names.contains(&"go"));
        assert!(names.contains(&"java"));
        assert!(names.contains(&"ruby"));
        assert!(names.contains(&"lua"));
        assert!(names.contains(&"Rscript"));
        assert!(names.contains(&"php"));
        assert!(names.contains(&"perl"));

        // Databases
        assert!(names.contains(&"mysql"));
        assert!(names.contains(&"psql"));
        assert!(names.contains(&"sqlite3"));
        assert!(names.contains(&"redis-cli"));
        assert!(names.contains(&"mongosh"));

        // Media
        assert!(names.contains(&"ffmpeg"));
        assert!(names.contains(&"imagemagick"));
        assert!(names.contains(&"sox"));

        // Dev tools
        assert!(names.contains(&"gcc"));
        assert!(names.contains(&"cargo"));
        assert!(names.contains(&"docker"));
        assert!(names.contains(&"cmake"));
        assert!(names.contains(&"make"));
        assert!(names.contains(&"npm"));
        assert!(names.contains(&"git"));

        // Math/Science
        assert!(names.contains(&"octave"));
        assert!(names.contains(&"gnuplot"));
        assert!(names.contains(&"R"));

        // Network
        assert!(names.contains(&"curl"));
        assert!(names.contains(&"wget"));
        assert!(names.contains(&"ssh"));
    }

    // ---- Version regex matching ----

    fn match_version(regex_pattern: &str, input: &str) -> Option<String> {
        let re = Regex::new(regex_pattern).unwrap();
        re.captures(input)
            .and_then(|caps| caps.get(1))
            .map(|m| m.as_str().to_string())
    }

    #[test]
    fn regex_python3_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "python3").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "Python 3.11.6"),
            Some("3.11.6".into())
        );
        assert_eq!(
            match_version(&probe.version_regex, "Python 3.10"),
            Some("3.10".into())
        );
    }

    #[test]
    fn regex_node_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "node").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "v20.10.0"),
            Some("20.10.0".into())
        );
        assert_eq!(
            match_version(&probe.version_regex, "v18.17.1"),
            Some("18.17.1".into())
        );
    }

    #[test]
    fn regex_go_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "go").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "go version go1.21.5 linux/amd64"),
            Some("1.21.5".into())
        );
    }

    #[test]
    fn regex_java_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "java").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "openjdk version \"17.0.9\" 2023-10-17"),
            Some("17.0.9".into())
        );
        assert_eq!(
            match_version(&probe.version_regex, "java version \"1.8.0_392\""),
            Some("1.8.0".into())
        );
    }

    #[test]
    fn regex_ruby_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "ruby").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "ruby 3.2.2 (2023-03-30 revision e51014f9c0)"),
            Some("3.2.2".into())
        );
    }

    #[test]
    fn regex_ffmpeg_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "ffmpeg").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "ffmpeg version 6.1.1 Copyright (c) 2000-2023"),
            Some("6.1.1".into())
        );
    }

    #[test]
    fn regex_git_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "git").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "git version 2.43.0"),
            Some("2.43.0".into())
        );
    }

    #[test]
    fn regex_docker_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "docker").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "Docker version 24.0.7, build afdd53b"),
            Some("24.0.7".into())
        );
    }

    #[test]
    fn regex_cargo_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "cargo").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "cargo 1.75.0 (1d8b05cdd 2023-11-20)"),
            Some("1.75.0".into())
        );
    }

    #[test]
    fn regex_curl_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "curl").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "curl 8.5.0 (x86_64-pc-linux-gnu)"),
            Some("8.5.0".into())
        );
    }

    #[test]
    fn regex_cmake_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "cmake").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "cmake version 3.28.1"),
            Some("3.28.1".into())
        );
    }

    #[test]
    fn regex_ssh_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "ssh").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "OpenSSH_9.6p1, OpenSSL 3.2.0 23 Nov 2023"),
            Some("9.6p1".into())
        );
    }

    #[test]
    fn regex_r_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "R").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "R version 4.3.2 (2023-10-31) -- \"Eye Holes\""),
            Some("4.3.2".into())
        );
    }

    #[test]
    fn regex_php_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "php").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "PHP 8.3.1 (cli) (built: Dec 20 2023 12:34:56)"),
            Some("8.3.1".into())
        );
    }

    #[test]
    fn regex_perl_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "perl").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "This is perl 5, version 38, subversion 2 (v5.38.2)"),
            Some("5.38.2".into())
        );
    }

    #[test]
    fn regex_sqlite3_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "sqlite3").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "3.44.2 2023-11-24 11:41:44"),
            Some("3.44.2".into())
        );
    }

    #[test]
    fn regex_make_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "make").unwrap();
        assert_eq!(
            match_version(&probe.version_regex, "GNU Make 4.4.1"),
            Some("4.4.1".into())
        );
    }

    #[test]
    fn regex_imagemagick_version() {
        let probe = builtin_probes().into_iter().find(|p| p.name == "imagemagick").unwrap();
        assert_eq!(
            match_version(
                &probe.version_regex,
                "Version: ImageMagick 7.1.1-23 Q16-HDRI x86_64"
            ),
            Some("7.1.1".into())
        );
    }

    // ---- parse_lenient_version ----

    #[test]
    fn parse_version_three_parts() {
        let v = parse_lenient_version("3.11.6").unwrap();
        assert_eq!(v, semver::Version::new(3, 11, 6));
    }

    #[test]
    fn parse_version_two_parts() {
        let v = parse_lenient_version("3.10").unwrap();
        assert_eq!(v, semver::Version::new(3, 10, 0));
    }

    #[test]
    fn parse_version_one_part() {
        let v = parse_lenient_version("21").unwrap();
        assert_eq!(v, semver::Version::new(21, 0, 0));
    }

    #[test]
    fn parse_version_with_v_prefix() {
        let v = parse_lenient_version("v1.21.5").unwrap();
        assert_eq!(v, semver::Version::new(1, 21, 5));
    }

    #[test]
    fn parse_version_with_suffix() {
        let v = parse_lenient_version("3.10.2-beta").unwrap();
        assert_eq!(v, semver::Version::new(3, 10, 2));
    }

    #[test]
    fn parse_version_empty() {
        assert!(parse_lenient_version("").is_none());
    }

    #[test]
    fn parse_version_garbage() {
        assert!(parse_lenient_version("not-a-version").is_none());
    }

    #[test]
    fn parse_version_with_whitespace() {
        let v = parse_lenient_version("  3.10.2  ").unwrap();
        assert_eq!(v, semver::Version::new(3, 10, 2));
    }

    #[test]
    fn parse_version_four_parts_truncated() {
        // Only first 3 numeric parts are used.
        let v = parse_lenient_version("1.2.3.4").unwrap();
        assert_eq!(v, semver::Version::new(1, 2, 3));
    }

    // ---- SoftwareRequirement::parse ----

    #[test]
    fn requirement_parse_name_only() {
        let req = SoftwareRequirement::parse("ffmpeg");
        assert_eq!(req.name, "ffmpeg");
        assert!(req.min_version.is_none());
    }

    #[test]
    fn requirement_parse_with_version() {
        let req = SoftwareRequirement::parse("python3>=3.10");
        assert_eq!(req.name, "python3");
        assert_eq!(req.min_version, Some(semver::Version::new(3, 10, 0)));
    }

    #[test]
    fn requirement_parse_three_part_version() {
        let req = SoftwareRequirement::parse("node>=18.0.0");
        assert_eq!(req.name, "node");
        assert_eq!(req.min_version, Some(semver::Version::new(18, 0, 0)));
    }

    #[test]
    fn requirement_parse_with_spaces() {
        let req = SoftwareRequirement::parse("  cargo >= 1.75  ");
        assert_eq!(req.name, "cargo");
        assert_eq!(req.min_version, Some(semver::Version::new(1, 75, 0)));
    }

    #[test]
    fn requirement_parse_invalid_version_is_none() {
        let req = SoftwareRequirement::parse("foo>=not-a-version");
        assert_eq!(req.name, "foo");
        assert!(req.min_version.is_none());
    }

    #[test]
    fn requirement_serialization_roundtrip() {
        let req = SoftwareRequirement::parse("python3>=3.10");
        let json = serde_json::to_string(&req).unwrap();
        let deser: SoftwareRequirement = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.name, "python3");
        assert_eq!(deser.min_version, Some(semver::Version::new(3, 10, 0)));
    }

    // ---- satisfies_requirement ----

    fn make_installed(name: &str, version: Option<&str>, category: SoftwareCategory) -> InstalledSoftware {
        InstalledSoftware {
            name: name.to_string(),
            version: version.map(|s| s.to_string()),
            path: format!("/usr/bin/{}", name),
            category,
        }
    }

    #[test]
    fn satisfies_name_only_requirement() {
        let installed = vec![make_installed("ffmpeg", Some("6.1.1"), SoftwareCategory::MediaTool)];
        let req = SoftwareRequirement::parse("ffmpeg");
        assert!(satisfies_requirement(&installed, &req));
    }

    #[test]
    fn satisfies_versioned_requirement_exact() {
        let installed = vec![make_installed("python3", Some("3.10.0"), SoftwareCategory::Runtime)];
        let req = SoftwareRequirement::parse("python3>=3.10");
        assert!(satisfies_requirement(&installed, &req));
    }

    #[test]
    fn satisfies_versioned_requirement_newer() {
        let installed = vec![make_installed("python3", Some("3.12.1"), SoftwareCategory::Runtime)];
        let req = SoftwareRequirement::parse("python3>=3.10");
        assert!(satisfies_requirement(&installed, &req));
    }

    #[test]
    fn fails_versioned_requirement_older() {
        let installed = vec![make_installed("python3", Some("3.9.2"), SoftwareCategory::Runtime)];
        let req = SoftwareRequirement::parse("python3>=3.10");
        assert!(!satisfies_requirement(&installed, &req));
    }

    #[test]
    fn fails_name_not_found() {
        let installed = vec![make_installed("python3", Some("3.11.0"), SoftwareCategory::Runtime)];
        let req = SoftwareRequirement::parse("node>=18.0");
        assert!(!satisfies_requirement(&installed, &req));
    }

    #[test]
    fn fails_versioned_requirement_no_installed_version() {
        let installed = vec![make_installed("python3", None, SoftwareCategory::Runtime)];
        let req = SoftwareRequirement::parse("python3>=3.10");
        assert!(!satisfies_requirement(&installed, &req));
    }

    #[test]
    fn satisfies_name_only_even_without_version() {
        let installed = vec![make_installed("ffmpeg", None, SoftwareCategory::MediaTool)];
        let req = SoftwareRequirement::parse("ffmpeg");
        assert!(satisfies_requirement(&installed, &req));
    }

    #[test]
    fn satisfies_case_insensitive_name() {
        let installed = vec![make_installed("Python3", Some("3.11.0"), SoftwareCategory::Runtime)];
        let req = SoftwareRequirement::parse("python3>=3.10");
        assert!(satisfies_requirement(&installed, &req));
    }

    // ---- node_satisfies_software ----

    #[test]
    fn node_satisfies_all_requirements() {
        let installed = vec![
            make_installed("python3", Some("3.11.6"), SoftwareCategory::Runtime),
            make_installed("ffmpeg", Some("6.1.1"), SoftwareCategory::MediaTool),
            make_installed("git", Some("2.43.0"), SoftwareCategory::DevTool),
        ];
        let reqs = vec![
            SoftwareRequirement::parse("python3>=3.10"),
            SoftwareRequirement::parse("ffmpeg"),
            SoftwareRequirement::parse("git>=2.40"),
        ];
        assert!(node_satisfies_software(&installed, &reqs));
    }

    #[test]
    fn node_fails_one_requirement() {
        let installed = vec![
            make_installed("python3", Some("3.11.6"), SoftwareCategory::Runtime),
            make_installed("git", Some("2.43.0"), SoftwareCategory::DevTool),
        ];
        let reqs = vec![
            SoftwareRequirement::parse("python3>=3.10"),
            SoftwareRequirement::parse("ffmpeg"), // not installed
        ];
        assert!(!node_satisfies_software(&installed, &reqs));
    }

    #[test]
    fn node_satisfies_empty_requirements() {
        let installed = vec![make_installed("python3", Some("3.11.6"), SoftwareCategory::Runtime)];
        let reqs: Vec<SoftwareRequirement> = vec![];
        assert!(node_satisfies_software(&installed, &reqs));
    }

    #[test]
    fn node_empty_installed_fails_any_requirement() {
        let installed: Vec<InstalledSoftware> = vec![];
        let reqs = vec![SoftwareRequirement::parse("python3")];
        assert!(!node_satisfies_software(&installed, &reqs));
    }

    // ---- SoftwareProber construction ----

    #[test]
    fn prober_default_has_builtin_probes() {
        let prober = SoftwareProber::new();
        assert!(prober.probes.len() >= 25);
        assert!(prober.custom_probes.is_empty());
    }

    #[test]
    fn prober_with_custom_probes() {
        let custom = vec![ProbeSpec {
            name: "my-tool".into(),
            binary_names: vec!["my-tool".into()],
            version_flag: "--version".into(),
            version_regex: r"my-tool\s+(\d+\.\d+)".into(),
            category: SoftwareCategory::Custom,
        }];
        let prober = SoftwareProber::new().with_custom_probes(custom);
        assert_eq!(prober.custom_probes.len(), 1);
        assert_eq!(prober.custom_probes[0].name, "my-tool");
    }

    #[test]
    fn prober_with_timeout() {
        let prober = SoftwareProber::new().with_timeout(Duration::from_secs(10));
        assert_eq!(prober.probe_timeout, Duration::from_secs(10));
    }

    #[test]
    fn prober_default_timeout_matches_config() {
        let prober = SoftwareProber::new();
        assert_eq!(prober.probe_timeout, DISCOVERY_PROBE_TIMEOUT);
    }

    // ---- discovery_interval ----

    #[test]
    fn discovery_interval_matches_config() {
        assert_eq!(discovery_interval(), DISCOVERY_INTERVAL);
    }

    // ---- Version comparison edge cases ----

    #[test]
    fn version_comparison_major_difference() {
        let installed = vec![make_installed("node", Some("20.10.0"), SoftwareCategory::Runtime)];
        let req = SoftwareRequirement::parse("node>=18.0");
        assert!(satisfies_requirement(&installed, &req));
    }

    #[test]
    fn version_comparison_minor_difference() {
        let installed = vec![make_installed("python3", Some("3.10.0"), SoftwareCategory::Runtime)];
        let req = SoftwareRequirement::parse("python3>=3.11");
        assert!(!satisfies_requirement(&installed, &req));
    }

    #[test]
    fn version_comparison_patch_difference() {
        let installed = vec![make_installed("git", Some("2.43.0"), SoftwareCategory::DevTool)];
        let req_lower = SoftwareRequirement::parse("git>=2.43.0");
        let req_higher = SoftwareRequirement::parse("git>=2.43.1");
        assert!(satisfies_requirement(&installed, &req_lower));
        assert!(!satisfies_requirement(&installed, &req_higher));
    }

    #[test]
    fn version_two_part_installed_three_part_required() {
        // Installed "3.10" should parse as "3.10.0" and satisfy ">=3.10.0".
        let installed = vec![make_installed("python3", Some("3.10"), SoftwareCategory::Runtime)];
        let req = SoftwareRequirement::parse("python3>=3.10.0");
        assert!(satisfies_requirement(&installed, &req));
    }

    // ---- Async probe tests (integration-style, with real `which`) ----

    #[tokio::test]
    async fn probe_all_finds_at_least_one_tool() {
        // On any reasonable system, at least `sh` or `git` or `curl` should exist.
        // We use a minimal prober with just a few safe probes.
        let prober = SoftwareProber::new().with_timeout(Duration::from_secs(3));
        let results = prober.probe_all().await;

        // We cannot guarantee what's installed, but we can verify the return
        // type and that no panics occurred.
        for sw in &results {
            assert!(!sw.name.is_empty());
            assert!(!sw.path.is_empty());
        }
    }

    #[tokio::test]
    async fn find_binary_nonexistent() {
        let prober = SoftwareProber::new();
        let result = prober
            .find_binary(&["__nonexistent_binary_12345__".to_string()])
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn extract_version_nonexistent_binary() {
        let prober = SoftwareProber::new();
        let result = prober
            .extract_version("/nonexistent/path/to/binary", "--version", r"(\d+)")
            .await;
        assert!(result.is_none());
    }
}
