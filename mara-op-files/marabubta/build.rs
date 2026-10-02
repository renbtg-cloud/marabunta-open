// Marabunta - Licensed under the MIT License.
//! Build script for Marabunta Compute.
//!
//! When a compliance feature (e.g. `compliance-soc2`) is enabled, this reads
//! the corresponding `compliance/*.toml` file and generates Rust code that
//! produces `ComplianceOverride` values at compile time.
//!
//! Generated file: `$OUT_DIR/compliance_generated.rs`
//! Consumed via `include!` in `src/swarm/config.rs`.

use std::env;
use std::fs;
use std::path::PathBuf;

extern crate cc;

fn main() {
    // --- STAGE 5.1: eBPF Thermal Guardian Compilation ---
    // Note: In a production environment with libelf-dev installed, this block 
    // automatically compiles the C code in src/bpf/ into a Rust skeleton.
    /*
    #[cfg(target_os = "linux")]
    {
        use libbpf_cargo::SkeletonBuilder;
        let bpf_src = "src/bpf/thermal_guardian.bpf.c";
        let bpf_obj = out_dir.join("thermal_guardian.bpf.o");
        let bpf_skel = out_dir.join("thermal_guardian.skel.rs");

        println!("cargo:rerun-if-changed={}", bpf_src);
        SkeletonBuilder::new()
            .source(bpf_src)
            .build(&bpf_skel)
            .expect("Failed to build BPF skeleton");
    }
    */

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());

    // Determine which compliance profile to use.
    // Priority: COMPLIANCE_TOML env var > Cargo feature flags > none.
    let profile_path = if let Ok(custom) = env::var("COMPLIANCE_TOML") {
        Some(PathBuf::from(custom))
    } else if cfg!(feature = "compliance-fedramp") {
        Some(manifest_dir.join("compliance/fedramp.toml"))
    } else if cfg!(feature = "compliance-hipaa") {
        Some(manifest_dir.join("compliance/hipaa.toml"))
    } else if cfg!(feature = "compliance-gdpr") {
        Some(manifest_dir.join("compliance/gdpr.toml"))
    } else if cfg!(feature = "compliance-soc2") {
        Some(manifest_dir.join("compliance/soc2.toml"))
    } else if cfg!(feature = "compliance-highestsec") {
        Some(manifest_dir.join("compliance/highestsec.toml"))
    } else {
        None
    };

    // Emit rerun-if-changed for compliance files.
    let compliance_dir = manifest_dir.join("compliance");
    if compliance_dir.exists() {
        for entry in fs::read_dir(&compliance_dir).unwrap().flatten() {
            if entry.path().extension().map_or(false, |e| e == "toml") {
                println!("cargo:rerun-if-changed={}", entry.path().display());
            }
        }
    }
    println!("cargo:rerun-if-env-changed=COMPLIANCE_TOML");

    let generated = if let Some(path) = profile_path {
        generate_compliance_code(&path)
    } else {
        generate_empty_compliance()
    };

    let out_file = out_dir.join("compliance_generated.rs");
    fs::write(&out_file, generated).expect("Failed to write compliance_generated.rs");

    // Compile the forbidden syscall test executable for Linux with highestsec-sandbox.
    #[cfg(all(target_os = "linux", feature = "highestsec-sandbox"))]
    {
        println!("cargo:rerun-if-changed=forbidden_syscall_test.c");
        cc::Build::new()
            .file("forbidden_syscall_test.c")
            .out_dir(&out_dir)
            .compile("forbidden_syscall_test");
        println!("cargo:rustc-env=FORBIDDEN_SYSCALL_TEST_PATH={}", out_dir.join("forbidden_syscall_test").display());
    }
}

fn generate_empty_compliance() -> String {
    r#"
/// No compliance profile active.
pub const COMPLIANCE_PROFILE_NAME: Option<&str> = None;
/// No compliance profile version.
pub const COMPLIANCE_PROFILE_VERSION: Option<&str> = None;
/// No compliance profile description.
pub const COMPLIANCE_PROFILE_DESCRIPTION: Option<&str> = None;

/// Returns an empty list of compliance overrides (no profile active).
pub fn compliance_overrides() -> Vec<crate::swarm::config_meta::ComplianceOverride> {
    Vec::new()
}
"#.to_string()
}

fn generate_compliance_code(path: &PathBuf) -> String {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read compliance file {:?}: {}", path, e));

    let doc: toml::Value = content.parse()
        .unwrap_or_else(|e| panic!("Failed to parse compliance TOML {:?}: {}", path, e));

    let profile = doc.get("profile").expect("Missing [profile] section");
    let name = profile.get("name").and_then(|v| v.as_str()).unwrap_or("unknown");
    let version = profile.get("version").and_then(|v| v.as_str()).unwrap_or("0.0");
    let description = profile.get("description").and_then(|v| v.as_str()).unwrap_or("");

    let overrides = doc.get("overrides")
        .and_then(|v| v.as_array())
        .map(|arr| arr.as_slice())
        .unwrap_or(&[]);

    let mut code = String::new();

    // Profile metadata constants.
    code.push_str(&format!(
        r#"
/// Active compliance profile name.
pub const COMPLIANCE_PROFILE_NAME: Option<&str> = Some("{}");
/// Active compliance profile version.
pub const COMPLIANCE_PROFILE_VERSION: Option<&str> = Some("{}");
/// Active compliance profile description.
pub const COMPLIANCE_PROFILE_DESCRIPTION: Option<&str> = Some("{}");
"#,
        escape_str(name),
        escape_str(version),
        escape_str(description),
    ));

    // Generate the overrides function.
    code.push_str("\n/// Returns compliance overrides from the build-time profile.\n");
    code.push_str("pub fn compliance_overrides() -> Vec<crate::swarm::config_meta::ComplianceOverride> {\n");
    code.push_str("    vec![\n");

    for ovr in overrides {
        let key = ovr.get("key").and_then(|v| v.as_str()).unwrap_or("");
        let value_toml = ovr.get("value").unwrap_or(&toml::Value::Boolean(false));
        let ui_vis = ovr.get("ui_visibility").and_then(|v| v.as_str()).unwrap_or("visible_readonly");
        let constraint = ovr.get("constraint_type").and_then(|v| v.as_str()).unwrap_or("exact");
        let justification = ovr.get("justification").and_then(|v| v.as_str()).unwrap_or("");

        let value_json = toml_value_to_json_literal(value_toml);
        let ui_vis_variant = match ui_vis {
            "visible_mutable" => "VisibleMutable",
            "hidden" => "Hidden",
            _ => "VisibleReadonly",
        };

        code.push_str(&format!(
            r#"        crate::swarm::config_meta::ComplianceOverride {{
            key: "{}".to_string(),
            value: serde_json::json!({}),
            ui_visibility: crate::swarm::config_meta::UiVisibility::{},
            constraint_type: "{}".to_string(),
            justification: "{}".to_string(),
            profile_name: "{}".to_string(),
        }},
"#,
            escape_str(key),
            value_json,
            ui_vis_variant,
            escape_str(constraint),
            escape_str(justification),
            escape_str(name),
        ));
    }

    code.push_str("    ]\n");
    code.push_str("}\n");

    code
}

fn toml_value_to_json_literal(v: &toml::Value) -> String {
    match v {
        toml::Value::Boolean(b) => format!("{}", b),
        toml::Value::Integer(i) => format!("{}", i),
        toml::Value::Float(f) => format!("{}", f),
        toml::Value::String(s) => format!("\"{}\"", escape_str(s)),
        _ => "null".to_string(),
    }
}

fn escape_str(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}
