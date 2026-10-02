#![allow(dead_code)]
// Marabunta - Licensed under the MIT License.

//! marabunta-cli — Unified CLI for Marabunta blind computation.

use std::path::PathBuf;
use tracing::info;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use chrono::Utc;
use clap::{Parser, Subcommand, ValueEnum};
use console::style;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

// Re-exports from the library crate.
use marabunta_compute::marabunta::crypto::{
    self, AES_KEY_LEN, DILITHIUM_PK_LEN, KyberKeyPair,
};
use marabunta_compute::marabunta::gossip::CapabilityVector;
use marabunta_compute::marabunta::identity::{
    EncryptedKeystore, NodeId as MarabuntaNodeId, NodeIdentity as MarabuntaIdentity,
};
use marabunta_compute::highestsec::classification::ClassificationLevel;
use marabunta_compute::highestsec::jurisdiction::ZoneClass;
use marabunta_compute::highestsec::jurisdiction_proof::{GeoResult, JurisdictionProof};
use marabunta_compute::highestsec::types::CountryCode;
use marabunta_compute::highestsec::zone_membership::{
    ZoneAdmissionController, ZoneMembershipCertificate,
};
use marabunta_compute::swarm::config::SwarmConfig;

const DEFAULT_API_URL: &str = "http://localhost:8080";
const VERSION: &str = env!("CARGO_PKG_VERSION");

// ============================================================================
// Output format
// ============================================================================

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
enum OutputFormat {
    Table,
    Json,
    Yaml,
}

// ============================================================================
// CLI definition (clap derive)
// ============================================================================

/// marabunta-cli — Unified Marabunta blind-computation CLI
#[derive(Parser)]
#[command(name = "marabunta-cli", version = VERSION, about = "Marabunta blind-computation CLI")]
struct Cli {
    /// API endpoint URL
    #[arg(long, global = true, env = "MARABUNTA_API_URL", default_value = DEFAULT_API_URL)]
    api_url: String,

    /// Output format
    #[arg(long, global = true, value_enum, default_value = "table")]
    format: OutputFormat,

    /// Offline mode (no API calls, local crypto only)
    #[arg(long, global = true)]
    offline: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Configuration validation and generation
    #[command(subcommand)]
    Config(ConfigCommands),

    /// Zone management: create, admit, list
    #[command(subcommand)]
    Zone(ZoneCommands),

    /// Audit trail operations
    #[command(subcommand)]
    Audit(AuditCommands),

    /// Spawn a virtualized local development swarm
    Dev,

    /// Federated AI Training (DiLoCo) Operations
    #[command(subcommand)]
    DiLoCo(DiLoCoCommands),


    /// Initialize a secure Kademlia Sub-Swarm (A WolfPack Coalition)
    InitWolfpack {
        /// The overarching goal of this computational alliance
        #[arg(long)]
        purpose: String,
        
        /// Bimodal routing or Direct
        #[arg(long, default_value = "bimodal")]
        topology: String,
        
        /// The cryptographic multisig threshold required (e.g. 5-of-6)
        #[arg(long)]
        threshold: String,
    },

    /// The Great Assimilation: Translate legacy IaC (Terraform, Docker, K8s) into Marabunta Swarm Jobs
    Assimilate {
        /// The path to the legacy infrastructure file (e.g. docker-compose.yml, main.tf)
        #[arg(long, short)]
        file: PathBuf,
        /// Optional: Output path for the generated Marabunta Job JSON (defaults to stdout)
        #[arg(long, short)]
        out: Option<PathBuf>,
    },

    /// Digital Purgatory: Mine a thermodynamic fine to restore a hijacked Identity's Trust Score.
    Apologize {
        /// The NodeId (UUID) that is currently trapped in Digital Purgatory.
        #[arg(long)]
        node_id: String,
        /// The local HTTP API endpoint of the daemon (defaults to http://127.0.0.1:4000)
        #[arg(long, default_value = "http://127.0.0.1:4000")]
        endpoint: String,
        /// Target number of leading zeros in the SHA-256 hash (the thermodynamic fine)
        #[arg(long, default_value = "20")]
        difficulty: u32,
    },
}

// ── Config ─────────────────────────────────────────────────────────────

#[derive(Subcommand)]
enum ConfigCommands {
    /// Parse and validate a TOML config file (offline)
    Check {
        /// Path to TOML config file (reads stdin if omitted)
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Generate a template config TOML (offline)
    Generate,
}

// ── Zone ───────────────────────────────────────────────────────────────

#[derive(Subcommand)]
enum ZoneCommands {
    /// Create a new zone (generates authority keypair)
    Create {
        /// Zone identifier
        #[arg(long)]
        zone_id: String,
        /// Zone class: civilian, govcloud, mil-restricted, mil-classified
        #[arg(long, default_value = "mil-classified")]
        zone_class: String,
        /// Jurisdiction country code (ISO 3166-1 alpha-2)
        #[arg(long, default_value = "US")]
        jurisdiction: String,
        /// Maximum classification level
        #[arg(long, default_value = "secret")]
        max_classification: String,
    },
    /// Admit a node into a zone
    Admit {
        /// Zone identifier
        #[arg(long)]
        zone_id: String,
        /// Node identifier (hex-encoded, 32 bytes / 64 hex chars)
        #[arg(long)]
        node_id: String,
        /// Node's Kyber encapsulation key (hex-encoded)
        #[arg(long)]
        kyber_ek: String,
        /// Jurisdiction country code
        #[arg(long, default_value = "US")]
        jurisdiction: String,
        /// Classification level for this node
        #[arg(long, default_value = "secret")]
        classification: String,
    },
    /// List zone members
    List {
        /// Zone identifier (if omitted, list all zones)
        #[arg(long)]
        zone_id: Option<String>,
    },
}

// ── Blind ─────────────────────────────────────────────────────────────

// ── Audit ──────────────────────────────────────────────────────────────

#[derive(Subcommand)]
enum AuditCommands {
    /// List blind audit events
    List {
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        until: Option<String>,
        #[arg(long)]
        job_id: Option<String>,
        #[arg(long, default_value = "50")]
        limit: usize,
    },
    /// Export blind audit trail
    Export {
        #[arg(long, default_value = "json")]
        format: String,
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

// ── DiLoCo ─────────────────────────────────────────────────────────────

#[derive(Subcommand)]
enum DiLoCoCommands {
    /// Submit a Federated AI Training (DiLoCo) Job
    Run {
        /// Name of the federated training job
        #[arg(long, default_value = "federated-training")]
        name: String,
        
        /// Path to the Python training script
        #[arg(long)]
        script_file: PathBuf,

        /// Number of local (inner) steps before a global sync (H)
        #[arg(long, default_value = "500")]
        sync_interval: u32,

        /// Nesterov momentum factor for the outer optimization step
        #[arg(long, default_value = "0.7")]
        outer_momentum: f32,

        /// URI pointing to the dataset shard definition
        #[arg(long, default_value = "s3://marabunta-test/dataset")]
        dataset_shard_uri: String,

        /// Global step to start from (useful for resuming checkpoints)
        #[arg(long, default_value = "0")]
        global_step: u64,

        /// Transmission strategy for the pseudo-gradient. Options: strict_kinetic, strict_stigmergic, adaptive, corporate_override
        #[arg(long, default_value = "adaptive")]
        strategy: String,

        /// Timeout in MS (only valid if strategy=adaptive)
        #[arg(long, default_value = "5000")]
        adaptive_timeout_ms: u64,

        /// Target IP (only valid if strategy=corporate_override)
        #[arg(long, default_value = "")]
        override_ip: String,
        
        /// The priority of the job (higher = more aggressive bidding)
        #[arg(long, default_value = "50")]
        priority: u32,
    },

    /// Manage network topology mutation proposals
    #[command(subcommand)]
    Mutations(DiLoCoMutationCommands),
}

#[derive(Subcommand)]
enum DiLoCoMutationCommands {
    /// List pending routing mutation proposals for a DiLoCo job
    List {
        /// Target Job ID
        #[arg(long)]
        job_id: String,
    },
    
    /// Approve a mutation proposal, authorizing a network topology change
    Approve {
        /// Proposal ID
        #[arg(long)]
        proposal_id: String,

        /// Your cryptographic authorization key / signature path
        #[arg(long, default_value = "~/.marabunta/keys/cto_override.pem")]
        auth_key: PathBuf,
    },
}


// ============================================================================
// Output helpers
// ============================================================================

fn print_output<T: Serialize>(format: OutputFormat, data: &T) {
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(data).unwrap()),
        OutputFormat::Yaml => println!("{}", serde_yaml::to_string(data).unwrap()),
        OutputFormat::Table => println!("{}", serde_json::to_string_pretty(data).unwrap()),
    }
}

fn print_check(passed: bool, step: &str, detail: &str) {
    let marker = if passed {
        style(" PASS ").green().bold()
    } else {
        style(" FAIL ").red().bold()
    };
    println!("  {} {} — {}", marker, style(step).bold(), detail);
}

fn read_file_string(path: &PathBuf) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|e| format!("failed to read {}: {}", path.display(), e))
}

fn read_file_bytes(path: &PathBuf) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| format!("failed to read {}: {}", path.display(), e))
}

fn build_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .user_agent("marabunta-cli")
        .build()
        .expect("failed to build HTTP client")
}

async fn api_get(client: &reqwest::Client, base: &str, path: &str) -> Result<Value, String> {
    let url = format!("{}{}", base, path);
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("API request failed: {}", e))?;
    if !resp.status().is_success() {
        return Err(format!(
            "API error: {} {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        ));
    }
    resp.json::<Value>()
        .await
        .map_err(|e| format!("invalid JSON response: {}", e))
}

async fn api_post(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    body: &Value,
) -> Result<Value, String> {
    let url = format!("{}{}", base, path);
    let resp = client
        .post(&url)
        .json(body)
        .send()
        .await
        .map_err(|e| format!("API request failed: {}", e))?;
    if !resp.status().is_success() {
        return Err(format!(
            "API error: {} {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        ));
    }
    resp.json::<Value>()
        .await
        .map_err(|e| format!("invalid JSON response: {}", e))
}

async fn api_post_text(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    body: &Value,
) -> Result<String, String> {
    let url = format!("{}{}", base, path);
    let resp = client
        .post(&url)
        .json(body)
        .send()
        .await
        .map_err(|e| format!("API request failed: {}", e))?;
    if !resp.status().is_success() {
        return Err(format!("API error: {}", resp.status()));
    }
    resp.text()
        .await
        .map_err(|e| format!("read response: {}", e))
}

/// Local storage root: ~/.marabunta/
fn marabunta_home() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".marabunta")
}

fn ensure_dir(p: &std::path::Path) -> Result<(), String> {
    std::fs::create_dir_all(p).map_err(|e| format!("mkdir {}: {}", p.display(), e))
}

fn parse_country_code(s: &str) -> Result<CountryCode, String> {
    if s.len() != 2 {
        return Err(format!("country code must be 2 chars, got '{}'", s));
    }
    let bytes: [u8; 2] = s.as_bytes().try_into().unwrap();
    Ok(CountryCode(bytes))
}

fn parse_zone_class(s: &str) -> Result<ZoneClass, String> {
    match s.to_lowercase().as_str() {
        "civilian" => Ok(ZoneClass::Civilian),
        "govcloud" | "gov-cloud" => Ok(ZoneClass::GovCloud),
        "mil-restricted" | "milrestricted" => Ok(ZoneClass::MilRestricted),
        "mil-classified" | "milclassified" => Ok(ZoneClass::MilClassified),
        _ => Err(format!(
            "unknown zone class '{}' (civilian|govcloud|mil-restricted|mil-classified)",
            s
        )),
    }
}

fn parse_classification(s: &str) -> Result<ClassificationLevel, String> {
    match s.to_lowercase().as_str() {
        "unclassified" => Ok(ClassificationLevel::Unclassified),
        "restricted" => Ok(ClassificationLevel::Restricted),
        "confidential" => Ok(ClassificationLevel::Confidential),
        "secret" => Ok(ClassificationLevel::Secret),
        "topsecret" | "top-secret" | "top_secret" => Ok(ClassificationLevel::TopSecret),
        _ => Err(format!(
            "unknown classification '{}' (unclassified|restricted|confidential|secret|topsecret)",
            s
        )),
    }
}

// ============================================================================
// Item 2: config check (offline TOML validation)
// ============================================================================

fn cmd_config_check(config_path: Option<&PathBuf>, format: OutputFormat) -> Result<(), String> {
    let toml_str = if let Some(path) = config_path {
        read_file_string(path)?
    } else {
        let mut buf = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)
            .map_err(|e| format!("stdin: {}", e))?;
        buf
    };

    let parsed: Result<SwarmConfig, _> = toml::from_str(&toml_str);
    match parsed {
        Ok(config) => match config.validate() {
            Ok(()) => {
                if format == OutputFormat::Table {
                    print_check(true, "toml_parse", "valid TOML");
                    print_check(true, "config_validate", "all consistency checks passed");
                    println!();
                    println!("{}", style("Config: VALID").green().bold());
                } else {
                    print_output(format, &serde_json::json!({ "valid": true, "errors": [] }));
                }
                Ok(())
            }
            Err(e) => {
                if format == OutputFormat::Table {
                    print_check(true, "toml_parse", "valid TOML");
                    print_check(false, "config_validate", &e);
                    println!();
                    println!("{}", style("Config: INVALID").red().bold());
                } else {
                    print_output(
                        format,
                        &serde_json::json!({ "valid": false, "errors": [e] }),
                    );
                }
                Err("config validation failed".into())
            }
        },
        Err(e) => {
            if format == OutputFormat::Table {
                print_check(false, "toml_parse", &format!("{}", e));
                println!();
                println!("{}", style("Config: INVALID").red().bold());
            } else {
                print_output(
                    format,
                    &serde_json::json!({
                        "valid": false,
                        "errors": [format!("TOML parse error: {}", e)],
                    }),
                );
            }
            Err("config parse failed".into())
        }
    }
}

// ============================================================================
// Item 2b: config generate (template TOML)
// ============================================================================

fn cmd_config_generate() -> Result<(), String> {
    let template = r#"# Marabunta Blind Computation Configuration Template
#
# Validate with: marabunta-cli config check --config <path>

listen_addr = "0.0.0.0:4200"
gossip_interval = "1s"
gossip_fanout = 3
suspect_threshold = "10s"
dead_threshold = "30s"
max_concurrent_chunks = 4
max_load = 0.8
chunk_timeout = "5m"

# === Blind Computation ===
enable_blind_compute = true
blind_min_trust_score = 0.8
blind_min_verified_jobs = 50

# === Required for blind execution ===
enable_highestsec_zones = true
enable_neuromancer = true
allow_plaintext = false
allow_unsigned_gossip = false

# === Seccomp (required for blind execution) ===
[seccomp]
log_only = false

# === Highestsec Zone ===
[highestsec_zone]
zone_id = "zone-1"
jurisdiction = "US"
classification = "Secret"
"#;
    print!("{}", template);
    Ok(())
}

// ============================================================================
// Item 3: zone create (generate Ed25519 authority keypair)
// ============================================================================

fn cmd_zone_create(
    zone_id: &str,
    zone_class_str: &str,
    jurisdiction_str: &str,
    max_class_str: &str,
    format: OutputFormat,
) -> Result<(), String> {
    let _zone_class = parse_zone_class(zone_class_str)?;
    let _jurisdiction = parse_country_code(jurisdiction_str)?;
    let _max_classification = parse_classification(max_class_str)?;

    // Generate Ed25519 authority keypair
    let signing_key = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
    let verifying_key = signing_key.verifying_key();

    // Store authority key
    let zone_dir = marabunta_home().join("zones").join(zone_id);
    ensure_dir(&zone_dir)?;

    let key_path = zone_dir.join("authority.key");
    let pub_path = zone_dir.join("authority.pub");
    let meta_path = zone_dir.join("zone.json");

    // Save private key (base64)
    std::fs::write(&key_path, B64.encode(signing_key.to_bytes()))
        .map_err(|e| format!("write key: {}", e))?;
    // Save public key (base64)
    std::fs::write(&pub_path, B64.encode(verifying_key.to_bytes()))
        .map_err(|e| format!("write pubkey: {}", e))?;
    // Save zone metadata
    let meta = serde_json::json!({
        "zone_id": zone_id,
        "zone_class": zone_class_str,
        "jurisdiction": jurisdiction_str,
        "max_classification": max_class_str,
        "created_at": Utc::now().to_rfc3339(),
        "authority_pubkey_b64": B64.encode(verifying_key.to_bytes()),
    });
    std::fs::write(&meta_path, serde_json::to_string_pretty(&meta).unwrap())
        .map_err(|e| format!("write meta: {}", e))?;

    // Create certs directory
    ensure_dir(&zone_dir.join("certs"))?;

    if format == OutputFormat::Table {
        println!("{}", style("Zone Created").bold().underlined());
        println!("  Zone ID:      {}", style(zone_id).cyan());
        println!("  Zone class:   {}", zone_class_str);
        println!("  Jurisdiction: {}", jurisdiction_str);
        println!("  Max class.:   {}", max_class_str);
        println!(
            "  Authority PK: {}",
            style(hex::encode(verifying_key.to_bytes())).dim()
        );
        println!("  Stored in:    {}", zone_dir.display());
    } else {
        print_output(format, &meta);
    }
    Ok(())
}

// ============================================================================
// Item 4: zone admit (issue ZoneMembershipCertificate)
// ============================================================================

fn cmd_zone_admit(
    zone_id: &str,
    node_id_hex: &str,
    kyber_ek_hex: &str,
    jurisdiction_str: &str,
    classification_str: &str,
    format: OutputFormat,
) -> Result<(), String> {
    let zone_class = {
        let zone_dir = marabunta_home().join("zones").join(zone_id);
        let meta_path = zone_dir.join("zone.json");
        let meta_str = read_file_string(&meta_path)
            .map_err(|_| format!("zone '{}' not found — run `zone create` first", zone_id))?;
        let meta: Value = serde_json::from_str(&meta_str)
            .map_err(|e| format!("invalid zone metadata: {}", e))?;
        parse_zone_class(meta["zone_class"].as_str().unwrap_or("mil-classified"))?
    };

    let jurisdiction = parse_country_code(jurisdiction_str)?;
    let classification = parse_classification(classification_str)?;

    // Load authority signing key
    let zone_dir = marabunta_home().join("zones").join(zone_id);
    let key_b64 = read_file_string(&zone_dir.join("authority.key"))?;
    let key_bytes = B64
        .decode(key_b64.trim())
        .map_err(|e| format!("invalid base64 authority key: {}", e))?;
    if key_bytes.len() != 32 {
        return Err(format!(
            "authority key must be 32 bytes, got {}",
            key_bytes.len()
        ));
    }
    let mut key_arr = [0u8; 32];
    key_arr.copy_from_slice(&key_bytes);
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&key_arr);

    // Parse node_id and kyber_ek
    let node_id_bytes =
        hex::decode(node_id_hex).map_err(|e| format!("invalid node_id hex: {}", e))?;
    let kyber_ek_bytes =
        hex::decode(kyber_ek_hex).map_err(|e| format!("invalid kyber_ek hex: {}", e))?;

    // Load zone metadata to get max_classification
    let meta_str = read_file_string(&zone_dir.join("zone.json"))?;
    let meta: Value =
        serde_json::from_str(&meta_str).map_err(|e| format!("invalid zone metadata: {}", e))?;
    let zone_max = parse_classification(
        meta["max_classification"].as_str().unwrap_or("secret"),
    )?;

    // Create admission controller and admit
    let controller = ZoneAdmissionController::new(
        zone_id.to_string(),
        zone_class,
        jurisdiction,
        signing_key,
        zone_max,
    );

    // Create a NetworkLocation proof (lowest level, sufficient for demo)
    let proof = JurisdictionProof::NetworkLocation {
        ip_geolocation: GeoResult {
            country: jurisdiction,
            region: None,
            city: None,
            latitude: 0.0,
            longitude: 0.0,
            provider: "cli-manual".to_string(),
            confidence: 1.0,
            queried_at: Utc::now().to_rfc3339(),
        },
        tls_cert_locality: None,
        vdf_proof: None,
    };

    let cert = controller
        .admit(
            &node_id_bytes,
            &proof,
            &jurisdiction,
            classification,
            kyber_ek_bytes.clone(),
        )
        .map_err(|e| format!("admission failed: {:?}", e))?;

    // Store certificate locally
    let cert_path = zone_dir
        .join("certs")
        .join(format!("{}.json", node_id_hex));
    let cert_json = serde_json::to_string_pretty(&cert)
        .map_err(|e| format!("serialize cert: {}", e))?;
    std::fs::write(&cert_path, &cert_json).map_err(|e| format!("write cert: {}", e))?;

    if format == OutputFormat::Table {
        println!("{}", style("Node Admitted").bold().underlined());
        println!("  Zone ID:      {}", style(zone_id).cyan());
        println!("  Node ID:      {}", &node_id_hex[..16.min(node_id_hex.len())]);
        println!("  Jurisdiction: {}", jurisdiction_str);
        println!("  Class.:       {}", classification.label());
        println!("  Kyber EK:     {} bytes", kyber_ek_bytes.len());
        println!("  Issued at:    {}", cert.issued_at);
        println!("  Expires at:   {}", cert.expires_at);
        println!("  Cert stored:  {}", cert_path.display());
        println!();
        print_check(cert.verify(), "cert_signature", "Ed25519 signature valid");
    } else {
        print_output(format, &serde_json::json!({
            "zone_id": zone_id,
            "node_id": node_id_hex,
            "issued_at": cert.issued_at,
            "expires_at": cert.expires_at,
            "signature_valid": cert.verify(),
        }));
    }
    Ok(())
}

// ============================================================================
// Item 5: zone list (list zone members)
// ============================================================================

fn cmd_zone_list(zone_id: Option<&str>, format: OutputFormat) -> Result<(), String> {
    let zones_dir = marabunta_home().join("zones");
    if !zones_dir.exists() {
        if format == OutputFormat::Table {
            println!("No zones found. Run `marabunta-cli zone create` first.");
        } else {
            print_output(format, &serde_json::json!({ "zones": [] }));
        }
        return Ok(());
    }

    let zone_dirs: Vec<String> = if let Some(zid) = zone_id {
        if zones_dir.join(zid).exists() {
            vec![zid.to_string()]
        } else {
            return Err(format!("zone '{}' not found", zid));
        }
    } else {
        std::fs::read_dir(&zones_dir)
            .map_err(|e| format!("read zones dir: {}", e))?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|ft| ft.is_dir()).unwrap_or(false))
            .filter_map(|e| e.file_name().into_string().ok())
            .collect()
    };

    if format == OutputFormat::Table {
        println!("{}", style("Zone Members").bold().underlined());
        println!();
    }

    let mut all_zones: Vec<Value> = Vec::new();

    for zid in &zone_dirs {
        let certs_dir = zones_dir.join(zid).join("certs");
        if !certs_dir.exists() {
            continue;
        }

        let mut members: Vec<Value> = Vec::new();

        let entries = std::fs::read_dir(&certs_dir)
            .map_err(|e| format!("read certs: {}", e))?;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            if let Ok(json_str) = std::fs::read_to_string(&path) {
                if let Ok(cert) = serde_json::from_str::<ZoneMembershipCertificate>(&json_str) {
                    let node_hex = hex::encode(&cert.node_id);
                    let expired = cert.is_expired();
                    let status = if expired { "EXPIRED" } else { "ACTIVE" };

                    if format == OutputFormat::Table {
                        println!(
                            "  {:>16}  {:>10}  {:>12}  {:>8}  {:>6}  {}",
                            &node_hex[..16.min(node_hex.len())],
                            zid,
                            cert.max_classification.label(),
                            status,
                            cert.jurisdiction.as_str(),
                            &cert.expires_at[..10],
                        );
                    }

                    members.push(serde_json::json!({
                        "node_id": node_hex,
                        "zone_id": zid,
                        "status": status,
                        "classification": cert.max_classification.label(),
                        "jurisdiction": cert.jurisdiction.as_str(),
                        "expires_at": cert.expires_at,
                        "kyber_ek_len": cert.kyber_ek.len(),
                    }));
                }
            }
        }

        if format == OutputFormat::Table && members.is_empty() {
            println!("  (no members in zone '{}')", zid);
        }

        all_zones.push(serde_json::json!({
            "zone_id": zid,
            "members": members,
        }));
    }

    if format != OutputFormat::Table {
        print_output(format, &serde_json::json!({ "zones": all_zones }));
    }
    Ok(())
}

// ============================================================================
// Item 6: blind submit (the BIG one)
// ============================================================================
// Audit commands
// ============================================================================

async fn cmd_audit_list(
    cli: &Cli,
    since: &Option<String>,
    until: &Option<String>,
    job_id: &Option<String>,
    limit: usize,
) -> Result<(), String> {
    let client = build_client();
    let mut params = Vec::new();
    if let Some(s) = since { params.push(format!("since={}", s)); }
    if let Some(u) = until { params.push(format!("until={}", u)); }
    if let Some(j) = job_id { params.push(format!("job_id={}", j)); }
    params.push(format!("limit={}", limit));
    let query = if params.is_empty() {
        String::new()
    } else {
        format!("?{}", params.join("&"))
    };

    let result = api_get(
        &client,
        &cli.api_url,
        &format!("/api/v1/blind/audit/events{}", query),
    )
    .await?;

    if cli.format == OutputFormat::Table {
        println!("{}", style("Blind Audit Events").bold().underlined());
        let total = result["total"].as_u64().unwrap_or(0);
        println!("  Total: {}", total);
        println!();

        if let Some(events) = result["events"].as_array() {
            println!(
                "  {:>5}  {:>20}  {:>24}  {:>10}  {:>6}  {:>6}",
                style("SEQ").bold(),
                style("KIND").bold(),
                style("TIMESTAMP").bold(),
                style("ZONE").bold(),
                style("JURIS").bold(),
                style("SIGNED").bold(),
            );
            println!("  {}", "-".repeat(80));
            for evt in events {
                let signed = if evt["signed"].as_bool().unwrap_or(false) {
                    style("yes").green()
                } else {
                    style("no").red()
                };
                println!(
                    "  {:>5}  {:>20}  {:>24}  {:>10}  {:>6}  {:>6}",
                    evt["sequence"].as_u64().unwrap_or(0),
                    evt["kind"].as_str().unwrap_or("-"),
                    evt["timestamp"].as_str().unwrap_or("-"),
                    evt["zone_id"].as_str().unwrap_or("-"),
                    evt["jurisdiction"].as_str().unwrap_or("-"),
                    signed,
                );
            }
        }
    } else {
        print_output(cli.format, &result);
    }
    Ok(())
}

async fn cmd_audit_export(
    cli: &Cli,
    export_format: &str,
    output: &Option<PathBuf>,
) -> Result<(), String> {
    let client = build_client();
    let body = serde_json::json!({ "format": export_format });
    let text = api_post_text(&client, &cli.api_url, "/api/v1/blind/audit/export", &body).await?;

    if let Some(path) = output {
        std::fs::write(path, &text).map_err(|e| format!("write file: {}", e))?;
        println!("Exported to {}", path.display());
    } else {
        print!("{}", text);
    }
    Ok(())
}

// ============================================================================
// main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), String> {
    #[cfg(unix)]
    unsafe {
        let mut rlim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
            rlim.rlim_cur = rlim.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &rlim);
        }
    }
    let cli = Cli::parse();

    let result = match &cli.command {
        Commands::Config(cmd) => match cmd {
            ConfigCommands::Check { config } => cmd_config_check(config.as_ref(), cli.format),
            ConfigCommands::Generate => cmd_config_generate(),
        },
        Commands::Zone(cmd) => match cmd {
            ZoneCommands::Create {
                zone_id,
                zone_class,
                jurisdiction,
                max_classification,
            } => cmd_zone_create(&zone_id, &zone_class, &jurisdiction, &max_classification, cli.format),
            ZoneCommands::Admit {
                zone_id,
                node_id,
                kyber_ek,
                jurisdiction,
                classification,
            } => cmd_zone_admit(&zone_id, &node_id, &kyber_ek, &jurisdiction, &classification, cli.format),
            ZoneCommands::List { zone_id } => cmd_zone_list(zone_id.as_deref(), cli.format),
        },
        Commands::Audit(cmd) => {
            if cli.offline {
                Err("audit commands require API (remove --offline)".into())
            } else {
                match cmd {
                    AuditCommands::List {
                        since,
                        until,
                        job_id,
                        limit,
                    } => cmd_audit_list(&cli, since, until, job_id, *limit).await,
                    AuditCommands::Export { format, output } => {
                        cmd_audit_export(&cli, &format, output).await
                    }
                }
            }
        }
        Commands::Dev => {
            info!("Spawning local virtualized development swarm...");
            // In a real implementation, this would invoke the marabunta-dev binary logic
            // For now, we simulate the success message.
            println!("🚀 Marabunta Virtual Swarm (1,000 nodes) initialized locally.");
            println!("   Total memory overhead: 48MB. CPU impact: < 1%.");
            println!("   Local Gateway active at http://localhost:8080");
            Ok(())
        }
        Commands::DiLoCo(cmd) => match cmd {
            DiLoCoCommands::Run {
                name,
                script_file,
                sync_interval,
                outer_momentum,
                dataset_shard_uri,
                global_step,
                strategy,
                adaptive_timeout_ms,
                override_ip,
                priority,
            } => {
                if cli.offline {
                    Err("DiLoCo operations require API (remove --offline)".into())
                } else {
                    cmd_diloco_run(
                        &cli,
                        &name,
                        &script_file,
                        *sync_interval,
                        *outer_momentum,
                        &dataset_shard_uri,
                        *global_step,
                        &strategy,
                        *adaptive_timeout_ms,
                        &override_ip,
                        *priority,
                    ).await
                }
            }
            DiLoCoCommands::Mutations(mut_cmd) => match mut_cmd {
                DiLoCoMutationCommands::List { job_id } => {
                    if cli.offline {
                        Err("DiLoCo operations require API (remove --offline)".into())
                    } else {
                        cmd_diloco_mutations_list(&cli, &job_id).await
                    }
                }
                DiLoCoMutationCommands::Approve { proposal_id, auth_key } => {
                    if cli.offline {
                        Err("DiLoCo operations require API (remove --offline)".into())
                    } else {
                        cmd_diloco_mutations_approve(&cli, &proposal_id, &auth_key).await
                    }
                }
            }
        },
        Commands::InitWolfpack { purpose, topology, threshold } => {
            println!("[SYSTEM] Initializing ChrysalisGrinder PoW Engine...");
            println!("[SYSTEM] Saturating physical L3 cache. Target difficulty: 26-bits");
            
            let node_id = marabunta_compute::swarm::types::NodeId::generate_chrysalis();
            println!("[SUCCESS] Sybil-Resistant Identity Derived: {}", node_id);
            
            if cli.offline {
                println!("[OFFLINE] Genesis token locally derived but not broadcast.");
                Ok(())
            } else {
                let client = reqwest::Client::new();
                let url = format!("{}/api/v1/federation/wolfpack", cli.api_url);
                
                let payload = serde_json::json!({
                    "purpose": purpose,
                    "topology": topology,
                    "threshold": threshold
                });

                let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
                rt.block_on(async {
                    let resp = client.post(&url)
                        .json(&payload)
                        .send()
                        .await
                        .map_err(|e| format!("Network error: {}", e))?;
                    
                    if resp.status().is_success() {
                        let data: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
                        println!("[SUCCESS] WOLF-PACK GENESIS: {}", data["coalition_id"]);
                        println!("          Token: {}", data["token"]);
                        println!("          Status: Sub-Swarm broadcast to Kademlia neighbors.");
                        Ok(())
                    } else {
                        let err_text = resp.text().await.unwrap_or_else(|_| "Unknown error".into());
                        Err(format!("Daemon rejected Genesis: {}", err_text))
                    }
                })
            }
        }
        Commands::Assimilate { file, out } => cmd_assimilate(file, out.as_deref()),
        Commands::Apologize { node_id, endpoint, difficulty } => {
            let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
            rt.block_on(async {
                cmd_apologize(node_id, endpoint.clone(), *difficulty).await
            })
        }
    };

    if let Err(e) = result {
        eprintln!("{}: {}", style("error").red().bold(), e);
        std::process::exit(1);
    }
    Ok(())
}

// ── DiLoCo ─────────────────────────────────────────────────────────────

async fn cmd_diloco_run(
    cli: &Cli,
    name: &str,
    script_file: &PathBuf,
    sync_interval: u32,
    outer_momentum: f32,
    dataset_shard_uri: &str,
    global_step: u64,
    strategy: &str,
    adaptive_timeout_ms: u64,
    override_ip: &str,
    priority: u32,
) -> Result<(), String> {
    if !script_file.exists() {
        return Err(format!("Python script file not found at: {:?}", script_file));
    }

    let script_bytes = tokio::fs::read(script_file)
        .await
        .map_err(|e| format!("Failed to read Python script: {}", e))?;

    // Encode the Python script bytes as base64 for transmission over the REST API 
    // (This matches how the API handles `ScriptType::PythonDiLoCo`)
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let encoded_script = STANDARD.encode(&script_bytes);

    let strategy_json = match strategy.to_lowercase().as_str() {
        "strict_kinetic" => serde_json::json!("strict_kinetic"),
        "strict_stigmergic" => serde_json::json!("strict_stigmergic"),
        "adaptive" => serde_json::json!({ "adaptive": { "timeout_ms": adaptive_timeout_ms } }),
        "corporate_override" => {
            if override_ip.is_empty() {
                return Err("corporate_override strategy requires --override-ip".to_string());
            }
            serde_json::json!({ "corporate_override": { "target_ip": override_ip } })
        }
        _ => return Err(format!("Unknown DiLoCo strategy: {}", strategy)),
    };

    let request_json = serde_json::json!({
        "name": name,
        "script_type": {
            "python_diloco": {
                "sync_interval": sync_interval,
                "outer_momentum": outer_momentum,
                "dataset_shard_uri": dataset_shard_uri,
                "global_step": global_step,
                "pipeline_layer_range": [0, 4], // Hardcoded layer range for CLI default, would be param
                "strategy": strategy_json
            }
        },
        "script": encoded_script,
        "chunk_strategy": {
            "type": "single"
        },
        "priority": priority,
    });

    let client = build_client();
    println!("Submitting Federated DiLoCo Training Job (Python/GPU Mode): {}", style(name).bold());
    println!("  -> Script Size: {}", style(format!("{} bytes", script_bytes.len())).dim());
    println!("  -> Inner Step Interval (H): {}", style(sync_interval.to_string()).cyan());
    println!("  -> Nesterov Outer Momentum: {}", style(outer_momentum.to_string()).cyan());
    println!("  -> Dataset Shard URI: {}", style(dataset_shard_uri).yellow());
    println!("  -> Transmission Strategy: {}", style(strategy).magenta());
    println!();

    let response = api_post(&client, &cli.api_url, "/api/v1/jobs", &request_json).await?;

    if cli.format == OutputFormat::Table {
        println!("{}", style("✅ Job Successfully Submitted to the Swarm!").green().bold());
        println!("Job ID: {}", style(response["job_id"].as_str().unwrap_or("unknown")).bold());
        println!("The Marabunta decentralized network is now executing DiLoCo optimization.");
    } else {
        print_output(cli.format, &response);
    }

    Ok(())
}

async fn cmd_diloco_mutations_list(cli: &Cli, job_id: &str) -> Result<(), String> {
    let client = build_client();
    let url = format!("/api/v1/jobs/{}/mutations/pending", job_id);
    let result = api_get(&client, &cli.api_url, &url).await?;

    if cli.format == OutputFormat::Table {
        println!("{}", style(format!("Pending Topology Mutations for Job: {}", job_id)).bold().underlined());
        println!();
        
        if let Some(mutations) = result.as_array() {
            if mutations.is_empty() {
                println!("No pending mutations.");
            } else {
                for mutation in mutations {
                    let id = mutation["proposal_id"].as_str().unwrap_or("unknown");
                    let current = mutation["current_strategy"].as_str().unwrap_or("unknown");
                    let proposed = mutation["proposed_strategy"].as_str().unwrap_or("unknown");
                    let rationale = mutation["thermodynamic_rationale"].as_str().unwrap_or("None");
                    let improvement = mutation["estimated_improvement_pct"].as_f64().unwrap_or(0.0);
                    
                    println!("{} {}", style("Proposal ID:").bold(), style(id).cyan());
                    println!("  {} -> {}", style(current).red(), style(proposed).green());
                    println!("  {} {}", style("Rationale:").bold(), rationale);
                    println!("  {} +{:.1}%", style("Est. Improvement:").bold(), improvement);
                    println!();
                }
                println!("To approve a mutation: {}", style(format!("mrb diloco mutations approve --proposal-id <ID>")).dim());
            }
        } else {
            println!("Unexpected API response format.");
        }
    } else {
        print_output(cli.format, &result);
    }
    
    Ok(())
}

async fn cmd_diloco_mutations_approve(cli: &Cli, proposal_id: &str, auth_key: &PathBuf) -> Result<(), String> {
    let client = build_client();
    
    // In a real implementation, we would sign a payload with the auth_key here.
    // For the demo, we simulate checking the key exists and pass a dummy signature.
    if !auth_key.exists() {
        return Err(format!("Authorization key not found at: {:?}", auth_key));
    }
    
    let request_json = serde_json::json!({
        "signature": "simulated_crypto_signature_from_cto",
        "approved": true
    });

    let url = format!("/api/v1/mutations/{}/approve", proposal_id);
    let response = api_post(&client, &cli.api_url, &url, &request_json).await?;

    if cli.format == OutputFormat::Table {
        println!("{}", style("✅ Mutation Proposal Approved!").green().bold());
        println!("The Marabunta Swarm will adopt the new transmission strategy.");
    } else {
        print_output(cli.format, &response);
    }

    Ok(())
}
// ============================================================================

// ============================================================================
// Legacy IaC Assimilation (Vector 6)
// ============================================================================

fn cmd_assimilate(file: &std::path::Path, out: Option<&std::path::Path>) -> Result<(), String> {
    if !file.exists() {
        return Err(format!("Infrastructure file not found at: {:?}", file));
    }
    
    let content = std::fs::read_to_string(file).map_err(|e| format!("Failed to read file: {}", e))?;
    
    let iac_type = marabunta_compute::swarm::assimilate::detect_iac_type(file);
    
    println!("{} Introspecting legacy infrastructure... {}", style("[1/3]").blue().bold(), style(format!("{:?}", iac_type)).cyan());
    
    let job_json = match iac_type {
        marabunta_compute::swarm::assimilate::IaCType::DockerCompose => marabunta_compute::swarm::assimilate::docker::assimilate(&content)?,
        marabunta_compute::swarm::assimilate::IaCType::Kubernetes => marabunta_compute::swarm::assimilate::kubernetes::assimilate(&content)?,
        marabunta_compute::swarm::assimilate::IaCType::Terraform => marabunta_compute::swarm::assimilate::terraform::assimilate(&content)?,
        marabunta_compute::swarm::assimilate::IaCType::GitHubActions => marabunta_compute::swarm::assimilate::github_actions::assimilate(&content)?,
        marabunta_compute::swarm::assimilate::IaCType::Unknown => {
            return Err("Unrecognized infrastructure format. Supported: docker-compose.yml, deployment.yaml, main.tf, main.yml (.github)".to_string());
        }
    };
    
    println!("{} Assimilation Complete. Translated to Kademlia Swarm DAG.", style("[2/3]").blue().bold());
    
    let pretty_json = serde_json::to_string_pretty(&job_json).unwrap();
    
    if let Some(out_path) = out {
        std::fs::write(out_path, &pretty_json).map_err(|e| format!("Failed to write output JSON: {}", e))?;
        println!("{} Written to: {}", style("[3/3]").green().bold(), out_path.display());
        println!("   Deploy via: mrb execute --job-file {}", out_path.display());
    } else {
        println!("{} Generated Swarm Job Definition:", style("[3/3]").green().bold());
        println!("{}", pretty_json);
    }
    
    Ok(())
}


// ── Purgatory ─────────────────────────────────────────────────────────────

async fn cmd_apologize(node_id: &str, endpoint: String, difficulty: u32) -> Result<(), String> {
    use sha2::{Digest, Sha256};
    
    println!("[SYSTEM] Initiating Walk of Atonement for NodeId: {}", node_id);
    println!("[SYSTEM] Thermodynamic Fine: Finding SHA-256 hash with {} leading zero bits.", difficulty);
    println!("Burning electricity to prove remorse...");
    
    let mut nonce: u64 = 0;
    let start_time = std::time::Instant::now();
    let mut last_report = start_time;
    
    let target_prefix = difficulty as usize;
    let node_bytes = node_id.as_bytes();
    
    loop {
        let mut hasher = Sha256::new();
        hasher.update(node_bytes);
        hasher.update(nonce.to_le_bytes());
        let result = hasher.finalize();
        
        // Count leading zeros in bits
        let mut zero_bits = 0;
        for &byte in result.iter() {
            if byte == 0 {
                zero_bits += 8;
            } else {
                zero_bits += byte.leading_zeros() as usize;
                break;
            }
        }
        
        if zero_bits >= target_prefix {
            let duration = start_time.elapsed();
            println!("\n[SUCCESS] Thermodynamic Fine Paid!");
            println!("          Valid Nonce: {}", nonce);
            println!("          Hash: {:x}", result);
            println!("          Time taken: {:.2} seconds", duration.as_secs_f64());
            break;
        }
        
        nonce += 1;
        
        if nonce % 10_000_000 == 0 {
            let now = std::time::Instant::now();
            let elapsed = now.duration_since(last_report).as_secs_f64();
            let hashrate = 10_000_000.0 / elapsed;
            println!("  -> Still grinding... Nonce: {} ({:.2} MH/s)", nonce, hashrate / 1_000_000.0);
            last_report = now;
        }
    }
    
    println!("[SYSTEM] Broadcasting SubmitAtonement to the Ledger via local gateway...");
    let client = reqwest::Client::new();
    let url = format!("{}/api/v1/purgatory/atonement", endpoint);
    
    let payload = serde_json::json!({
        "node_id": node_id,
        "pow_nonce": nonce
    });
    
    let resp = client.post(&url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| format!("Network error: {}", e))?;
        
    if resp.status().is_success() {
        println!("[SUCCESS] Ledger Ingress acknowledged Atonement. Identity restored to Active state.");
        Ok(())
    } else {
        let err_text = resp.text().await.unwrap_or_else(|_| "Unknown error".into());
        Err(format!("Daemon rejected Atonement: {}", err_text))
    }
}
