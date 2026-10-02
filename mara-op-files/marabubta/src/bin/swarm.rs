// Marabunta - Licensed under the MIT License.
//! Marabunta Swarm Node — Marabunta Radiation Architecture
//!
//! A single binary that can be any role in the swarm.
//! No fixed coordinator, master, or worker — just a node with dynamic traits.

use std::time::Duration;

use clap::Parser;
use marabunta_compute::swarm::config::{
    SwarmConfig, COMPLIANCE_PROFILE_DESCRIPTION, COMPLIANCE_PROFILE_NAME,
    COMPLIANCE_PROFILE_VERSION, compliance_overrides,
};
use marabunta_compute::swarm::config_live::LiveConfig;
use marabunta_compute::swarm::config_meta::ConfigTier;
use marabunta_compute::swarm::SwarmNode;

#[derive(Parser)]
#[command(name = "marabunta-swarm")]
#[command(about = "Run a Marabunta swarm node (Marabunta Radiation architecture)")]
struct Args {
    /// Listen address (e.g., "0.0.0.0:4200")
    #[arg(short, long, default_value = "0.0.0.0:4200")]
    listen: String,

    /// Bootstrap/seed server addresses (comma-separated)
    #[arg(short, long, value_delimiter = ',')]
    bootstrap: Vec<String>,

    /// Path to config file (TOML)
    #[arg(short, long)]
    config: Option<String>,

    /// Path to identity file (persists NodeId across restarts)
    #[arg(short, long)]
    identity: Option<String>,

    /// Force-enable these traits (comma-separated: can_execute,can_forward,...)
    #[arg(long, value_delimiter = ',')]
    force_traits: Vec<String>,

    /// Force-disable these traits (comma-separated)
    #[arg(long, value_delimiter = ',')]
    deny_traits: Vec<String>,

    /// Maximum concurrent chunk executions
    #[arg(long, default_value = "4")]
    max_concurrent: usize,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Node type (bare-metal, cloud-vm, lambda, container, android, browser, desktop)
    #[arg(long, default_value = "desktop")]
    node_type: String,

    /// Maximum collectives this node can join
    #[arg(long, default_value = "3")]
    max_collectives: usize,

    /// Marketplace bid window in seconds
    #[arg(long, default_value = "5")]
    bid_window: u64,

    /// Minimum capability match score to bid (0.0-1.0)
    #[arg(long, default_value = "0.5")]
    min_capability_match: f32,

    /// Specializations this node offers (comma-separated)
    #[arg(long, default_value = "")]
    specializations: String,

    /// Work types this node prefers (comma-separated)
    #[arg(long, default_value = "")]
    preferred_work: String,

    /// Work types this node avoids (comma-separated)
    #[arg(long, default_value = "")]
    avoided_work: String,

    /// HTTP API port (0 = disabled)
    #[arg(long, default_value = "8080")]
    api_port: u16,

    /// Resource strategy preset to use as default
    #[arg(long, default_value = "balanced")]
    strategy: String,

    /// Path to energy price CSV file to load on startup
    #[arg(long)]
    energy_csv: Option<String>,

    /// Sandbox mode (auto, bwrap, landlock, none)
    #[arg(long, default_value = "auto")]
    sandbox_mode: String,

    /// Blob store directory
    #[arg(long)]
    blob_dir: Option<String>,

    /// Maximum blob store size in MB
    #[arg(long, default_value = "10240")]
    blob_max_mb: u64,

    /// Geo region for energy price matching (e.g., "us-east", "eu-west")
    #[arg(long)]
    geo_region: Option<String>,

    /// Enable software discovery on startup
    #[arg(long, default_value = "true")]
    discover_software: bool,

    /// Custom capabilities to advertise (comma-separated)
    #[arg(long, value_delimiter = ',')]
    capabilities: Vec<String>,

    /// (Windows only) Run as a Windows Service (called by SCM).
    /// Use marabunta-swarm-service binary instead for production service deployment.
    #[arg(long)]
    service: bool,

    /// (Windows only) Install the swarm node as a Windows Service.
    /// Registers MarabuntaSwarm with the Service Control Manager.
    #[arg(long)]
    install_service: bool,

    /// (Windows only) Uninstall the swarm node Windows Service.
    /// Removes MarabuntaSwarm from the Service Control Manager.
    #[arg(long)]
    uninstall_service: bool,

    /// PostgreSQL connection URL for config persistence (e.g., "postgres://localhost:5433/marabunta")
    #[arg(long)]
    config_db_url: Option<String>,

    /// Compliance profile name (informational — actual enforcement is at build time)
    #[arg(long)]
    compliance_profile: Option<String>,

    /// Accept the swarm configuration manifest automatically (headless/automated deployments)
    #[arg(long)]
    accept_manifest: bool,

    /// Enable auto-update: periodically check for and install new versions
    #[arg(long)]
    auto_update: bool,

    /// URL of the update server to check for new versions
    #[arg(long)]
    update_url: Option<String>,

    /// Ed25519 public key (base64-encoded) for verifying release signatures
    #[arg(long)]
    update_signing_key: Option<String>,

    /// One-shot: check for an available update and exit (useful for cron jobs)
    #[arg(long)]
    check_update: bool,
}

/// Windows Service management helpers (only compiled on Windows).
#[cfg(windows)]
mod windows_service_mgmt {
    use std::process::Command;

    const SERVICE_NAME: &str = "MarabuntaSwarm";
    const SERVICE_DISPLAY_NAME: &str = "Marabunta Compute Swarm Node";
    const SERVICE_DESCRIPTION: &str = "Marabunta Compute distributed swarm node";

    /// Install the swarm node as a Windows Service via `sc.exe`.
    /// This registers the dedicated `marabunta-swarm-service.exe` binary.
    pub fn install_service() -> Result<(), Box<dyn std::error::Error>> {
        // Look for marabunta-swarm-service.exe next to the current executable.
        let exe_path = std::env::current_exe()?;
        let exe_dir = exe_path.parent().ok_or("cannot determine executable directory")?;
        let service_binary = exe_dir.join("marabunta-swarm-service.exe");

        if !service_binary.exists() {
            return Err(format!(
                "Service binary not found at '{}'. Build with:\n  \
                 cargo build --release --features windows-service --bin marabunta-swarm-service",
                service_binary.display()
            ).into());
        }

        let binpath = format!("\"{}\"", service_binary.display());

        println!("Installing Windows Service '{}'...", SERVICE_NAME);
        println!("  Binary: {}", service_binary.display());

        // Create the service.
        let status = Command::new("sc.exe")
            .args(["create", SERVICE_NAME])
            .arg(format!("binpath= {}", binpath))
            .arg(format!("DisplayName= {}", SERVICE_DISPLAY_NAME))
            .args(["start=", "auto"])
            .status()?;

        if !status.success() {
            return Err(format!(
                "sc.exe create failed (exit code {:?}). Are you running as Administrator?",
                status.code()
            ).into());
        }

        // Set the description.
        let _ = Command::new("sc.exe")
            .args(["description", SERVICE_NAME, SERVICE_DESCRIPTION])
            .status();

        // Set recovery options: restart on failure.
        let _ = Command::new("sc.exe")
            .args([
                "failure", SERVICE_NAME,
                "reset=", "86400",
                "actions=", "restart/60000/restart/300000/restart/600000",
            ])
            .status();

        println!("Service '{}' installed successfully.", SERVICE_NAME);
        println!();
        println!("Next steps:");
        println!("  1. Create a config file at %ProgramData%\\MarabuntaCompute\\config\\swarm.toml");
        println!("  2. Start the service: sc.exe start {}", SERVICE_NAME);
        println!("  Or use the PowerShell installer: scripts\\install-windows-service.ps1");
        Ok(())
    }

    /// Uninstall the swarm node Windows Service via `sc.exe`.
    pub fn uninstall_service() -> Result<(), Box<dyn std::error::Error>> {
        println!("Stopping service '{}'...", SERVICE_NAME);
        let _ = Command::new("sc.exe")
            .args(["stop", SERVICE_NAME])
            .status();

        // Brief delay for the service to stop.
        std::thread::sleep(std::time::Duration::from_secs(2));

        println!("Deleting service '{}'...", SERVICE_NAME);
        let status = Command::new("sc.exe")
            .args(["delete", SERVICE_NAME])
            .status()?;

        if !status.success() {
            return Err(format!(
                "sc.exe delete failed (exit code {:?}). Are you running as Administrator?",
                status.code()
            ).into());
        }

        println!("Service '{}' uninstalled successfully.", SERVICE_NAME);
        Ok(())
    }

    /// Redirect to the dedicated service binary for SCM-initiated runs.
    pub fn run_as_service() -> Result<(), Box<dyn std::error::Error>> {
        eprintln!("Error: --service flag is not supported directly in this binary.");
        eprintln!();
        eprintln!("The Windows Service entry point is in a separate binary:");
        eprintln!("  marabunta-swarm-service.exe");
        eprintln!();
        eprintln!("To install the service, use one of:");
        eprintln!("  marabunta-swarm --install-service");
        eprintln!("  scripts\\install-windows-service.ps1");
        std::process::exit(1);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Handle Windows Service management flags (Windows only).
    #[cfg(windows)]
    {
        if args.install_service {
            return windows_service_mgmt::install_service();
        }
        if args.uninstall_service {
            return windows_service_mgmt::uninstall_service();
        }
        if args.service {
            return windows_service_mgmt::run_as_service();
        }
    }

    #[cfg(not(windows))]
    {
        if args.service || args.install_service || args.uninstall_service {
            eprintln!("Error: --service, --install-service, and --uninstall-service flags are Windows-only.");
            std::process::exit(1);
        }
    }

    // Load config from file if provided, otherwise start with defaults.
    let mut config = if let Some(ref path) = args.config {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            format!("failed to read config file '{}': {}", path, e)
        })?;
        toml::from_str::<SwarmConfig>(&contents).map_err(|e| {
            format!("failed to parse config file '{}': {}", path, e)
        })?
    } else {
        SwarmConfig::default()
    };

    // Overlay CLI args onto the file config. CLI values take precedence.
    config.listen_addr = args.listen.clone();
    config.max_concurrent_chunks = args.max_concurrent;

    if !args.bootstrap.is_empty() {
        config.bootstrap_servers = args.bootstrap.clone();
    }

    if let Some(ref identity) = args.identity {
        config.identity_file = Some(identity.clone());
    }

    if !args.force_traits.is_empty() {
        config.force_traits = args.force_traits.clone();
    }

    if !args.deny_traits.is_empty() {
        config.deny_traits = args.deny_traits.clone();
    }

    // Overlay organic swarm CLI args onto the config.
    config.node_type = Some(args.node_type.clone());
    config.max_collectives = args.max_collectives;
    config.bid_window = Duration::from_secs(args.bid_window);
    config.min_capability_match = args.min_capability_match;

    // Overlay auto-update CLI args onto the config.
    if args.auto_update {
        config.auto_update = true;
    }
    if let Some(ref url) = args.update_url {
        config.update_url = Some(url.clone());
    }
    if let Some(ref key) = args.update_signing_key {
        config.update_signing_key = Some(key.clone());
    }

    // Initialize tracing subscriber with env-filter.
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| {
            tracing_subscriber::EnvFilter::new(&args.log_level)
        });

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    // ================================================================
    // Build the three-tier config system: LiveConfig (ArcSwap-backed).
    // ================================================================
    let overrides = compliance_overrides();
    let live_config = LiveConfig::new(config.clone(), overrides);

    // Count settings by tier for the banner.
    let registry = live_config.registry();
    let all_settings = registry.schema();
    let hardwired_count = all_settings
        .iter()
        .filter(|s| s.tier == ConfigTier::Hardwired)
        .count();
    let runtime_count = all_settings
        .iter()
        .filter(|s| s.tier == ConfigTier::Runtime)
        .count();
    let total_settings = all_settings.len();

    // Print compliance banner.
    let profile_name = COMPLIANCE_PROFILE_NAME.unwrap_or("None");
    let profile_version = COMPLIANCE_PROFILE_VERSION.unwrap_or("-");
    let _profile_desc = COMPLIANCE_PROFILE_DESCRIPTION.unwrap_or("");
    let pg_status = if config.enable_postgres {
        format!("auto (port {})", config.postgres.as_ref().map_or(5433, |p| p.pg_port))
    } else {
        "disabled".to_string()
    };

    eprintln!("\u{2554}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2557}");
    eprintln!("\u{2551}  Compliance Profile: {:>10} {:<20}\u{2551}", profile_name, profile_version);
    eprintln!("\u{2551}  Hardwired settings: {:>4}  (immutable at runtime)    \u{2551}", hardwired_count);
    eprintln!("\u{2551}  Runtime-mutable:    {:>4}  Total registered: {:>4}  \u{2551}", runtime_count, total_settings);
    eprintln!("\u{2551}  PostgreSQL: {:<40}\u{2551}", pg_status);
    if let Some(ref db_url) = args.config_db_url {
        eprintln!("\u{2551}  Config DB: {:<41}\u{2551}", db_url);
    }
    if args.accept_manifest {
        eprintln!("\u{2551}  Manifest: auto-accepted (headless)              \u{2551}");
    }
    eprintln!("\u{255a}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{2550}\u{255d}");

    // Handle --check-update one-shot mode: check for updates and exit.
    if args.check_update {
        let update_config = marabunta_compute::swarm::updater::UpdateConfig {
            enabled: true,
            update_url: config.update_url.clone(),
            check_interval: config.update_check_interval,
            signing_public_key: config.update_signing_key.clone(),
        };
        let checker = marabunta_compute::swarm::updater::UpdateChecker::new(&update_config);
        if let Err(e) = checker.check_and_report().await {
            tracing::error!(error = %e, "update check failed");
            std::process::exit(1);
        }
        return Ok(());
    }

    // Save config values for the banner before moving config into the node.
    let listen_addr = config.listen_addr.clone();
    let bootstrap_servers = config.bootstrap_servers.clone();
    let max_concurrent = config.max_concurrent_chunks;
    let force_traits = config.force_traits.clone();
    let deny_traits = config.deny_traits.clone();
    let node_type = config.node_type.clone().unwrap_or_default();
    let max_collectives = config.max_collectives;
    let bid_window = config.bid_window;
    let min_cap_match = config.min_capability_match;
    let specializations = args.specializations.clone();
    let preferred_work = args.preferred_work.clone();
    let avoided_work = args.avoided_work.clone();

    // Create the SwarmNode from the merged configuration.
    let node = SwarmNode::new(config)?;

    // Load energy price CSV if provided.
    if let Some(ref csv_path) = args.energy_csv {
        match std::fs::read_to_string(csv_path) {
            Ok(csv_data) => {
                match marabunta_compute::swarm::energy::parse_price_csv(&csv_data, csv_path) {
                    Ok(schedule) => {
                        node.energy_estimator().add_schedule(schedule);
                        tracing::info!("loaded energy price schedule from {}", csv_path);
                    }
                    Err(e) => {
                        tracing::warn!("failed to parse energy CSV '{}': {}", csv_path, e);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("failed to read energy CSV '{}': {}", csv_path, e);
            }
        }
    }

    // Startup banner.
    tracing::info!(
        node_id = %node.id(),
        listen = %listen_addr,
        bootstrap_servers = ?bootstrap_servers,
        max_concurrent = max_concurrent,
        force_traits = ?force_traits,
        deny_traits = ?deny_traits,
        "Marabunta Swarm node starting",
    );
    tracing::info!("--------------------------------------------------");
    tracing::info!("  Node ID : {}", node.id());
    tracing::info!("  Listen  : {}", listen_addr);
    if bootstrap_servers.is_empty() {
        tracing::info!("  Seeds   : (none — standalone mode)");
    } else {
        for (i, seed) in bootstrap_servers.iter().enumerate() {
            tracing::info!("  Seed[{}] : {}", i, seed);
        }
    }
    tracing::info!("  Node Type       : {}", node_type);
    tracing::info!("  Max Collectives : {}", max_collectives);
    tracing::info!("  Bid Window      : {:?}", bid_window);
    tracing::info!("  Min Cap Match   : {}", min_cap_match);
    if !specializations.is_empty() {
        tracing::info!("  Specializations : {}", specializations);
    }
    if !preferred_work.is_empty() {
        tracing::info!("  Preferred Work  : {}", preferred_work);
    }
    if !avoided_work.is_empty() {
        tracing::info!("  Avoided Work    : {}", avoided_work);
    }
    tracing::info!("  API Port        : {}", args.api_port);
    tracing::info!("  Strategy        : {}", args.strategy);
    tracing::info!("  Sandbox Mode    : {}", args.sandbox_mode);
    if let Some(ref csv) = args.energy_csv {
        tracing::info!("  Energy CSV      : {}", csv);
    }
    if let Some(ref region) = args.geo_region {
        tracing::info!("  Geo Region      : {}", region);
    }
    if let Some(ref dir) = args.blob_dir {
        tracing::info!("  Blob Store      : {}", dir);
    }
    if !args.capabilities.is_empty() {
        tracing::info!("  Capabilities    : {:?}", args.capabilities);
    }
    if args.auto_update {
        tracing::info!("  Auto-Update     : enabled");
        if let Some(ref url) = args.update_url {
            tracing::info!("  Update URL      : {}", url);
        }
    }
    tracing::info!("--------------------------------------------------");

    // TODO: Once SwarmNodeStatus includes organic swarm fields, display them:
    //   println!("  Profiles known: {}", status.profiles_known);
    //   println!("  Active collectives: {}", status.collectives_active);
    //   println!("  Marketplace postings: {}", status.marketplace_postings);
    //   println!("  Reputation records: {}", status.reputation_records);
    //   println!("  Policy version: {}", status.policy_version);

    // Set up Ctrl+C handler for graceful shutdown.
    // node.run() takes &self and blocks until shutdown is signalled.
    // We use select! to race it against Ctrl+C, then call node.shutdown().
    let shutdown_signal = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        tracing::info!("Ctrl+C received, initiating graceful shutdown...");
    };

    tokio::select! {
        result = node.run() => {
            match result {
                Ok(()) => {
                    tracing::info!("Swarm node exited normally");
                }
                Err(e) => {
                    tracing::error!(error = %e, "Swarm node exited with error");
                    return Err(Box::new(e) as Box<dyn std::error::Error>);
                }
            }
        }
        _ = shutdown_signal => {
            node.shutdown();
            // Give it a moment for the shutdown signal to propagate.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    tracing::info!(
        node_id = %node.id(),
        "Marabunta Swarm node stopped. Goodbye.",
    );

    Ok(())
}
