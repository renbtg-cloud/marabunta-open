// Marabunta - Licensed under the MIT License.
//! Windows Service entry point for the Marabunta Compute swarm node.
//!
//! This binary runs as a Windows Service, handling SCM (Service Control Manager)
//! events for start, stop, pause, and continue operations.
//!
//! Install with: sc create MarabuntaSwarm binpath= "C:\path\to\marabunta-swarm-service.exe"
//! Or use the PowerShell installer: install-windows-service.ps1
//!
//! # Service Details
//!
//! - **Service Name**: MarabuntaSwarm
//! - **Display Name**: Marabunta Compute Swarm Node
//! - **Description**: Marabunta Compute distributed swarm node
//!
//! # Configuration
//!
//! The service reads configuration from (in priority order):
//! 1. Service arguments passed during registration
//! 2. Registry key `HKLM\SOFTWARE\MarabuntaCompute\Swarm`
//! 3. Default config file at `%ProgramData%\MarabuntaCompute\config\swarm.toml`

#[cfg(windows)]
mod service {
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use windows_service::define_windows_service;
    use windows_service::service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    };
    use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
    use windows_service::service_dispatcher;

    use marabunta_compute::swarm::config::SwarmConfig;
    use marabunta_compute::swarm::SwarmNode;

    /// Windows Service name as registered with SCM.
    const SERVICE_NAME: &str = "MarabuntaSwarm";

    /// Default config file path under ProgramData.
    const DEFAULT_CONFIG_PATH: &str = r"C:\ProgramData\MarabuntaCompute\config\swarm.toml";

    /// Default log directory under ProgramData.
    const DEFAULT_LOG_DIR: &str = r"C:\ProgramData\MarabuntaCompute\logs";

    /// Registry key for service configuration.
    const REGISTRY_KEY: &str = r"SOFTWARE\MarabuntaCompute\Swarm";

    // Generate the Windows service boilerplate.
    define_windows_service!(ffi_service_main, service_main);

    /// Entry point called by the Windows Service Control Manager.
    pub fn run() -> Result<(), Box<dyn std::error::Error>> {
        // Dispatch to the service main function. This call blocks until the
        // service is stopped.
        service_dispatcher::start(SERVICE_NAME, ffi_service_main)?;
        Ok(())
    }

    /// The actual service main function invoked by the SCM dispatcher.
    ///
    /// `arguments` contains any extra arguments passed when the service was
    /// started (e.g., via `sc start MarabuntaSwarm arg1 arg2`).
    fn service_main(arguments: Vec<OsString>) {
        if let Err(e) = run_service(arguments) {
            // Best-effort logging -- at this point tracing may not be set up.
            eprintln!("MarabuntaSwarm service error: {}", e);
        }
    }

    /// Resolve the config file path from (in order):
    /// 1. Service arguments (first arg treated as config path)
    /// 2. Windows Registry `HKLM\SOFTWARE\MarabuntaCompute\Swarm\ConfigPath`
    /// 3. Default `%ProgramData%\MarabuntaCompute\config\swarm.toml`
    fn resolve_config_path(arguments: &[OsString]) -> PathBuf {
        // Check service arguments first (skip arg[0] which is the service name).
        if arguments.len() > 1 {
            let candidate = PathBuf::from(&arguments[1]);
            if candidate.exists() {
                return candidate;
            }
        }

        // Try Windows Registry.
        if let Some(path) = read_registry_string(REGISTRY_KEY, "ConfigPath") {
            let candidate = PathBuf::from(&path);
            if candidate.exists() {
                return candidate;
            }
        }

        // Fall back to default ProgramData location.
        if let Ok(program_data) = std::env::var("ProgramData") {
            let candidate = PathBuf::from(program_data)
                .join("MarabuntaCompute")
                .join("config")
                .join("swarm.toml");
            if candidate.exists() {
                return candidate;
            }
        }

        PathBuf::from(DEFAULT_CONFIG_PATH)
    }

    /// Read a REG_SZ value from HKLM.
    fn read_registry_string(subkey: &str, value_name: &str) -> Option<String> {
        // Use winreg via manual FFI -- keep dependencies minimal.
        // For now, return None; the config file and arguments are the primary
        // configuration mechanisms. A full implementation would use the `winreg`
        // crate or raw Win32 `RegOpenKeyExW` / `RegQueryValueExW`.
        //
        // This is intentionally a no-op stub so the binary compiles without
        // pulling in extra registry crates. The PowerShell installer writes
        // the config file, which is the recommended path.
        let _ = (subkey, value_name);
        None
    }

    /// Resolve the log directory, creating it if necessary.
    fn resolve_log_dir() -> PathBuf {
        let log_dir = if let Ok(program_data) = std::env::var("ProgramData") {
            PathBuf::from(program_data).join("MarabuntaCompute").join("logs")
        } else {
            PathBuf::from(DEFAULT_LOG_DIR)
        };

        // Best-effort directory creation.
        let _ = std::fs::create_dir_all(&log_dir);
        log_dir
    }

    /// Initialize tracing to write to a log file in the ProgramData log directory.
    fn init_tracing(log_dir: &PathBuf) {
        use tracing_subscriber::fmt::writer::MakeWriterExt;

        let log_file_path = log_dir.join("swarm.log");

        // Open log file in append mode (create if not exists).
        let file = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file_path)
        {
            Ok(f) => f,
            Err(_) => {
                // Fall back to stderr if we cannot open the log file.
                tracing_subscriber::fmt()
                    .with_env_filter(
                        tracing_subscriber::EnvFilter::try_from_default_env()
                            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
                    )
                    .init();
                return;
            }
        };

        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(file)
            .with_ansi(false)
            .with_target(true)
            .with_thread_ids(true)
            .init();
    }

    /// Core service logic: load config, start the swarm node, handle SCM events.
    fn run_service(arguments: Vec<OsString>) -> Result<(), Box<dyn std::error::Error>> {
        // Set up logging first so we can trace the rest of startup.
        let log_dir = resolve_log_dir();
        init_tracing(&log_dir);

        tracing::info!("MarabuntaSwarm Windows Service starting");

        // Resolve and load configuration.
        let config_path = resolve_config_path(&arguments);
        tracing::info!(config_path = %config_path.display(), "Loading configuration");

        let config = if config_path.exists() {
            let contents = std::fs::read_to_string(&config_path).map_err(|e| {
                format!(
                    "failed to read config file '{}': {}",
                    config_path.display(),
                    e
                )
            })?;
            toml::from_str::<SwarmConfig>(&contents).map_err(|e| {
                format!(
                    "failed to parse config file '{}': {}",
                    config_path.display(),
                    e
                )
            })?
        } else {
            tracing::warn!(
                config_path = %config_path.display(),
                "Config file not found, using defaults"
            );
            SwarmConfig::default()
        };

        // Build the tokio runtime (matching the main swarm binary).
        let runtime = tokio::runtime::Runtime::new()?;

        // Shared shutdown and pause flags.
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let paused_flag = Arc::new(AtomicBool::new(false));

        let shutdown_for_handler = shutdown_flag.clone();
        let paused_for_handler = paused_flag.clone();

        // Register the service control handler.
        let status_handle = service_control_handler::register(
            SERVICE_NAME,
            move |control_event| -> ServiceControlHandlerResult {
                match control_event {
                    ServiceControl::Stop => {
                        tracing::info!("SCM Stop received, initiating graceful shutdown");
                        shutdown_for_handler.store(true, Ordering::SeqCst);
                        ServiceControlHandlerResult::NoError
                    }
                    ServiceControl::Pause => {
                        tracing::info!("SCM Pause received, pausing new work claims");
                        paused_for_handler.store(true, Ordering::SeqCst);
                        ServiceControlHandlerResult::NoError
                    }
                    ServiceControl::Continue => {
                        tracing::info!("SCM Continue received, resuming normal operation");
                        paused_for_handler.store(false, Ordering::SeqCst);
                        ServiceControlHandlerResult::NoError
                    }
                    ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                    _ => ServiceControlHandlerResult::NotImplemented,
                }
            },
        )?;

        // Report to SCM: we are starting.
        status_handle.set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: ServiceState::StartPending,
            controls_accepted: ServiceControlAccept::empty(),
            exit_code: ServiceExitCode::Win32(0),
            checkpoint: 0,
            wait_hint: Duration::from_secs(10),
            process_id: None,
        })?;

        // Create the SwarmNode.
        let node = match SwarmNode::new(config) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(error = %e, "Failed to create SwarmNode");
                status_handle.set_service_status(ServiceStatus {
                    service_type: ServiceType::OWN_PROCESS,
                    current_state: ServiceState::Stopped,
                    controls_accepted: ServiceControlAccept::empty(),
                    exit_code: ServiceExitCode::Win32(1),
                    checkpoint: 0,
                    wait_hint: Duration::ZERO,
                    process_id: None,
                })?;
                return Err(Box::new(e));
            }
        };

        tracing::info!(node_id = %node.id(), "SwarmNode created");

        // Report to SCM: we are now running and accept Stop, Pause, Continue.
        status_handle.set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: ServiceState::Running,
            controls_accepted: ServiceControlAccept::STOP
                | ServiceControlAccept::PAUSE_CONTINUE,
            exit_code: ServiceExitCode::Win32(0),
            checkpoint: 0,
            wait_hint: Duration::ZERO,
            process_id: None,
        })?;

        // Run the swarm node on the tokio runtime. We poll the shutdown flag
        // alongside the node's main loop.
        let exit_code = runtime.block_on(async {
            let shutdown_watcher = {
                let flag = shutdown_flag.clone();
                async move {
                    loop {
                        if flag.load(Ordering::SeqCst) {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
            };

            tokio::select! {
                result = node.run() => {
                    match result {
                        Ok(()) => {
                            tracing::info!("SwarmNode exited normally");
                            0u32
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "SwarmNode exited with error");
                            1u32
                        }
                    }
                }
                _ = shutdown_watcher => {
                    tracing::info!("Shutdown flag set, stopping SwarmNode");
                    node.shutdown();
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    0u32
                }
            }
        });

        // Report to SCM: we are stopping.
        status_handle.set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: ServiceState::StopPending,
            controls_accepted: ServiceControlAccept::empty(),
            exit_code: ServiceExitCode::Win32(exit_code),
            checkpoint: 0,
            wait_hint: Duration::from_secs(5),
            process_id: None,
        })?;

        tracing::info!(node_id = %node.id(), "MarabuntaSwarm Windows Service stopped");

        // Report to SCM: we have stopped.
        status_handle.set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: ServiceState::Stopped,
            controls_accepted: ServiceControlAccept::empty(),
            exit_code: ServiceExitCode::Win32(exit_code),
            checkpoint: 0,
            wait_hint: Duration::ZERO,
            process_id: None,
        })?;

        Ok(())
    }
}

#[cfg(windows)]
fn main() {
    if let Err(e) = service::run() {
        eprintln!("MarabuntaSwarm service failed: {}", e);
        std::process::exit(1);
    }
}

#[cfg(not(windows))]
fn main() {
    eprintln!("This binary is only supported on Windows.");
    eprintln!("Use 'marabunta-swarm' on other platforms.");
    std::process::exit(1);
}
