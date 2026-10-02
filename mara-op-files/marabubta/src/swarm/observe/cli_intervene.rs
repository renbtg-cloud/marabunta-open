// Marabunta - Licensed under the MIT License.
use clap::Subcommand;

use super::cli::{CliError, GlobalOpts, MarabuntaClient, OutputFormat, confirm_intervention, print_output};

/// `marabunta node` — node lifecycle interventions.
#[derive(Debug, Clone, Subcommand)]
pub enum NodeCommand {
    /// Drain a node (finish current work, accept no new work).
    Drain {
        /// Node ID.
        node_id: String,
        /// Drain timeout in seconds.
        #[arg(long, default_value = "300")]
        timeout: u64,
        /// Reason for draining.
        #[arg(long, default_value = "operator-initiated")]
        reason: String,
        /// Skip guard evaluation.
        #[arg(long)]
        no_guard: bool,
        /// Force past safety blocks.
        #[arg(long)]
        force: bool,
    },
    /// Cordon a node (stop accepting new work immediately).
    Cordon {
        /// Node ID.
        node_id: String,
        /// Reason for cordoning.
        #[arg(long, default_value = "operator-initiated")]
        reason: String,
        /// Skip guard evaluation.
        #[arg(long)]
        no_guard: bool,
        /// Force past safety blocks.
        #[arg(long)]
        force: bool,
    },
    /// Uncordon a node (allow work again).
    Uncordon {
        /// Node ID.
        node_id: String,
        /// Skip guard evaluation.
        #[arg(long)]
        no_guard: bool,
    },
    /// Quarantine a node (isolate from swarm).
    Quarantine {
        /// Node ID.
        node_id: String,
        /// Reason for quarantine.
        #[arg(long, default_value = "operator-initiated")]
        reason: String,
        /// Skip guard evaluation.
        #[arg(long)]
        no_guard: bool,
        /// Force past safety blocks.
        #[arg(long)]
        force: bool,
    },
    /// Unquarantine a node (rejoin swarm).
    Unquarantine {
        /// Node ID.
        node_id: String,
        /// Skip guard evaluation.
        #[arg(long)]
        no_guard: bool,
    },
}

/// `marabunta job` — job control interventions.
#[derive(Debug, Clone, Subcommand)]
pub enum JobCommand {
    /// Cancel a job.
    Cancel {
        /// Job ID.
        job_id: String,
        /// Reason for cancellation.
        #[arg(long, default_value = "operator-cancelled")]
        reason: String,
        /// Skip guard evaluation.
        #[arg(long)]
        no_guard: bool,
    },
    /// Reprioritize a job.
    Priority {
        /// Job ID.
        job_id: String,
        /// New priority value (lower = higher priority).
        #[arg()]
        priority: u32,
        /// Skip guard evaluation.
        #[arg(long)]
        no_guard: bool,
    },
}

/// `marabunta chunk` — chunk control interventions.
#[derive(Debug, Clone, Subcommand)]
pub enum ChunkCommand {
    /// Pause a chunk's execution.
    Pause {
        /// Chunk ID.
        chunk_id: String,
        /// Skip guard evaluation.
        #[arg(long)]
        no_guard: bool,
    },
    /// Resume a paused chunk.
    Resume {
        /// Chunk ID.
        chunk_id: String,
        /// Skip guard evaluation.
        #[arg(long)]
        no_guard: bool,
    },
    /// Reassign a chunk to a different node.
    Reassign {
        /// Chunk ID.
        chunk_id: String,
        /// Target node ID.
        #[arg(long)]
        to: String,
        /// Skip guard evaluation.
        #[arg(long)]
        no_guard: bool,
    },
}

/// `marabunta collective` — collective interventions.
#[derive(Debug, Clone, Subcommand)]
pub enum CollectiveCommand {
    /// Eject a node from a collective.
    Eject {
        /// Node ID to eject.
        node_id: String,
        /// Collective ID.
        #[arg(long)]
        collective: String,
        /// Reason for ejection.
        #[arg(long)]
        reason: Option<String>,
    },
}

/// `marabunta reputation` — reputation interventions.
#[derive(Debug, Clone, Subcommand)]
pub enum ReputationCommand {
    /// Adjust a node's reputation badge.
    Adjust {
        /// Node ID.
        node_id: String,
        /// Badge to adjust.
        #[arg(long)]
        badge: String,
        /// Remove the badge instead of adding.
        #[arg(long)]
        remove: bool,
        /// Reason for adjustment.
        #[arg(long)]
        reason: Option<String>,
    },
}

/// `marabunta neuro` — Neuromancer interventions.
#[derive(Debug, Clone, Subcommand)]
pub enum NeuroCommand {
    /// Trigger a Lazarus checkpoint.
    Checkpoint {
        /// Entity ID.
        entity_id: String,
    },
    /// Force a node resurrection.
    Resurrect {
        /// Node ID.
        node_id: String,
    },
    /// Toggle a honeypot.
    Honeypot {
        /// Enable the honeypot.
        #[arg(long)]
        enable: bool,
        /// Disable the honeypot.
        #[arg(long)]
        disable: bool,
        /// Entity ID.
        entity_id: String,
    },
    /// Release a node from quarantine.
    ReleaseQuarantine {
        /// Node ID.
        node_id: String,
    },
    /// Override anomaly threshold.
    Threshold {
        /// New threshold value.
        #[arg(long)]
        value: f64,
    },
}

/// `marabunta plugin` — plugin interventions.
#[derive(Debug, Clone, Subcommand)]
pub enum PluginCommand {
    /// Restart a plugin.
    Restart {
        /// Plugin name.
        plugin_name: String,
        /// Graceful restart.
        #[arg(long)]
        graceful: bool,
    },
    /// Stop a plugin.
    Stop {
        /// Plugin name.
        plugin_name: String,
        /// Graceful stop.
        #[arg(long)]
        graceful: bool,
    },
    /// Update a plugin config key.
    Config {
        /// Plugin name.
        plugin_name: String,
        /// Config key.
        #[arg(long)]
        key: String,
        /// Config value (JSON or string).
        #[arg(long)]
        value: String,
    },
}

/// `marabunta config` — configuration interventions.
#[derive(Debug, Clone, Subcommand)]
pub enum ConfigCommand {
    /// Hot-reload a config key.
    Set {
        /// Config key.
        key: String,
        /// Config value (JSON or string).
        value: String,
    },
    /// Override gossip tuning parameter.
    Gossip {
        /// Gossip config key.
        key: String,
        /// Gossip config value.
        value: String,
    },
    /// Adjust a threshold parameter.
    Threshold {
        /// Threshold key.
        key: String,
        /// Threshold value.
        value: String,
    },
}

// ---- Command Handlers ----

/// Execute a node lifecycle intervention with dry-run, confirmation, and execution.
pub async fn cmd_node(opts: &GlobalOpts, cmd: &NodeCommand) -> Result<(), CliError> {
    let client = MarabuntaClient::new(opts);

    let (action, node_id, params, skip_guard, force) = match cmd {
        NodeCommand::Drain {
            node_id,
            timeout,
            reason,
            no_guard,
            force,
        } => (
            "drain",
            node_id.as_str(),
            serde_json::json!({"timeout_secs": timeout, "reason": reason}),
            *no_guard,
            *force,
        ),
        NodeCommand::Cordon {
            node_id,
            reason,
            no_guard,
            force,
        } => (
            "cordon",
            node_id.as_str(),
            serde_json::json!({"reason": reason}),
            *no_guard,
            *force,
        ),
        NodeCommand::Uncordon { node_id, no_guard } => (
            "uncordon",
            node_id.as_str(),
            serde_json::json!({}),
            *no_guard,
            false,
        ),
        NodeCommand::Quarantine {
            node_id,
            reason,
            no_guard,
            force,
        } => (
            "quarantine",
            node_id.as_str(),
            serde_json::json!({"reason": reason}),
            *no_guard,
            *force,
        ),
        NodeCommand::Unquarantine { node_id, no_guard } => (
            "unquarantine",
            node_id.as_str(),
            serde_json::json!({}),
            *no_guard,
            false,
        ),
    };

    // 1. Dry-run first to get impact assessment
    let dry_run_body = serde_json::json!({
        "target_type": "node",
        "target_id": node_id,
        "params": params,
        "skip_guard": skip_guard,
        "force": force,
    });

    let dry_resp = client
        .post(
            &format!("/api/v1/intervene/dry-run/node_lifecycle/{}", action),
            &dry_run_body,
        )
        .await?;

    let dry_status = dry_resp.status();
    if !dry_status.is_success() {
        let body: serde_json::Value = dry_resp.json().await.unwrap_or_default();
        return Err(CliError::Api {
            status: dry_status.as_u16(),
            body,
        });
    }

    let dry_result: serde_json::Value = dry_resp
        .json()
        .await
        .map_err(|e| CliError::Local(e.to_string()))?;

    // 2. Show impact and confirm
    let impact_summary = dry_result
        .get("impact")
        .and_then(|i| i.get("summary"))
        .and_then(|s| s.as_str());

    let guard_info = if skip_guard {
        Some("guard skipped")
    } else {
        dry_result
            .get("guard_evaluation")
            .map(|_| "auto-guard will be evaluated at execution time")
    };

    if !confirm_intervention(action, node_id, impact_summary, guard_info, opts.yes) {
        return Err(CliError::Local("cancelled by user".to_string()));
    }

    // 3. Execute for real
    let exec_body = serde_json::json!({
        "target_type": "node",
        "target_id": node_id,
        "params": params,
        "skip_guard": skip_guard,
        "force": force,
    });

    let resp = client
        .post(
            &format!("/api/v1/intervene/node_lifecycle/{}", action),
            &exec_body,
        )
        .await?;

    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or_default();

    if !status.is_success() {
        return Err(CliError::Api {
            status: status.as_u16(),
            body,
        });
    }

    match opts.format {
        OutputFormat::Table => {
            let result_status = body
                .get("result")
                .and_then(|r| r.get("status"))
                .and_then(|s| s.as_str())
                .unwrap_or("unknown");
            eprintln!("Done: node {} -> {}", node_id, result_status);
            if let Some(audit_id) = body.get("audit_entry_id").and_then(|v| v.as_u64()) {
                eprintln!("Audit: entry #{}", audit_id);
            }
        }
        _ => print_output(&body, opts.format),
    }

    Ok(())
}

pub async fn cmd_job(opts: &GlobalOpts, cmd: &JobCommand) -> Result<(), CliError> {
    let client = MarabuntaClient::new(opts);

    let (action, target_id, params, skip_guard) = match cmd {
        JobCommand::Cancel {
            job_id,
            reason,
            no_guard,
        } => (
            "cancel_job",
            job_id.as_str(),
            serde_json::json!({"reason": reason}),
            *no_guard,
        ),
        JobCommand::Priority {
            job_id,
            priority,
            no_guard,
        } => (
            "reprioritize_job",
            job_id.as_str(),
            serde_json::json!({"priority": priority}),
            *no_guard,
        ),
    };

    execute_tier_command(
        &client,
        opts,
        "work_control",
        action,
        "job",
        target_id,
        params,
        skip_guard,
        false,
    )
    .await
}

pub async fn cmd_chunk(opts: &GlobalOpts, cmd: &ChunkCommand) -> Result<(), CliError> {
    let client = MarabuntaClient::new(opts);

    let (action, target_id, params, skip_guard) = match cmd {
        ChunkCommand::Pause {
            chunk_id,
            no_guard,
        } => (
            "pause_chunk",
            chunk_id.as_str(),
            serde_json::json!({}),
            *no_guard,
        ),
        ChunkCommand::Resume {
            chunk_id,
            no_guard,
        } => (
            "resume_chunk",
            chunk_id.as_str(),
            serde_json::json!({}),
            *no_guard,
        ),
        ChunkCommand::Reassign {
            chunk_id,
            to,
            no_guard,
        } => (
            "reassign_chunk",
            chunk_id.as_str(),
            serde_json::json!({"target_node": to}),
            *no_guard,
        ),
    };

    execute_tier_command(
        &client,
        opts,
        "work_control",
        action,
        "chunk",
        target_id,
        params,
        skip_guard,
        false,
    )
    .await
}

pub async fn cmd_collective(
    opts: &GlobalOpts,
    cmd: &CollectiveCommand,
) -> Result<(), CliError> {
    let client = MarabuntaClient::new(opts);
    match cmd {
        CollectiveCommand::Eject {
            node_id,
            collective,
            reason,
        } => {
            let params = serde_json::json!({
                "collective_id": collective,
                "reason": reason.as_deref().unwrap_or("operator-initiated"),
            });
            execute_tier_command(
                &client,
                opts,
                "organic",
                "eject_collective_member",
                "node",
                node_id,
                params,
                false,
                false,
            )
            .await
        }
    }
}

pub async fn cmd_reputation(
    opts: &GlobalOpts,
    cmd: &ReputationCommand,
) -> Result<(), CliError> {
    let client = MarabuntaClient::new(opts);
    match cmd {
        ReputationCommand::Adjust {
            node_id,
            badge,
            remove,
            reason,
        } => {
            let params = serde_json::json!({
                "badge": badge,
                "add": !remove,
                "reason": reason.as_deref().unwrap_or("operator adjustment"),
            });
            execute_tier_command(
                &client,
                opts,
                "organic",
                "adjust_reputation",
                "node",
                node_id,
                params,
                false,
                false,
            )
            .await
        }
    }
}

pub async fn cmd_neuro(opts: &GlobalOpts, cmd: &NeuroCommand) -> Result<(), CliError> {
    let client = MarabuntaClient::new(opts);
    match cmd {
        NeuroCommand::Checkpoint { entity_id } => {
            execute_tier_command(
                &client,
                opts,
                "neuromancer",
                "trigger_checkpoint",
                "node",
                entity_id,
                serde_json::json!({}),
                false,
                false,
            )
            .await
        }
        NeuroCommand::Resurrect { node_id } => {
            execute_tier_command(
                &client,
                opts,
                "neuromancer",
                "force_resurrection",
                "node",
                node_id,
                serde_json::json!({}),
                false,
                false,
            )
            .await
        }
        NeuroCommand::Honeypot {
            enable,
            disable,
            entity_id,
        } => {
            let on = *enable || !*disable;
            execute_tier_command(
                &client,
                opts,
                "neuromancer",
                "toggle_honeypot",
                "node",
                entity_id,
                serde_json::json!({"enable": on}),
                false,
                false,
            )
            .await
        }
        NeuroCommand::ReleaseQuarantine { node_id } => {
            execute_tier_command(
                &client,
                opts,
                "neuromancer",
                "release_quarantine",
                "node",
                node_id,
                serde_json::json!({}),
                false,
                false,
            )
            .await
        }
        NeuroCommand::Threshold { value } => {
            execute_tier_command(
                &client,
                opts,
                "neuromancer",
                "override_anomaly_threshold",
                "config",
                "spider",
                serde_json::json!({"threshold": value}),
                true,
                false,
            )
            .await
        }
    }
}

pub async fn cmd_plugin(opts: &GlobalOpts, cmd: &PluginCommand) -> Result<(), CliError> {
    let client = MarabuntaClient::new(opts);
    match cmd {
        PluginCommand::Restart {
            plugin_name,
            graceful,
        } => {
            execute_tier_command(
                &client,
                opts,
                "plugin",
                "restart_plugin",
                "plugin",
                plugin_name,
                serde_json::json!({"graceful": graceful}),
                true,
                false,
            )
            .await
        }
        PluginCommand::Stop {
            plugin_name,
            graceful,
        } => {
            execute_tier_command(
                &client,
                opts,
                "plugin",
                "stop_plugin",
                "plugin",
                plugin_name,
                serde_json::json!({"graceful": graceful}),
                true,
                false,
            )
            .await
        }
        PluginCommand::Config {
            plugin_name,
            key,
            value,
        } => {
            let val: serde_json::Value = serde_json::from_str(value)
                .unwrap_or_else(|_| serde_json::Value::String(value.clone()));
            execute_tier_command(
                &client,
                opts,
                "plugin",
                "update_plugin_config",
                "plugin",
                plugin_name,
                serde_json::json!({"key": key, "value": val}),
                true,
                false,
            )
            .await
        }
    }
}

pub async fn cmd_config(opts: &GlobalOpts, cmd: &ConfigCommand) -> Result<(), CliError> {
    let client = MarabuntaClient::new(opts);
    let (action, key, value) = match cmd {
        ConfigCommand::Set { key, value } => ("hot_reload", key.as_str(), value.as_str()),
        ConfigCommand::Gossip { key, value } => {
            ("override_gossip", key.as_str(), value.as_str())
        }
        ConfigCommand::Threshold { key, value } => {
            ("adjust_threshold", key.as_str(), value.as_str())
        }
    };

    let val: serde_json::Value = serde_json::from_str(value)
        .unwrap_or_else(|_| serde_json::Value::String(value.to_string()));

    let params = serde_json::json!({
        "changes": { key: val }
    });

    execute_tier_command(
        &client,
        opts,
        "configuration",
        action,
        "config",
        "swarm",
        params,
        true,
        false,
    )
    .await
}

// ---- Shared Execution Helper ----

/// Execute a tier intervention command through the API.
async fn execute_tier_command(
    client: &MarabuntaClient,
    opts: &GlobalOpts,
    tier: &str,
    action: &str,
    target_type: &str,
    target_id: &str,
    params: serde_json::Value,
    skip_guard: bool,
    force: bool,
) -> Result<(), CliError> {
    // Confirmation for non-trivial guarded actions
    if !opts.yes && !skip_guard && !super::cli::confirm(
        &format!(
            "Execute {} {} on {}:{}?",
            tier, action, target_type, target_id
        ),
        opts.yes,
    ) {
        return Err(CliError::Local("cancelled by user".to_string()));
    }

    let body = serde_json::json!({
        "target_type": target_type,
        "target_id": target_id,
        "params": params,
        "skip_guard": skip_guard,
        "force": force,
    });

    let resp = client
        .post(&format!("/api/v1/intervene/{}/{}", tier, action), &body)
        .await?;

    let status = resp.status();
    let result: serde_json::Value = resp.json().await.unwrap_or_default();

    if !status.is_success() {
        return Err(CliError::Api {
            status: status.as_u16(),
            body: result,
        });
    }

    match opts.format {
        OutputFormat::Table => {
            let exec_status = result
                .get("result")
                .and_then(|r| r.get("status"))
                .and_then(|s| s.as_str())
                .unwrap_or("done");
            eprintln!("{}:{} -> {}", target_type, target_id, exec_status);
        }
        _ => print_output(&result, opts.format),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Debug, Parser)]
    struct TestNodeApp {
        #[command(subcommand)]
        cmd: NodeCommand,
    }

    #[derive(Debug, Parser)]
    struct TestJobApp {
        #[command(subcommand)]
        cmd: JobCommand,
    }

    #[derive(Debug, Parser)]
    struct TestChunkApp {
        #[command(subcommand)]
        cmd: ChunkCommand,
    }

    #[derive(Debug, Parser)]
    struct TestConfigApp {
        #[command(subcommand)]
        cmd: ConfigCommand,
    }

    #[derive(Debug, Parser)]
    struct TestNeuroApp {
        #[command(subcommand)]
        cmd: NeuroCommand,
    }

    #[derive(Debug, Parser)]
    struct TestPluginApp {
        #[command(subcommand)]
        cmd: PluginCommand,
    }

    #[derive(Debug, Parser)]
    struct TestCollectiveApp {
        #[command(subcommand)]
        cmd: CollectiveCommand,
    }

    #[derive(Debug, Parser)]
    struct TestReputationApp {
        #[command(subcommand)]
        cmd: ReputationCommand,
    }

    #[test]
    fn test_node_drain_args() {
        let app = TestNodeApp::try_parse_from([
            "test", "drain", "node-a", "--timeout", "60", "--reason", "test",
        ])
        .expect("parse");
        match app.cmd {
            NodeCommand::Drain {
                node_id,
                timeout,
                reason,
                no_guard,
                force,
            } => {
                assert_eq!(node_id, "node-a");
                assert_eq!(timeout, 60);
                assert_eq!(reason, "test");
                assert!(!no_guard);
                assert!(!force);
            }
            _ => panic!("expected Drain"),
        }
    }

    #[test]
    fn test_node_quarantine_force() {
        let app =
            TestNodeApp::try_parse_from(["test", "quarantine", "node-a", "--force"])
                .expect("parse");
        match app.cmd {
            NodeCommand::Quarantine { node_id, force, .. } => {
                assert_eq!(node_id, "node-a");
                assert!(force);
            }
            _ => panic!("expected Quarantine"),
        }
    }

    #[test]
    fn test_node_uncordon_defaults() {
        let app =
            TestNodeApp::try_parse_from(["test", "uncordon", "node-x"]).expect("parse");
        match app.cmd {
            NodeCommand::Uncordon { node_id, no_guard } => {
                assert_eq!(node_id, "node-x");
                assert!(!no_guard);
            }
            _ => panic!("expected Uncordon"),
        }
    }

    #[test]
    fn test_job_cancel_args() {
        let app = TestJobApp::try_parse_from([
            "test", "cancel", "job-1", "--reason", "test",
        ])
        .expect("parse");
        match app.cmd {
            JobCommand::Cancel {
                job_id,
                reason,
                no_guard,
            } => {
                assert_eq!(job_id, "job-1");
                assert_eq!(reason, "test");
                assert!(!no_guard);
            }
            _ => panic!("expected Cancel"),
        }
    }

    #[test]
    fn test_chunk_reassign_args() {
        let app = TestChunkApp::try_parse_from([
            "test", "reassign", "chunk-1", "--to", "node-b",
        ])
        .expect("parse");
        match app.cmd {
            ChunkCommand::Reassign {
                chunk_id,
                to,
                no_guard,
            } => {
                assert_eq!(chunk_id, "chunk-1");
                assert_eq!(to, "node-b");
                assert!(!no_guard);
            }
            _ => panic!("expected Reassign"),
        }
    }

    #[test]
    fn test_config_set_args() {
        let app = TestConfigApp::try_parse_from(["test", "set", "some.key", "some-value"])
            .expect("parse");
        match app.cmd {
            ConfigCommand::Set { key, value } => {
                assert_eq!(key, "some.key");
                assert_eq!(value, "some-value");
            }
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn test_neuro_threshold_args() {
        let app = TestNeuroApp::try_parse_from(["test", "threshold", "--value", "0.75"])
            .expect("parse");
        match app.cmd {
            NeuroCommand::Threshold { value } => {
                assert!((value - 0.75).abs() < f64::EPSILON);
            }
            _ => panic!("expected Threshold"),
        }
    }

    #[test]
    fn test_plugin_restart_args() {
        let app =
            TestPluginApp::try_parse_from(["test", "restart", "redis", "--graceful"])
                .expect("parse");
        match app.cmd {
            PluginCommand::Restart {
                plugin_name,
                graceful,
            } => {
                assert_eq!(plugin_name, "redis");
                assert!(graceful);
            }
            _ => panic!("expected Restart"),
        }
    }

    #[test]
    fn test_collective_eject_args() {
        let app = TestCollectiveApp::try_parse_from([
            "test",
            "eject",
            "node-a",
            "--collective",
            "group-1",
            "--reason",
            "misbehaving",
        ])
        .expect("parse");
        match app.cmd {
            CollectiveCommand::Eject {
                node_id,
                collective,
                reason,
            } => {
                assert_eq!(node_id, "node-a");
                assert_eq!(collective, "group-1");
                assert_eq!(reason, Some("misbehaving".to_string()));
            }
        }
    }

    #[test]
    fn test_reputation_adjust_remove() {
        let app = TestReputationApp::try_parse_from([
            "test",
            "adjust",
            "node-a",
            "--badge",
            "reliable",
            "--remove",
        ])
        .expect("parse");
        match app.cmd {
            ReputationCommand::Adjust {
                node_id,
                badge,
                remove,
                reason,
            } => {
                assert_eq!(node_id, "node-a");
                assert_eq!(badge, "reliable");
                assert!(remove);
                assert!(reason.is_none());
            }
        }
    }
}
