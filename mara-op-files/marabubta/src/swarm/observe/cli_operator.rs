// Marabunta - Licensed under the MIT License.
use clap::Subcommand;

use super::cli::{CliConfig, CliError, GlobalOpts, MarabuntaClient, OutputFormat, print_output, print_table};

/// `marabunta operator` — operator identity and management.
#[derive(Debug, Clone, Subcommand)]
pub enum OperatorCommand {
    /// Show current operator identity.
    Whoami,
    /// Query intervention audit trail.
    Audit {
        /// Filter by entity (e.g., "node:abc123").
        #[arg(long)]
        entity: Option<String>,
        /// Show interventions since (e.g., "1h", "30m", "2024-01-01T00:00:00Z").
        #[arg(long)]
        since: Option<String>,
        /// Max entries to show.
        #[arg(long, default_value = "50")]
        limit: usize,
    },
    /// Incident session management.
    #[command(subcommand)]
    Session(SessionSubcommand),
}

/// Session management subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum SessionSubcommand {
    /// Start a new incident session.
    Start {
        /// Session title.
        title: String,
        /// Optional description.
        #[arg(long)]
        description: Option<String>,
    },
    /// End an active session.
    End {
        /// Session ID.
        session_id: String,
    },
    /// Show session details.
    Show {
        /// Session ID.
        session_id: String,
    },
    /// List all sessions.
    List {
        /// Show only active sessions.
        #[arg(long)]
        active: bool,
    },
}

// ---- Command Handlers ----

pub async fn cmd_operator(
    opts: &GlobalOpts,
    cmd: &OperatorCommand,
) -> Result<(), CliError> {
    let client = MarabuntaClient::new(opts);

    match cmd {
        OperatorCommand::Whoami => {
            let resp = client.get("/api/v1/operator/oai/whoami").await?;
            let status = resp.status();
            if !status.is_success() {
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                return Err(CliError::Api {
                    status: status.as_u16(),
                    body,
                });
            }
            let op: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| CliError::Local(e.to_string()))?;

            match opts.format {
                OutputFormat::Table => {
                    let name = op.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                    let id = op.get("id").and_then(|v| v.as_str()).unwrap_or("?");
                    let tier = op
                        .get("safety_tier")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    let scope = op.get("scope").unwrap_or(&serde_json::Value::Null);
                    eprintln!("Operator: {} ({})", name, id);
                    eprintln!("Safety:   Tier {} ({})", tier, safety_tier_label(tier));
                    eprintln!("Scope:    {}", scope);
                }
                _ => print_output(&op, opts.format),
            }
            Ok(())
        }

        OperatorCommand::Audit {
            entity,
            since,
            limit,
        } => {
            let mut query = vec![format!("limit={}", limit)];
            if let Some(ref e) = entity {
                query.push(format!("entity={}", e));
            }
            if let Some(ref s) = since {
                query.push(format!("since={}", s));
            }

            let resp = client
                .get(&format!(
                    "/api/v1/operator/oai/audit?{}",
                    query.join("&")
                ))
                .await?;
            let status = resp.status();
            if !status.is_success() {
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                return Err(CliError::Api {
                    status: status.as_u16(),
                    body,
                });
            }

            let entries: Vec<serde_json::Value> = resp
                .json()
                .await
                .map_err(|e| CliError::Local(e.to_string()))?;

            if entries.is_empty() {
                eprintln!("No audit entries found.");
                return Ok(());
            }

            print_table(
                &entries,
                &["action", "target", "outcome", "at"],
                opts.format,
            );
            Ok(())
        }

        OperatorCommand::Session(sub) => cmd_session(opts, &client, sub).await,
    }
}

async fn cmd_session(
    opts: &GlobalOpts,
    client: &MarabuntaClient,
    cmd: &SessionSubcommand,
) -> Result<(), CliError> {
    match cmd {
        SessionSubcommand::Start { title, description } => {
            let body = serde_json::json!({
                "title": title,
                "description": description,
            });
            let resp = client
                .post("/api/v1/operator/oai/session", &body)
                .await?;
            let status = resp.status();
            if !status.is_success() {
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                return Err(CliError::Api {
                    status: status.as_u16(),
                    body,
                });
            }
            let session: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| CliError::Local(e.to_string()))?;

            let sid = session
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            eprintln!("Session started: {}", sid);
            eprintln!(
                "Use --session {} with intervention commands to tag them.",
                sid
            );
            eprintln!("End with: marabunta operator session end {}", sid);
            Ok(())
        }

        SessionSubcommand::End { session_id } => {
            let resp = client
                .post(
                    &format!("/api/v1/operator/oai/session/{}/end", session_id),
                    &serde_json::json!({}),
                )
                .await?;
            let status = resp.status();
            if !status.is_success() {
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                return Err(CliError::Api {
                    status: status.as_u16(),
                    body,
                });
            }
            eprintln!("Session {} ended.", session_id);
            Ok(())
        }

        SessionSubcommand::Show { session_id } => {
            let resp = client
                .get(&format!(
                    "/api/v1/operator/oai/session/{}",
                    session_id
                ))
                .await?;
            let status = resp.status();
            if !status.is_success() {
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                return Err(CliError::Api {
                    status: status.as_u16(),
                    body,
                });
            }
            let session: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| CliError::Local(e.to_string()))?;
            print_output(&session, opts.format);
            Ok(())
        }

        SessionSubcommand::List { active } => {
            let path = if *active {
                "/api/v1/operator/oai/sessions?active=true"
            } else {
                "/api/v1/operator/oai/sessions"
            };
            let resp = client.get(path).await?;
            let status = resp.status();
            if !status.is_success() {
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                return Err(CliError::Api {
                    status: status.as_u16(),
                    body,
                });
            }
            let sessions: Vec<serde_json::Value> = resp
                .json()
                .await
                .map_err(|e| CliError::Local(e.to_string()))?;
            print_table(
                &sessions,
                &[
                    "id",
                    "title",
                    "started_at",
                    "ended_at",
                    "intervention_count",
                ],
                opts.format,
            );
            Ok(())
        }
    }
}

/// Health check for CLI + API connectivity.
pub async fn cmd_doctor(opts: &GlobalOpts) -> Result<(), CliError> {
    eprintln!("Marabunta Doctor — checking system health...");
    eprintln!();

    let client = MarabuntaClient::new(opts);
    let mut all_ok = true;

    // 1. Check API connectivity
    eprint!("  API connection ({})... ", opts.api_url);
    match client.get("/api/v1/status").await {
        Ok(resp) if resp.status().is_success() => {
            eprintln!("OK");
        }
        Ok(resp) => {
            eprintln!("WARN (status {})", resp.status());
            all_ok = false;
        }
        Err(e) => {
            eprintln!("FAIL ({})", e);
            all_ok = false;
        }
    }

    // 2. Check auth
    eprint!("  Authentication... ");
    if opts.token.is_some() {
        match client.get("/api/v1/operator/oai/whoami").await {
            Ok(resp) if resp.status().is_success() => eprintln!("OK"),
            Ok(resp) if resp.status().as_u16() == 401 => {
                eprintln!("FAIL (invalid token)");
                all_ok = false;
            }
            _ => {
                eprintln!("SKIP (OAI not enabled)");
            }
        }
    } else {
        eprintln!("SKIP (no token configured)");
    }

    // 3. Check OAI subsystem
    eprint!("  Observe-and-Interfere... ");
    match client.get("/api/v1/observe/topology").await {
        Ok(resp) if resp.status().is_success() => eprintln!("OK"),
        Ok(resp) if resp.status().as_u16() == 503 => {
            eprintln!("DISABLED (not enabled in config)");
        }
        _ => {
            eprintln!("UNAVAILABLE");
            all_ok = false;
        }
    }

    // 4. Check PG
    eprint!("  PostgreSQL backbone... ");
    match client
        .get("/api/v1/observe/entity/node/test?at=2000-01-01T00:00:00Z")
        .await
    {
        Ok(resp) if resp.status().as_u16() == 404 => {
            eprintln!("OK (time-travel available)")
        }
        Ok(resp) if resp.status().as_u16() == 503 => {
            eprintln!("DISABLED (PG not configured)")
        }
        _ => eprintln!("UNKNOWN"),
    }

    // 5. Check config file
    eprint!("  Config file ({})... ", opts.config);
    let config = CliConfig::load(&opts.config);
    if config.api_url.is_some() || config.token.is_some() {
        eprintln!("OK (loaded)");
    } else {
        eprintln!("OK (using defaults)");
    }

    eprintln!();
    if all_ok {
        eprintln!("All checks passed.");
        Ok(())
    } else {
        eprintln!("Some checks failed. See above for details.");
        Err(CliError::Local("health check failed".to_string()))
    }
}

/// Guided walkthrough of available commands.
pub fn cmd_tour() {
    eprintln!("=== Marabunta CLI Tour ===");
    eprintln!();
    eprintln!("Welcome! This tour introduces the key commands for observing and");
    eprintln!("interacting with your Marabunta swarm.");
    eprintln!();

    eprintln!("--- Step 1: See your swarm ---");
    eprintln!();
    eprintln!("  marabunta topology        Show all nodes and their status");
    eprintln!("  marabunta heatmap         Show resource utilization per node");
    eprintln!("  marabunta watch           Stream live events (Ctrl+C to stop)");
    eprintln!();

    eprintln!("--- Step 2: Inspect an entity ---");
    eprintln!();
    eprintln!("  marabunta inspect node <id>           Current state");
    eprintln!("  marabunta inspect node <id> --at <ts>  Historical state (requires PG)");
    eprintln!("  marabunta inspect job <id>            Job details");
    eprintln!();

    eprintln!("--- Step 3: Intervene ---");
    eprintln!();
    eprintln!("  marabunta node drain <id>        Drain a node");
    eprintln!("  marabunta node cordon <id>       Stop accepting work");
    eprintln!("  marabunta job cancel <id>        Cancel a job");
    eprintln!("  marabunta chunk pause <id>       Pause a chunk");
    eprintln!();
    eprintln!("  Every intervention:");
    eprintln!("    1. Auto-generates a guard from current state");
    eprintln!("    2. Shows impact assessment");
    eprintln!("    3. Asks for confirmation");
    eprintln!("    4. Re-checks the guard at execution time");
    eprintln!("    5. Records in the audit trail");
    eprintln!();

    eprintln!("--- Step 4: Track your work ---");
    eprintln!();
    eprintln!("  marabunta operator whoami         Show your identity");
    eprintln!("  marabunta operator audit          Review your interventions");
    eprintln!("  marabunta operator session start   Group related interventions");
    eprintln!();

    eprintln!("--- Step 5: Debug ---");
    eprintln!();
    eprintln!("  marabunta trace <correlation_id>  Follow an event chain");
    eprintln!("  marabunta doctor                  Check CLI + API health");
    eprintln!();

    eprintln!("--- Output Formats ---");
    eprintln!();
    eprintln!("  --format json    JSON (pipe to jq)");
    eprintln!("  --format jsonl   JSON lines (one per line)");
    eprintln!("  --format csv     CSV (spreadsheet-friendly)");
    eprintln!("  --format table   Human-readable table (default)");
    eprintln!();

    eprintln!("Run `marabunta doctor` to verify your setup.");
}

/// Demo mode — instructions for running with synthetic events.
pub fn cmd_demo() {
    eprintln!("Starting demo mode...");
    eprintln!("This generates synthetic swarm events for testing the CLI.");
    eprintln!("Press Ctrl+C to stop.");
    eprintln!();

    eprintln!("To use demo mode:");
    eprintln!("  1. Start a swarm with: marabunta-swarm --config demo.toml");
    eprintln!("  2. In another terminal: marabunta watch");
    eprintln!("  3. Try: marabunta topology");
    eprintln!("  4. Try: marabunta node drain <pick-a-node-id>");
}

/// Map safety tier number to human label.
pub fn safety_tier_label(tier: u64) -> &'static str {
    match tier {
        0 => "unrestricted",
        1 => "informed",
        2 => "guarded",
        3 => "supervised",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Debug, Parser)]
    struct TestOperatorApp {
        #[command(subcommand)]
        cmd: OperatorCommand,
    }

    #[test]
    fn test_operator_whoami_cmd() {
        let app = TestOperatorApp::try_parse_from(["test", "whoami"]).expect("parse");
        assert!(matches!(app.cmd, OperatorCommand::Whoami));
    }

    #[test]
    fn test_operator_audit_args() {
        let app = TestOperatorApp::try_parse_from([
            "test", "audit", "--entity", "node:abc", "--limit", "25",
        ])
        .expect("parse");
        match app.cmd {
            OperatorCommand::Audit { entity, limit, .. } => {
                assert_eq!(entity, Some("node:abc".to_string()));
                assert_eq!(limit, 25);
            }
            _ => panic!("expected Audit"),
        }
    }

    #[test]
    fn test_session_start_args() {
        let app = TestOperatorApp::try_parse_from([
            "test",
            "session",
            "start",
            "Incident 42",
            "--description",
            "Node failure",
        ])
        .expect("parse");
        match app.cmd {
            OperatorCommand::Session(SessionSubcommand::Start {
                title,
                description,
            }) => {
                assert_eq!(title, "Incident 42");
                assert_eq!(description, Some("Node failure".to_string()));
            }
            _ => panic!("expected Session Start"),
        }
    }

    #[test]
    fn test_session_end_args() {
        let app = TestOperatorApp::try_parse_from(["test", "session", "end", "sess-123"])
            .expect("parse");
        match app.cmd {
            OperatorCommand::Session(SessionSubcommand::End { session_id }) => {
                assert_eq!(session_id, "sess-123");
            }
            _ => panic!("expected Session End"),
        }
    }

    #[test]
    fn test_session_list_active() {
        let app =
            TestOperatorApp::try_parse_from(["test", "session", "list", "--active"])
                .expect("parse");
        match app.cmd {
            OperatorCommand::Session(SessionSubcommand::List { active }) => {
                assert!(active);
            }
            _ => panic!("expected Session List"),
        }
    }

    #[test]
    fn test_safety_tier_labels() {
        assert_eq!(safety_tier_label(0), "unrestricted");
        assert_eq!(safety_tier_label(1), "informed");
        assert_eq!(safety_tier_label(2), "guarded");
        assert_eq!(safety_tier_label(3), "supervised");
        assert_eq!(safety_tier_label(99), "unknown");
    }

    #[test]
    fn test_tour_does_not_panic() {
        cmd_tour();
    }

    #[test]
    fn test_demo_does_not_panic() {
        cmd_demo();
    }
}
