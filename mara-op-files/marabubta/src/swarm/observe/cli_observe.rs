// Marabunta - Licensed under the MIT License.
use clap::Parser;

use super::cli::{CliError, GlobalOpts, MarabuntaClient, OutputFormat, print_output, print_table};

/// `marabunta watch` — stream live swarm events.
#[derive(Debug, Clone, Parser)]
pub struct WatchArgs {
    /// Filter by event domain (e.g., "fleet", "work", "security").
    #[arg(long, short = 'd')]
    pub domain: Option<String>,

    /// Filter by minimum severity (debug, info, warning, critical).
    #[arg(long, short = 's')]
    pub severity: Option<String>,

    /// Filter by source node ID.
    #[arg(long)]
    pub node: Option<String>,

    /// Filter by correlation ID.
    #[arg(long)]
    pub correlation: Option<String>,

    /// Text search in event summaries.
    #[arg(long, short = 'k')]
    pub keyword: Option<String>,

    /// Maximum events to display (0 = unlimited).
    #[arg(long, default_value = "0")]
    pub limit: usize,
}

/// `marabunta inspect` — point-in-time entity snapshot.
#[derive(Debug, Clone, Parser)]
pub struct InspectArgs {
    /// Entity type (node, job, chunk, collective, plugin).
    #[arg()]
    pub entity_type: String,

    /// Entity ID.
    #[arg()]
    pub entity_id: String,

    /// Time-travel: show state at this timestamp (RFC3339).
    #[arg(long)]
    pub at: Option<String>,
}

/// `marabunta trace` — causal chain reconstruction.
#[derive(Debug, Clone, Parser)]
pub struct TraceArgs {
    /// Correlation ID to trace.
    #[arg()]
    pub correlation_id: String,

    /// Maximum events to return.
    #[arg(long, default_value = "100")]
    pub limit: usize,

    /// Only show events since this timestamp (RFC3339).
    #[arg(long)]
    pub since: Option<String>,
}

/// Format a JSON event for table display.
pub fn format_event_for_table(event: &serde_json::Value) -> String {
    let ts = event
        .get("timestamp")
        .and_then(|v| v.as_str())
        .unwrap_or("?");
    let domain = event
        .get("domain")
        .and_then(|v| v.as_str())
        .unwrap_or("?");
    let sev = event
        .get("severity")
        .and_then(|v| v.as_str())
        .unwrap_or("?");
    let summary = event
        .get("summary")
        .and_then(|v| v.as_str())
        .unwrap_or("?");
    format!("[{}] [{}] [{}] {}", ts, domain, sev, summary)
}

/// Parse SSE data lines from a raw buffer, returning extracted data and remaining buffer.
pub fn extract_sse_data(buffer: &str) -> (Vec<String>, String) {
    let mut data_lines = Vec::new();
    let mut remaining = buffer.to_string();

    while let Some(pos) = remaining.find("\n\n") {
        let event_block = remaining[..pos].to_string();
        remaining = remaining[pos + 2..].to_string();

        for line in event_block.lines() {
            if let Some(data) = line.strip_prefix("data: ") {
                data_lines.push(data.to_string());
            }
        }
    }

    (data_lines, remaining)
}

/// Stream live swarm events via SSE.
pub async fn cmd_watch(opts: &GlobalOpts, args: &WatchArgs) -> Result<(), CliError> {
    let client = MarabuntaClient::new(opts);

    // Create a managed subscription via the API
    let filter = serde_json::json!({
        "domain": args.domain,
        "min_severity": args.severity,
        "source_node": args.node,
        "correlation_id": args.correlation,
    });

    let resp = client
        .post(
            "/api/v1/observe/subscriptions",
            &serde_json::json!({ "filter": filter }),
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

    let sub: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| CliError::Local(e.to_string()))?;
    let sub_id = sub.get("id").and_then(|v| v.as_str()).unwrap_or("");

    if sub_id.is_empty() {
        return Err(CliError::Local(
            "subscription created but no ID returned".to_string(),
        ));
    }

    // Stream SSE events from the subscription
    let mut sse_resp = client
        .sse(&format!(
            "/api/v1/observe/subscriptions/{}/stream",
            sub_id
        ))
        .await?;

    let sse_status = sse_resp.status();
    if !sse_status.is_success() {
        let body: serde_json::Value = sse_resp.json().await.unwrap_or_default();
        return Err(CliError::Api {
            status: sse_status.as_u16(),
            body,
        });
    }

    let mut count = 0usize;
    let mut buffer = String::new();
    eprintln!("Watching swarm events (Ctrl+C to stop)...");

    loop {
        match sse_resp.chunk().await {
            Ok(Some(bytes)) => {
                buffer.push_str(&String::from_utf8_lossy(&bytes));

                let (data_lines, remaining) = extract_sse_data(&buffer);
                buffer = remaining;

                for data in &data_lines {
                    // Apply keyword filter client-side
                    if let Some(ref kw) = args.keyword {
                        if !data.to_lowercase().contains(&kw.to_lowercase()) {
                            continue;
                        }
                    }

                    match opts.format {
                        OutputFormat::Json | OutputFormat::Jsonl => {
                            println!("{}", data);
                        }
                        OutputFormat::Table => {
                            if let Ok(event) =
                                serde_json::from_str::<serde_json::Value>(data)
                            {
                                println!("{}", format_event_for_table(&event));
                            } else {
                                println!("{}", data);
                            }
                        }
                        OutputFormat::Csv => {
                            println!("{}", data);
                        }
                    }

                    count += 1;
                    if args.limit > 0 && count >= args.limit {
                        return Ok(());
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                eprintln!("stream error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

/// Inspect a single entity's current or historical state.
pub async fn cmd_inspect(opts: &GlobalOpts, args: &InspectArgs) -> Result<(), CliError> {
    let client = MarabuntaClient::new(opts);

    let mut path = format!(
        "/api/v1/observe/entity/{}/{}",
        args.entity_type, args.entity_id
    );
    if let Some(ref at) = args.at {
        path.push_str(&format!("?at={}", at));
    }

    let resp = client.get(&path).await?;
    let status = resp.status();

    if !status.is_success() {
        let status_code = status.as_u16();
        let body: serde_json::Value = if status_code == 404 {
            serde_json::json!({
                "error": format!("{} '{}' not found", args.entity_type, args.entity_id)
            })
        } else {
            resp.json().await.unwrap_or_default()
        };
        return Err(CliError::Api {
            status: status_code,
            body,
        });
    }

    let entity: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| CliError::Local(e.to_string()))?;

    print_output(&entity, opts.format);
    Ok(())
}

/// Trace events by correlation ID.
pub async fn cmd_trace(opts: &GlobalOpts, args: &TraceArgs) -> Result<(), CliError> {
    let client = MarabuntaClient::new(opts);

    let mut query_parts = vec![
        format!("correlation_id={}", args.correlation_id),
        format!("limit={}", args.limit),
    ];
    if let Some(ref since) = args.since {
        query_parts.push(format!("since={}", since));
    }

    let path = format!("/api/v1/observe/trace?{}", query_parts.join("&"));
    let resp = client.get(&path).await?;

    let status = resp.status();
    if !status.is_success() {
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        return Err(CliError::Api {
            status: status.as_u16(),
            body,
        });
    }

    let events: Vec<serde_json::Value> = resp
        .json()
        .await
        .map_err(|e| CliError::Local(e.to_string()))?;

    if events.is_empty() {
        eprintln!(
            "No events found for correlation_id '{}'",
            args.correlation_id
        );
        return Ok(());
    }

    match opts.format {
        OutputFormat::Table => {
            eprintln!(
                "Trace: {} ({} events)",
                args.correlation_id,
                events.len()
            );
            eprintln!("{}", "-".repeat(60));
            for event in &events {
                println!("  {}", format_event_for_table(event));
            }
        }
        _ => {
            print_output(&events, opts.format);
        }
    }

    Ok(())
}

/// Display swarm topology.
pub async fn cmd_topology(opts: &GlobalOpts) -> Result<(), CliError> {
    let client = MarabuntaClient::new(opts);
    let resp = client.get("/api/v1/observe/topology").await?;

    let status = resp.status();
    if !status.is_success() {
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        return Err(CliError::Api {
            status: status.as_u16(),
            body,
        });
    }

    let topo: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| CliError::Local(e.to_string()))?;

    match opts.format {
        OutputFormat::Table => {
            let total = topo
                .get("total_nodes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let alive = topo
                .get("alive_nodes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            eprintln!("Swarm Topology: {}/{} nodes alive", alive, total);
            eprintln!();

            if let Some(nodes) = topo.get("nodes").and_then(|v| v.as_array()) {
                print_table(
                    nodes,
                    &["id", "status", "fleet_state", "load"],
                    opts.format,
                );
            }
        }
        _ => print_output(&topo, opts.format),
    }

    Ok(())
}

/// Display resource heatmap.
pub async fn cmd_heatmap(opts: &GlobalOpts) -> Result<(), CliError> {
    let client = MarabuntaClient::new(opts);
    let resp = client.get("/api/v1/observe/heatmap").await?;

    let status = resp.status();
    if !status.is_success() {
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        return Err(CliError::Api {
            status: status.as_u16(),
            body,
        });
    }

    let heatmap: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| CliError::Local(e.to_string()))?;

    match opts.format {
        OutputFormat::Table => {
            if let Some(entries) = heatmap.get("entries").and_then(|v| v.as_array()) {
                print_table(
                    entries,
                    &["node_id", "cpu_pct", "memory_pct", "disk_pct", "active_chunks"],
                    opts.format,
                );
            }
        }
        _ => print_output(&heatmap, opts.format),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Debug, Parser)]
    struct TestWatchApp {
        #[command(flatten)]
        args: WatchArgs,
    }

    #[derive(Debug, Parser)]
    struct TestInspectApp {
        #[command(flatten)]
        args: InspectArgs,
    }

    #[derive(Debug, Parser)]
    struct TestTraceApp {
        #[command(flatten)]
        args: TraceArgs,
    }

    #[test]
    fn test_watch_args_parsing() {
        let app = TestWatchApp::try_parse_from([
            "test",
            "--domain",
            "fleet",
            "--severity",
            "warning",
        ])
        .expect("parse");
        assert_eq!(app.args.domain, Some("fleet".to_string()));
        assert_eq!(app.args.severity, Some("warning".to_string()));
        assert!(app.args.node.is_none());
        assert_eq!(app.args.limit, 0);
    }

    #[test]
    fn test_watch_args_with_keyword() {
        let app =
            TestWatchApp::try_parse_from(["test", "--keyword", "error", "--limit", "10"])
                .expect("parse");
        assert_eq!(app.args.keyword, Some("error".to_string()));
        assert_eq!(app.args.limit, 10);
    }

    #[test]
    fn test_inspect_args_parsing() {
        let app =
            TestInspectApp::try_parse_from(["test", "node", "abc123"]).expect("parse");
        assert_eq!(app.args.entity_type, "node");
        assert_eq!(app.args.entity_id, "abc123");
        assert!(app.args.at.is_none());
    }

    #[test]
    fn test_inspect_with_at() {
        let app = TestInspectApp::try_parse_from([
            "test",
            "node",
            "abc123",
            "--at",
            "2024-01-01T00:00:00Z",
        ])
        .expect("parse");
        assert_eq!(
            app.args.at,
            Some("2024-01-01T00:00:00Z".to_string())
        );
    }

    #[test]
    fn test_trace_args_parsing() {
        let app = TestTraceApp::try_parse_from(["test", "corr-123", "--limit", "50"])
            .expect("parse");
        assert_eq!(app.args.correlation_id, "corr-123");
        assert_eq!(app.args.limit, 50);
        assert!(app.args.since.is_none());
    }

    #[test]
    fn test_format_event_for_table() {
        let event = serde_json::json!({
            "timestamp": "2024-01-01T00:00:00Z",
            "domain": "fleet",
            "severity": "warning",
            "summary": "node went down"
        });
        let formatted = format_event_for_table(&event);
        assert!(formatted.contains("2024-01-01T00:00:00Z"));
        assert!(formatted.contains("fleet"));
        assert!(formatted.contains("warning"));
        assert!(formatted.contains("node went down"));
    }

    #[test]
    fn test_format_event_for_table_missing_fields() {
        let event = serde_json::json!({});
        let formatted = format_event_for_table(&event);
        assert_eq!(formatted, "[?] [?] [?] ?");
    }

    #[test]
    fn test_extract_sse_data_single() {
        let buffer = "data: {\"test\":1}\n\nremaining";
        let (data, remaining) = extract_sse_data(buffer);
        assert_eq!(data, vec!["{\"test\":1}"]);
        assert_eq!(remaining, "remaining");
    }

    #[test]
    fn test_extract_sse_data_multiple() {
        let buffer = "data: first\n\ndata: second\n\n";
        let (data, remaining) = extract_sse_data(buffer);
        assert_eq!(data, vec!["first", "second"]);
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_extract_sse_data_incomplete() {
        let buffer = "data: partial";
        let (data, remaining) = extract_sse_data(buffer);
        assert!(data.is_empty());
        assert_eq!(remaining, "data: partial");
    }
}
