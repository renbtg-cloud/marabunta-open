// Marabunta - Licensed under the MIT License.
//! Nodes listing command
//!
//! Lists and filters available compute nodes in the cluster.

use clap::Args;

use crate::cli::client::CoordinatorClient;
use crate::cli::config::Config;
use crate::cli::display::{format_bytes, print_nodes_detail, print_nodes_summary};
use crate::cli::types::{parse_size, CliError, NodeFilter, NodeInfo, OutputFormat};

// ─────────────────────────────────────────────────────────────────────────────
// NODES ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for the nodes command
#[derive(Args)]
pub struct NodesArgs {
    /// Filter by runtime support
    #[arg(long)]
    pub runtime: Option<String>,

    /// Filter by minimum memory (e.g., "2GB", "512MB")
    #[arg(long)]
    pub memory_min: Option<String>,

    /// Filter by region
    #[arg(long)]
    pub region: Option<String>,

    /// Filter by architecture (e.g., "x86_64", "arm64")
    #[arg(long)]
    pub arch: Option<String>,

    /// Filter by status (ready, busy, offline)
    #[arg(long)]
    pub status: Option<String>,

    /// Show detailed info
    #[arg(long, short)]
    pub verbose: bool,

    /// Output format (human, json, csv, yaml)
    #[arg(long, value_enum)]
    pub format: Option<OutputFormat>,

    /// Sort by field (cores, memory, reliability, load)
    #[arg(long, default_value = "cores")]
    pub sort: String,

    /// Reverse sort order
    #[arg(long)]
    pub reverse: bool,

    /// Limit number of nodes shown
    #[arg(long)]
    pub limit: Option<usize>,

    /// Show only summary statistics
    #[arg(long)]
    pub summary_only: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// EXECUTE NODES
// ─────────────────────────────────────────────────────────────────────────────

/// Execute the nodes command
pub async fn execute_nodes(args: NodesArgs, config: &Config) -> Result<(), CliError> {
    let client = CoordinatorClient::from_config(config).await?;

    // Build filter
    let filter = NodeFilter {
        runtime: args.runtime.clone(),
        memory_min: parse_size(&args.memory_min)?,
        region: args.region.clone(),
        architecture: args.arch.clone(),
        status: args.status.clone(),
    };

    // Fetch nodes
    let mut nodes = client.list_nodes(filter).await?;

    // Sort nodes
    sort_nodes(&mut nodes, &args.sort, args.reverse);

    // Limit if specified
    if let Some(limit) = args.limit {
        nodes.truncate(limit);
    }

    // Output based on format
    let format = args.format.unwrap_or(config.output_format);
    match format {
        OutputFormat::Human => {
            print_nodes_summary(&nodes);
            if args.verbose && !args.summary_only {
                print_nodes_detail(&nodes);
            }
            if !args.summary_only {
                print_cluster_totals(&nodes);
            }
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&nodes)?);
        }
        OutputFormat::Csv => {
            print_nodes_csv(&nodes);
        }
        OutputFormat::Yaml => {
            print_nodes_yaml(&nodes)?;
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// SORTING
// ─────────────────────────────────────────────────────────────────────────────

/// Sort nodes by field
fn sort_nodes(nodes: &mut [NodeInfo], sort_by: &str, reverse: bool) {
    nodes.sort_by(|a, b| {
        let ordering = match sort_by.to_lowercase().as_str() {
            "cores" => a.cores.cmp(&b.cores),
            "memory" => a.memory.cmp(&b.memory),
            "reliability" => a
                .reliability
                .partial_cmp(&b.reliability)
                .unwrap_or(std::cmp::Ordering::Equal),
            "load" => a
                .current_load
                .partial_cmp(&b.current_load)
                .unwrap_or(std::cmp::Ordering::Equal),
            "type" => a.node_type.cmp(&b.node_type),
            "status" => a.status.cmp(&b.status),
            "tasks" => a.running_tasks.cmp(&b.running_tasks),
            _ => a.cores.cmp(&b.cores),
        };

        if reverse {
            ordering.reverse()
        } else {
            ordering
        }
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// CLUSTER TOTALS
// ─────────────────────────────────────────────────────────────────────────────

/// Print cluster totals
fn print_cluster_totals(nodes: &[NodeInfo]) {
    use console::style;

    // Calculate totals
    let total_cores: u32 = nodes.iter().map(|n| n.cores).sum();
    let total_memory: u64 = nodes.iter().map(|n| n.memory).sum();
    let total_disk: u64 = nodes.iter().map(|n| n.disk).sum();

    // Available resources (from nodes that are ready)
    let ready_nodes: Vec<&NodeInfo> = nodes
        .iter()
        .filter(|n| n.status.to_lowercase() == "ready")
        .collect();
    let available_cores: u32 = ready_nodes.iter().map(|n| n.cores).sum();
    let available_memory: u64 = ready_nodes.iter().map(|n| n.memory).sum();

    // Running tasks
    let running_tasks: u32 = nodes.iter().map(|n| n.running_tasks).sum();

    // Average reliability
    let avg_reliability: f32 = if !nodes.is_empty() {
        nodes.iter().map(|n| n.reliability).sum::<f32>() / nodes.len() as f32
    } else {
        0.0
    };

    println!();
    println!("   {} Cluster Totals:", style("").cyan());
    println!("   {}", style("─".repeat(50)).dim());
    println!("   Total Nodes:       {}", style(nodes.len()).green());
    println!("   Ready Nodes:       {}", style(ready_nodes.len()).green());
    println!("   Total Cores:       {}", style(total_cores).yellow());
    println!("   Available Cores:   {}", style(available_cores).yellow());
    println!("   Total Memory:      {}", format_bytes(total_memory));
    println!("   Available Memory:  {}", format_bytes(available_memory));
    println!("   Total Disk:        {}", format_bytes(total_disk));
    println!("   Running Tasks:     {}", style(running_tasks).cyan());
    println!("   Avg Reliability:   {:.1}%", avg_reliability * 100.0);
}

// ─────────────────────────────────────────────────────────────────────────────
// OUTPUT FORMATTING
// ─────────────────────────────────────────────────────────────────────────────

/// Print nodes as CSV
fn print_nodes_csv(nodes: &[NodeInfo]) {
    println!("id,type,cores,memory,disk,status,architecture,reliability,load,running_tasks,battery,region,runtimes");
    for node in nodes {
        println!(
            "{},{},{},{},{},{},{},{:.2},{:.2},{},{},{},\"{}\"",
            node.id,
            node.node_type,
            node.cores,
            node.memory,
            node.disk,
            node.status,
            node.architecture,
            node.reliability,
            node.current_load,
            node.running_tasks,
            node.battery_powered,
            node.region.as_deref().unwrap_or(""),
            node.runtimes.join(",")
        );
    }
}

/// Print nodes as YAML
fn print_nodes_yaml(nodes: &[NodeInfo]) -> Result<(), CliError> {
    println!("nodes:");
    for node in nodes {
        println!("  - id: {}", node.id);
        println!("    type: {}", node.node_type);
        println!("    cores: {}", node.cores);
        println!("    memory: {}", node.memory);
        println!("    disk: {}", node.disk);
        println!("    status: {}", node.status);
        println!("    architecture: {}", node.architecture);
        println!("    reliability: {:.2}", node.reliability);
        println!("    current_load: {:.2}", node.current_load);
        println!("    running_tasks: {}", node.running_tasks);
        println!("    battery_powered: {}", node.battery_powered);
        if let Some(region) = &node.region {
            println!("    region: {}", region);
        }
        println!("    runtimes:");
        for runtime in &node.runtimes {
            println!("      - {}", runtime);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_nodes() -> Vec<NodeInfo> {
        vec![
            NodeInfo {
                id: "node-001".to_string(),
                node_type: "raspberry_pi".to_string(),
                cores: 4,
                memory: 4 * 1024 * 1024 * 1024,
                disk: 32 * 1024 * 1024 * 1024,
                status: "ready".to_string(),
                runtimes: vec!["python3".to_string(), "wasm".to_string()],
                region: Some("us-west".to_string()),
                architecture: "arm64".to_string(),
                reliability: 0.95,
                battery_powered: false,
                current_load: 0.3,
                running_tasks: 2,
            },
            NodeInfo {
                id: "node-002".to_string(),
                node_type: "desktop".to_string(),
                cores: 8,
                memory: 16 * 1024 * 1024 * 1024,
                disk: 256 * 1024 * 1024 * 1024,
                status: "ready".to_string(),
                runtimes: vec![
                    "python3".to_string(),
                    "wasm".to_string(),
                    "native".to_string(),
                ],
                region: Some("us-east".to_string()),
                architecture: "x86_64".to_string(),
                reliability: 0.98,
                battery_powered: false,
                current_load: 0.1,
                running_tasks: 1,
            },
        ]
    }

    #[test]
    fn test_sort_by_cores() {
        let mut nodes = create_test_nodes();
        sort_nodes(&mut nodes, "cores", false);
        assert_eq!(nodes[0].cores, 4);
        assert_eq!(nodes[1].cores, 8);
    }

    #[test]
    fn test_sort_by_cores_reverse() {
        let mut nodes = create_test_nodes();
        sort_nodes(&mut nodes, "cores", true);
        assert_eq!(nodes[0].cores, 8);
        assert_eq!(nodes[1].cores, 4);
    }

    #[test]
    fn test_sort_by_memory() {
        let mut nodes = create_test_nodes();
        sort_nodes(&mut nodes, "memory", false);
        assert_eq!(nodes[0].memory, 4 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_sort_by_reliability() {
        let mut nodes = create_test_nodes();
        sort_nodes(&mut nodes, "reliability", false);
        assert!(nodes[0].reliability < nodes[1].reliability);
    }
}
