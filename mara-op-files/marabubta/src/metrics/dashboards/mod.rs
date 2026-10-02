// Marabunta - Licensed under the MIT License.
//! Pre-built Grafana dashboards for Marabunta Compute
//!
//! This module provides dashboard definitions that can be imported
//! into Grafana for monitoring Marabunta clusters.

/// Cluster overview dashboard JSON
pub const CLUSTER_OVERVIEW: &str = include_str!("cluster_overview.json");

/// Node health dashboard JSON
pub const NODE_HEALTH: &str = include_str!("node_health.json");

/// Job monitoring dashboard JSON
pub const JOB_MONITORING: &str = include_str!("job_monitoring.json");

/// Get all available dashboard definitions
pub fn all_dashboards() -> Vec<(&'static str, &'static str)> {
    vec![
        ("marabunta-cluster-overview", CLUSTER_OVERVIEW),
        ("marabunta-node-health", NODE_HEALTH),
        ("marabunta-job-monitoring", JOB_MONITORING),
    ]
}

/// Dashboard metadata
#[derive(Debug, Clone)]
pub struct DashboardInfo {
    /// Dashboard unique ID
    pub uid: &'static str,
    /// Dashboard title
    pub title: &'static str,
    /// Description
    pub description: &'static str,
    /// Tags
    pub tags: &'static [&'static str],
}

/// Get metadata for all dashboards
pub fn dashboard_metadata() -> Vec<DashboardInfo> {
    vec![
        DashboardInfo {
            uid: "marabunta-cluster-overview",
            title: "Marabunta Cluster Overview",
            description: "High-level view of cluster health, job throughput, and resource utilization",
            tags: &["marabunta", "cluster", "overview"],
        },
        DashboardInfo {
            uid: "marabunta-node-health",
            title: "Marabunta Node Health",
            description: "Detailed node metrics including CPU, memory, GPU, and network utilization",
            tags: &["marabunta", "nodes", "health"],
        },
        DashboardInfo {
            uid: "marabunta-job-monitoring",
            title: "Marabunta Job Monitoring",
            description: "Job and task execution metrics with latency distributions and SLA tracking",
            tags: &["marabunta", "jobs", "tasks"],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dashboards_load() {
        // Verify all dashboards can be loaded
        assert!(!CLUSTER_OVERVIEW.is_empty());
        assert!(!NODE_HEALTH.is_empty());
        assert!(!JOB_MONITORING.is_empty());
    }

    #[test]
    fn test_all_dashboards() {
        let dashboards = all_dashboards();
        assert_eq!(dashboards.len(), 3);
    }

    #[test]
    fn test_dashboard_metadata() {
        let metadata = dashboard_metadata();
        assert_eq!(metadata.len(), 3);

        // Verify cluster overview
        let cluster = &metadata[0];
        assert_eq!(cluster.uid, "marabunta-cluster-overview");
        assert!(cluster.tags.contains(&"cluster"));
    }

    #[test]
    fn test_dashboards_are_valid_json() {
        for (name, json) in all_dashboards() {
            let result: Result<serde_json::Value, _> = serde_json::from_str(json);
            assert!(result.is_ok(), "Dashboard {} is not valid JSON: {:?}", name, result.err());
        }
    }
}
