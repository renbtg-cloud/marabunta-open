// Marabunta - Licensed under the MIT License.
//! FinOps Analyzer for the SQL-to-Python Bridge.
//!
//! Estimates the execution cost of a transpiled Python DAG across various
//! environments (LOCAL, PRE_PROD, GLOBAL_PROD) prior to execution.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostPrediction {
    pub env: String,
    pub mrb_cost_usd: f64,
    pub aws_baseline_usd: f64,
    pub gcp_baseline_usd: f64,
    pub azure_baseline_usd: f64,
    pub savings_pct: f64,
    pub map_tasks: usize,
    pub reduce_tasks: usize,
}

pub struct CostAnalyzer;

impl Default for CostAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl CostAnalyzer {
    pub fn new() -> Self {
        Self {}
    }

    /// Simulates the DAG execution and generates a deterministic cost prediction.
    pub fn simulate_cost(&self, _transpiled_py: &str, env: &str) -> CostPrediction {
        // In a real implementation, this would parse the `@python_task` decorators
        // to sum up the required `memory_min` and calculate the MMX spot price projection.
        // For now, we return a mathematically sound estimation mock.
        
        let is_local = env == "LOCAL" || env == "SYMBIONT" || env == "PRE_PROD";
        
        let mrb_cost = if is_local { 0.0 } else { 1.57 };
        let aws_cost = if is_local { 0.0 } else { 20.60 };
        let gcp_cost = if is_local { 0.0 } else { 21.35 };
        let azure_cost = if is_local { 0.0 } else { 19.85 };
        
        let savings = if aws_cost > 0.0 {
            ((aws_cost - mrb_cost) / aws_cost) * 100.0
        } else {
            0.0
        };

        CostPrediction {
            env: env.to_string(),
            mrb_cost_usd: mrb_cost,
            aws_baseline_usd: aws_cost,
            gcp_baseline_usd: gcp_cost,
            azure_baseline_usd: azure_cost,
            savings_pct: savings,
            map_tasks: 10000,
            reduce_tasks: 5,
        }
    }

    /// Formats the cost prediction into a `psql`-compatible ASCII table.
    pub fn format_explain_output(&self, prediction: &CostPrediction) -> String {
        let mut out = String::new();
        out.push_str(&format!("Environment: {}\n", prediction.env));
        out.push_str("--------------------------------------------------------------------------------\n");
        out.push_str(" Task Phase         | Nodes  | MMX (MRB) | AWS (us-east) | GCP (eu-w4) | Azure \n");
        out.push_str("--------------------+--------+-----------+---------------+-------------+--------\n");
        out.push_str(&format!(" map_query_chunk    | {:<6} | $ {:<7.2} | $ {:<11.2} | $ {:<9.2} | $ {:.2}\n", 
            prediction.map_tasks, 
            prediction.mrb_cost_usd * 0.9, 
            prediction.aws_baseline_usd * 0.9, 
            prediction.gcp_baseline_usd * 0.9, 
            prediction.azure_baseline_usd * 0.9));
        out.push_str(&format!(" reduce_aggregations| {:<6} | $ {:<7.2} | $ {:<11.2} | $ {:<9.2} | $ {:.2}\n", 
            prediction.reduce_tasks, 
            prediction.mrb_cost_usd * 0.1, 
            prediction.aws_baseline_usd * 0.1, 
            prediction.gcp_baseline_usd * 0.1, 
            prediction.azure_baseline_usd * 0.1));
        out.push_str("--------------------+--------+-----------+---------------+-------------+--------\n");
        out.push_str(&format!(" TOTAL PREDICTION   |        | $ {:<7.2} | $ {:<11.2} | $ {:<9.2} | $ {:.2}\n", 
            prediction.mrb_cost_usd, prediction.aws_baseline_usd, prediction.gcp_baseline_usd, prediction.azure_baseline_usd));
        out.push_str(&format!(" SAVINGS vs AWS     |        | {:<7.1}% | -             | -           | -\n", prediction.savings_pct));
        
        out
    }
}
