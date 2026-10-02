// Marabunta - Licensed under the MIT License.
//! ETL Pipeline Using Workflow Engine
//!
//! This example demonstrates how to use Marabunta Compute's workflow engine to build
//! a complete ETL (Extract, Transform, Load) pipeline with conditional logic,
//! parallel processing, and error handling.
//!
//! # Pipeline Structure
//!
//! ```text
//!                      +----------+
//!                      |  Extract |
//!                      +----+-----+
//!                           |
//!              +------------+------------+
//!              |            |            |
//!              v            v            v
//!         +--------+   +--------+   +--------+
//!         |Validate|   |Validate|   |Validate|   <- Parallel validation
//!         +----+---+   +----+---+   +----+---+
//!              |            |            |
//!              +------------+------------+
//!                           |
//!                           v
//!                    +------------+
//!                    |  Transform |
//!                    +------+-----+
//!                           |
//!                    +------+------+
//!                    |             |
//!                    v             v
//!              +-----------+ +-----------+
//!              | Aggregate | | Enrich    |       <- Parallel transform
//!              +-----+-----+ +-----+-----+
//!                    |             |
//!                    +------+------+
//!                           |
//!                           v
//!                      +----+----+
//!                      |  Load   |
//!                      +----+----+
//!                           |
//!               +-----------+-----------+
//!               v                       v
//!         +----------+            +----------+
//!         |  Report  | ---[if]--- |  Alert   |   <- Conditional
//!         +----------+            +----------+
//! ```
//!
//! # Running this example
//!
//! ```bash
//! # Submit workflow to cluster
//! marabunta workflow submit examples/workflow_etl.rs
//!
//! # Or run locally for testing
//! cargo run --example workflow_etl
//! ```

use marabunta_compute::workflow::types::{
    ComparisonOperator, Condition, JobStepConfig, LoopCondition, ResourceRequirements, StepState,
    StepStatus, Workflow, WorkflowRun, WorkflowStatus, WorkflowStep,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// ETL Data Types
// ============================================================================

/// Raw record from source system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawRecord {
    pub id: String,
    pub timestamp: String,
    pub data: HashMap<String, serde_json::Value>,
    pub source: String,
}

/// Validated record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatedRecord {
    pub id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub data: HashMap<String, serde_json::Value>,
    pub source: String,
    pub validation_status: ValidationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationStatus {
    Valid,
    Warning(String),
    Error(String),
}

/// Transformed record ready for loading
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformedRecord {
    pub id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub dimensions: HashMap<String, String>,
    pub metrics: HashMap<String, f64>,
    pub enrichments: HashMap<String, serde_json::Value>,
}

/// ETL job configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtlConfig {
    /// Source configuration
    pub source: SourceConfig,
    /// Validation rules
    pub validation_rules: Vec<ValidationRule>,
    /// Transformation specifications
    pub transformations: Vec<TransformSpec>,
    /// Target configuration
    pub target: TargetConfig,
    /// Error handling policy
    pub error_policy: ErrorPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub source_type: String,
    pub connection_string: String,
    pub query: Option<String>,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetConfig {
    pub target_type: String,
    pub connection_string: String,
    pub table_name: String,
    pub write_mode: WriteMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WriteMode {
    Append,
    Overwrite,
    Upsert,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationRule {
    pub field: String,
    pub rule_type: ValidationRuleType,
    pub error_level: ErrorLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationRuleType {
    NotNull,
    NotEmpty,
    InRange { min: f64, max: f64 },
    Pattern(String),
    InList(Vec<String>),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ErrorLevel {
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformSpec {
    pub input_field: String,
    pub output_field: String,
    pub transform_type: TransformType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransformType {
    Rename,
    Cast(String),
    Compute(String),
    Lookup { table: String, key: String },
    Default(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorPolicy {
    FailFast,
    SkipInvalid,
    LogAndContinue,
}

// ============================================================================
// Pipeline Step Results
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractResult {
    pub records_extracted: u64,
    pub batches: u64,
    pub source_info: HashMap<String, String>,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub total_records: u64,
    pub valid_records: u64,
    pub warning_records: u64,
    pub error_records: u64,
    pub errors_by_field: HashMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformResult {
    pub records_transformed: u64,
    pub transformations_applied: HashMap<String, u64>,
    pub enrichments_added: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadResult {
    pub records_loaded: u64,
    pub records_rejected: u64,
    pub target_info: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineResult {
    pub extract: ExtractResult,
    pub validation: ValidationResult,
    pub transform: TransformResult,
    pub load: LoadResult,
    pub total_elapsed_ms: u64,
    pub status: PipelineStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PipelineStatus {
    Success,
    PartialSuccess { warnings: Vec<String> },
    Failed { errors: Vec<String> },
}

// ============================================================================
// Workflow Builder
// ============================================================================

/// Build an ETL workflow using the Marabunta workflow engine
pub fn build_etl_workflow(config: &EtlConfig) -> Workflow {
    let mut workflow = Workflow::new("etl-pipeline");
    workflow.description = Some("ETL Pipeline: Extract, Transform, Load".to_string());
    workflow.tags = vec!["etl".to_string(), "data-pipeline".to_string()];

    // Set workflow variables from config
    workflow.set_variable(
        "source_config",
        serde_json::to_value(&config.source).unwrap(),
    );
    workflow.set_variable(
        "target_config",
        serde_json::to_value(&config.target).unwrap(),
    );
    workflow.set_variable(
        "error_policy",
        serde_json::to_value(&config.error_policy).unwrap(),
    );

    // Step 1: Extract data from source
    let extract_step = WorkflowStep::job(
        "extract",
        JobStepConfig::new("extract-data", "python3")
            .with_script(
                r#"
import json
import sys

# In a real pipeline, this would connect to the actual data source
config = json.loads(sys.argv[1])
print(f"Extracting from {config['source_type']}: {config['connection_string']}")

# Simulate extraction
result = {
    "records_extracted": 10000,
    "batches": 10,
    "source_info": {"type": config["source_type"]},
    "elapsed_ms": 1500
}
print(json.dumps(result))
"#,
            )
            .with_arg("config", serde_json::json!("{{ source_config }}"))
            .with_output("extract_result"),
    )
    .with_name("Extract Data")
    .with_timeout(3600)
    .with_retries(3);

    workflow.add_step(extract_step);

    // Step 2: Parallel validation (split by data source)
    let validate_step = WorkflowStep::parallel(
        "validate_parallel",
        vec![
            WorkflowStep::job(
                "validate_schema",
                JobStepConfig::new("validate-schema", "python3")
                    .with_script("print('Schema validation passed')")
                    .with_output("schema_result"),
            )
            .with_name("Validate Schema"),
            WorkflowStep::job(
                "validate_data_quality",
                JobStepConfig::new("validate-quality", "python3")
                    .with_script("print('Data quality checks passed')")
                    .with_output("quality_result"),
            )
            .with_name("Validate Data Quality"),
            WorkflowStep::job(
                "validate_referential",
                JobStepConfig::new("validate-refs", "python3")
                    .with_script("print('Referential integrity verified')")
                    .with_output("ref_result"),
            )
            .with_name("Validate References"),
        ],
    )
    .with_name("Parallel Validation")
    .depends_on("extract");

    workflow.add_step(validate_step);

    // Step 3: Conditional - proceed or handle errors
    let validation_check = WorkflowStep::conditional(
        "validation_check",
        Condition::OnVariable {
            variable: "quality_result.errors".to_string(),
            value: serde_json::json!(0),
            operator: ComparisonOperator::Equals,
        },
        vec![
            // If validation passes, continue to transform
            WorkflowStep::job(
                "log_validation_success",
                JobStepConfig::new("log-success", "shell")
                    .with_script("echo 'Validation successful, proceeding to transform'"),
            )
            .with_name("Log Validation Success"),
        ],
    )
    .depends_on("validate_parallel");

    workflow.add_step(validation_check);

    // Step 4: Transform - parallel aggregation and enrichment
    let transform_step = WorkflowStep::parallel(
        "transform_parallel",
        vec![
            WorkflowStep::job(
                "aggregate",
                JobStepConfig::new("aggregate-data", "python3")
                    .with_script(
                        r#"
import json
# Aggregate data by dimensions
result = {"records_aggregated": 5000, "groups": 250}
print(json.dumps(result))
"#,
                    )
                    .with_output("aggregate_result"),
            )
            .with_name("Aggregate Data"),
            WorkflowStep::job(
                "enrich",
                JobStepConfig::new("enrich-data", "python3")
                    .with_script(
                        r#"
import json
# Enrich with external data
result = {"records_enriched": 10000, "enrichment_sources": ["geo", "demographics"]}
print(json.dumps(result))
"#,
                    )
                    .with_output("enrich_result"),
            )
            .with_name("Enrich Data"),
        ],
    )
    .with_name("Parallel Transform")
    .depends_on("validation_check");

    workflow.add_step(transform_step);

    // Step 5: Load to target
    let load_step = WorkflowStep::job(
        "load",
        JobStepConfig::new("load-data", "python3")
            .with_script(
                r#"
import json
import sys

config = json.loads(sys.argv[1])
print(f"Loading to {config['target_type']}: {config['table_name']}")

result = {
    "records_loaded": 10000,
    "records_rejected": 0,
    "target_info": {"table": config["table_name"]}
}
print(json.dumps(result))
"#,
            )
            .with_arg("config", serde_json::json!("{{ target_config }}"))
            .with_output("load_result"),
    )
    .with_name("Load Data")
    .with_timeout(7200)
    .with_retries(2)
    .depends_on("transform_parallel");

    workflow.add_step(load_step);

    // Step 6: Generate report
    let report_step = WorkflowStep::job(
        "report",
        JobStepConfig::new("generate-report", "python3")
            .with_script(
                r#"
import json
# Generate pipeline report
report = {
    "status": "success",
    "summary": {
        "extracted": "{{ extract_result.records_extracted }}",
        "loaded": "{{ load_result.records_loaded }}"
    }
}
print(json.dumps(report))
"#,
            )
            .with_output("report_result"),
    )
    .with_name("Generate Report")
    .depends_on("load");

    workflow.add_step(report_step);

    // Step 7: Conditional alert if there were issues
    let alert_step = WorkflowStep::conditional(
        "alert_check",
        Condition::OnVariable {
            variable: "load_result.records_rejected".to_string(),
            value: serde_json::json!(0),
            operator: ComparisonOperator::GreaterThan,
        },
        vec![WorkflowStep::job(
            "send_alert",
            JobStepConfig::new("send-alert", "shell")
                .with_script("echo 'ALERT: Some records were rejected during load'"),
        )
        .with_name("Send Alert")],
    )
    .depends_on("load")
    .continue_on_fail();

    workflow.add_step(alert_step);

    workflow
}

/// Build a workflow with retry loop for resilient ETL
pub fn build_resilient_etl_workflow(config: &EtlConfig, max_retries: u32) -> Workflow {
    let mut workflow = Workflow::new("resilient-etl-pipeline");
    workflow.description = Some("Resilient ETL Pipeline with Retry Logic".to_string());

    // Initialize retry counter
    workflow.set_variable("retry_count", serde_json::json!(0));
    workflow.set_variable("max_retries", serde_json::json!(max_retries));
    workflow.set_variable("success", serde_json::json!(false));

    // Main ETL with retry loop
    let etl_with_retry = WorkflowStep::loop_step(
        "etl_retry_loop",
        LoopCondition::Until {
            condition: Box::new(Condition::Or {
                conditions: vec![
                    Condition::OnVariable {
                        variable: "success".to_string(),
                        value: serde_json::json!(true),
                        operator: ComparisonOperator::Equals,
                    },
                    Condition::OnVariable {
                        variable: "retry_count".to_string(),
                        value: serde_json::json!(max_retries),
                        operator: ComparisonOperator::GreaterThanOrEquals,
                    },
                ],
            }),
            max_iterations: max_retries + 1,
        },
        vec![
            // Increment retry counter
            WorkflowStep::job(
                "increment_retry",
                JobStepConfig::new("increment", "shell")
                    .with_script("echo 'Attempt {{ retry_count }}'"),
            ),
            // Main ETL step
            WorkflowStep::job(
                "run_etl",
                JobStepConfig::new("etl-main", "python3")
                    .with_script(
                        r#"
import json
import random

# Simulate ETL with possible failure
if random.random() < 0.7:  # 70% success rate
    print(json.dumps({"status": "success", "records": 10000}))
else:
    raise Exception("ETL failed - will retry")
"#,
                    )
                    .with_output("etl_result"),
            )
            .with_retries(0)
            .continue_on_fail(),
            // Check result and set success flag
            WorkflowStep::conditional(
                "check_success",
                Condition::on_success("run_etl"),
                vec![WorkflowStep::job(
                    "mark_success",
                    JobStepConfig::new("mark-success", "shell")
                        .with_script("echo 'ETL completed successfully'"),
                )],
            ),
        ],
    )
    .with_name("ETL with Retry");

    workflow.add_step(etl_with_retry);

    // Final status check
    let final_check = WorkflowStep::conditional(
        "final_status",
        Condition::OnVariable {
            variable: "success".to_string(),
            value: serde_json::json!(true),
            operator: ComparisonOperator::Equals,
        },
        vec![WorkflowStep::job(
            "success_notification",
            JobStepConfig::new("notify-success", "shell")
                .with_script("echo 'Pipeline completed successfully'"),
        )],
    )
    .depends_on("etl_retry_loop");

    workflow.add_step(final_check);

    workflow
}

// ============================================================================
// Workflow Execution Simulation
// ============================================================================

/// Simulate workflow execution (for demonstration)
pub fn simulate_workflow_execution(workflow: &Workflow) -> WorkflowRun {
    let mut run = WorkflowRun::new(workflow);
    run.status = WorkflowStatus::Running;

    println!("Executing workflow: {}", workflow.name);
    println!("Steps to execute: {}", workflow.steps.len());
    println!();

    // Simulate executing each step
    for step in &workflow.steps {
        simulate_step_execution(&mut run, step);
    }

    // Mark workflow complete
    run.status = WorkflowStatus::Completed;
    run.completed_at = Some(chrono::Utc::now());

    run
}

fn simulate_step_execution(run: &mut WorkflowRun, step: &WorkflowStep) {
    let mut state = StepState::new(&step.id);
    state.start();

    println!(
        "Executing step: {} ({})",
        step.name.as_deref().unwrap_or(&step.id),
        step.id
    );

    // Simulate execution time
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Mark as completed
    state.complete(Some(serde_json::json!({
        "status": "completed",
        "step_id": step.id
    })));

    run.step_states.insert(step.id.clone(), state);

    // Handle nested steps for parallel/conditional/loop
    match &step.step_type {
        marabunta_compute::workflow::types::StepType::Parallel { branches } => {
            println!("  (Executing {} parallel branches)", branches.len());
            for branch in branches {
                simulate_step_execution(run, branch);
            }
        }
        marabunta_compute::workflow::types::StepType::Conditional {
            then, else_branch, ..
        } => {
            println!("  (Evaluating condition)");
            for then_step in then {
                simulate_step_execution(run, then_step);
            }
            if let Some(else_steps) = else_branch {
                for else_step in else_steps {
                    simulate_step_execution(run, else_step);
                }
            }
        }
        marabunta_compute::workflow::types::StepType::Loop { body, .. } => {
            println!("  (Executing loop iteration)");
            for body_step in body {
                simulate_step_execution(run, body_step);
            }
        }
        _ => {}
    }
}

// ============================================================================
// Main Entry Point (Demo)
// ============================================================================

fn main() {
    println!("=== Marabunta Compute: ETL Pipeline Workflow ===\n");

    // Create ETL configuration
    let config = EtlConfig {
        source: SourceConfig {
            source_type: "postgresql".to_string(),
            connection_string: "postgresql://localhost/source_db".to_string(),
            query: Some("SELECT * FROM raw_events WHERE date >= '2024-01-01'".to_string()),
            batch_size: 10000,
        },
        validation_rules: vec![
            ValidationRule {
                field: "event_id".to_string(),
                rule_type: ValidationRuleType::NotNull,
                error_level: ErrorLevel::Error,
            },
            ValidationRule {
                field: "timestamp".to_string(),
                rule_type: ValidationRuleType::Pattern(r"^\d{4}-\d{2}-\d{2}".to_string()),
                error_level: ErrorLevel::Error,
            },
            ValidationRule {
                field: "amount".to_string(),
                rule_type: ValidationRuleType::InRange {
                    min: 0.0,
                    max: 1000000.0,
                },
                error_level: ErrorLevel::Warning,
            },
        ],
        transformations: vec![
            TransformSpec {
                input_field: "event_timestamp".to_string(),
                output_field: "timestamp".to_string(),
                transform_type: TransformType::Cast("timestamp".to_string()),
            },
            TransformSpec {
                input_field: "user_id".to_string(),
                output_field: "user_name".to_string(),
                transform_type: TransformType::Lookup {
                    table: "users".to_string(),
                    key: "user_id".to_string(),
                },
            },
        ],
        target: TargetConfig {
            target_type: "snowflake".to_string(),
            connection_string: "snowflake://account/warehouse".to_string(),
            table_name: "processed_events".to_string(),
            write_mode: WriteMode::Append,
        },
        error_policy: ErrorPolicy::LogAndContinue,
    };

    // Build the workflow
    println!("--- Building ETL Workflow ---");
    let workflow = build_etl_workflow(&config);

    // Validate workflow
    match workflow.validate() {
        Ok(()) => println!("Workflow validation: PASSED"),
        Err(e) => println!("Workflow validation failed: {}", e),
    }

    println!("\nWorkflow structure:");
    println!("  ID: {}", workflow.id);
    println!("  Name: {}", workflow.name);
    println!("  Version: {}", workflow.version);
    println!("  Steps: {}", workflow.steps.len());
    println!("  Tags: {:?}", workflow.tags);
    println!();

    // Show workflow as JSON
    println!("Workflow definition (abbreviated):");
    let workflow_json = serde_json::to_string_pretty(&serde_json::json!({
        "id": workflow.id.to_string(),
        "name": workflow.name,
        "steps": workflow.steps.iter().map(|s| {
            serde_json::json!({
                "id": s.id,
                "name": s.name,
                "depends_on": s.depends_on,
            })
        }).collect::<Vec<_>>(),
    }))
    .unwrap();
    println!("{}", workflow_json);
    println!();

    // Simulate execution
    println!("--- Simulating Workflow Execution ---");
    let run = simulate_workflow_execution(&workflow);

    println!();
    println!("Execution completed:");
    println!("  Run ID: {}", run.id);
    println!("  Status: {:?}", run.status);
    println!("  Steps executed: {}", run.step_states.len());
    println!("  Progress: {:.0}%", run.progress() * 100.0);

    // Show step states
    println!("\nStep states:");
    for (step_id, state) in &run.step_states {
        println!(
            "  {} - {:?} ({}ms)",
            step_id,
            state.status,
            state
                .completed_at
                .map(|t| t
                    .signed_duration_since(state.started_at.unwrap())
                    .num_milliseconds())
                .unwrap_or(0)
        );
    }

    println!();

    // Build resilient workflow with retries
    println!("--- Building Resilient ETL Workflow ---");
    let resilient_workflow = build_resilient_etl_workflow(&config, 3);
    println!("  Name: {}", resilient_workflow.name);
    println!("  Max retries: 3");
    println!("  Steps: {}", resilient_workflow.steps.len());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_etl_workflow() {
        let config = EtlConfig {
            source: SourceConfig {
                source_type: "test".to_string(),
                connection_string: "test://".to_string(),
                query: None,
                batch_size: 100,
            },
            validation_rules: vec![],
            transformations: vec![],
            target: TargetConfig {
                target_type: "test".to_string(),
                connection_string: "test://".to_string(),
                table_name: "test".to_string(),
                write_mode: WriteMode::Append,
            },
            error_policy: ErrorPolicy::FailFast,
        };

        let workflow = build_etl_workflow(&config);

        assert_eq!(workflow.name, "etl-pipeline");
        assert!(!workflow.steps.is_empty());
        assert!(workflow.validate().is_ok());
    }

    #[test]
    fn test_workflow_validation() {
        let config = EtlConfig {
            source: SourceConfig {
                source_type: "test".to_string(),
                connection_string: "test://".to_string(),
                query: None,
                batch_size: 100,
            },
            validation_rules: vec![],
            transformations: vec![],
            target: TargetConfig {
                target_type: "test".to_string(),
                connection_string: "test://".to_string(),
                table_name: "test".to_string(),
                write_mode: WriteMode::Append,
            },
            error_policy: ErrorPolicy::FailFast,
        };

        let workflow = build_etl_workflow(&config);
        assert!(workflow.validate().is_ok());
    }

    #[test]
    fn test_resilient_workflow() {
        let config = EtlConfig {
            source: SourceConfig {
                source_type: "test".to_string(),
                connection_string: "test://".to_string(),
                query: None,
                batch_size: 100,
            },
            validation_rules: vec![],
            transformations: vec![],
            target: TargetConfig {
                target_type: "test".to_string(),
                connection_string: "test://".to_string(),
                table_name: "test".to_string(),
                write_mode: WriteMode::Append,
            },
            error_policy: ErrorPolicy::FailFast,
        };

        let workflow = build_resilient_etl_workflow(&config, 3);

        assert_eq!(workflow.name, "resilient-etl-pipeline");
        assert!(workflow.validate().is_ok());
    }
}
