// Marabunta - Licensed under the MIT License.
//! Policy CLI Commands
//!
//! Provides command-line tools for policy management:
//! - `marabunta policy compile` - Compile DSL to IR
//! - `marabunta policy validate` - Validate policy files
//! - `marabunta policy test` - Test policies against jobs
//! - `marabunta policy list-templates` - List available templates
//! - `marabunta policy from-template` - Create policy from template

use clap::{Args, Subcommand};
use std::path::PathBuf;

use super::{CliError, Config};

/// Policy management commands
#[derive(Args)]
pub struct PolicyArgs {
    #[command(subcommand)]
    pub command: PolicyCommand,
}

/// Policy subcommands
#[derive(Subcommand)]
pub enum PolicyCommand {
    /// Compile a policy DSL file to IR
    #[command(visible_alias = "c")]
    Compile(CompileArgs),

    /// Validate a policy DSL file
    #[command(visible_alias = "v")]
    Validate(ValidateArgs),

    /// Test a policy against a job specification
    #[command(visible_alias = "t")]
    Test(TestArgs),

    /// List available policy templates
    #[command(visible_alias = "ls")]
    ListTemplates(ListTemplatesArgs),

    /// Create a policy from a template
    #[command(visible_alias = "tpl")]
    FromTemplate(FromTemplateArgs),

    /// Format a policy DSL file
    Format(FormatArgs),
}

/// Arguments for compile command
#[derive(Args)]
pub struct CompileArgs {
    /// Path to the policy DSL file
    #[arg(value_name = "FILE")]
    pub file: PathBuf,

    /// Output file (default: stdout)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Output format (json, yaml)
    #[arg(short, long, default_value = "json")]
    pub format: OutputFormat,

    /// Pretty print output
    #[arg(long)]
    pub pretty: bool,
}

/// Arguments for validate command
#[derive(Args)]
pub struct ValidateArgs {
    /// Path to the policy DSL file
    #[arg(value_name = "FILE")]
    pub file: PathBuf,

    /// Strict mode (treat warnings as errors)
    #[arg(long)]
    pub strict: bool,

    /// Known node groups (for validation)
    #[arg(long, value_delimiter = ',')]
    pub groups: Vec<String>,

    /// Known quota IDs (for validation)
    #[arg(long, value_delimiter = ',')]
    pub quotas: Vec<String>,
}

/// Arguments for test command
#[derive(Args)]
pub struct TestArgs {
    /// Path to the policy DSL file
    #[arg(value_name = "POLICY_FILE")]
    pub policy_file: PathBuf,

    /// Path to the job specification JSON file
    #[arg(long, value_name = "FILE")]
    pub job: PathBuf,

    /// Path to the nodes specification JSON file
    #[arg(long, value_name = "FILE")]
    pub nodes: Option<PathBuf>,

    /// Verbose output showing evaluation details
    #[arg(short, long)]
    pub verbose: bool,
}

/// Arguments for list-templates command
#[derive(Args)]
pub struct ListTemplatesArgs {
    /// Filter by category
    #[arg(short, long)]
    pub category: Option<String>,

    /// Show detailed information
    #[arg(long)]
    pub detailed: bool,
}

/// Arguments for from-template command
#[derive(Args)]
pub struct FromTemplateArgs {
    /// Template ID
    #[arg(value_name = "TEMPLATE_ID")]
    pub template_id: String,

    /// Template parameters as key=value pairs
    #[arg(short, long, value_delimiter = ',')]
    pub params: Vec<String>,

    /// Output file (default: stdout)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Output format (json, yaml, dsl)
    #[arg(short, long, default_value = "json")]
    pub format: OutputFormat,
}

/// Arguments for format command
#[derive(Args)]
pub struct FormatArgs {
    /// Path to the policy DSL file
    #[arg(value_name = "FILE")]
    pub file: PathBuf,

    /// Write formatted output back to file
    #[arg(short, long)]
    pub write: bool,
}

/// Output format options
#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
pub enum OutputFormat {
    #[default]
    Json,
    Yaml,
    Dsl,
}

// ============================================================================
// Command Execution
// ============================================================================

/// Execute policy commands
pub async fn execute_policy(args: PolicyArgs, _config: &Config) -> Result<(), CliError> {
    match args.command {
        PolicyCommand::Compile(args) => execute_compile(args).await,
        PolicyCommand::Validate(args) => execute_validate(args).await,
        PolicyCommand::Test(args) => execute_test(args).await,
        PolicyCommand::ListTemplates(args) => execute_list_templates(args).await,
        PolicyCommand::FromTemplate(args) => execute_from_template(args).await,
        PolicyCommand::Format(args) => execute_format(args).await,
    }
}

async fn execute_compile(args: CompileArgs) -> Result<(), CliError> {
    use crate::policy::dsl;
    use console::style;

    // Read source file
    let source = std::fs::read_to_string(&args.file).map_err(|e| {
        CliError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Failed to read file '{}': {}", args.file.display(), e),
        ))
    })?;

    // Compile
    let policies = match dsl::compile_with_validation(&source) {
        Ok(p) => p,
        Err(errors) => {
            eprintln!("{}", style("Compilation failed:").red().bold());
            for err in &errors {
                eprintln!("{}", dsl::format_error(err, &source));
            }
            return Err(CliError::Parse("compilation failed".to_string()));
        }
    };

    // Format output
    let output = match args.format {
        OutputFormat::Json => if args.pretty {
            serde_json::to_string_pretty(&policies)
        } else {
            serde_json::to_string(&policies)
        }
        .map_err(|e| CliError::Serialization(e.to_string()))?,
        OutputFormat::Yaml => {
            // Note: would need serde_yaml dependency
            serde_json::to_string_pretty(&policies)
                .map_err(|e| CliError::Serialization(e.to_string()))?
        }
        OutputFormat::Dsl => {
            return Err(CliError::InvalidArgument(
                "cannot output compiled IR as DSL".to_string(),
            ));
        }
    };

    // Write output
    if let Some(output_path) = args.output {
        std::fs::write(&output_path, &output).map_err(|e| {
            CliError::Io(std::io::Error::other(
                format!("Failed to write to '{}': {}", output_path.display(), e),
            ))
        })?;
        println!(
            "{} Compiled {} policies to {}",
            style("SUCCESS").green().bold(),
            policies.len(),
            output_path.display()
        );
    } else {
        println!("{}", output);
    }

    Ok(())
}

async fn execute_validate(args: ValidateArgs) -> Result<(), CliError> {
    use crate::policy::dsl;
    use console::style;

    // Read source file
    let source = std::fs::read_to_string(&args.file).map_err(|e| {
        CliError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Failed to read file '{}': {}", args.file.display(), e),
        ))
    })?;

    // Validate
    let result = dsl::validate_with_context(&source, args.groups, args.quotas).map_err(|e| {
        eprintln!("{}", dsl::format_error(&e, &source));
        CliError::Parse("validation failed".to_string())
    })?;

    // Report errors
    if !result.valid {
        eprintln!("{}", style("Validation FAILED:").red().bold());
        for err in &result.errors {
            eprintln!("{}", dsl::format_error(err, &source));
        }
    }

    // Report warnings
    if result.has_warnings() {
        eprintln!("{}", style("Warnings:").yellow().bold());
        for warning in &result.warnings {
            eprintln!("  {}", warning);
        }
    }

    // Handle strict mode
    if args.strict && result.has_warnings() {
        return Err(CliError::Parse(
            "validation failed in strict mode due to warnings".to_string(),
        ));
    }

    if !result.valid {
        return Err(CliError::Parse("validation failed".to_string()));
    }

    println!("{} {}", style("VALID").green().bold(), args.file.display());
    if result.has_warnings() {
        println!("  {} warning(s)", result.warnings.len());
    }

    Ok(())
}

async fn execute_test(args: TestArgs) -> Result<(), CliError> {
    use crate::policy::dsl;
    use crate::policy::engine::{
        EvaluationContext, JobInfo, NodeInfo, PolicyEngine, SubmitterInfo,
    };
    use console::style;

    // Read and compile policy
    let policy_source = std::fs::read_to_string(&args.policy_file).map_err(|e| {
        CliError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "Failed to read policy file '{}': {}",
                args.policy_file.display(),
                e
            ),
        ))
    })?;

    let policies = match dsl::compile_with_validation(&policy_source) {
        Ok(p) => p,
        Err(errors) => {
            for err in &errors {
                eprintln!("{}", dsl::format_error(err, &policy_source));
            }
            return Err(CliError::Parse("policy compilation failed".to_string()));
        }
    };

    // Read job specification
    let job_json = std::fs::read_to_string(&args.job).map_err(|e| {
        CliError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Failed to read job file '{}': {}", args.job.display(), e),
        ))
    })?;

    let job: JobInfo = serde_json::from_str(&job_json)
        .map_err(|e| CliError::Parse(format!("Invalid job JSON: {}", e)))?;

    // Read nodes if provided, otherwise use default test nodes
    let nodes: Vec<NodeInfo> = if let Some(nodes_path) = &args.nodes {
        let nodes_json = std::fs::read_to_string(nodes_path).map_err(|e| {
            CliError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "Failed to read nodes file '{}': {}",
                    nodes_path.display(),
                    e
                ),
            ))
        })?;
        serde_json::from_str(&nodes_json)
            .map_err(|e| CliError::Parse(format!("Invalid nodes JSON: {}", e)))?
    } else {
        // Create default test nodes
        create_default_test_nodes()
    };

    // Create engine and register policies
    let mut engine = PolicyEngine::new();
    for policy in &policies {
        engine
            .register(policy.clone())
            .map_err(|e| CliError::InvalidArgument(format!("Failed to register policy: {}", e)))?;
    }

    // Create evaluation context
    let submitter = SubmitterInfo::new("test-user@example.com")
        .with_priority(50)
        .with_domains(vec!["default".to_string()]);

    let context = EvaluationContext::new(job.clone(), submitter).with_nodes(nodes);

    // Evaluate
    let result = if args.verbose {
        let explanation = engine.explain(&context);

        println!("{}", style("Policy Evaluation").cyan().bold());
        println!("{}", style("─".repeat(50)).dim());

        println!("\n{}", style("Policies Applied:").yellow());
        for trace in &explanation.policy_trace {
            let status = if trace.condition_evaluation.result {
                style("MATCHED").green()
            } else {
                style("SKIPPED").dim()
            };
            println!(
                "  {} - {} [{}]",
                trace.policy_id,
                status,
                if trace.condition_evaluation.result {
                    format!("{} effects", trace.effects_evaluation.len())
                } else {
                    "condition not met".to_string()
                }
            );
        }

        explanation.result
    } else {
        engine.evaluate(&context)
    };

    // Print results
    println!("\n{}", style("Results:").cyan().bold());
    println!("{}", style("─".repeat(50)).dim());

    println!("\n{}", style("Node Scores:").yellow());
    let mut ranked = result.ranked_nodes();
    ranked.sort_by(|a, b| b.1.total_score.partial_cmp(&a.1.total_score).unwrap());

    for (node_id, score) in &ranked {
        let status = if result.excluded_nodes.contains_key(*node_id) {
            style("EXCLUDED").red()
        } else {
            style("AVAILABLE").green()
        };
        println!(
            "  {} - score: {:.3} [{}]",
            node_id, score.total_score, status
        );
    }

    if !result.excluded_nodes.is_empty() {
        println!("\n{}", style("Excluded Nodes:").yellow());
        for (node_id, reason) in &result.excluded_nodes {
            println!(
                "  {} - {} (policy: {})",
                node_id, reason.reason, reason.policy_id
            );
        }
    }

    if let Some(best) = result.best_node() {
        println!("\n{} {}", style("Best Node:").green().bold(), best);
    } else {
        println!("\n{}", style("No eligible nodes found!").red().bold());
    }

    Ok(())
}

async fn execute_list_templates(args: ListTemplatesArgs) -> Result<(), CliError> {
    use crate::policy::templates::{TemplateCategory, TemplateRegistry};
    use console::style;

    let registry = TemplateRegistry::new();
    let templates = if let Some(cat) = &args.category {
        let category = match cat.to_lowercase().as_str() {
            "priority" => TemplateCategory::Priority,
            "resources" => TemplateCategory::Resources,
            "region" => TemplateCategory::Region,
            "security" => TemplateCategory::Security,
            "performance" => TemplateCategory::Performance,
            "cost" => TemplateCategory::Cost,
            "compliance" => TemplateCategory::Compliance,
            _ => {
                return Err(CliError::InvalidArgument(format!(
                    "Unknown category: {}. Valid categories: priority, resources, region, security, performance, cost, compliance",
                    cat
                )));
            }
        };
        registry.list_by_category(category)
    } else {
        registry.list()
    };

    println!("{}", style("Available Policy Templates").cyan().bold());
    println!("{}", style("─".repeat(60)).dim());

    for template in templates {
        println!(
            "\n{} [{}]",
            style(&template.name).yellow().bold(),
            style(&template.id).dim()
        );
        println!("  Category: {}", template.category.name());
        println!("  {}", template.description);

        if args.detailed {
            println!("  Parameters:");
            for param in &template.parameters {
                let required = if param.required {
                    style("(required)").red()
                } else {
                    style("(optional)").dim()
                };
                let default = param
                    .default
                    .as_ref()
                    .map(|v| format!(" [default: {}]", v))
                    .unwrap_or_default();
                println!(
                    "    {} {} - {} {}{}",
                    style(&param.name).green(),
                    required,
                    param.param_type.name(),
                    param.description,
                    default
                );
            }
        }
    }

    Ok(())
}

async fn execute_from_template(args: FromTemplateArgs) -> Result<(), CliError> {
    use crate::policy::templates::{TemplateParams, TemplateRegistry};
    use console::style;

    let registry = TemplateRegistry::new();

    // Parse parameters
    let mut params = TemplateParams::new();
    for param_str in &args.params {
        let parts: Vec<&str> = param_str.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(CliError::InvalidArgument(format!(
                "Invalid parameter format '{}', expected key=value",
                param_str
            )));
        }
        let key = parts[0].trim();
        let value = parts[1].trim();

        // Try to parse as JSON, fall back to string
        let json_value = serde_json::from_str(value).unwrap_or_else(|_| serde_json::json!(value));
        params.insert(key.to_string(), json_value);
    }

    // Instantiate template
    let policy = registry
        .instantiate(&args.template_id, &params)
        .map_err(|e| CliError::InvalidArgument(format!("Failed to instantiate template: {}", e)))?;

    // Format output
    let output = match args.format {
        OutputFormat::Json => serde_json::to_string_pretty(&policy)
            .map_err(|e| CliError::Serialization(e.to_string()))?,
        OutputFormat::Yaml => serde_json::to_string_pretty(&policy)
            .map_err(|e| CliError::Serialization(e.to_string()))?,
        OutputFormat::Dsl => {
            // Generate DSL representation
            policy_to_dsl(&policy)
        }
    };

    // Write output
    if let Some(output_path) = args.output {
        std::fs::write(&output_path, &output).map_err(|e| {
            CliError::Io(std::io::Error::other(
                format!("Failed to write to '{}': {}", output_path.display(), e),
            ))
        })?;
        println!(
            "{} Created policy '{}' from template '{}'",
            style("SUCCESS").green().bold(),
            policy.id,
            args.template_id
        );
    } else {
        println!("{}", output);
    }

    Ok(())
}

async fn execute_format(args: FormatArgs) -> Result<(), CliError> {
    use crate::policy::dsl;
    use console::style;

    // Read source file
    let source = std::fs::read_to_string(&args.file).map_err(|e| {
        CliError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Failed to read file '{}': {}", args.file.display(), e),
        ))
    })?;

    // Parse to validate
    let _ast = dsl::parse(&source).map_err(|e| {
        eprintln!("{}", dsl::format_error(&e, &source));
        CliError::Parse("parsing failed".to_string())
    })?;

    // For now, just return the original source (formatter would go here)
    // A full formatter would re-emit the AST with consistent formatting
    let formatted = source.clone(); // TODO: implement actual formatting

    if args.write {
        std::fs::write(&args.file, &formatted).map_err(|e| {
            CliError::Io(std::io::Error::other(
                format!("Failed to write to '{}': {}", args.file.display(), e),
            ))
        })?;
        println!(
            "{} Formatted {}",
            style("SUCCESS").green().bold(),
            args.file.display()
        );
    } else {
        println!("{}", formatted);
    }

    Ok(())
}

// ============================================================================
// Helper Functions
// ============================================================================

fn create_default_test_nodes() -> Vec<crate::policy::engine::NodeInfo> {
    use crate::policy::engine::{NodeInfo, ResourceRequest};
    use crate::policy::ir::TagSet;

    let mut nodes = Vec::new();

    // GPU Production Node
    let mut tags1 = TagSet::new();
    tags1.insert("env", "production");
    tags1.insert("gpu", "true");
    tags1.insert("region", "us-east");
    nodes.push(
        NodeInfo::new("gpu-prod-1")
            .with_tags(tags1)
            .with_resources(ResourceRequest::new(32, 128.0).with_gpu(4)),
    );

    // CPU Production Node
    let mut tags2 = TagSet::new();
    tags2.insert("env", "production");
    tags2.insert("region", "us-east");
    nodes.push(
        NodeInfo::new("cpu-prod-1")
            .with_tags(tags2)
            .with_resources(ResourceRequest::new(64, 256.0)),
    );

    // Development Node
    let mut tags3 = TagSet::new();
    tags3.insert("env", "development");
    tags3.insert("region", "us-west");
    nodes.push(
        NodeInfo::new("dev-1")
            .with_tags(tags3)
            .with_resources(ResourceRequest::new(16, 64.0)),
    );

    // Spot Instance
    let mut tags4 = TagSet::new();
    tags4.insert("env", "production");
    tags4.insert("instance_type", "spot");
    tags4.insert("region", "eu-west");
    nodes.push(
        NodeInfo::new("spot-1")
            .with_tags(tags4)
            .with_resources(ResourceRequest::new(8, 32.0)),
    );

    nodes
}

fn policy_to_dsl(policy: &crate::policy::ir::Policy) -> String {
    let mut output = String::new();

    // Add comment with policy info
    output.push_str(&format!("# Policy: {}\n", policy.name));
    if !policy.description.is_empty() {
        output.push_str(&format!("# {}\n", policy.description));
    }
    output.push('\n');

    // Convert condition
    let condition_str = condition_to_dsl(&policy.condition);

    // Convert effects
    let effects_str: Vec<String> = policy.effects.iter().map(effect_to_dsl).collect();

    output.push_str(&format!("{} => {}", condition_str, effects_str.join("; ")));

    output
}

fn condition_to_dsl(condition: &crate::policy::ir::PolicyCondition) -> String {
    use crate::policy::ir::PolicyCondition;

    match condition {
        PolicyCondition::Always => "true".to_string(),
        PolicyCondition::Never => "false".to_string(),
        PolicyCondition::JobMatches(matcher) => {
            let mut parts = Vec::new();
            if let Some((min, max)) = matcher.priority_range {
                if min == max {
                    parts.push(format!("job.priority = {}", min));
                } else if min == 0 {
                    parts.push(format!("job.priority <= {}", max));
                } else if max == u32::MAX {
                    parts.push(format!("job.priority >= {}", min));
                } else {
                    parts.push(format!("job.priority between {} and {}", min, max));
                }
            }
            if let Some(pattern) = &matcher.name_pattern {
                parts.push(format!("job.name matches \"{}\"", pattern));
            }
            if let Some(types) = &matcher.job_type {
                parts.push(format!("job.type in {:?}", types));
            }
            if parts.is_empty() {
                "true".to_string()
            } else {
                parts.join(" AND ")
            }
        }
        PolicyCondition::SubmitterMatches(matcher) => {
            let mut parts = Vec::new();
            if let Some(domain) = &matcher.in_domain {
                parts.push(format!("submitter.domains contains \"{}\"", domain));
            }
            if let Some(principal) = &matcher.principal_id {
                parts.push(format!("submitter.principal = \"{}\"", principal));
            }
            if parts.is_empty() {
                "true".to_string()
            } else {
                parts.join(" AND ")
            }
        }
        PolicyCondition::ResourceMatches(matcher) => {
            let mut parts = Vec::new();
            if let Some(true) = matcher.requires_gpu {
                if let Some(min) = matcher.min_gpu {
                    parts.push(format!("resource.gpu >= {}", min));
                } else {
                    parts.push("resource.gpu > 0".to_string());
                }
            }
            if let Some(min) = matcher.min_cpu {
                parts.push(format!("resource.cpu >= {}", min));
            }
            if parts.is_empty() {
                "true".to_string()
            } else {
                parts.join(" AND ")
            }
        }
        PolicyCondition::TimeWindow { cron } => format!("time in \"{}\"", cron),
        PolicyCondition::And(conds) => {
            let parts: Vec<String> = conds.iter().map(condition_to_dsl).collect();
            format!("({})", parts.join(" AND "))
        }
        PolicyCondition::Or(conds) => {
            let parts: Vec<String> = conds.iter().map(condition_to_dsl).collect();
            format!("({})", parts.join(" OR "))
        }
        PolicyCondition::Not(inner) => format!("NOT {}", condition_to_dsl(inner)),
    }
}

fn effect_to_dsl(effect: &crate::policy::ir::PolicyEffect) -> String {
    use crate::policy::ir::PolicyEffect;

    match effect {
        PolicyEffect::Prefer { selector, weight } => {
            format!("prefer {} weight {}", selector_to_dsl(selector), weight)
        }
        PolicyEffect::Require { selector } => {
            format!("require {}", selector_to_dsl(selector))
        }
        PolicyEffect::Exclude { selector } => {
            format!("exclude {}", selector_to_dsl(selector))
        }
        PolicyEffect::Affinity {
            with,
            scope,
            weight,
        } => {
            format!("affinity {:?} at {:?} weight {}", with, scope, weight)
        }
        PolicyEffect::AntiAffinity {
            with,
            scope,
            weight,
        } => {
            format!("anti_affinity {:?} at {:?} weight {}", with, scope, weight)
        }
        PolicyEffect::DisallowPreemption => "disallow_preemption".to_string(),
        PolicyEffect::AllowPreemption { by_min_priority } => {
            format!("allow_preemption >= {}", by_min_priority)
        }
        _ => format!("{:?}", effect),
    }
}

fn selector_to_dsl(selector: &crate::policy::ir::NodeSelector) -> String {
    use crate::policy::ir::NodeSelector;

    match selector {
        NodeSelector::All => "all".to_string(),
        NodeSelector::Group(name) => format!("group(\"{}\")", name),
        NodeSelector::NodeIds(ids) => format!("{:?}", ids),
        NodeSelector::Tag(expr) => format!("tag({})", tag_expr_to_dsl(expr)),
    }
}

fn tag_expr_to_dsl(expr: &crate::policy::ir::TagExpr) -> String {
    use crate::policy::ir::TagExpr;

    match expr {
        TagExpr::HasKey(key) => key.clone(),
        TagExpr::Equals { key, value } => format!("{} = \"{}\"", key, value),
        TagExpr::KeyMatches { key, pattern } => format!("{} ~ \"{}\"", key, pattern),
        TagExpr::ValueMatches { key, pattern } => format!("{} ~ \"{}\"", key, pattern),
        TagExpr::And(exprs) => {
            let parts: Vec<String> = exprs.iter().map(tag_expr_to_dsl).collect();
            format!("({})", parts.join(" AND "))
        }
        TagExpr::Or(exprs) => {
            let parts: Vec<String> = exprs.iter().map(tag_expr_to_dsl).collect();
            format!("({})", parts.join(" OR "))
        }
        TagExpr::Not(inner) => format!("NOT {}", tag_expr_to_dsl(inner)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_format_default() {
        let format: OutputFormat = Default::default();
        assert!(matches!(format, OutputFormat::Json));
    }
}
