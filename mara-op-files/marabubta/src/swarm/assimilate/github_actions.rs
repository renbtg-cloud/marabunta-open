// Marabunta - Licensed under the MIT License.
use serde_yaml::Value as YamlValue;
use serde_json::{json, Value};

/// Translates a GitHub Actions CI pipeline into a sequence of shell execution chunks.
/// Converts the workflow `jobs` and `steps` into an interconnected execution DAG
/// that runs natively (serverless) on the Marabunta Swarm instead of GitHub Runners.
pub fn assimilate(yaml_content: &str) -> Result<Value, String> {
    let parsed: YamlValue = serde_yaml::from_str(yaml_content)
        .map_err(|e| format!("Invalid GitHub Actions YAML: {}", e))?;
    
    let jobs = parsed.get("jobs").and_then(|j| j.as_mapping())
        .ok_or("No 'jobs' block found in GitHub Actions YAML")?;
    
    let mut tasks = Vec::new();
    let mut global_cel = Vec::new();
    
    for (job_name, job_config) in jobs {
        let name_str = job_name.as_str().unwrap_or("unknown_job");
        
        // If it runs-on ubuntu-latest, we require Linux
        if let Some(runs_on) = job_config.get("runs-on").and_then(|r| r.as_str()) {
            if runs_on.contains("ubuntu") || runs_on.contains("linux") {
                global_cel.push("node.os == 'linux'".to_string());
            } else if runs_on.contains("windows") {
                global_cel.push("node.os == 'windows'".to_string());
            } else if runs_on.contains("mac") || runs_on.contains("macos") {
                global_cel.push("node.os == 'macos'".to_string());
            }
        }
        
        // Map steps to sequential shell execution payloads
        if let Some(steps) = job_config.get("steps").and_then(|s| s.as_sequence()) {
            for (i, step) in steps.iter().enumerate() {
                let step_name = step.get("name").and_then(|n| n.as_str()).unwrap_or_else(|| "unnamed_step");
                
                if let Some(run_cmd) = step.get("run").and_then(|r| r.as_str()) {
                    tasks.push(json!({
                        "type": "shell",
                        "command": "bash",
                        "args": ["-c", run_cmd],
                        "description": format!("CI Job '{}' Step {}: {}", name_str, i + 1, step_name),
                        "depends_on": if i > 0 { vec![format!("step_{}_{}", name_str, i)] } else { vec![] },
                        "id": format!("step_{}_{}", name_str, i + 1)
                    }));
                } else if let Some(uses) = step.get("uses").and_then(|u| u.as_str()) {
                    // Actions translate to WASM plugins theoretically
                    tasks.push(json!({
                        "type": "wasm_plugin",
                        "plugin_uri": uses,
                        "description": format!("Assimilated GitHub Action: {}", uses),
                        "depends_on": if i > 0 { vec![format!("step_{}_{}", name_str, i)] } else { vec![] },
                        "id": format!("step_{}_{}", name_str, i + 1)
                    }));
                }
            }
        }
    }
    
    if tasks.is_empty() {
        return Err("No executable steps found in GitHub Actions workflow.".to_string());
    }

    let dag_expr = if global_cel.is_empty() {
        "true".to_string()
    } else {
        let mut unique: Vec<String> = global_cel.into_iter().collect::<std::collections::HashSet<_>>().into_iter().collect();
        unique.sort();
        unique.join(" && ")
    };

    Ok(json!({
        "name": "assimilated-github-actions-pipeline",
        "tasks": tasks,
        "reduce_spec": "concat_stdout",
        "description": "Serverless CI/CD executed natively on the Swarm (No centralized runners required)",
        "requirements": {
            "advanced_placement_dag": dag_expr
        }
    }))
}
