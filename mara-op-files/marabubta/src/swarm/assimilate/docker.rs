// Marabunta - Licensed under the MIT License.
use serde_json::{json, Value};
use std::collections::HashMap;

/// Translates a Docker Compose YAML AST into a baseline Marabunta Swarm Job.
/// Maps containers to standard WASM payloads. (Enterprise DiLoCo/Isomorphic features disabled).
pub fn assimilate(yaml_content: &str) -> Result<Value, String> {
    let mut parsed: serde_yaml::Value = serde_yaml::from_str(yaml_content)
        .map_err(|e| format!("Invalid Docker Compose YAML: {}", e))?;
    
    let services = parsed.get_mut("services")
        .and_then(|s| s.as_mapping_mut())
        .ok_or("No 'services' block found in Docker Compose configuration.")?;
    
    let mut tasks = Vec::new();
    let mut cel_constraints = Vec::new();
    
    for (name, config) in services.iter() {
        let name_str = name.as_str().unwrap_or("unknown_service");
        let image = config.get("image").and_then(|i| i.as_str()).unwrap_or("unknown_image");
        
        // Introspect Resource Limits
        if let Some(deploy) = config.get("deploy") {
            if let Some(resources) = deploy.get("resources") {
                if let Some(limits) = resources.get("limits") {
                    if let Some(cpus) = limits.get("cpus") {
                        cel_constraints.push(format!("node.cpu_cores >= {}", cpus.as_str().unwrap_or("1")));
                    }
                    if let Some(memory) = limits.get("memory") {
                        let mem_str = memory.as_str().unwrap_or("0");
                        if mem_str.to_lowercase().ends_with('g') || mem_str.to_lowercase().ends_with("gb") {
                            let val: f64 = mem_str.trim_end_matches(|c: char| !c.is_numeric()).parse().unwrap_or(0.0);
                            cel_constraints.push(format!("node.ram_total_gb >= {}", val));
                        }
                    }
                }
            }
        }
        
        let mut dependencies = Vec::new();
        if let Some(deps) = config.get("depends_on") {
            if let Some(dep_seq) = deps.as_sequence() {
                for d in dep_seq {
                    if let Some(d_str) = d.as_str() { dependencies.push(d_str.to_string()); }
                }
            } else if let Some(dep_map) = deps.as_mapping() {
                for (k, _) in dep_map {
                    if let Some(k_str) = k.as_str() { dependencies.push(k_str.to_string()); }
                }
            }
        }
        
        // Standard mapping: no fake DiLoCo or Redis logic
        tasks.push(json!({
            "type": "wasm_service",
            "name": name_str,
            "original_image": image,
            "description": format!("Mapped container '{}' to baseline WASM sandbox", name_str),
            "depends_on": dependencies
        }));
    }
    
    let dag_expr = if cel_constraints.is_empty() {
        "true".to_string()
    } else {
        let mut unique: Vec<String> = cel_constraints.into_iter().collect::<std::collections::HashSet<_>>().into_iter().collect();
        unique.sort();
        unique.join(" && ")
    };

    Ok(json!({
        "name": "assimilated-docker-compose",
        "tasks": tasks,
        "requirements": {
            "advanced_placement_dag": dag_expr
        }
    }))
}