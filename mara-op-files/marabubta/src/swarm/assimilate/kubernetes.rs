// Marabunta - Licensed under the MIT License.
use serde_json::{json, Value};

/// Translates a Kubernetes YAML manifest (Deployment or Pod) into a baseline Swarm Job.
/// (Enterprise DiLoCo/Isomorphic features disabled).
pub fn assimilate(yaml_content: &str) -> Result<Value, String> {
    let parsed: serde_yaml::Value = serde_yaml::from_str(yaml_content)
        .map_err(|e| format!("Invalid Kubernetes YAML: {}", e))?;
    
    let kind = parsed.get("kind").and_then(|k| k.as_str()).unwrap_or("");
    if kind != "Deployment" && kind != "Pod" {
        return Err(format!("Unsupported K8s kind: '{}'. Only Deployment and Pod are currently supported.", kind));
    }

    let metadata = parsed.get("metadata").and_then(|m| m.as_mapping());
    let name = metadata
        .and_then(|m| m.get(&serde_yaml::Value::String("name".into())))
        .and_then(|n| n.as_str())
        .unwrap_or("k8s-workload");

    let mut cel_constraints = Vec::new();
    let mut tasks = Vec::new();
    
    let spec = parsed.get("spec").and_then(|s| s.as_mapping());
    let template_spec = if kind == "Deployment" {
        spec.and_then(|s| s.get(&serde_yaml::Value::String("template".into())))
            .and_then(|t| t.as_mapping())
            .and_then(|t| t.get(&serde_yaml::Value::String("spec".into())))
            .and_then(|s| s.as_mapping())
    } else {
        spec
    };

    if let Some(pod_spec) = template_spec {
        if let Some(selectors) = pod_spec.get(&serde_yaml::Value::String("nodeSelector".into())).and_then(|s| s.as_mapping()) {
            for (k, v) in selectors {
                let key_str = k.as_str().unwrap_or("");
                let val_str = v.as_str().unwrap_or("");
                if key_str == "topology.kubernetes.io/region" || key_str == "failure-domain.beta.kubernetes.io/region" {
                    cel_constraints.push(format!("node.geo_region == '{}'", val_str));
                } else if key_str == "accelerator" || key_str == "cloud.google.com/gke-accelerator" {
                    cel_constraints.push("node.has_gpu == true".to_string());
                } else if key_str == "kubernetes.io/arch" && val_str == "arm64" {
                    cel_constraints.push("node.arch == 'aarch64'".to_string());
                }
            }
        }

        if let Some(containers) = pod_spec.get(&serde_yaml::Value::String("containers".into())).and_then(|c| c.as_sequence()) {
            for container in containers {
                let c_name = container.get("name").and_then(|n| n.as_str()).unwrap_or("container");
                let image = container.get("image").and_then(|i| i.as_str()).unwrap_or("unknown");
                
                if let Some(resources) = container.get("resources").and_then(|r| r.as_mapping()) {
                    if let Some(requests) = resources.get(&serde_yaml::Value::String("requests".into())).and_then(|r| r.as_mapping()) {
                        if let Some(mem) = requests.get(&serde_yaml::Value::String("memory".into())).and_then(|m| m.as_str()) {
                            if mem.ends_with("Gi") {
                                let val: f64 = mem.trim_end_matches("Gi").parse().unwrap_or(0.0);
                                cel_constraints.push(format!("node.ram_total_gb >= {}", val));
                            }
                        }
                        if let Some(cpu) = requests.get(&serde_yaml::Value::String("cpu".into())).and_then(|c| c.as_str()) {
                            cel_constraints.push(format!("node.cpu_cores >= {}", cpu.parse::<u32>().unwrap_or(1)));
                        }
                    }
                }

                // Standard mapping: no fake DiLoCo tensor parsing
                tasks.push(json!({
                    "type": "wasm_service",
                    "name": c_name,
                    "original_image": image,
                    "description": format!("Mapped K8s Pod '{}' to baseline WASM sandbox", c_name)
                }));
            }
        }
    }

    if tasks.is_empty() {
        return Err("No containers found in Kubernetes spec to assimilate.".to_string());
    }

    let dag_expr = if cel_constraints.is_empty() {
        "true".to_string()
    } else {
        let mut unique: Vec<String> = cel_constraints.into_iter().collect::<std::collections::HashSet<_>>().into_iter().collect();
        unique.sort();
        unique.join(" && ")
    };

    Ok(json!({
        "name": format!("assimilated-k8s-{}", name),
        "tasks": tasks,
        "requirements": {
            "advanced_placement_dag": dag_expr
        }
    }))
}