// Marabunta - Licensed under the MIT License.
use serde_json::{json, Value};

/// Introspects Terraform HCL files to map EC2 definitions into baseline Kademlia Bidding constraints.
/// (Enterprise KMS / Highestsec features disabled).
pub fn assimilate(tf_content: &str) -> Result<Value, String> {
    let parsed: Value = hcl::from_str(tf_content)
        .map_err(|e| format!("Invalid Terraform HCL: {}", e))?;
    
    let mut tasks = Vec::new();
    let mut cel_constraints = Vec::new();
    let mut extracted_resources = 0;
    
    if let Some(resources) = parsed.get("resource").and_then(|r| r.as_object()) {
        for (res_type, res_instances) in resources {
            if let Some(instances) = res_instances.as_object() {
                for (res_name, res_config) in instances {
                    if res_type == "aws_instance" {
                        extracted_resources += 1;
                        
                        if let Some(instance_type) = res_config.get("instance_type").and_then(|t| t.as_str()) {
                            if instance_type.starts_with("p3") || instance_type.starts_with("p4") || instance_type.starts_with("g4") {
                                cel_constraints.push("node.has_gpu == true".to_string());
                            } else if instance_type.starts_with("c5") || instance_type.starts_with("c6") {
                                cel_constraints.push("node.cpu_cores >= 8".to_string());
                            }
                        }
                        
                        tasks.push(json!({
                            "type": "provision_bidding",
                            "original_resource": format!("{}.{}", res_type, res_name),
                            "description": format!("Mapped AWS EC2 '{}' to standard Swarm Spot Market Bid", res_name)
                        }));
                    } else if res_type == "aws_kms_key" {
                        // Honest failure for enterprise feature
                        return Err(format!("Assimilation of aws_kms_key '{}' requires the Highestsec Enterprise FHE Engine, which is not available in this build.", res_name));
                    }
                }
            } else if res_instances.is_array() {
                 if res_type == "aws_instance" {
                     extracted_resources += 1;
                     tasks.push(json!({
                            "type": "provision_bidding",
                            "original_resource": res_type,
                            "description": format!("Mapped AWS resource '{}' to Swarm Spot Market Bid", res_type)
                     }));
                 }
            }
        }
    }
    
    if extracted_resources == 0 {
        return Err("No supported Terraform resources found for baseline assimilation (looking for `aws_instance`).".to_string());
    }

    let dag_expr = if cel_constraints.is_empty() {
        "true".to_string()
    } else {
        let mut unique: Vec<String> = cel_constraints.into_iter().collect::<std::collections::HashSet<_>>().into_iter().collect();
        unique.sort();
        unique.join(" && ")
    };

    Ok(json!({
        "name": "assimilated-terraform-infrastructure",
        "tasks": tasks,
        "requirements": {
            "advanced_placement_dag": dag_expr
        }
    }))
}