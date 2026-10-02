// Marabunta - Licensed under the MIT License.
pub mod docker;
pub mod kubernetes;
pub mod terraform;
pub mod github_actions;

use std::path::Path;

#[derive(Debug, PartialEq, Eq)]
pub enum IaCType {
    DockerCompose,
    Kubernetes,
    Terraform,
    GitHubActions,
    Unknown,
}

/// Introspects the file metadata to determine the legacy infrastructure format.
pub fn detect_iac_type(path: &Path) -> IaCType {
    let filename = path.file_name().unwrap_or_default().to_string_lossy().to_lowercase();
    let ext = path.extension().unwrap_or_default().to_string_lossy().to_lowercase();

    if filename.contains("docker-compose") || filename == "compose.yaml" || filename == "compose.yml" {
        IaCType::DockerCompose
    } else if filename.contains("deployment") || (ext == "yaml" && filename.contains("k8s")) || filename.contains("pod") {
        IaCType::Kubernetes
    } else if ext == "tf" {
        IaCType::Terraform
    } else if path.components().any(|c| c.as_os_str() == ".github") && (ext == "yml" || ext == "yaml") {
        IaCType::GitHubActions
    } else {
        IaCType::Unknown
    }
}
