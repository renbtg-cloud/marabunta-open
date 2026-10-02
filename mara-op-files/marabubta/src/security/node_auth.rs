// Marabunta - Licensed under the MIT License.
//! Node-to-node authentication with mutual TLS.
//!
//! This module provides authentication for nodes in the Marabunta Compute cluster,
//! supporting mutual TLS verification and registration tokens for new nodes.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::errors::{SecurityError, SecurityResult};

/// Node type in the cluster hierarchy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    /// Coordinator node (cluster leader)
    Coordinator,
    /// Master node (regional scheduler)
    Master,
    /// Worker node (task executor)
    Worker,
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::Coordinator => write!(f, "coordinator"),
            NodeType::Master => write!(f, "master"),
            NodeType::Worker => write!(f, "worker"),
        }
    }
}

/// Identity information for a registered node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeIdentity {
    /// Unique node identifier
    pub id: String,
    /// Node type
    pub node_type: NodeType,
    /// Common Name from the node's certificate
    pub certificate_cn: Option<String>,
    /// SHA-256 fingerprint of the node's certificate
    pub certificate_fingerprint: Option<String>,
    /// Network address of the node
    pub address: String,
    /// Region the node belongs to (for masters and workers)
    pub region: Option<String>,
    /// When the node was registered
    pub registered_at: DateTime<Utc>,
    /// Last time the node was verified
    pub last_verified_at: Option<DateTime<Utc>>,
    /// Node status
    pub status: NodeStatus,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Status of a registered node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    /// Node is pending verification
    Pending,
    /// Node is active and verified
    Active,
    /// Node is suspended (temporarily disabled)
    Suspended,
    /// Node has been removed from the cluster
    Removed,
}

/// Registration token for new nodes joining the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegistrationToken {
    /// Unique token identifier
    pub id: String,
    /// Token type (determines what kind of node can use it)
    pub allowed_node_type: NodeType,
    /// When the token was created
    pub created_at: DateTime<Utc>,
    /// When the token expires
    pub expires_at: DateTime<Utc>,
    /// Maximum number of uses (None = unlimited)
    pub max_uses: Option<u32>,
    /// Current number of uses
    pub use_count: u32,
    /// Region restriction (if any)
    pub allowed_region: Option<String>,
    /// Who created this token
    pub created_by: String,
    /// Whether the token has been revoked
    pub revoked: bool,
    /// Notes/description
    pub notes: Option<String>,
}

impl NodeRegistrationToken {
    /// Check if the token is valid for use
    pub fn is_valid(&self) -> bool {
        !self.revoked && !self.is_expired() && !self.is_exhausted()
    }

    /// Check if the token is expired
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }

    /// Check if the token has reached its maximum uses
    pub fn is_exhausted(&self) -> bool {
        if let Some(max) = self.max_uses {
            self.use_count >= max
        } else {
            false
        }
    }

    /// Check if this token allows the given node type
    pub fn allows_node_type(&self, node_type: NodeType) -> bool {
        self.allowed_node_type == node_type
    }

    /// Check if this token allows the given region
    pub fn allows_region(&self, region: Option<&str>) -> bool {
        match (&self.allowed_region, region) {
            (None, _) => true,
            (Some(allowed), Some(actual)) => allowed == actual,
            (Some(_), None) => false,
        }
    }
}

/// Credentials presented by a node for authentication
#[derive(Debug, Clone)]
pub struct NodeCredentials {
    /// Registration token (for initial registration)
    pub registration_token: Option<String>,
    /// Node ID (for already registered nodes)
    pub node_id: Option<String>,
    /// Certificate Common Name
    pub certificate_cn: Option<String>,
    /// Certificate fingerprint (SHA-256)
    pub certificate_fingerprint: Option<String>,
    /// Network address the node is connecting from
    pub address: String,
    /// Requested node type
    pub node_type: NodeType,
    /// Requested region
    pub region: Option<String>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Result of node verification
#[derive(Debug, Clone)]
pub enum NodeVerification {
    /// Node is verified and authenticated
    Verified(NodeIdentity),
    /// Node needs to register first
    NeedsRegistration,
    /// Registration token is invalid
    InvalidRegistrationToken,
    /// Registration token is expired
    ExpiredRegistrationToken,
    /// Certificate mismatch
    CertificateMismatch,
    /// Node is suspended
    NodeSuspended,
    /// Node is not found
    NodeNotFound,
    /// Other verification failure
    Failed(String),
}

impl NodeVerification {
    /// Check if verification was successful
    pub fn is_verified(&self) -> bool {
        matches!(self, NodeVerification::Verified(_))
    }

    /// Get the node identity if verified
    pub fn identity(&self) -> Option<&NodeIdentity> {
        match self {
            NodeVerification::Verified(identity) => Some(identity),
            _ => None,
        }
    }

    /// Convert to a Result
    pub fn into_result(self) -> SecurityResult<NodeIdentity> {
        match self {
            NodeVerification::Verified(identity) => Ok(identity),
            NodeVerification::NeedsRegistration => Err(SecurityError::NodeNotRegistered(
                "Node needs to register".to_string(),
            )),
            NodeVerification::InvalidRegistrationToken => {
                Err(SecurityError::InvalidRegistrationToken)
            }
            NodeVerification::ExpiredRegistrationToken => {
                Err(SecurityError::RegistrationTokenExpired)
            }
            NodeVerification::CertificateMismatch => Err(SecurityError::NodeVerificationFailed(
                "Certificate mismatch".to_string(),
            )),
            NodeVerification::NodeSuspended => Err(SecurityError::NodeVerificationFailed(
                "Node is suspended".to_string(),
            )),
            NodeVerification::NodeNotFound => Err(SecurityError::NodeNotRegistered(
                "Node not found".to_string(),
            )),
            NodeVerification::Failed(msg) => Err(SecurityError::NodeVerificationFailed(msg)),
        }
    }
}

/// Node authenticator for managing cluster node authentication
pub struct NodeAuthenticator {
    /// Registered nodes by ID
    nodes: RwLock<HashMap<String, NodeIdentity>>,
    /// Nodes by certificate fingerprint for quick lookup
    nodes_by_fingerprint: RwLock<HashMap<String, String>>,
    /// Registration tokens by hash
    registration_tokens: RwLock<HashMap<String, NodeRegistrationToken>>,
    /// Token prefix for registration tokens
    token_prefix: String,
}

impl NodeAuthenticator {
    /// Create a new node authenticator
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            nodes_by_fingerprint: RwLock::new(HashMap::new()),
            registration_tokens: RwLock::new(HashMap::new()),
            token_prefix: "marabunta_node_".to_string(),
        }
    }

    /// Generate a secure random token
    fn generate_token_secret(&self) -> String {
        let mut rng = rand::thread_rng();
        let random_bytes: [u8; 32] = rng.gen();
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            random_bytes,
        );
        format!("{}{}", self.token_prefix, encoded)
    }

    /// Hash a token for storage
    fn hash_token(&self, token: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        let result = hasher.finalize();
        hex::encode(result)
    }

    /// Generate a unique node ID
    fn generate_node_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    /// Create a registration token for new nodes
    pub fn create_registration_token(
        &self,
        node_type: NodeType,
        expires_in: Duration,
        max_uses: Option<u32>,
        allowed_region: Option<String>,
        created_by: &str,
        notes: Option<String>,
    ) -> (String, NodeRegistrationToken) {
        let secret = self.generate_token_secret();
        let hash = self.hash_token(&secret);

        let token = NodeRegistrationToken {
            id: uuid::Uuid::new_v4().to_string(),
            allowed_node_type: node_type,
            created_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::from_std(expires_in).unwrap(),
            max_uses,
            use_count: 0,
            allowed_region,
            created_by: created_by.to_string(),
            revoked: false,
            notes,
        };

        self.registration_tokens.write().insert(hash, token.clone());

        (secret, token)
    }

    /// Validate a registration token
    pub fn validate_registration_token(
        &self,
        token_secret: &str,
        node_type: NodeType,
        region: Option<&str>,
    ) -> SecurityResult<NodeRegistrationToken> {
        if !token_secret.starts_with(&self.token_prefix) {
            return Err(SecurityError::InvalidRegistrationToken);
        }

        let hash = self.hash_token(token_secret);
        let tokens = self.registration_tokens.read();

        match tokens.get(&hash) {
            Some(token) => {
                if token.revoked {
                    return Err(SecurityError::InvalidRegistrationToken);
                }
                if token.is_expired() {
                    return Err(SecurityError::RegistrationTokenExpired);
                }
                if token.is_exhausted() {
                    return Err(SecurityError::RegistrationTokenUsed);
                }
                if !token.allows_node_type(node_type) {
                    return Err(SecurityError::InvalidRegistrationToken);
                }
                if !token.allows_region(region) {
                    return Err(SecurityError::InvalidRegistrationToken);
                }
                Ok(token.clone())
            }
            None => Err(SecurityError::InvalidRegistrationToken),
        }
    }

    /// Register a new node with a registration token
    pub fn register_node(&self, credentials: &NodeCredentials) -> SecurityResult<NodeIdentity> {
        let token_secret = credentials
            .registration_token
            .as_ref()
            .ok_or(SecurityError::InvalidRegistrationToken)?;

        // Validate the token
        let _token = self.validate_registration_token(
            token_secret,
            credentials.node_type,
            credentials.region.as_deref(),
        )?;

        // Increment token use count
        let hash = self.hash_token(token_secret);
        if let Some(token) = self.registration_tokens.write().get_mut(&hash) {
            token.use_count += 1;
        }

        // Create node identity
        let node_id = self.generate_node_id();
        let identity = NodeIdentity {
            id: node_id.clone(),
            node_type: credentials.node_type,
            certificate_cn: credentials.certificate_cn.clone(),
            certificate_fingerprint: credentials.certificate_fingerprint.clone(),
            address: credentials.address.clone(),
            region: credentials.region.clone(),
            registered_at: Utc::now(),
            last_verified_at: Some(Utc::now()),
            status: NodeStatus::Active,
            metadata: credentials.metadata.clone(),
        };

        // Store the node
        self.nodes.write().insert(node_id.clone(), identity.clone());

        // Index by certificate fingerprint if available
        if let Some(ref fingerprint) = credentials.certificate_fingerprint {
            self.nodes_by_fingerprint
                .write()
                .insert(fingerprint.clone(), node_id);
        }

        Ok(identity)
    }

    /// Verify an already registered node
    pub fn verify_node(&self, credentials: &NodeCredentials) -> NodeVerification {
        // First, try to find by node ID
        if let Some(ref node_id) = credentials.node_id {
            let identity = {
                let nodes = self.nodes.read();
                nodes.get(node_id).cloned()
            };
            if let Some(identity) = identity {
                return self.verify_identity(&identity, credentials);
            }
        }

        // Try to find by certificate fingerprint
        if let Some(ref fingerprint) = credentials.certificate_fingerprint {
            let node_id = {
                let nodes_by_fp = self.nodes_by_fingerprint.read();
                nodes_by_fp.get(fingerprint).cloned()
            };
            if let Some(node_id) = node_id {
                let identity = {
                    let nodes = self.nodes.read();
                    nodes.get(&node_id).cloned()
                };
                if let Some(identity) = identity {
                    return self.verify_identity(&identity, credentials);
                }
            }
        }

        // Node not found - needs to register
        if credentials.registration_token.is_some() {
            // Has registration token, should register
            NodeVerification::NeedsRegistration
        } else {
            NodeVerification::NodeNotFound
        }
    }

    /// Verify a node's identity against provided credentials
    fn verify_identity(
        &self,
        identity: &NodeIdentity,
        credentials: &NodeCredentials,
    ) -> NodeVerification {
        // Check node status
        match identity.status {
            NodeStatus::Suspended => return NodeVerification::NodeSuspended,
            NodeStatus::Removed => return NodeVerification::NodeNotFound,
            NodeStatus::Pending => {
                // Pending nodes can verify if they provide matching certificate
            }
            NodeStatus::Active => {}
        }

        // Verify certificate fingerprint if we have one stored
        if let Some(ref stored_fp) = identity.certificate_fingerprint {
            if let Some(ref provided_fp) = credentials.certificate_fingerprint {
                if stored_fp != provided_fp {
                    return NodeVerification::CertificateMismatch;
                }
            }
        }

        // Verify node type matches
        if identity.node_type != credentials.node_type {
            return NodeVerification::Failed("Node type mismatch".to_string());
        }

        // Update last verified time
        let mut updated_identity = identity.clone();
        updated_identity.last_verified_at = Some(Utc::now());
        self.nodes
            .write()
            .insert(identity.id.clone(), updated_identity.clone());

        NodeVerification::Verified(updated_identity)
    }

    /// Get a node by ID
    pub fn get_node(&self, node_id: &str) -> Option<NodeIdentity> {
        self.nodes.read().get(node_id).cloned()
    }

    /// Get a node by certificate fingerprint
    pub fn get_node_by_fingerprint(&self, fingerprint: &str) -> Option<NodeIdentity> {
        let node_id = self.nodes_by_fingerprint.read().get(fingerprint).cloned()?;
        self.nodes.read().get(&node_id).cloned()
    }

    /// List all nodes
    pub fn list_nodes(&self) -> Vec<NodeIdentity> {
        self.nodes.read().values().cloned().collect()
    }

    /// List nodes by type
    pub fn list_nodes_by_type(&self, node_type: NodeType) -> Vec<NodeIdentity> {
        self.nodes
            .read()
            .values()
            .filter(|n| n.node_type == node_type)
            .cloned()
            .collect()
    }

    /// Suspend a node
    pub fn suspend_node(&self, node_id: &str) -> SecurityResult<()> {
        let mut nodes = self.nodes.write();
        match nodes.get_mut(node_id) {
            Some(node) => {
                node.status = NodeStatus::Suspended;
                Ok(())
            }
            None => Err(SecurityError::NodeNotRegistered(node_id.to_string())),
        }
    }

    /// Reactivate a suspended node
    pub fn reactivate_node(&self, node_id: &str) -> SecurityResult<()> {
        let mut nodes = self.nodes.write();
        match nodes.get_mut(node_id) {
            Some(node) => {
                if node.status == NodeStatus::Suspended {
                    node.status = NodeStatus::Active;
                    Ok(())
                } else {
                    Err(SecurityError::NodeVerificationFailed(
                        "Node is not suspended".to_string(),
                    ))
                }
            }
            None => Err(SecurityError::NodeNotRegistered(node_id.to_string())),
        }
    }

    /// Remove a node from the cluster
    pub fn remove_node(&self, node_id: &str) -> SecurityResult<()> {
        let mut nodes = self.nodes.write();
        match nodes.get_mut(node_id) {
            Some(node) => {
                node.status = NodeStatus::Removed;

                // Remove from fingerprint index
                if let Some(ref fingerprint) = node.certificate_fingerprint {
                    self.nodes_by_fingerprint.write().remove(fingerprint);
                }

                Ok(())
            }
            None => Err(SecurityError::NodeNotRegistered(node_id.to_string())),
        }
    }

    /// Revoke a registration token
    pub fn revoke_registration_token(&self, token_secret: &str) -> SecurityResult<bool> {
        let hash = self.hash_token(token_secret);
        let mut tokens = self.registration_tokens.write();

        match tokens.get_mut(&hash) {
            Some(token) => {
                token.revoked = true;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// List all registration tokens
    pub fn list_registration_tokens(&self) -> Vec<NodeRegistrationToken> {
        self.registration_tokens.read().values().cloned().collect()
    }

    /// Clean up expired registration tokens
    pub fn cleanup_expired_tokens(&self) -> u32 {
        let mut tokens = self.registration_tokens.write();
        let before_count = tokens.len();

        tokens.retain(|_, token| !token.is_expired());

        (before_count - tokens.len()) as u32
    }

    /// Get statistics about registered nodes
    pub fn stats(&self) -> NodeAuthStats {
        let nodes = self.nodes.read();
        let tokens = self.registration_tokens.read();

        let mut stats = NodeAuthStats::default();

        for node in nodes.values() {
            stats.total_nodes += 1;
            match node.node_type {
                NodeType::Coordinator => stats.coordinators += 1,
                NodeType::Master => stats.masters += 1,
                NodeType::Worker => stats.workers += 1,
            }
            match node.status {
                NodeStatus::Active => stats.active_nodes += 1,
                NodeStatus::Suspended => stats.suspended_nodes += 1,
                NodeStatus::Pending => stats.pending_nodes += 1,
                NodeStatus::Removed => {}
            }
        }

        stats.total_tokens = tokens.len() as u32;
        stats.valid_tokens = tokens.values().filter(|t| t.is_valid()).count() as u32;

        stats
    }
}

impl Default for NodeAuthenticator {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about node authentication
#[derive(Debug, Clone, Default)]
pub struct NodeAuthStats {
    pub total_nodes: u32,
    pub active_nodes: u32,
    pub suspended_nodes: u32,
    pub pending_nodes: u32,
    pub coordinators: u32,
    pub masters: u32,
    pub workers: u32,
    pub total_tokens: u32,
    pub valid_tokens: u32,
}

// Hex encoding helper
mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes
            .as_ref()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect()
    }
}

/// Calculate SHA-256 fingerprint of a DER-encoded certificate
pub fn certificate_fingerprint(cert_der: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(cert_der);
    let result = hasher.finalize();
    hex::encode(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_type_display() {
        assert_eq!(NodeType::Coordinator.to_string(), "coordinator");
        assert_eq!(NodeType::Master.to_string(), "master");
        assert_eq!(NodeType::Worker.to_string(), "worker");
    }

    #[test]
    fn test_registration_token_validity() {
        let token = NodeRegistrationToken {
            id: "test".to_string(),
            allowed_node_type: NodeType::Worker,
            created_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            max_uses: Some(5),
            use_count: 0,
            allowed_region: None,
            created_by: "admin".to_string(),
            revoked: false,
            notes: None,
        };

        assert!(token.is_valid());
        assert!(!token.is_expired());
        assert!(!token.is_exhausted());
    }

    #[test]
    fn test_registration_token_expired() {
        let token = NodeRegistrationToken {
            id: "test".to_string(),
            allowed_node_type: NodeType::Worker,
            created_at: Utc::now() - chrono::Duration::hours(2),
            expires_at: Utc::now() - chrono::Duration::hours(1),
            max_uses: None,
            use_count: 0,
            allowed_region: None,
            created_by: "admin".to_string(),
            revoked: false,
            notes: None,
        };

        assert!(!token.is_valid());
        assert!(token.is_expired());
    }

    #[test]
    fn test_registration_token_exhausted() {
        let token = NodeRegistrationToken {
            id: "test".to_string(),
            allowed_node_type: NodeType::Worker,
            created_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            max_uses: Some(5),
            use_count: 5,
            allowed_region: None,
            created_by: "admin".to_string(),
            revoked: false,
            notes: None,
        };

        assert!(!token.is_valid());
        assert!(token.is_exhausted());
    }

    #[test]
    fn test_registration_token_region_check() {
        let token = NodeRegistrationToken {
            id: "test".to_string(),
            allowed_node_type: NodeType::Worker,
            created_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            max_uses: None,
            use_count: 0,
            allowed_region: Some("us-east-1".to_string()),
            created_by: "admin".to_string(),
            revoked: false,
            notes: None,
        };

        assert!(token.allows_region(Some("us-east-1")));
        assert!(!token.allows_region(Some("us-west-1")));
        assert!(!token.allows_region(None));

        // Token without region restriction
        let unrestricted = NodeRegistrationToken {
            allowed_region: None,
            ..token
        };
        assert!(unrestricted.allows_region(Some("us-east-1")));
        assert!(unrestricted.allows_region(Some("us-west-1")));
        assert!(unrestricted.allows_region(None));
    }

    #[test]
    fn test_create_registration_token() {
        let auth = NodeAuthenticator::new();

        let (secret, token) = auth.create_registration_token(
            NodeType::Worker,
            Duration::from_secs(3600),
            Some(10),
            Some("us-east-1".to_string()),
            "admin",
            Some("Test token".to_string()),
        );

        assert!(secret.starts_with("marabunta_node_"));
        assert_eq!(token.allowed_node_type, NodeType::Worker);
        assert_eq!(token.max_uses, Some(10));
        assert_eq!(token.allowed_region, Some("us-east-1".to_string()));
        assert_eq!(token.created_by, "admin");
        assert!(token.is_valid());
    }

    #[test]
    fn test_register_node() {
        let auth = NodeAuthenticator::new();

        let (secret, _) = auth.create_registration_token(
            NodeType::Worker,
            Duration::from_secs(3600),
            Some(10),
            None,
            "admin",
            None,
        );

        let credentials = NodeCredentials {
            registration_token: Some(secret),
            node_id: None,
            certificate_cn: Some("worker-1.marabunta.local".to_string()),
            certificate_fingerprint: Some("abc123".to_string()),
            address: "192.168.1.100:7200".to_string(),
            node_type: NodeType::Worker,
            region: None,
            metadata: HashMap::new(),
        };

        let identity = auth.register_node(&credentials).unwrap();

        assert_eq!(identity.node_type, NodeType::Worker);
        assert_eq!(identity.address, "192.168.1.100:7200");
        assert_eq!(identity.status, NodeStatus::Active);
        assert_eq!(
            identity.certificate_cn,
            Some("worker-1.marabunta.local".to_string())
        );
    }

    #[test]
    fn test_verify_registered_node() {
        let auth = NodeAuthenticator::new();

        // Register a node first
        let (secret, _) = auth.create_registration_token(
            NodeType::Worker,
            Duration::from_secs(3600),
            None,
            None,
            "admin",
            None,
        );

        let reg_credentials = NodeCredentials {
            registration_token: Some(secret),
            node_id: None,
            certificate_cn: None,
            certificate_fingerprint: Some("fingerprint123".to_string()),
            address: "192.168.1.100:7200".to_string(),
            node_type: NodeType::Worker,
            region: None,
            metadata: HashMap::new(),
        };

        let identity = auth.register_node(&reg_credentials).unwrap();
        let node_id = identity.id.clone();

        // Now verify with node ID
        let verify_credentials = NodeCredentials {
            registration_token: None,
            node_id: Some(node_id),
            certificate_cn: None,
            certificate_fingerprint: Some("fingerprint123".to_string()),
            address: "192.168.1.100:7200".to_string(),
            node_type: NodeType::Worker,
            region: None,
            metadata: HashMap::new(),
        };

        let verification = auth.verify_node(&verify_credentials);
        assert!(verification.is_verified());

        // Verify by fingerprint
        let verify_by_fp = NodeCredentials {
            registration_token: None,
            node_id: None,
            certificate_cn: None,
            certificate_fingerprint: Some("fingerprint123".to_string()),
            address: "192.168.1.100:7200".to_string(),
            node_type: NodeType::Worker,
            region: None,
            metadata: HashMap::new(),
        };

        let verification = auth.verify_node(&verify_by_fp);
        assert!(verification.is_verified());
    }

    #[test]
    fn test_certificate_mismatch() {
        let auth = NodeAuthenticator::new();

        // Register a node
        let (secret, _) = auth.create_registration_token(
            NodeType::Worker,
            Duration::from_secs(3600),
            None,
            None,
            "admin",
            None,
        );

        let reg_credentials = NodeCredentials {
            registration_token: Some(secret),
            node_id: None,
            certificate_cn: None,
            certificate_fingerprint: Some("original_fingerprint".to_string()),
            address: "192.168.1.100:7200".to_string(),
            node_type: NodeType::Worker,
            region: None,
            metadata: HashMap::new(),
        };

        let identity = auth.register_node(&reg_credentials).unwrap();

        // Try to verify with different fingerprint
        let verify_credentials = NodeCredentials {
            registration_token: None,
            node_id: Some(identity.id),
            certificate_cn: None,
            certificate_fingerprint: Some("different_fingerprint".to_string()),
            address: "192.168.1.100:7200".to_string(),
            node_type: NodeType::Worker,
            region: None,
            metadata: HashMap::new(),
        };

        let verification = auth.verify_node(&verify_credentials);
        assert!(matches!(
            verification,
            NodeVerification::CertificateMismatch
        ));
    }

    #[test]
    fn test_suspend_and_reactivate_node() {
        let auth = NodeAuthenticator::new();

        // Register a node
        let (secret, _) = auth.create_registration_token(
            NodeType::Worker,
            Duration::from_secs(3600),
            None,
            None,
            "admin",
            None,
        );

        let credentials = NodeCredentials {
            registration_token: Some(secret),
            node_id: None,
            certificate_cn: None,
            certificate_fingerprint: None,
            address: "192.168.1.100:7200".to_string(),
            node_type: NodeType::Worker,
            region: None,
            metadata: HashMap::new(),
        };

        let identity = auth.register_node(&credentials).unwrap();
        let node_id = identity.id.clone();

        // Suspend
        auth.suspend_node(&node_id).unwrap();
        let node = auth.get_node(&node_id).unwrap();
        assert_eq!(node.status, NodeStatus::Suspended);

        // Verify should fail
        let verify_credentials = NodeCredentials {
            registration_token: None,
            node_id: Some(node_id.clone()),
            certificate_cn: None,
            certificate_fingerprint: None,
            address: "192.168.1.100:7200".to_string(),
            node_type: NodeType::Worker,
            region: None,
            metadata: HashMap::new(),
        };
        let verification = auth.verify_node(&verify_credentials);
        assert!(matches!(verification, NodeVerification::NodeSuspended));

        // Reactivate
        auth.reactivate_node(&node_id).unwrap();
        let node = auth.get_node(&node_id).unwrap();
        assert_eq!(node.status, NodeStatus::Active);
    }

    #[test]
    fn test_list_nodes_by_type() {
        let auth = NodeAuthenticator::new();

        // Register different node types
        for node_type in [NodeType::Master, NodeType::Master, NodeType::Worker] {
            let (secret, _) = auth.create_registration_token(
                node_type,
                Duration::from_secs(3600),
                None,
                None,
                "admin",
                None,
            );

            let credentials = NodeCredentials {
                registration_token: Some(secret),
                node_id: None,
                certificate_cn: None,
                certificate_fingerprint: None,
                address: "192.168.1.100:7200".to_string(),
                node_type,
                region: None,
                metadata: HashMap::new(),
            };

            auth.register_node(&credentials).unwrap();
        }

        let masters = auth.list_nodes_by_type(NodeType::Master);
        assert_eq!(masters.len(), 2);

        let workers = auth.list_nodes_by_type(NodeType::Worker);
        assert_eq!(workers.len(), 1);

        let coordinators = auth.list_nodes_by_type(NodeType::Coordinator);
        assert!(coordinators.is_empty());
    }

    #[test]
    fn test_revoke_registration_token() {
        let auth = NodeAuthenticator::new();

        let (secret, _) = auth.create_registration_token(
            NodeType::Worker,
            Duration::from_secs(3600),
            None,
            None,
            "admin",
            None,
        );

        // Revoke
        let revoked = auth.revoke_registration_token(&secret).unwrap();
        assert!(revoked);

        // Try to use revoked token
        let result = auth.validate_registration_token(&secret, NodeType::Worker, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_stats() {
        let auth = NodeAuthenticator::new();

        // Create tokens
        for _ in 0..3 {
            auth.create_registration_token(
                NodeType::Worker,
                Duration::from_secs(3600),
                None,
                None,
                "admin",
                None,
            );
        }

        // Register some nodes
        for i in 0..2 {
            let (secret, _) = auth.create_registration_token(
                NodeType::Worker,
                Duration::from_secs(3600),
                None,
                None,
                "admin",
                None,
            );

            let credentials = NodeCredentials {
                registration_token: Some(secret),
                node_id: None,
                certificate_cn: None,
                certificate_fingerprint: Some(format!("fp{}", i)),
                address: format!("192.168.1.{}:7200", i),
                node_type: NodeType::Worker,
                region: None,
                metadata: HashMap::new(),
            };

            auth.register_node(&credentials).unwrap();
        }

        let stats = auth.stats();
        assert_eq!(stats.total_nodes, 2);
        assert_eq!(stats.active_nodes, 2);
        assert_eq!(stats.workers, 2);
        assert!(stats.total_tokens >= 5); // At least 5 tokens created
    }

    #[test]
    fn test_certificate_fingerprint() {
        let cert_data = b"fake certificate data";
        let fingerprint = certificate_fingerprint(cert_data);

        // Should be a hex-encoded SHA-256 hash (64 characters)
        assert_eq!(fingerprint.len(), 64);
        assert!(fingerprint.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
