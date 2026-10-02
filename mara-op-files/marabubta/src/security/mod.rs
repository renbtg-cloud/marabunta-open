// Marabunta - Licensed under the MIT License.
//! Security module for Marabunta Compute
//!
//! This module provides comprehensive security features for the distributed
//! computing system including:
//!
//! - [`tls`] - TLS configuration and context management using rustls
//! - [`auth`] - API token authentication with permission levels
//! - [`node_auth`] - Node-to-node authentication with mutual TLS
//! - [`errors`] - Security-related error types
//!
//! # Architecture
//!
//! The security system is designed around a Coordinator -> Master -> Worker
//! hierarchy where:
//!
//! - **Coordinators** issue registration tokens for new nodes
//! - **Masters** register with coordinators using registration tokens
//! - **Workers** register with masters using registration tokens
//! - **API clients** authenticate using Bearer tokens
//!
//! # TLS Configuration
//!
//! TLS can be configured in three modes:
//!
//! 1. **Production**: Full certificate validation with CA chain
//! 2. **Dev/Self-signed**: Accepts self-signed certificates
//! 3. **Disabled**: Plain TCP (not recommended for production)
//!
//! # Example Usage
//!
//! ```rust,no_run
//! use marabunta_compute::security::{
//!     TlsConfig, TokenManager, Permission, NodeAuthenticator,
//! };
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create TLS configuration
//!     let tls_config = TlsConfig::builder()
//!         .cert_path("/etc/marabunta/certs/server.crt")
//!         .key_path("/etc/marabunta/certs/server.key")
//!         .ca_path("/etc/marabunta/certs/ca.crt")
//!         .build()?;
//!
//!     // Create server TLS context
//!     let server_config = tls_config.server_config()?;
//!
//!     // Create token manager for API authentication
//!     let token_manager = TokenManager::new();
//!     let (token, _) = token_manager.create_token(
//!         "admin-user",
//!         Permission::Admin,
//!         None,
//!     ).await?;
//!
//!     // Create node authenticator for cluster communication
//!     let node_auth = NodeAuthenticator::new();
//!
//!     Ok(())
//! }
//! ```
//!
//! # Permission Levels
//!
//! - **Admin**: Full access to all operations
//! - **Operator**: Can manage jobs and view cluster state
//! - **User**: Can submit and manage own jobs
//! - **ReadOnly**: Can only view status and results
//!
//! # Thread Safety
//!
//! All components are thread-safe and designed for concurrent async access.

pub mod auth;
pub mod errors;
pub mod middleware;
pub mod node_auth;
pub mod rbac;
pub mod tls;

// Re-export commonly used types
pub use auth::{AuthToken, AuthTokenInfo, Permission, TokenManager, TokenStore, TokenValidation};
pub use errors::{SecurityError, SecurityResult};
pub use middleware::{AuthLayer, AuthState, RequireAuth};
pub use node_auth::{
    NodeAuthenticator, NodeCredentials, NodeIdentity, NodeRegistrationToken, NodeVerification,
};
pub use tls::{TlsConfig, TlsConfigBuilder, TlsMode};
