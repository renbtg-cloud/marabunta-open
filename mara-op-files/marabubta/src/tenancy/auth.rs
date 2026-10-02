// Marabunta - Licensed under the MIT License.
//! Tenant Authentication
//!
//! This module provides authentication and authorization mechanisms for
//! multi-tenant access control, including tenant-scoped tokens and
//! cross-tenant access controls.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::errors::{TenancyError, TenancyResult};
use super::registry::TenantRegistry;
use super::types::{TenantId, TenantRole};

/// Simple hex encoding for signature generation.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Token type for tenant authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenType {
    /// Access token for API calls
    Access,
    /// Refresh token for getting new access tokens
    Refresh,
    /// Service token for inter-service communication
    Service,
    /// API key for machine-to-machine
    ApiKey,
}

/// Claims embedded in a tenant token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantTokenClaims {
    /// Token ID
    pub token_id: String,
    /// Token type
    pub token_type: TokenType,
    /// User ID
    pub user_id: String,
    /// Primary tenant ID (current context)
    pub tenant_id: TenantId,
    /// Additional tenant IDs the user can access
    pub accessible_tenants: Vec<TenantId>,
    /// Role in the primary tenant
    pub role: TenantRole,
    /// Scopes/permissions granted
    pub scopes: Vec<String>,
    /// When the token was issued
    pub issued_at: DateTime<Utc>,
    /// When the token expires
    pub expires_at: DateTime<Utc>,
    /// Custom claims
    pub custom_claims: HashMap<String, serde_json::Value>,
}

impl TenantTokenClaims {
    /// Creates new token claims.
    pub fn new(
        user_id: impl Into<String>,
        tenant_id: TenantId,
        role: TenantRole,
        token_type: TokenType,
        duration: Duration,
    ) -> Self {
        let now = Utc::now();
        Self {
            token_id: uuid::Uuid::new_v4().to_string(),
            token_type,
            user_id: user_id.into(),
            tenant_id,
            accessible_tenants: vec![tenant_id],
            role,
            scopes: Vec::new(),
            issued_at: now,
            expires_at: now + duration,
            custom_claims: HashMap::new(),
        }
    }

    /// Adds accessible tenants.
    pub fn with_accessible_tenants(mut self, tenants: Vec<TenantId>) -> Self {
        self.accessible_tenants = tenants;
        if !self.accessible_tenants.contains(&self.tenant_id) {
            self.accessible_tenants.push(self.tenant_id);
        }
        self
    }

    /// Adds scopes.
    pub fn with_scopes(mut self, scopes: Vec<String>) -> Self {
        self.scopes = scopes;
        self
    }

    /// Adds a custom claim.
    pub fn with_claim(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.custom_claims.insert(key.into(), value);
        self
    }

    /// Checks if the token is expired.
    pub fn is_expired(&self) -> bool {
        Utc::now() >= self.expires_at
    }

    /// Checks if the token has a specific scope.
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope || s == "*")
    }

    /// Checks if the token can access a tenant.
    pub fn can_access_tenant(&self, tenant_id: &TenantId) -> bool {
        self.accessible_tenants.contains(tenant_id)
    }
}

/// A tenant-scoped authentication token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantToken {
    /// The token string
    pub token: String,
    /// The claims
    pub claims: TenantTokenClaims,
}

impl TenantToken {
    /// Creates a new token from claims.
    pub fn new(claims: TenantTokenClaims, secret: &[u8]) -> Self {
        // Create a simple signed token (in production, use JWT or similar)
        let claims_json = serde_json::to_string(&claims).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(claims_json.as_bytes());
        hasher.update(secret);
        let signature = hex_encode(&hasher.finalize());

        let payload = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            claims_json.as_bytes(),
        );

        Self {
            token: format!("{}.{}", payload, &signature[..16]),
            claims,
        }
    }

    /// Validates and decodes a token.
    pub fn validate(token: &str, secret: &[u8]) -> Option<Self> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 2 {
            return None;
        }

        let payload_bytes =
            base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, parts[0])
                .ok()?;
        let claims_json = String::from_utf8(payload_bytes).ok()?;
        let claims: TenantTokenClaims = serde_json::from_str(&claims_json).ok()?;

        // Verify signature
        let mut hasher = Sha256::new();
        hasher.update(claims_json.as_bytes());
        hasher.update(secret);
        let expected_signature = hex_encode(&hasher.finalize());

        if &expected_signature[..16] != parts[1] {
            return None;
        }

        if claims.is_expired() {
            return None;
        }

        Some(Self {
            token: token.to_string(),
            claims,
        })
    }
}

/// Permissions that can be granted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    /// Read tenant details
    TenantRead,
    /// Write tenant details
    TenantWrite,
    /// Manage tenant members
    TenantManageMembers,
    /// Submit jobs
    JobSubmit,
    /// Read jobs
    JobRead,
    /// Cancel jobs
    JobCancel,
    /// Read nodes
    NodeRead,
    /// Manage quotas
    QuotaManage,
    /// Manage policies
    PolicyManage,
    /// All permissions
    Admin,
}

impl Permission {
    /// Returns the scope string for this permission.
    pub fn scope(&self) -> &'static str {
        match self {
            Permission::TenantRead => "tenant:read",
            Permission::TenantWrite => "tenant:write",
            Permission::TenantManageMembers => "tenant:manage_members",
            Permission::JobSubmit => "job:submit",
            Permission::JobRead => "job:read",
            Permission::JobCancel => "job:cancel",
            Permission::NodeRead => "node:read",
            Permission::QuotaManage => "quota:manage",
            Permission::PolicyManage => "policy:manage",
            Permission::Admin => "*",
        }
    }
}

/// Default permissions for each role.
pub fn default_permissions_for_role(role: TenantRole) -> Vec<Permission> {
    match role {
        TenantRole::Owner => vec![Permission::Admin],
        TenantRole::Admin => vec![
            Permission::TenantRead,
            Permission::TenantWrite,
            Permission::TenantManageMembers,
            Permission::JobSubmit,
            Permission::JobRead,
            Permission::JobCancel,
            Permission::NodeRead,
            Permission::QuotaManage,
            Permission::PolicyManage,
        ],
        TenantRole::Member => vec![
            Permission::TenantRead,
            Permission::JobSubmit,
            Permission::JobRead,
            Permission::JobCancel,
            Permission::NodeRead,
        ],
        TenantRole::Viewer => vec![
            Permission::TenantRead,
            Permission::JobRead,
            Permission::NodeRead,
        ],
    }
}

/// Cross-tenant access grant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossTenantGrant {
    /// Source tenant (grantor)
    pub source_tenant_id: TenantId,
    /// Target tenant (grantee)
    pub target_tenant_id: TenantId,
    /// Permissions granted
    pub permissions: Vec<Permission>,
    /// When the grant was created
    pub created_at: DateTime<Utc>,
    /// When the grant expires
    pub expires_at: Option<DateTime<Utc>>,
    /// Who created the grant
    pub created_by: String,
    /// Description of why the grant was created
    pub reason: String,
}

impl CrossTenantGrant {
    /// Creates a new cross-tenant grant.
    pub fn new(
        source_tenant_id: TenantId,
        target_tenant_id: TenantId,
        permissions: Vec<Permission>,
        created_by: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            source_tenant_id,
            target_tenant_id,
            permissions,
            created_at: Utc::now(),
            expires_at: None,
            created_by: created_by.into(),
            reason: reason.into(),
        }
    }

    /// Sets expiration.
    pub fn expires_at(mut self, when: DateTime<Utc>) -> Self {
        self.expires_at = Some(when);
        self
    }

    /// Sets expiration from duration.
    pub fn expires_in(mut self, duration: Duration) -> Self {
        self.expires_at = Some(Utc::now() + duration);
        self
    }

    /// Checks if the grant is valid.
    pub fn is_valid(&self) -> bool {
        self.expires_at.map_or(true, |exp| exp > Utc::now())
    }

    /// Checks if the grant includes a permission.
    pub fn has_permission(&self, permission: Permission) -> bool {
        self.permissions.contains(&permission) || self.permissions.contains(&Permission::Admin)
    }
}

/// Manager for tenant authentication and authorization.
pub struct TenantAuthManager {
    /// Tenant registry
    registry: Arc<TenantRegistry>,
    /// Token signing secret
    secret: Vec<u8>,
    /// Revoked token IDs
    revoked_tokens: Arc<RwLock<HashSet<String>>>,
    /// Cross-tenant grants (source -> target -> grant)
    cross_tenant_grants: Arc<RwLock<HashMap<(TenantId, TenantId), CrossTenantGrant>>>,
    /// Access token duration
    access_token_duration: Duration,
    /// Refresh token duration
    refresh_token_duration: Duration,
}

impl TenantAuthManager {
    /// Creates a new auth manager.
    pub fn new(registry: Arc<TenantRegistry>, secret: impl Into<Vec<u8>>) -> Self {
        Self {
            registry,
            secret: secret.into(),
            revoked_tokens: Arc::new(RwLock::new(HashSet::new())),
            cross_tenant_grants: Arc::new(RwLock::new(HashMap::new())),
            access_token_duration: Duration::hours(1),
            refresh_token_duration: Duration::days(7),
        }
    }

    /// Sets token durations.
    pub fn with_token_durations(mut self, access: Duration, refresh: Duration) -> Self {
        self.access_token_duration = access;
        self.refresh_token_duration = refresh;
        self
    }

    /// Issues an access token for a user.
    pub async fn issue_access_token(
        &self,
        user_id: &str,
        tenant_id: &TenantId,
    ) -> TenancyResult<TenantToken> {
        // Get user's role in the tenant
        let role = self
            .registry
            .get_user_role(user_id, tenant_id)
            .await
            .ok_or_else(|| TenancyError::NotMember(user_id.to_string(), *tenant_id))?;

        // Get all accessible tenants
        let memberships = self.registry.get_user_memberships(user_id).await;
        let accessible_tenants: Vec<TenantId> = memberships.iter().map(|m| m.tenant_id).collect();

        // Get permissions for role
        let permissions = default_permissions_for_role(role);
        let scopes: Vec<String> = permissions.iter().map(|p| p.scope().to_string()).collect();

        let claims = TenantTokenClaims::new(
            user_id,
            *tenant_id,
            role,
            TokenType::Access,
            self.access_token_duration,
        )
        .with_accessible_tenants(accessible_tenants)
        .with_scopes(scopes);

        Ok(TenantToken::new(claims, &self.secret))
    }

    /// Issues a refresh token.
    pub async fn issue_refresh_token(
        &self,
        user_id: &str,
        tenant_id: &TenantId,
    ) -> TenancyResult<TenantToken> {
        let role = self
            .registry
            .get_user_role(user_id, tenant_id)
            .await
            .ok_or_else(|| TenancyError::NotMember(user_id.to_string(), *tenant_id))?;

        let claims = TenantTokenClaims::new(
            user_id,
            *tenant_id,
            role,
            TokenType::Refresh,
            self.refresh_token_duration,
        );

        Ok(TenantToken::new(claims, &self.secret))
    }

    /// Validates a token string and returns the claims.
    pub async fn validate_token(&self, token: &str) -> Option<TenantTokenClaims> {
        let token = TenantToken::validate(token, &self.secret)?;

        // Check if revoked
        if self
            .revoked_tokens
            .read()
            .await
            .contains(&token.claims.token_id)
        {
            return None;
        }

        Some(token.claims)
    }

    /// Revokes a token.
    pub async fn revoke_token(&self, token_id: &str) {
        self.revoked_tokens
            .write()
            .await
            .insert(token_id.to_string());
    }

    /// Refreshes an access token using a refresh token.
    pub async fn refresh_access_token(&self, refresh_token: &str) -> TenancyResult<TenantToken> {
        let claims = self
            .validate_token(refresh_token)
            .await
            .ok_or_else(|| TenancyError::Unauthorized("Invalid refresh token".to_string()))?;

        if claims.token_type != TokenType::Refresh {
            return Err(TenancyError::Unauthorized(
                "Not a refresh token".to_string(),
            ));
        }

        self.issue_access_token(&claims.user_id, &claims.tenant_id)
            .await
    }

    /// Checks if a user can perform an action in a tenant.
    pub async fn check_permission(
        &self,
        user_id: &str,
        tenant_id: &TenantId,
        permission: Permission,
    ) -> bool {
        // Get user's role
        let role = match self.registry.get_user_role(user_id, tenant_id).await {
            Some(r) => r,
            None => {
                // Check cross-tenant grants
                return self
                    .check_cross_tenant_permission(user_id, tenant_id, permission)
                    .await;
            }
        };

        // Check if role has permission
        let permissions = default_permissions_for_role(role);
        permissions.contains(&permission) || permissions.contains(&Permission::Admin)
    }

    /// Checks cross-tenant permissions.
    async fn check_cross_tenant_permission(
        &self,
        user_id: &str,
        target_tenant_id: &TenantId,
        permission: Permission,
    ) -> bool {
        let memberships = self.registry.get_user_memberships(user_id).await;
        let grants = self.cross_tenant_grants.read().await;

        // Check if any of the user's tenants have granted access to the target
        for membership in memberships {
            let key = (membership.tenant_id, *target_tenant_id);
            if let Some(grant) = grants.get(&key) {
                if grant.is_valid() && grant.has_permission(permission) {
                    return true;
                }
            }
        }

        false
    }

    /// Creates a cross-tenant access grant.
    pub async fn create_cross_tenant_grant(&self, grant: CrossTenantGrant) -> TenancyResult<()> {
        // Verify both tenants exist
        if self
            .registry
            .get_tenant(&grant.source_tenant_id)
            .await
            .is_none()
        {
            return Err(TenancyError::TenantNotFound(grant.source_tenant_id));
        }
        if self
            .registry
            .get_tenant(&grant.target_tenant_id)
            .await
            .is_none()
        {
            return Err(TenancyError::TenantNotFound(grant.target_tenant_id));
        }

        let key = (grant.source_tenant_id, grant.target_tenant_id);
        self.cross_tenant_grants.write().await.insert(key, grant);

        Ok(())
    }

    /// Revokes a cross-tenant grant.
    pub async fn revoke_cross_tenant_grant(
        &self,
        source_tenant_id: &TenantId,
        target_tenant_id: &TenantId,
    ) -> Option<CrossTenantGrant> {
        let key = (*source_tenant_id, *target_tenant_id);
        self.cross_tenant_grants.write().await.remove(&key)
    }

    /// Gets a cross-tenant grant.
    pub async fn get_cross_tenant_grant(
        &self,
        source_tenant_id: &TenantId,
        target_tenant_id: &TenantId,
    ) -> Option<CrossTenantGrant> {
        let key = (*source_tenant_id, *target_tenant_id);
        self.cross_tenant_grants.read().await.get(&key).cloned()
    }

    /// Lists all grants from a tenant.
    pub async fn list_grants_from(&self, source_tenant_id: &TenantId) -> Vec<CrossTenantGrant> {
        self.cross_tenant_grants
            .read()
            .await
            .iter()
            .filter(|((source, _), _)| source == source_tenant_id)
            .map(|(_, grant)| grant.clone())
            .collect()
    }

    /// Lists all grants to a tenant.
    pub async fn list_grants_to(&self, target_tenant_id: &TenantId) -> Vec<CrossTenantGrant> {
        self.cross_tenant_grants
            .read()
            .await
            .iter()
            .filter(|((_, target), _)| target == target_tenant_id)
            .map(|(_, grant)| grant.clone())
            .collect()
    }

    /// Cleans up expired grants and tokens.
    pub async fn cleanup_expired(&self) {
        // Remove expired grants
        let mut grants = self.cross_tenant_grants.write().await;
        grants.retain(|_, grant| grant.is_valid());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_claims() {
        let tenant_id = TenantId::new();
        let claims = TenantTokenClaims::new(
            "user@example.com",
            tenant_id,
            TenantRole::Member,
            TokenType::Access,
            Duration::hours(1),
        )
        .with_scopes(vec!["job:submit".to_string(), "job:read".to_string()]);

        assert!(!claims.is_expired());
        assert!(claims.has_scope("job:submit"));
        assert!(!claims.has_scope("admin"));
        assert!(claims.can_access_tenant(&tenant_id));
    }

    #[test]
    fn test_token_create_validate() {
        let tenant_id = TenantId::new();
        let claims = TenantTokenClaims::new(
            "user@example.com",
            tenant_id,
            TenantRole::Member,
            TokenType::Access,
            Duration::hours(1),
        );

        let secret = b"test-secret";
        let token = TenantToken::new(claims, secret);

        // Valid token
        let validated = TenantToken::validate(&token.token, secret);
        assert!(validated.is_some());
        assert_eq!(validated.unwrap().claims.user_id, "user@example.com");

        // Invalid secret
        let invalid = TenantToken::validate(&token.token, b"wrong-secret");
        assert!(invalid.is_none());
    }

    #[test]
    fn test_expired_token() {
        let tenant_id = TenantId::new();
        let claims = TenantTokenClaims::new(
            "user@example.com",
            tenant_id,
            TenantRole::Member,
            TokenType::Access,
            Duration::seconds(-1), // Already expired
        );

        assert!(claims.is_expired());

        let secret = b"test-secret";
        let token = TenantToken::new(claims, secret);

        // Should not validate expired token
        let validated = TenantToken::validate(&token.token, secret);
        assert!(validated.is_none());
    }

    #[test]
    fn test_default_permissions() {
        let owner_perms = default_permissions_for_role(TenantRole::Owner);
        assert!(owner_perms.contains(&Permission::Admin));

        let member_perms = default_permissions_for_role(TenantRole::Member);
        assert!(member_perms.contains(&Permission::JobSubmit));
        assert!(!member_perms.contains(&Permission::Admin));

        let viewer_perms = default_permissions_for_role(TenantRole::Viewer);
        assert!(viewer_perms.contains(&Permission::JobRead));
        assert!(!viewer_perms.contains(&Permission::JobSubmit));
    }

    #[test]
    fn test_cross_tenant_grant() {
        let source = TenantId::new();
        let target = TenantId::new();

        let grant = CrossTenantGrant::new(
            source,
            target,
            vec![Permission::JobRead],
            "admin",
            "Collaboration",
        )
        .expires_in(Duration::days(7));

        assert!(grant.is_valid());
        assert!(grant.has_permission(Permission::JobRead));
        assert!(!grant.has_permission(Permission::JobSubmit));
    }

    #[tokio::test]
    async fn test_auth_manager() {
        let registry = Arc::new(TenantRegistry::new());

        // Create tenant and add user
        let org = super::super::types::Tenant::new_organization("Org", "org");
        let tenant_id = registry.create_tenant(org).await.unwrap();
        registry
            .add_member(&tenant_id, "user@example.com", TenantRole::Member)
            .await
            .unwrap();

        let auth = TenantAuthManager::new(registry, b"test-secret".to_vec());

        // Issue token
        let token = auth
            .issue_access_token("user@example.com", &tenant_id)
            .await
            .unwrap();

        // Validate token
        let claims = auth.validate_token(&token.token).await.unwrap();
        assert_eq!(claims.user_id, "user@example.com");
        assert_eq!(claims.tenant_id, tenant_id);

        // Revoke token
        auth.revoke_token(&claims.token_id).await;
        assert!(auth.validate_token(&token.token).await.is_none());
    }

    #[tokio::test]
    async fn test_permission_check() {
        let registry = Arc::new(TenantRegistry::new());

        let org = super::super::types::Tenant::new_organization("Org", "org");
        let tenant_id = registry.create_tenant(org).await.unwrap();

        registry
            .add_member(&tenant_id, "admin@example.com", TenantRole::Admin)
            .await
            .unwrap();
        registry
            .add_member(&tenant_id, "viewer@example.com", TenantRole::Viewer)
            .await
            .unwrap();

        let auth = TenantAuthManager::new(registry, b"test-secret".to_vec());

        // Admin can submit jobs
        assert!(
            auth.check_permission("admin@example.com", &tenant_id, Permission::JobSubmit)
                .await
        );

        // Viewer cannot submit jobs
        assert!(
            !auth
                .check_permission("viewer@example.com", &tenant_id, Permission::JobSubmit)
                .await
        );

        // Both can read jobs
        assert!(
            auth.check_permission("admin@example.com", &tenant_id, Permission::JobRead)
                .await
        );
        assert!(
            auth.check_permission("viewer@example.com", &tenant_id, Permission::JobRead)
                .await
        );
    }

    #[tokio::test]
    async fn test_cross_tenant_access() {
        let registry = Arc::new(TenantRegistry::new());

        let org1 = super::super::types::Tenant::new_organization("Org1", "org1");
        let tenant1 = registry.create_tenant(org1).await.unwrap();

        let org2 = super::super::types::Tenant::new_organization("Org2", "org2");
        let tenant2 = registry.create_tenant(org2).await.unwrap();

        registry
            .add_member(&tenant1, "user@example.com", TenantRole::Member)
            .await
            .unwrap();

        let auth = TenantAuthManager::new(registry, b"test-secret".to_vec());

        // User cannot access tenant2 by default
        assert!(
            !auth
                .check_permission("user@example.com", &tenant2, Permission::JobRead)
                .await
        );

        // Create cross-tenant grant
        let grant = CrossTenantGrant::new(
            tenant1,
            tenant2,
            vec![Permission::JobRead],
            "admin",
            "Collaboration",
        );
        auth.create_cross_tenant_grant(grant).await.unwrap();

        // Now user can access tenant2 for job:read
        assert!(
            auth.check_permission("user@example.com", &tenant2, Permission::JobRead)
                .await
        );

        // But not for job:submit
        assert!(
            !auth
                .check_permission("user@example.com", &tenant2, Permission::JobSubmit)
                .await
        );
    }
}
