// Marabunta - Licensed under the MIT License.
//! Tenant Model Types
//!
//! This module defines the core types for multi-tenancy isolation including
//! tenant structures, identifiers, quotas, policies, and hierarchical organization.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use uuid::Uuid;

/// Unique identifier for a tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantId(pub Uuid);

impl TenantId {
    /// Creates a new random tenant ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Creates a tenant ID from a name (deterministic).
    pub fn from_name(name: &str) -> Self {
        Self(Uuid::new_v5(&Uuid::NAMESPACE_DNS, name.as_bytes()))
    }

    /// Returns the UUID representation.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for TenantId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tenant-{}", &self.0.to_string()[..8])
    }
}

impl From<Uuid> for TenantId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl std::str::FromStr for TenantId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Handle "tenant-" prefix
        let s = s.strip_prefix("tenant-").unwrap_or(s);
        Ok(Self(Uuid::parse_str(s)?))
    }
}

/// Tenant hierarchy level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantLevel {
    /// Top-level organization (company, institution)
    Organization,
    /// Department or team within an organization
    Team,
    /// Individual user or project
    User,
}

impl fmt::Display for TenantLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TenantLevel::Organization => write!(f, "organization"),
            TenantLevel::Team => write!(f, "team"),
            TenantLevel::User => write!(f, "user"),
        }
    }
}

/// Tenant status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum TenantStatus {
    /// Tenant is active and can submit jobs
    #[default]
    Active,
    /// Tenant is suspended (cannot submit new jobs)
    Suspended,
    /// Tenant is in read-only mode (can view but not modify)
    ReadOnly,
    /// Tenant is pending approval
    Pending,
    /// Tenant is archived (soft deleted)
    Archived,
}


/// Resource quotas for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantQuotas {
    /// Maximum concurrent jobs
    pub max_concurrent_jobs: Option<u32>,
    /// Maximum concurrent tasks
    pub max_concurrent_tasks: Option<u32>,
    /// Maximum CPU hours per period
    pub max_cpu_hours: Option<f64>,
    /// Maximum GPU hours per period
    pub max_gpu_hours: Option<f64>,
    /// Maximum storage in GB
    pub max_storage_gb: Option<f64>,
    /// Maximum network egress in GB per period
    pub max_network_egress_gb: Option<f64>,
    /// Custom resource limits
    pub custom_limits: HashMap<String, f64>,
    /// Priority ceiling (max priority jobs can have)
    pub max_priority: Option<u32>,
    /// Quota period for time-based limits
    pub period: QuotaPeriod,
}

impl Default for TenantQuotas {
    fn default() -> Self {
        Self {
            max_concurrent_jobs: Some(100),
            max_concurrent_tasks: Some(1000),
            max_cpu_hours: Some(10000.0),
            max_gpu_hours: Some(1000.0),
            max_storage_gb: Some(1000.0),
            max_network_egress_gb: Some(500.0),
            custom_limits: HashMap::new(),
            max_priority: Some(500),
            period: QuotaPeriod::Monthly,
        }
    }
}

impl TenantQuotas {
    /// Creates unlimited quotas.
    pub fn unlimited() -> Self {
        Self {
            max_concurrent_jobs: None,
            max_concurrent_tasks: None,
            max_cpu_hours: None,
            max_gpu_hours: None,
            max_storage_gb: None,
            max_network_egress_gb: None,
            custom_limits: HashMap::new(),
            max_priority: None,
            period: QuotaPeriod::Monthly,
        }
    }

    /// Creates minimal quotas for trial tenants.
    pub fn trial() -> Self {
        Self {
            max_concurrent_jobs: Some(5),
            max_concurrent_tasks: Some(50),
            max_cpu_hours: Some(100.0),
            max_gpu_hours: Some(10.0),
            max_storage_gb: Some(10.0),
            max_network_egress_gb: Some(10.0),
            custom_limits: HashMap::new(),
            max_priority: Some(100),
            period: QuotaPeriod::Monthly,
        }
    }

    /// Sets a custom limit.
    pub fn with_custom_limit(mut self, name: impl Into<String>, value: f64) -> Self {
        self.custom_limits.insert(name.into(), value);
        self
    }
}

/// Period for quota resets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum QuotaPeriod {
    /// Daily reset
    Daily,
    /// Weekly reset
    Weekly,
    /// Monthly reset
    #[default]
    Monthly,
    /// Quarterly reset
    Quarterly,
    /// Yearly reset
    Yearly,
    /// Never resets
    Never,
}


/// Tenant-specific policies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantPolicies {
    /// Can this tenant use phantom (anonymous) nodes?
    pub allow_phantom_nodes: bool,
    /// Can this tenant use infrastructure nodes?
    pub allow_infrastructure_nodes: bool,
    /// Required SLA tier for jobs
    pub required_sla_tier: Option<String>,
    /// Allowed regions (empty = all regions)
    pub allowed_regions: Vec<String>,
    /// Blocked regions
    pub blocked_regions: Vec<String>,
    /// Data retention period in days
    pub data_retention_days: Option<u32>,
    /// Required encryption for data at rest
    pub require_encryption: bool,
    /// Audit logging level
    pub audit_level: AuditLevel,
    /// Custom policy extensions
    pub custom_policies: HashMap<String, serde_json::Value>,
}

impl Default for TenantPolicies {
    fn default() -> Self {
        Self {
            allow_phantom_nodes: true,
            allow_infrastructure_nodes: true,
            required_sla_tier: None,
            allowed_regions: Vec::new(),
            blocked_regions: Vec::new(),
            data_retention_days: Some(90),
            require_encryption: false,
            audit_level: AuditLevel::Standard,
            custom_policies: HashMap::new(),
        }
    }
}

impl TenantPolicies {
    /// Creates strict policies for high-security tenants.
    pub fn strict() -> Self {
        Self {
            allow_phantom_nodes: false,
            allow_infrastructure_nodes: true,
            required_sla_tier: Some("production".to_string()),
            allowed_regions: Vec::new(),
            blocked_regions: Vec::new(),
            data_retention_days: Some(365),
            require_encryption: true,
            audit_level: AuditLevel::Verbose,
            custom_policies: HashMap::new(),
        }
    }

    /// Sets a custom policy.
    pub fn with_custom_policy(mut self, name: impl Into<String>, value: serde_json::Value) -> Self {
        self.custom_policies.insert(name.into(), value);
        self
    }

    /// Checks if a region is allowed.
    pub fn is_region_allowed(&self, region: &str) -> bool {
        // If blocked, not allowed
        if self.blocked_regions.contains(&region.to_string()) {
            return false;
        }
        // If allowed_regions is empty, all non-blocked regions are allowed
        // Otherwise, must be in allowed_regions
        self.allowed_regions.is_empty() || self.allowed_regions.contains(&region.to_string())
    }
}

/// Audit logging level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum AuditLevel {
    /// No audit logging
    None,
    /// Basic audit logging (job submit/complete)
    Basic,
    /// Standard audit logging (all API calls)
    #[default]
    Standard,
    /// Verbose audit logging (all operations including internal)
    Verbose,
}


/// Billing configuration for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantBilling {
    /// Billing plan identifier
    pub plan_id: String,
    /// Cost center or billing code
    pub cost_center: Option<String>,
    /// Billing contact email
    pub billing_email: Option<String>,
    /// Current billing status
    pub status: BillingStatus,
    /// Credit balance (if applicable)
    pub credit_balance: f64,
    /// Usage this billing period
    pub current_period_usage: f64,
}

impl Default for TenantBilling {
    fn default() -> Self {
        Self {
            plan_id: "default".to_string(),
            cost_center: None,
            billing_email: None,
            status: BillingStatus::Active,
            credit_balance: 0.0,
            current_period_usage: 0.0,
        }
    }
}

/// Billing status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BillingStatus {
    /// Billing is active
    Active,
    /// Payment is past due
    PastDue,
    /// Account is suspended for non-payment
    Suspended,
    /// Free tier (no billing)
    Free,
}

/// A tenant in the multi-tenancy system.
///
/// Tenants are organized hierarchically: Organization -> Team -> User.
/// Each tenant has quotas, policies, and can contain child tenants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    /// Unique identifier
    pub id: TenantId,
    /// Human-readable name
    pub name: String,
    /// Unique slug (URL-safe name)
    pub slug: String,
    /// Description
    pub description: String,
    /// Tenant level in hierarchy
    pub level: TenantLevel,
    /// Parent tenant ID (None for top-level orgs)
    pub parent_id: Option<TenantId>,
    /// Child tenant IDs
    pub children: Vec<TenantId>,
    /// Current status
    pub status: TenantStatus,
    /// Resource quotas
    pub quotas: TenantQuotas,
    /// Policies
    pub policies: TenantPolicies,
    /// Billing configuration
    pub billing: TenantBilling,
    /// Admin user IDs
    pub admins: Vec<String>,
    /// Member user IDs (non-admin)
    pub members: Vec<String>,
    /// Contact email
    pub contact_email: Option<String>,
    /// When created
    pub created_at: DateTime<Utc>,
    /// Last updated
    pub updated_at: DateTime<Utc>,
    /// Who created this tenant
    pub created_by: Option<String>,
    /// Metadata for extensions
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Tenant {
    /// Creates a new organization-level tenant.
    pub fn new_organization(name: impl Into<String>, slug: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: TenantId::new(),
            name: name.into(),
            slug: slug.into(),
            description: String::new(),
            level: TenantLevel::Organization,
            parent_id: None,
            children: Vec::new(),
            status: TenantStatus::Active,
            quotas: TenantQuotas::default(),
            policies: TenantPolicies::default(),
            billing: TenantBilling::default(),
            admins: Vec::new(),
            members: Vec::new(),
            contact_email: None,
            created_at: now,
            updated_at: now,
            created_by: None,
            metadata: HashMap::new(),
        }
    }

    /// Creates a new team-level tenant under a parent organization.
    pub fn new_team(name: impl Into<String>, slug: impl Into<String>, parent_id: TenantId) -> Self {
        let now = Utc::now();
        Self {
            id: TenantId::new(),
            name: name.into(),
            slug: slug.into(),
            description: String::new(),
            level: TenantLevel::Team,
            parent_id: Some(parent_id),
            children: Vec::new(),
            status: TenantStatus::Active,
            quotas: TenantQuotas::default(),
            policies: TenantPolicies::default(),
            billing: TenantBilling::default(),
            admins: Vec::new(),
            members: Vec::new(),
            contact_email: None,
            created_at: now,
            updated_at: now,
            created_by: None,
            metadata: HashMap::new(),
        }
    }

    /// Creates a new user-level tenant under a parent team.
    pub fn new_user(
        name: impl Into<String>,
        slug: impl Into<String>,
        parent_id: TenantId,
        user_id: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        let user_id = user_id.into();
        Self {
            id: TenantId::new(),
            name: name.into(),
            slug: slug.into(),
            description: String::new(),
            level: TenantLevel::User,
            parent_id: Some(parent_id),
            children: Vec::new(),
            status: TenantStatus::Active,
            quotas: TenantQuotas::default(),
            policies: TenantPolicies::default(),
            billing: TenantBilling::default(),
            admins: vec![user_id.clone()],
            members: vec![user_id],
            contact_email: None,
            created_at: now,
            updated_at: now,
            created_by: None,
            metadata: HashMap::new(),
        }
    }

    /// Sets the description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self.updated_at = Utc::now();
        self
    }

    /// Sets the quotas.
    pub fn with_quotas(mut self, quotas: TenantQuotas) -> Self {
        self.quotas = quotas;
        self.updated_at = Utc::now();
        self
    }

    /// Sets the policies.
    pub fn with_policies(mut self, policies: TenantPolicies) -> Self {
        self.policies = policies;
        self.updated_at = Utc::now();
        self
    }

    /// Sets the billing configuration.
    pub fn with_billing(mut self, billing: TenantBilling) -> Self {
        self.billing = billing;
        self.updated_at = Utc::now();
        self
    }

    /// Sets the contact email.
    pub fn with_contact_email(mut self, email: impl Into<String>) -> Self {
        self.contact_email = Some(email.into());
        self.updated_at = Utc::now();
        self
    }

    /// Sets the creator.
    pub fn with_created_by(mut self, creator: impl Into<String>) -> Self {
        self.created_by = Some(creator.into());
        self
    }

    /// Adds an admin.
    pub fn add_admin(&mut self, user_id: impl Into<String>) {
        let user_id = user_id.into();
        if !self.admins.contains(&user_id) {
            self.admins.push(user_id.clone());
        }
        if !self.members.contains(&user_id) {
            self.members.push(user_id);
        }
        self.updated_at = Utc::now();
    }

    /// Removes an admin (but keeps as member).
    pub fn remove_admin(&mut self, user_id: &str) {
        self.admins.retain(|id| id != user_id);
        self.updated_at = Utc::now();
    }

    /// Adds a member.
    pub fn add_member(&mut self, user_id: impl Into<String>) {
        let user_id = user_id.into();
        if !self.members.contains(&user_id) {
            self.members.push(user_id);
        }
        self.updated_at = Utc::now();
    }

    /// Removes a member (also removes from admins).
    pub fn remove_member(&mut self, user_id: &str) {
        self.members.retain(|id| id != user_id);
        self.admins.retain(|id| id != user_id);
        self.updated_at = Utc::now();
    }

    /// Checks if a user is a member.
    pub fn is_member(&self, user_id: &str) -> bool {
        self.members.contains(&user_id.to_string())
    }

    /// Checks if a user is an admin.
    pub fn is_admin(&self, user_id: &str) -> bool {
        self.admins.contains(&user_id.to_string())
    }

    /// Adds a child tenant.
    pub fn add_child(&mut self, child_id: TenantId) {
        if !self.children.contains(&child_id) {
            self.children.push(child_id);
        }
        self.updated_at = Utc::now();
    }

    /// Removes a child tenant.
    pub fn remove_child(&mut self, child_id: &TenantId) {
        self.children.retain(|id| id != child_id);
        self.updated_at = Utc::now();
    }

    /// Sets metadata.
    pub fn set_metadata(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.metadata.insert(key.into(), value);
        self.updated_at = Utc::now();
    }

    /// Gets the full path for this tenant (e.g., "org/team/user").
    pub fn path(&self, ancestors: &[&Tenant]) -> String {
        let mut parts: Vec<&str> = ancestors.iter().map(|t| t.slug.as_str()).collect();
        parts.push(&self.slug);
        parts.join("/")
    }

    /// Returns the namespace prefix for this tenant's data.
    pub fn namespace(&self) -> String {
        format!("tenant:{}", self.id.0)
    }

    /// Checks if this tenant is active.
    pub fn is_active(&self) -> bool {
        self.status == TenantStatus::Active
    }
}

impl fmt::Display for Tenant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}, {})", self.name, self.id, self.level)
    }
}

/// Tenant membership with role.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantMembership {
    /// Tenant ID
    pub tenant_id: TenantId,
    /// User ID
    pub user_id: String,
    /// Role within the tenant
    pub role: TenantRole,
    /// When joined
    pub joined_at: DateTime<Utc>,
    /// When membership expires (if applicable)
    pub expires_at: Option<DateTime<Utc>>,
}

/// Role within a tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantRole {
    /// Owner - full control
    Owner,
    /// Admin - can manage members and settings
    Admin,
    /// Member - can submit and view jobs
    Member,
    /// Viewer - read-only access
    Viewer,
}

impl TenantRole {
    /// Returns the priority of this role (higher = more permissions).
    pub fn priority(&self) -> u32 {
        match self {
            TenantRole::Owner => 1000,
            TenantRole::Admin => 500,
            TenantRole::Member => 100,
            TenantRole::Viewer => 10,
        }
    }

    /// Checks if this role has at least the permissions of another role.
    pub fn has_permission_of(&self, other: TenantRole) -> bool {
        self.priority() >= other.priority()
    }
}

impl fmt::Display for TenantRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TenantRole::Owner => write!(f, "owner"),
            TenantRole::Admin => write!(f, "admin"),
            TenantRole::Member => write!(f, "member"),
            TenantRole::Viewer => write!(f, "viewer"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_id_creation() {
        let id1 = TenantId::new();
        let id2 = TenantId::new();
        assert_ne!(id1, id2);

        let id3 = TenantId::from_name("test-org");
        let id4 = TenantId::from_name("test-org");
        assert_eq!(id3, id4);
    }

    #[test]
    fn test_tenant_id_display() {
        let id = TenantId::new();
        let display = format!("{}", id);
        assert!(display.starts_with("tenant-"));
    }

    #[test]
    fn test_tenant_organization() {
        let org = Tenant::new_organization("Acme Corp", "acme")
            .with_description("Acme Corporation")
            .with_contact_email("admin@acme.com");

        assert_eq!(org.name, "Acme Corp");
        assert_eq!(org.slug, "acme");
        assert_eq!(org.level, TenantLevel::Organization);
        assert!(org.parent_id.is_none());
    }

    #[test]
    fn test_tenant_hierarchy() {
        let org = Tenant::new_organization("Org", "org");
        let team = Tenant::new_team("Team A", "team-a", org.id);
        let user = Tenant::new_user("User 1", "user-1", team.id, "user@example.com");

        assert_eq!(org.level, TenantLevel::Organization);
        assert_eq!(team.level, TenantLevel::Team);
        assert_eq!(user.level, TenantLevel::User);

        assert!(org.parent_id.is_none());
        assert_eq!(team.parent_id, Some(org.id));
        assert_eq!(user.parent_id, Some(team.id));
    }

    #[test]
    fn test_tenant_members() {
        let mut tenant = Tenant::new_organization("Org", "org");

        tenant.add_admin("admin@example.com");
        assert!(tenant.is_admin("admin@example.com"));
        assert!(tenant.is_member("admin@example.com"));

        tenant.add_member("user@example.com");
        assert!(!tenant.is_admin("user@example.com"));
        assert!(tenant.is_member("user@example.com"));

        tenant.remove_member("user@example.com");
        assert!(!tenant.is_member("user@example.com"));
    }

    #[test]
    fn test_tenant_quotas() {
        let quotas = TenantQuotas::default();
        assert_eq!(quotas.max_concurrent_jobs, Some(100));

        let unlimited = TenantQuotas::unlimited();
        assert!(unlimited.max_concurrent_jobs.is_none());

        let trial = TenantQuotas::trial();
        assert_eq!(trial.max_concurrent_jobs, Some(5));
    }

    #[test]
    fn test_tenant_policies() {
        let policies = TenantPolicies::default();
        assert!(policies.allow_phantom_nodes);

        let strict = TenantPolicies::strict();
        assert!(!strict.allow_phantom_nodes);
        assert!(strict.require_encryption);
    }

    #[test]
    fn test_region_allowed() {
        let mut policies = TenantPolicies::default();

        // Default: all regions allowed
        assert!(policies.is_region_allowed("us-east-1"));
        assert!(policies.is_region_allowed("eu-west-1"));

        // Block specific region
        policies.blocked_regions.push("cn-north-1".to_string());
        assert!(policies.is_region_allowed("us-east-1"));
        assert!(!policies.is_region_allowed("cn-north-1"));

        // Whitelist specific regions
        policies.allowed_regions = vec!["us-east-1".to_string(), "us-west-2".to_string()];
        assert!(policies.is_region_allowed("us-east-1"));
        assert!(!policies.is_region_allowed("eu-west-1"));
    }

    #[test]
    fn test_tenant_role_permissions() {
        assert!(TenantRole::Owner.has_permission_of(TenantRole::Admin));
        assert!(TenantRole::Admin.has_permission_of(TenantRole::Member));
        assert!(TenantRole::Member.has_permission_of(TenantRole::Viewer));
        assert!(!TenantRole::Viewer.has_permission_of(TenantRole::Member));
    }

    #[test]
    fn test_tenant_namespace() {
        let tenant = Tenant::new_organization("Org", "org");
        let namespace = tenant.namespace();
        assert!(namespace.starts_with("tenant:"));
    }
}
