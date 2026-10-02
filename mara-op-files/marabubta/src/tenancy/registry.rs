// Marabunta - Licensed under the MIT License.
//! Tenant Registry
//!
//! This module provides the central registry for tenant management including
//! CRUD operations, hierarchy management, and tenant lookups.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use chrono::Utc;

use super::errors::{TenancyError, TenancyResult};
use super::isolation::{IsolationManager, TenantIsolation};
use super::types::{
    Tenant, TenantId, TenantLevel, TenantMembership, TenantPolicies, TenantQuotas, TenantRole,
    TenantStatus,
};

/// Central registry for tenant management.
///
/// The registry maintains all tenants, their hierarchy relationships,
/// and provides methods for tenant lifecycle management.
pub struct TenantRegistry {
    /// All tenants indexed by ID
    tenants: Arc<RwLock<HashMap<TenantId, Tenant>>>,
    /// Tenant ID by slug for slug-based lookups
    slug_index: Arc<RwLock<HashMap<String, TenantId>>>,
    /// User to tenant memberships
    user_memberships: Arc<RwLock<HashMap<String, Vec<TenantMembership>>>>,
    /// Isolation manager
    isolation_manager: Arc<IsolationManager>,
}

impl TenantRegistry {
    /// Creates a new empty tenant registry.
    pub fn new() -> Self {
        Self {
            tenants: Arc::new(RwLock::new(HashMap::new())),
            slug_index: Arc::new(RwLock::new(HashMap::new())),
            user_memberships: Arc::new(RwLock::new(HashMap::new())),
            isolation_manager: Arc::new(IsolationManager::new()),
        }
    }

    /// Creates a registry with a shared isolation manager.
    pub fn with_isolation_manager(isolation_manager: Arc<IsolationManager>) -> Self {
        Self {
            tenants: Arc::new(RwLock::new(HashMap::new())),
            slug_index: Arc::new(RwLock::new(HashMap::new())),
            user_memberships: Arc::new(RwLock::new(HashMap::new())),
            isolation_manager,
        }
    }

    /// Gets the isolation manager.
    pub fn isolation_manager(&self) -> &Arc<IsolationManager> {
        &self.isolation_manager
    }

    // =========================================================================
    // Tenant CRUD Operations
    // =========================================================================

    /// Creates a new tenant.
    pub async fn create_tenant(&self, mut tenant: Tenant) -> TenancyResult<TenantId> {
        let tenant_id = tenant.id;
        let slug = tenant.slug.clone();

        // Check for existing tenant
        {
            let tenants = self.tenants.read().await;
            if tenants.contains_key(&tenant_id) {
                return Err(TenancyError::TenantAlreadyExists(tenant_id));
            }
        }

        // Check for slug collision
        {
            let slugs = self.slug_index.read().await;
            if slugs.contains_key(&slug) {
                return Err(TenancyError::SlugTaken(slug));
            }
        }

        // Validate parent for non-org tenants
        if let Some(parent_id) = tenant.parent_id {
            let tenants = self.tenants.read().await;
            let parent = tenants
                .get(&parent_id)
                .ok_or(TenancyError::ParentNotFound(parent_id))?;

            // Validate hierarchy
            match (parent.level, tenant.level) {
                (TenantLevel::Organization, TenantLevel::Team) => {}
                (TenantLevel::Team, TenantLevel::User) => {}
                (TenantLevel::Organization, TenantLevel::User) => {} // Direct user under org
                _ => {
                    return Err(TenancyError::InvalidHierarchy(format!(
                        "{:?} cannot be child of {:?}",
                        tenant.level, parent.level
                    )));
                }
            }
        } else if tenant.level != TenantLevel::Organization {
            return Err(TenancyError::InvalidHierarchy(
                "Non-organization tenants must have a parent".to_string(),
            ));
        }

        // Set creation timestamp
        tenant.created_at = Utc::now();
        tenant.updated_at = Utc::now();

        // Create default isolation configuration
        let isolation = TenantIsolation::new(tenant_id);
        self.isolation_manager.set_isolation(isolation).await;

        // Add to registry
        {
            let mut tenants = self.tenants.write().await;
            tenants.insert(tenant_id, tenant.clone());
        }

        // Update slug index
        {
            let mut slugs = self.slug_index.write().await;
            slugs.insert(slug, tenant_id);
        }

        // Update parent's children list
        if let Some(parent_id) = tenant.parent_id {
            let mut tenants = self.tenants.write().await;
            if let Some(parent) = tenants.get_mut(&parent_id) {
                parent.add_child(tenant_id);
            }
        }

        // Add admin memberships
        for admin in &tenant.admins {
            self.add_membership(
                admin.clone(),
                TenantMembership {
                    tenant_id,
                    user_id: admin.clone(),
                    role: TenantRole::Admin,
                    joined_at: Utc::now(),
                    expires_at: None,
                },
            )
            .await;
        }

        Ok(tenant_id)
    }

    /// Gets a tenant by ID.
    pub async fn get_tenant(&self, id: &TenantId) -> Option<Tenant> {
        self.tenants.read().await.get(id).cloned()
    }

    /// Gets a tenant by slug.
    pub async fn get_tenant_by_slug(&self, slug: &str) -> Option<Tenant> {
        let slug_index = self.slug_index.read().await;
        if let Some(tenant_id) = slug_index.get(slug) {
            self.tenants.read().await.get(tenant_id).cloned()
        } else {
            None
        }
    }

    /// Updates a tenant.
    pub async fn update_tenant(&self, tenant: Tenant) -> TenancyResult<()> {
        let tenant_id = tenant.id;

        // Check tenant exists
        {
            let tenants = self.tenants.read().await;
            if !tenants.contains_key(&tenant_id) {
                return Err(TenancyError::TenantNotFound(tenant_id));
            }
        }

        // Check slug if changed
        let old_slug = {
            let tenants = self.tenants.read().await;
            tenants.get(&tenant_id).map(|t| t.slug.clone())
        };

        if let Some(old) = old_slug {
            if old != tenant.slug {
                // Check new slug is available
                let slugs = self.slug_index.read().await;
                if slugs.contains_key(&tenant.slug) {
                    return Err(TenancyError::SlugTaken(tenant.slug.clone()));
                }

                // Update slug index
                drop(slugs);
                let mut slugs = self.slug_index.write().await;
                slugs.remove(&old);
                slugs.insert(tenant.slug.clone(), tenant_id);
            }
        }

        // Update tenant
        let mut updated = tenant;
        updated.updated_at = Utc::now();

        {
            let mut tenants = self.tenants.write().await;
            tenants.insert(tenant_id, updated);
        }

        Ok(())
    }

    /// Deletes a tenant.
    ///
    /// This will fail if the tenant has children. Use `delete_tenant_recursive`
    /// to delete a tenant and all its children.
    pub async fn delete_tenant(&self, id: &TenantId) -> TenancyResult<Tenant> {
        // Check tenant exists and has no children
        let tenant = {
            let tenants = self.tenants.read().await;
            let tenant = tenants
                .get(id)
                .ok_or(TenancyError::TenantNotFound(*id))?
                .clone();

            if !tenant.children.is_empty() {
                return Err(TenancyError::InvalidHierarchy(
                    "Cannot delete tenant with children".to_string(),
                ));
            }

            tenant
        };

        // Remove from parent's children list
        if let Some(parent_id) = tenant.parent_id {
            let mut tenants = self.tenants.write().await;
            if let Some(parent) = tenants.get_mut(&parent_id) {
                parent.remove_child(id);
            }
        }

        // Remove from registry
        {
            let mut tenants = self.tenants.write().await;
            tenants.remove(id);
        }

        // Remove slug
        {
            let mut slugs = self.slug_index.write().await;
            slugs.remove(&tenant.slug);
        }

        // Remove isolation
        self.isolation_manager.remove_isolation(id).await;

        // Remove memberships
        {
            let mut memberships = self.user_memberships.write().await;
            for user_memberships in memberships.values_mut() {
                user_memberships.retain(|m| m.tenant_id != *id);
            }
        }

        Ok(tenant)
    }

    /// Deletes a tenant and all its children recursively.
    pub async fn delete_tenant_recursive(&self, id: &TenantId) -> TenancyResult<Vec<Tenant>> {
        let mut deleted = Vec::new();

        // Get tenant and children
        let tenant = {
            let tenants = self.tenants.read().await;
            tenants
                .get(id)
                .ok_or(TenancyError::TenantNotFound(*id))?
                .clone()
        };

        // Delete children first
        for child_id in &tenant.children {
            let child_deleted = Box::pin(self.delete_tenant_recursive(child_id)).await?;
            deleted.extend(child_deleted);
        }

        // Delete this tenant
        let deleted_tenant = self.delete_tenant(id).await?;
        deleted.push(deleted_tenant);

        Ok(deleted)
    }

    /// Lists all tenants.
    pub async fn list_tenants(&self) -> Vec<Tenant> {
        self.tenants.read().await.values().cloned().collect()
    }

    /// Lists top-level organization tenants.
    pub async fn list_organizations(&self) -> Vec<Tenant> {
        self.tenants
            .read()
            .await
            .values()
            .filter(|t| t.level == TenantLevel::Organization)
            .cloned()
            .collect()
    }

    /// Lists children of a tenant.
    pub async fn list_children(&self, parent_id: &TenantId) -> Vec<Tenant> {
        let tenants = self.tenants.read().await;
        let parent = match tenants.get(parent_id) {
            Some(p) => p,
            None => return Vec::new(),
        };

        parent
            .children
            .iter()
            .filter_map(|child_id| tenants.get(child_id).cloned())
            .collect()
    }

    /// Gets the count of tenants.
    pub async fn count(&self) -> usize {
        self.tenants.read().await.len()
    }

    // =========================================================================
    // Tenant Hierarchy
    // =========================================================================

    /// Gets all ancestors of a tenant (parent, grandparent, etc.).
    pub async fn get_ancestors(&self, id: &TenantId) -> Vec<Tenant> {
        let mut ancestors = Vec::new();
        let tenants = self.tenants.read().await;

        let mut current_id = *id;
        while let Some(tenant) = tenants.get(&current_id) {
            if let Some(parent_id) = tenant.parent_id {
                if let Some(parent) = tenants.get(&parent_id) {
                    ancestors.push(parent.clone());
                    current_id = parent_id;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        ancestors
    }

    /// Gets all descendants of a tenant (children, grandchildren, etc.).
    pub async fn get_descendants(&self, id: &TenantId) -> Vec<Tenant> {
        let mut descendants = Vec::new();
        let tenants = self.tenants.read().await;

        let mut to_visit = vec![*id];
        while let Some(current_id) = to_visit.pop() {
            if let Some(tenant) = tenants.get(&current_id) {
                for child_id in &tenant.children {
                    if let Some(child) = tenants.get(child_id) {
                        descendants.push(child.clone());
                        to_visit.push(*child_id);
                    }
                }
            }
        }

        descendants
    }

    /// Gets the root organization for a tenant.
    pub async fn get_root_organization(&self, id: &TenantId) -> Option<Tenant> {
        let tenants = self.tenants.read().await;
        let mut current = tenants.get(id)?.clone();

        while let Some(parent_id) = current.parent_id {
            current = tenants.get(&parent_id)?.clone();
        }

        Some(current)
    }

    /// Gets the full path of a tenant.
    pub async fn get_tenant_path(&self, id: &TenantId) -> Option<String> {
        let tenant = self.get_tenant(id).await?;
        let ancestors = self.get_ancestors(id).await;

        let mut path_parts: Vec<&str> = ancestors.iter().rev().map(|t| t.slug.as_str()).collect();
        path_parts.push(&tenant.slug);

        Some(path_parts.join("/"))
    }

    // =========================================================================
    // Membership Management
    // =========================================================================

    /// Adds a membership.
    async fn add_membership(&self, user_id: String, membership: TenantMembership) {
        let mut memberships = self.user_memberships.write().await;
        memberships
            .entry(user_id)
            .or_insert_with(Vec::new)
            .push(membership);
    }

    /// Adds a member to a tenant.
    pub async fn add_member(
        &self,
        tenant_id: &TenantId,
        user_id: impl Into<String>,
        role: TenantRole,
    ) -> TenancyResult<()> {
        let user_id = user_id.into();

        // Update tenant
        {
            let mut tenants = self.tenants.write().await;
            let tenant = tenants
                .get_mut(tenant_id)
                .ok_or(TenancyError::TenantNotFound(*tenant_id))?;

            if role == TenantRole::Admin || role == TenantRole::Owner {
                tenant.add_admin(&user_id);
            } else {
                tenant.add_member(&user_id);
            }
        }

        // Add membership record
        self.add_membership(
            user_id.clone(),
            TenantMembership {
                tenant_id: *tenant_id,
                user_id,
                role,
                joined_at: Utc::now(),
                expires_at: None,
            },
        )
        .await;

        Ok(())
    }

    /// Removes a member from a tenant.
    pub async fn remove_member(&self, tenant_id: &TenantId, user_id: &str) -> TenancyResult<()> {
        // Update tenant
        {
            let mut tenants = self.tenants.write().await;
            let tenant = tenants
                .get_mut(tenant_id)
                .ok_or(TenancyError::TenantNotFound(*tenant_id))?;
            tenant.remove_member(user_id);
        }

        // Remove membership record
        {
            let mut memberships = self.user_memberships.write().await;
            if let Some(user_memberships) = memberships.get_mut(user_id) {
                user_memberships.retain(|m| m.tenant_id != *tenant_id);
            }
        }

        Ok(())
    }

    /// Gets all memberships for a user.
    pub async fn get_user_memberships(&self, user_id: &str) -> Vec<TenantMembership> {
        self.user_memberships
            .read()
            .await
            .get(user_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Gets all tenants a user is a member of.
    pub async fn get_user_tenants(&self, user_id: &str) -> Vec<Tenant> {
        let memberships = self.get_user_memberships(user_id).await;
        let tenants = self.tenants.read().await;

        memberships
            .iter()
            .filter_map(|m| tenants.get(&m.tenant_id).cloned())
            .collect()
    }

    /// Gets the user's role in a tenant.
    pub async fn get_user_role(&self, user_id: &str, tenant_id: &TenantId) -> Option<TenantRole> {
        self.user_memberships
            .read()
            .await
            .get(user_id)?
            .iter()
            .find(|m| m.tenant_id == *tenant_id)
            .map(|m| m.role)
    }

    /// Checks if a user is a member of a tenant (directly or via ancestry).
    pub async fn is_member(&self, user_id: &str, tenant_id: &TenantId) -> bool {
        // Check direct membership
        if self.get_user_role(user_id, tenant_id).await.is_some() {
            return true;
        }

        // Check ancestor memberships
        let ancestors = self.get_ancestors(tenant_id).await;
        for ancestor in ancestors {
            if self.get_user_role(user_id, &ancestor.id).await.is_some() {
                return true;
            }
        }

        false
    }

    // =========================================================================
    // Quotas and Policies
    // =========================================================================

    /// Updates tenant quotas.
    pub async fn set_quotas(
        &self,
        tenant_id: &TenantId,
        quotas: TenantQuotas,
    ) -> TenancyResult<()> {
        let mut tenants = self.tenants.write().await;
        let tenant = tenants
            .get_mut(tenant_id)
            .ok_or(TenancyError::TenantNotFound(*tenant_id))?;

        tenant.quotas = quotas;
        tenant.updated_at = Utc::now();

        Ok(())
    }

    /// Gets tenant quotas (merging with parent quotas).
    pub async fn get_effective_quotas(&self, tenant_id: &TenantId) -> Option<TenantQuotas> {
        let tenant = self.get_tenant(tenant_id).await?;
        // For now, return the tenant's quotas directly.
        // In a more complex system, this could merge with parent quotas.
        Some(tenant.quotas)
    }

    /// Updates tenant policies.
    pub async fn set_policies(
        &self,
        tenant_id: &TenantId,
        policies: TenantPolicies,
    ) -> TenancyResult<()> {
        let mut tenants = self.tenants.write().await;
        let tenant = tenants
            .get_mut(tenant_id)
            .ok_or(TenancyError::TenantNotFound(*tenant_id))?;

        tenant.policies = policies;
        tenant.updated_at = Utc::now();

        Ok(())
    }

    /// Gets tenant policies (merging with parent policies).
    pub async fn get_effective_policies(&self, tenant_id: &TenantId) -> Option<TenantPolicies> {
        let tenant = self.get_tenant(tenant_id).await?;
        let ancestors = self.get_ancestors(tenant_id).await;

        // Start with tenant's policies
        let mut effective = tenant.policies.clone();

        // Merge ancestor policies (most restrictive wins)
        for ancestor in ancestors {
            // Phantom nodes: only allowed if all ancestors allow
            effective.allow_phantom_nodes =
                effective.allow_phantom_nodes && ancestor.policies.allow_phantom_nodes;

            // Infrastructure nodes: only allowed if all ancestors allow
            effective.allow_infrastructure_nodes = effective.allow_infrastructure_nodes
                && ancestor.policies.allow_infrastructure_nodes;

            // Required SLA: use most restrictive
            if let Some(ref sla) = ancestor.policies.required_sla_tier {
                effective.required_sla_tier = Some(sla.clone());
            }

            // Blocked regions: union of all blocked
            for region in &ancestor.policies.blocked_regions {
                if !effective.blocked_regions.contains(region) {
                    effective.blocked_regions.push(region.clone());
                }
            }

            // Encryption: required if any ancestor requires
            effective.require_encryption =
                effective.require_encryption || ancestor.policies.require_encryption;

            // Audit: use highest level
            if (ancestor.policies.audit_level as u8) > (effective.audit_level as u8) {
                effective.audit_level = ancestor.policies.audit_level;
            }
        }

        Some(effective)
    }

    /// Updates tenant status.
    pub async fn set_status(
        &self,
        tenant_id: &TenantId,
        status: TenantStatus,
    ) -> TenancyResult<()> {
        let mut tenants = self.tenants.write().await;
        let tenant = tenants
            .get_mut(tenant_id)
            .ok_or(TenancyError::TenantNotFound(*tenant_id))?;

        tenant.status = status;
        tenant.updated_at = Utc::now();

        Ok(())
    }

    // =========================================================================
    // Export/Import
    // =========================================================================

    /// Exports the registry state.
    pub async fn export(&self) -> RegistryExport {
        RegistryExport {
            tenants: self.tenants.read().await.values().cloned().collect(),
            memberships: self.user_memberships.read().await.clone(),
            isolations: self.isolation_manager.list_all().await,
        }
    }

    /// Imports registry state.
    pub async fn import(&self, export: RegistryExport) -> TenancyResult<()> {
        // Clear existing state
        self.tenants.write().await.clear();
        self.slug_index.write().await.clear();
        self.user_memberships.write().await.clear();

        // Import tenants
        for tenant in export.tenants {
            let mut tenants = self.tenants.write().await;
            let mut slugs = self.slug_index.write().await;

            slugs.insert(tenant.slug.clone(), tenant.id);
            tenants.insert(tenant.id, tenant);
        }

        // Import memberships
        *self.user_memberships.write().await = export.memberships;

        // Import isolations
        for isolation in export.isolations {
            self.isolation_manager.set_isolation(isolation).await;
        }

        Ok(())
    }
}

impl Default for TenantRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Exported registry state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RegistryExport {
    /// All tenants
    pub tenants: Vec<Tenant>,
    /// All memberships
    pub memberships: HashMap<String, Vec<TenantMembership>>,
    /// All isolation configurations
    pub isolations: Vec<TenantIsolation>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_organization() {
        let registry = TenantRegistry::new();
        let org = Tenant::new_organization("Acme Corp", "acme");

        let id = registry.create_tenant(org.clone()).await.unwrap();

        let retrieved = registry.get_tenant(&id).await.unwrap();
        assert_eq!(retrieved.name, "Acme Corp");
        assert_eq!(retrieved.slug, "acme");
    }

    #[tokio::test]
    async fn test_slug_uniqueness() {
        let registry = TenantRegistry::new();

        let org1 = Tenant::new_organization("Org 1", "org");
        registry.create_tenant(org1).await.unwrap();

        let org2 = Tenant::new_organization("Org 2", "org");
        let result = registry.create_tenant(org2).await;

        assert!(matches!(result, Err(TenancyError::SlugTaken(_))));
    }

    #[tokio::test]
    async fn test_tenant_hierarchy() {
        let registry = TenantRegistry::new();

        let org = Tenant::new_organization("Org", "org");
        let org_id = registry.create_tenant(org).await.unwrap();

        let team = Tenant::new_team("Team A", "team-a", org_id);
        let team_id = registry.create_tenant(team).await.unwrap();

        let user = Tenant::new_user("User 1", "user-1", team_id, "user@example.com");
        let user_id = registry.create_tenant(user).await.unwrap();

        // Check hierarchy
        let ancestors = registry.get_ancestors(&user_id).await;
        assert_eq!(ancestors.len(), 2);

        let descendants = registry.get_descendants(&org_id).await;
        assert_eq!(descendants.len(), 2);

        let path = registry.get_tenant_path(&user_id).await.unwrap();
        assert_eq!(path, "org/team-a/user-1");
    }

    #[tokio::test]
    async fn test_invalid_hierarchy() {
        let registry = TenantRegistry::new();

        // Team without parent should fail
        let team = Tenant::new_team("Team", "team", TenantId::new());
        let result = registry.create_tenant(team).await;
        assert!(matches!(result, Err(TenancyError::ParentNotFound(_))));
    }

    #[tokio::test]
    async fn test_membership() {
        let registry = TenantRegistry::new();

        let org = Tenant::new_organization("Org", "org");
        let org_id = registry.create_tenant(org).await.unwrap();

        registry
            .add_member(&org_id, "user@example.com", TenantRole::Member)
            .await
            .unwrap();

        assert!(registry.is_member("user@example.com", &org_id).await);

        let role = registry.get_user_role("user@example.com", &org_id).await;
        assert_eq!(role, Some(TenantRole::Member));
    }

    #[tokio::test]
    async fn test_inherited_membership() {
        let registry = TenantRegistry::new();

        let org = Tenant::new_organization("Org", "org");
        let org_id = registry.create_tenant(org).await.unwrap();

        let team = Tenant::new_team("Team", "team", org_id);
        let team_id = registry.create_tenant(team).await.unwrap();

        // Add user as org admin
        registry
            .add_member(&org_id, "admin@example.com", TenantRole::Admin)
            .await
            .unwrap();

        // User should have access to team via org membership
        assert!(registry.is_member("admin@example.com", &team_id).await);
    }

    #[tokio::test]
    async fn test_effective_policies() {
        let registry = TenantRegistry::new();

        let mut org = Tenant::new_organization("Org", "org");
        org.policies.allow_phantom_nodes = false;
        let org_id = registry.create_tenant(org).await.unwrap();

        let team = Tenant::new_team("Team", "team", org_id);
        let team_id = registry.create_tenant(team).await.unwrap();

        // Team should inherit org's phantom policy
        let effective = registry.get_effective_policies(&team_id).await.unwrap();
        assert!(!effective.allow_phantom_nodes);
    }

    #[tokio::test]
    async fn test_delete_tenant() {
        let registry = TenantRegistry::new();

        let org = Tenant::new_organization("Org", "org");
        let org_id = registry.create_tenant(org).await.unwrap();

        let deleted = registry.delete_tenant(&org_id).await.unwrap();
        assert_eq!(deleted.name, "Org");

        assert!(registry.get_tenant(&org_id).await.is_none());
    }

    #[tokio::test]
    async fn test_delete_tenant_with_children_fails() {
        let registry = TenantRegistry::new();

        let org = Tenant::new_organization("Org", "org");
        let org_id = registry.create_tenant(org).await.unwrap();

        let team = Tenant::new_team("Team", "team", org_id);
        registry.create_tenant(team).await.unwrap();

        let result = registry.delete_tenant(&org_id).await;
        assert!(matches!(result, Err(TenancyError::InvalidHierarchy(_))));
    }

    #[tokio::test]
    async fn test_delete_tenant_recursive() {
        let registry = TenantRegistry::new();

        let org = Tenant::new_organization("Org", "org");
        let org_id = registry.create_tenant(org).await.unwrap();

        let team = Tenant::new_team("Team", "team", org_id);
        registry.create_tenant(team).await.unwrap();

        let deleted = registry.delete_tenant_recursive(&org_id).await.unwrap();
        assert_eq!(deleted.len(), 2);

        assert_eq!(registry.count().await, 0);
    }

    #[tokio::test]
    async fn test_export_import() {
        let registry = TenantRegistry::new();

        let org = Tenant::new_organization("Org", "org");
        let org_id = registry.create_tenant(org).await.unwrap();
        registry
            .add_member(&org_id, "user@example.com", TenantRole::Member)
            .await
            .unwrap();

        let export = registry.export().await;

        let registry2 = TenantRegistry::new();
        registry2.import(export).await.unwrap();

        assert_eq!(registry2.count().await, 1);
        assert!(registry2.get_tenant(&org_id).await.is_some());
    }
}
