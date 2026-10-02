// Marabunta - Licensed under the MIT License.
//! Tenant Management API
//!
//! This module provides REST API endpoints for tenant management including
//! CRUD operations, quota management, and membership management.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post, put},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};

use super::errors::TenancyError;
use super::isolation::TenantIsolation;
use super::registry::TenantRegistry;
use super::types::{
    Tenant, TenantId, TenantLevel, TenantPolicies, TenantQuotas, TenantRole, TenantStatus,
};

// ============================================================================
// API State
// ============================================================================

/// Shared state for the tenant management API.
pub struct TenantApiState {
    /// The tenant registry
    pub registry: Arc<TenantRegistry>,
    /// Authentication/authorization handler (optional)
    pub auth: Arc<dyn TenantAuthHandler>,
}

/// Trait for authentication and authorization.
#[async_trait::async_trait]
pub trait TenantAuthHandler: Send + Sync {
    /// Extracts the user ID from the request.
    async fn get_user_id(&self, headers: &axum::http::HeaderMap) -> Option<String>;

    /// Extracts the tenant ID from auth token (current tenant context).
    async fn get_tenant_id(&self, headers: &axum::http::HeaderMap) -> Option<TenantId>;

    /// Checks if a user can perform an action on a tenant.
    async fn can_access_tenant(
        &self,
        user_id: &str,
        tenant_id: &TenantId,
        action: TenantAction,
    ) -> bool;
}

/// Actions that can be performed on tenants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantAction {
    /// View tenant details
    Read,
    /// Create child tenants
    Create,
    /// Update tenant settings
    Update,
    /// Delete tenant
    Delete,
    /// Manage members
    ManageMembers,
    /// Manage quotas
    ManageQuotas,
    /// Manage policies
    ManagePolicies,
}

/// Default auth handler that allows everything (for testing).
pub struct NoOpAuthHandler;

#[async_trait::async_trait]
impl TenantAuthHandler for NoOpAuthHandler {
    async fn get_user_id(&self, headers: &axum::http::HeaderMap) -> Option<String> {
        headers
            .get("X-User-Id")
            .and_then(|v| v.to_str().ok())
            .map(String::from)
    }

    async fn get_tenant_id(&self, headers: &axum::http::HeaderMap) -> Option<TenantId> {
        headers
            .get("X-Tenant-Id")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
    }

    async fn can_access_tenant(
        &self,
        _user_id: &str,
        _tenant_id: &TenantId,
        _action: TenantAction,
    ) -> bool {
        true
    }
}

// ============================================================================
// Router Construction
// ============================================================================

/// Creates the tenant management API router.
pub fn create_tenant_router(state: Arc<TenantApiState>) -> Router {
    Router::new()
        // Tenant CRUD
        .route("/api/tenants", post(create_tenant))
        .route("/api/tenants", get(list_tenants))
        .route("/api/tenants/:id", get(get_tenant))
        .route("/api/tenants/:id", put(update_tenant))
        .route("/api/tenants/:id", delete(delete_tenant))
        // Tenant by slug
        .route("/api/tenants/by-slug/:slug", get(get_tenant_by_slug))
        // Hierarchy
        .route("/api/tenants/:id/children", get(list_children))
        .route("/api/tenants/:id/ancestors", get(list_ancestors))
        .route("/api/tenants/:id/descendants", get(list_descendants))
        // Quotas
        .route("/api/tenants/:id/quotas", get(get_quotas))
        .route("/api/tenants/:id/quotas", put(set_quotas))
        // Policies
        .route("/api/tenants/:id/policies", get(get_policies))
        .route("/api/tenants/:id/policies", put(set_policies))
        // Isolation
        .route("/api/tenants/:id/isolation", get(get_isolation))
        .route("/api/tenants/:id/isolation", put(set_isolation))
        // Members
        .route("/api/tenants/:id/members", get(list_members))
        .route("/api/tenants/:id/members", post(add_member))
        .route("/api/tenants/:id/members/:user_id", delete(remove_member))
        // Status
        .route("/api/tenants/:id/status", put(set_status))
        // User endpoints
        .route("/api/user/tenants", get(get_user_tenants))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
}

// ============================================================================
// Request/Response Types
// ============================================================================

/// Request to create a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTenantRequest {
    /// Tenant name
    pub name: String,
    /// Tenant slug (URL-safe identifier)
    pub slug: String,
    /// Description
    pub description: Option<String>,
    /// Tenant level (organization, team, user)
    pub level: TenantLevel,
    /// Parent tenant ID (required for team/user)
    pub parent_id: Option<TenantId>,
    /// Initial quotas
    pub quotas: Option<TenantQuotas>,
    /// Initial policies
    pub policies: Option<TenantPolicies>,
    /// Admin user IDs
    pub admins: Option<Vec<String>>,
    /// Contact email
    pub contact_email: Option<String>,
}

/// Request to update a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTenantRequest {
    /// Updated name
    pub name: Option<String>,
    /// Updated description
    pub description: Option<String>,
    /// Updated contact email
    pub contact_email: Option<String>,
    /// Updated metadata
    pub metadata: Option<std::collections::HashMap<String, serde_json::Value>>,
}

/// Response for tenant operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantResponse {
    /// Tenant ID
    pub id: TenantId,
    /// Name
    pub name: String,
    /// Slug
    pub slug: String,
    /// Description
    pub description: String,
    /// Level
    pub level: TenantLevel,
    /// Parent ID
    pub parent_id: Option<TenantId>,
    /// Status
    pub status: TenantStatus,
    /// Quotas
    pub quotas: TenantQuotas,
    /// Policies
    pub policies: TenantPolicies,
    /// Admin count
    pub admin_count: usize,
    /// Member count
    pub member_count: usize,
    /// Child count
    pub child_count: usize,
    /// Created at
    pub created_at: DateTime<Utc>,
    /// Updated at
    pub updated_at: DateTime<Utc>,
}

impl From<Tenant> for TenantResponse {
    fn from(t: Tenant) -> Self {
        Self {
            id: t.id,
            name: t.name,
            slug: t.slug,
            description: t.description,
            level: t.level,
            parent_id: t.parent_id,
            status: t.status,
            quotas: t.quotas,
            policies: t.policies,
            admin_count: t.admins.len(),
            member_count: t.members.len(),
            child_count: t.children.len(),
            created_at: t.created_at,
            updated_at: t.updated_at,
        }
    }
}

/// Request to add a member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddMemberRequest {
    /// User ID to add
    pub user_id: String,
    /// Role for the member
    pub role: TenantRole,
}

/// Member response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberResponse {
    /// User ID
    pub user_id: String,
    /// Role
    pub role: TenantRole,
    /// Is admin
    pub is_admin: bool,
}

/// Request to set tenant status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetStatusRequest {
    /// New status
    pub status: TenantStatus,
}

/// List tenants query parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListTenantsQuery {
    /// Filter by level
    pub level: Option<TenantLevel>,
    /// Filter by status
    pub status: Option<TenantStatus>,
    /// Filter by parent
    pub parent_id: Option<TenantId>,
    /// Pagination limit
    pub limit: Option<usize>,
    /// Pagination offset
    pub offset: Option<usize>,
}

/// List response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListResponse<T> {
    /// Items
    pub items: Vec<T>,
    /// Total count
    pub total: usize,
    /// Limit used
    pub limit: usize,
    /// Offset used
    pub offset: usize,
}

// ============================================================================
// Handlers
// ============================================================================

/// POST /api/tenants - Create a new tenant.
async fn create_tenant(
    State(state): State<Arc<TenantApiState>>,
    headers: HeaderMap,
    Json(body): Json<CreateTenantRequest>,
) -> Result<Json<TenantResponse>, ApiError> {
    let user_id = state
        .auth
        .get_user_id(&headers)
        .await
        .ok_or(ApiError::Unauthorized)?;

    // For child tenants, check parent access
    if let Some(parent_id) = body.parent_id {
        if !state
            .auth
            .can_access_tenant(&user_id, &parent_id, TenantAction::Create)
            .await
        {
            return Err(ApiError::Forbidden);
        }
    }

    let mut tenant = match body.level {
        TenantLevel::Organization => Tenant::new_organization(&body.name, &body.slug),
        TenantLevel::Team => {
            let parent_id = body
                .parent_id
                .ok_or(ApiError::BadRequest("Team requires parent_id".to_string()))?;
            Tenant::new_team(&body.name, &body.slug, parent_id)
        }
        TenantLevel::User => {
            let parent_id = body.parent_id.ok_or(ApiError::BadRequest(
                "User tenant requires parent_id".to_string(),
            ))?;
            Tenant::new_user(&body.name, &body.slug, parent_id, &user_id)
        }
    };

    if let Some(desc) = body.description {
        tenant = tenant.with_description(desc);
    }
    if let Some(quotas) = body.quotas {
        tenant = tenant.with_quotas(quotas);
    }
    if let Some(policies) = body.policies {
        tenant = tenant.with_policies(policies);
    }
    if let Some(email) = body.contact_email {
        tenant = tenant.with_contact_email(email);
    }
    if let Some(admins) = body.admins {
        for admin in admins {
            tenant.add_admin(admin);
        }
    }
    tenant = tenant.with_created_by(&user_id);

    state.registry.create_tenant(tenant.clone()).await?;

    Ok(Json(TenantResponse::from(tenant)))
}

/// GET /api/tenants - List tenants.
async fn list_tenants(
    State(state): State<Arc<TenantApiState>>,
    Query(query): Query<ListTenantsQuery>,
) -> Json<ListResponse<TenantResponse>> {
    let mut tenants = state.registry.list_tenants().await;

    // Apply filters
    if let Some(level) = query.level {
        tenants.retain(|t| t.level == level);
    }
    if let Some(status) = query.status {
        tenants.retain(|t| t.status == status);
    }
    if let Some(parent_id) = query.parent_id {
        tenants.retain(|t| t.parent_id == Some(parent_id));
    }

    let total = tenants.len();
    let limit = query.limit.unwrap_or(100);
    let offset = query.offset.unwrap_or(0);

    let items: Vec<TenantResponse> = tenants
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(TenantResponse::from)
        .collect();

    Json(ListResponse {
        items,
        total,
        limit,
        offset,
    })
}

/// GET /api/tenants/:id - Get tenant by ID.
async fn get_tenant(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
) -> Result<Json<TenantResponse>, ApiError> {
    let tenant = state
        .registry
        .get_tenant(&id)
        .await
        .ok_or(ApiError::NotFound)?;

    Ok(Json(TenantResponse::from(tenant)))
}

/// GET /api/tenants/by-slug/:slug - Get tenant by slug.
async fn get_tenant_by_slug(
    State(state): State<Arc<TenantApiState>>,
    Path(slug): Path<String>,
) -> Result<Json<TenantResponse>, ApiError> {
    let tenant = state
        .registry
        .get_tenant_by_slug(&slug)
        .await
        .ok_or(ApiError::NotFound)?;

    Ok(Json(TenantResponse::from(tenant)))
}

/// PUT /api/tenants/:id - Update tenant.
async fn update_tenant(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
    Json(body): Json<UpdateTenantRequest>,
) -> Result<Json<TenantResponse>, ApiError> {
    let mut tenant = state
        .registry
        .get_tenant(&id)
        .await
        .ok_or(ApiError::NotFound)?;

    if let Some(name) = body.name {
        tenant.name = name;
    }
    if let Some(desc) = body.description {
        tenant.description = desc;
    }
    if let Some(email) = body.contact_email {
        tenant.contact_email = Some(email);
    }
    if let Some(metadata) = body.metadata {
        for (key, value) in metadata {
            tenant.set_metadata(key, value);
        }
    }

    state.registry.update_tenant(tenant.clone()).await?;

    Ok(Json(TenantResponse::from(tenant)))
}

/// DELETE /api/tenants/:id - Delete tenant.
async fn delete_tenant(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
    Query(query): Query<DeleteQuery>,
) -> Result<Json<TenantResponse>, ApiError> {
    let tenant = if query.recursive.unwrap_or(false) {
        let deleted = state.registry.delete_tenant_recursive(&id).await?;
        deleted.into_iter().last().ok_or(ApiError::NotFound)?
    } else {
        state.registry.delete_tenant(&id).await?
    };

    Ok(Json(TenantResponse::from(tenant)))
}

#[derive(Debug, Deserialize)]
struct DeleteQuery {
    recursive: Option<bool>,
}

/// GET /api/tenants/:id/children - List children.
async fn list_children(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
) -> Json<Vec<TenantResponse>> {
    let children = state.registry.list_children(&id).await;
    Json(children.into_iter().map(TenantResponse::from).collect())
}

/// GET /api/tenants/:id/ancestors - List ancestors.
async fn list_ancestors(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
) -> Json<Vec<TenantResponse>> {
    let ancestors = state.registry.get_ancestors(&id).await;
    Json(ancestors.into_iter().map(TenantResponse::from).collect())
}

/// GET /api/tenants/:id/descendants - List descendants.
async fn list_descendants(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
) -> Json<Vec<TenantResponse>> {
    let descendants = state.registry.get_descendants(&id).await;
    Json(descendants.into_iter().map(TenantResponse::from).collect())
}

/// GET /api/tenants/:id/quotas - Get tenant quotas.
async fn get_quotas(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
) -> Result<Json<TenantQuotas>, ApiError> {
    let quotas = state
        .registry
        .get_effective_quotas(&id)
        .await
        .ok_or(ApiError::NotFound)?;

    Ok(Json(quotas))
}

/// PUT /api/tenants/:id/quotas - Set tenant quotas.
async fn set_quotas(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
    Json(quotas): Json<TenantQuotas>,
) -> Result<Json<TenantQuotas>, ApiError> {
    state.registry.set_quotas(&id, quotas.clone()).await?;
    Ok(Json(quotas))
}

/// GET /api/tenants/:id/policies - Get tenant policies.
async fn get_policies(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
) -> Result<Json<TenantPolicies>, ApiError> {
    let policies = state
        .registry
        .get_effective_policies(&id)
        .await
        .ok_or(ApiError::NotFound)?;

    Ok(Json(policies))
}

/// PUT /api/tenants/:id/policies - Set tenant policies.
async fn set_policies(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
    Json(policies): Json<TenantPolicies>,
) -> Result<Json<TenantPolicies>, ApiError> {
    state.registry.set_policies(&id, policies.clone()).await?;
    Ok(Json(policies))
}

/// GET /api/tenants/:id/isolation - Get isolation config.
async fn get_isolation(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
) -> Result<Json<TenantIsolation>, ApiError> {
    let isolation = state
        .registry
        .isolation_manager()
        .get_isolation(&id)
        .await
        .ok_or(ApiError::NotFound)?;

    Ok(Json(isolation))
}

/// PUT /api/tenants/:id/isolation - Set isolation config.
async fn set_isolation(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
    Json(isolation): Json<TenantIsolation>,
) -> Result<Json<TenantIsolation>, ApiError> {
    // Ensure the isolation is for the correct tenant
    if isolation.tenant_id != id {
        return Err(ApiError::BadRequest(
            "Isolation tenant_id must match path".to_string(),
        ));
    }

    state
        .registry
        .isolation_manager()
        .set_isolation(isolation.clone())
        .await;

    Ok(Json(isolation))
}

/// GET /api/tenants/:id/members - List members.
async fn list_members(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
) -> Result<Json<Vec<MemberResponse>>, ApiError> {
    let tenant = state
        .registry
        .get_tenant(&id)
        .await
        .ok_or(ApiError::NotFound)?;

    let members: Vec<MemberResponse> = tenant
        .members
        .iter()
        .map(|user_id| {
            let is_admin = tenant.admins.contains(user_id);
            MemberResponse {
                user_id: user_id.clone(),
                role: if is_admin {
                    TenantRole::Admin
                } else {
                    TenantRole::Member
                },
                is_admin,
            }
        })
        .collect();

    Ok(Json(members))
}

/// POST /api/tenants/:id/members - Add member.
async fn add_member(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
    Json(body): Json<AddMemberRequest>,
) -> Result<Json<MemberResponse>, ApiError> {
    state
        .registry
        .add_member(&id, &body.user_id, body.role)
        .await?;

    Ok(Json(MemberResponse {
        user_id: body.user_id,
        role: body.role,
        is_admin: body.role == TenantRole::Admin || body.role == TenantRole::Owner,
    }))
}

/// DELETE /api/tenants/:id/members/:user_id - Remove member.
async fn remove_member(
    State(state): State<Arc<TenantApiState>>,
    Path((id, user_id)): Path<(TenantId, String)>,
) -> Result<StatusCode, ApiError> {
    state.registry.remove_member(&id, &user_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// PUT /api/tenants/:id/status - Set tenant status.
async fn set_status(
    State(state): State<Arc<TenantApiState>>,
    Path(id): Path<TenantId>,
    Json(body): Json<SetStatusRequest>,
) -> Result<Json<TenantResponse>, ApiError> {
    state.registry.set_status(&id, body.status).await?;

    let tenant = state
        .registry
        .get_tenant(&id)
        .await
        .ok_or(ApiError::NotFound)?;

    Ok(Json(TenantResponse::from(tenant)))
}

/// GET /api/user/tenants - Get tenants for current user.
async fn get_user_tenants(
    State(state): State<Arc<TenantApiState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<TenantResponse>>, ApiError> {
    let user_id = state
        .auth
        .get_user_id(&headers)
        .await
        .ok_or(ApiError::Unauthorized)?;

    let tenants = state.registry.get_user_tenants(&user_id).await;

    Ok(Json(
        tenants.into_iter().map(TenantResponse::from).collect(),
    ))
}

// ============================================================================
// Error Handling
// ============================================================================

/// API error type.
#[derive(Debug)]
pub enum ApiError {
    /// Not found
    NotFound,
    /// Bad request
    BadRequest(String),
    /// Unauthorized
    Unauthorized,
    /// Forbidden
    Forbidden,
    /// Tenancy error
    Tenancy(TenancyError),
}

impl From<TenancyError> for ApiError {
    fn from(err: TenancyError) -> Self {
        match err {
            TenancyError::TenantNotFound(_) => ApiError::NotFound,
            TenancyError::Unauthorized(_) => ApiError::Unauthorized,
            TenancyError::NotMember(_, _) => ApiError::Forbidden,
            TenancyError::CrossTenantAccessDenied(_, _) => ApiError::Forbidden,
            _ => ApiError::Tenancy(err),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            ApiError::NotFound => (StatusCode::NOT_FOUND, "Not found".to_string()),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            ApiError::Forbidden => (StatusCode::FORBIDDEN, "Forbidden".to_string()),
            ApiError::Tenancy(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Tenancy error: {}", err),
            ),
        };

        let body = Json(serde_json::json!({
            "error": message,
            "status": status.as_u16(),
        }));

        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_response_from() {
        let tenant = Tenant::new_organization("Test Org", "test-org");
        let response = TenantResponse::from(tenant);

        assert_eq!(response.name, "Test Org");
        assert_eq!(response.slug, "test-org");
        assert_eq!(response.level, TenantLevel::Organization);
    }
}
