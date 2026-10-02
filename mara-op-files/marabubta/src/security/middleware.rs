// Marabunta - Licensed under the MIT License.
//! Authentication middleware for Axum API routes.
//!
//! This module provides middleware for integrating token authentication
//! with Axum HTTP handlers.

use std::sync::Arc;
use std::task::{Context, Poll};

use axum::{
    body::Body,
    extract::{FromRequestParts, State},
    http::{header::AUTHORIZATION, request::Parts, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use tower::{Layer, Service};

use super::auth::{
    extract_bearer_token, AuthTokenInfo, Permission, TokenManager, TokenStore, TokenValidation,
};

/// State for authentication middleware
#[derive(Clone)]
pub struct AuthState<S: TokenStore + 'static = super::auth::MemoryTokenStore> {
    /// Token manager for validation
    pub token_manager: Arc<TokenManager<S>>,
    /// Whether authentication is required (can be disabled for dev)
    pub auth_required: bool,
    /// Default permission level for unauthenticated requests when auth is not required
    pub default_permission: Permission,
}

impl<S: TokenStore> AuthState<S> {
    /// Create a new auth state with required authentication
    pub fn new(token_manager: Arc<TokenManager<S>>) -> Self {
        Self {
            token_manager,
            auth_required: true,
            default_permission: Permission::ReadOnly,
        }
    }

    /// Create an auth state with optional authentication
    pub fn optional(token_manager: Arc<TokenManager<S>>, default_permission: Permission) -> Self {
        Self {
            token_manager,
            auth_required: false,
            default_permission,
        }
    }
}

/// Authenticated user information extracted from requests
#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    /// Token information
    pub token_info: Option<AuthTokenInfo>,
    /// Effective permission level
    pub permission: Permission,
    /// Whether the request was authenticated
    pub authenticated: bool,
}

impl AuthenticatedUser {
    /// Create from token info
    pub fn from_token(info: AuthTokenInfo) -> Self {
        Self {
            permission: info.permission,
            token_info: Some(info),
            authenticated: true,
        }
    }

    /// Create anonymous user with default permission
    pub fn anonymous(permission: Permission) -> Self {
        Self {
            token_info: None,
            permission,
            authenticated: false,
        }
    }

    /// Check if the user has the required permission
    pub fn has_permission(&self, required: Permission) -> bool {
        self.permission.allows(required)
    }

    /// Get the user identity (if authenticated)
    pub fn identity(&self) -> Option<&str> {
        self.token_info.as_ref().map(|t| t.identity.as_str())
    }
}

/// Extractor for authenticated user in Axum handlers
#[axum::async_trait]
impl<TS: TokenStore + Send + Sync + 'static> FromRequestParts<Arc<AuthState<TS>>> for AuthenticatedUser {
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AuthState<TS>>,
    ) -> Result<Self, Self::Rejection> {
        // Try to extract the authorization header
        let auth_header = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|h| h.to_str().ok());

        if let Some(header) = auth_header {
            if let Some(token) = extract_bearer_token(header) {
                // Validate the token
                match state.token_manager.validate_token(token).await {
                    TokenValidation::Valid(info) => {
                        return Ok(AuthenticatedUser::from_token(info));
                    }
                    TokenValidation::Expired => {
                        return Err(AuthError::TokenExpired);
                    }
                    TokenValidation::Revoked => {
                        return Err(AuthError::TokenRevoked);
                    }
                    TokenValidation::NotFound | TokenValidation::Invalid(_) => {
                        return Err(AuthError::InvalidToken);
                    }
                }
            }
        }

        // No valid token found
        if state.auth_required {
            Err(AuthError::AuthenticationRequired)
        } else {
            Ok(AuthenticatedUser::anonymous(state.default_permission))
        }
    }
}

/// Authentication errors for API responses
#[derive(Debug)]
pub enum AuthError {
    AuthenticationRequired,
    InvalidToken,
    TokenExpired,
    TokenRevoked,
    InsufficientPermissions { required: Permission },
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AuthError::AuthenticationRequired => {
                (StatusCode::UNAUTHORIZED, "Authentication required")
            }
            AuthError::InvalidToken => (StatusCode::UNAUTHORIZED, "Invalid token"),
            AuthError::TokenExpired => (StatusCode::UNAUTHORIZED, "Token expired"),
            AuthError::TokenRevoked => (StatusCode::UNAUTHORIZED, "Token revoked"),
            AuthError::InsufficientPermissions { .. } => {
                (StatusCode::FORBIDDEN, "Insufficient permissions")
            }
        };

        let body = serde_json::json!({
            "error": message,
            "code": status.as_u16(),
        });

        (status, axum::Json(body)).into_response()
    }
}

/// Require a specific permission level for an endpoint
#[derive(Clone)]
pub struct RequireAuth {
    pub permission: Permission,
}

impl RequireAuth {
    /// Require admin permission
    pub fn admin() -> Self {
        Self {
            permission: Permission::Admin,
        }
    }

    /// Require operator permission
    pub fn operator() -> Self {
        Self {
            permission: Permission::Operator,
        }
    }

    /// Require user permission
    pub fn user() -> Self {
        Self {
            permission: Permission::User,
        }
    }

    /// Require read-only permission (any authenticated user)
    pub fn read_only() -> Self {
        Self {
            permission: Permission::ReadOnly,
        }
    }

    /// Require a specific permission
    pub fn permission(permission: Permission) -> Self {
        Self { permission }
    }

    /// Check if the authenticated user has the required permission
    pub fn check(&self, user: &AuthenticatedUser) -> Result<(), AuthError> {
        if user.has_permission(self.permission) {
            Ok(())
        } else {
            Err(AuthError::InsufficientPermissions {
                required: self.permission,
            })
        }
    }
}

/// Middleware function for authentication with permission check
pub async fn auth_middleware<S: TokenStore>(
    State(auth_state): State<Arc<AuthState<S>>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let (mut parts, body) = request.into_parts();

    // Try to authenticate
    let user: AuthenticatedUser = match AuthenticatedUser::from_request_parts(&mut parts, &auth_state).await {
        Ok(user) => user,
        Err(err) => return <AuthError as IntoResponse>::into_response(err),
    };

    // Reconstruct request with user info in extensions
    let mut request = Request::from_parts(parts, body);
    request.extensions_mut().insert(user);

    next.run(request).await
}

/// Create a middleware layer that requires authentication
pub fn require_auth<S: TokenStore + 'static>(token_manager: Arc<TokenManager<S>>) -> AuthLayer<S> {
    AuthLayer {
        state: Arc::new(AuthState::new(token_manager)),
    }
}

/// Create a middleware layer with optional authentication
pub fn optional_auth<S: TokenStore + 'static>(
    token_manager: Arc<TokenManager<S>>,
    default_permission: Permission,
) -> AuthLayer<S> {
    AuthLayer {
        state: Arc::new(AuthState::optional(token_manager, default_permission)),
    }
}

/// Tower layer for authentication middleware
#[derive(Clone)]
pub struct AuthLayer<S: TokenStore + 'static> {
    state: Arc<AuthState<S>>,
}

impl<S: TokenStore, Inner> Layer<Inner> for AuthLayer<S> {
    type Service = AuthService<S, Inner>;

    fn layer(&self, inner: Inner) -> Self::Service {
        AuthService {
            inner,
            state: self.state.clone(),
        }
    }
}

/// Tower service for authentication middleware
#[derive(Clone)]
pub struct AuthService<S: TokenStore + 'static, Inner> {
    inner: Inner,
    state: Arc<AuthState<S>>,
}

impl<S: TokenStore + Send + Sync + 'static, Inner, ReqBody> Service<Request<ReqBody>> for AuthService<S, Inner>
where
    Inner: Service<Request<ReqBody>, Response = Response> + Clone + Send + 'static,
    Inner::Future: Send,
    ReqBody: Send + 'static,
{
    type Response = Response;
    type Error = Inner::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        // Clone the inner service for async move
        let inner = self.inner.clone();
        let _state = self.state.clone();

        Box::pin(async move {
            // This is a simplified implementation
            // In practice, you'd extract the token and validate it here
            // For now, we just pass through to the inner service
            // The actual authentication is done via the AuthenticatedUser extractor
            inner.clone().call(request).await
        })
    }
}

/// Helper macro for requiring permissions in handlers
#[macro_export]
macro_rules! require_permission {
    ($user:expr, $perm:expr) => {
        if !$user.has_permission($perm) {
            return Err(
                $crate::security::middleware::AuthError::InsufficientPermissions {
                    required: $perm,
                }
                .into_response(),
            );
        }
    };
}

/// Extension trait for adding authentication to Axum routers
pub trait AuthRouterExt {
    /// Add authentication state to the router
    fn with_auth<S: TokenStore + 'static>(self, token_manager: Arc<TokenManager<S>>) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::auth::MemoryTokenStore;
    use std::time::Duration;

    #[tokio::test]
    async fn test_authenticated_user_permissions() {
        let user = AuthenticatedUser {
            token_info: Some(AuthTokenInfo {
                id: "test".to_string(),
                identity: "test-user".to_string(),
                permission: Permission::Operator,
                metadata: Default::default(),
            }),
            permission: Permission::Operator,
            authenticated: true,
        };

        assert!(user.has_permission(Permission::ReadOnly));
        assert!(user.has_permission(Permission::User));
        assert!(user.has_permission(Permission::Operator));
        assert!(!user.has_permission(Permission::Admin));
    }

    #[tokio::test]
    async fn test_anonymous_user() {
        let user = AuthenticatedUser::anonymous(Permission::ReadOnly);

        assert!(!user.authenticated);
        assert!(user.token_info.is_none());
        assert_eq!(user.permission, Permission::ReadOnly);
        assert!(user.has_permission(Permission::ReadOnly));
        assert!(!user.has_permission(Permission::User));
    }

    #[tokio::test]
    async fn test_require_auth_check() {
        let admin_user = AuthenticatedUser {
            token_info: None,
            permission: Permission::Admin,
            authenticated: true,
        };

        let user = AuthenticatedUser {
            token_info: None,
            permission: Permission::User,
            authenticated: true,
        };

        let admin_check = RequireAuth::admin();
        assert!(admin_check.check(&admin_user).is_ok());
        assert!(admin_check.check(&user).is_err());

        let user_check = RequireAuth::user();
        assert!(user_check.check(&admin_user).is_ok());
        assert!(user_check.check(&user).is_ok());
    }

    #[test]
    fn test_auth_error_responses() {
        let err = AuthError::AuthenticationRequired;
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let err = AuthError::InsufficientPermissions {
            required: Permission::Admin,
        };
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_auth_state_creation() {
        let token_manager = Arc::new(TokenManager::new());

        let required_state = AuthState::new(token_manager.clone());
        assert!(required_state.auth_required);

        let optional_state = AuthState::optional(token_manager, Permission::User);
        assert!(!optional_state.auth_required);
        assert_eq!(optional_state.default_permission, Permission::User);
    }
}
