// Marabunta - Licensed under the MIT License.
//! Rust-side embedding of the built dashboard (Vite/React SPA).
//!
//! Uses `rust-embed` to include all files from `dashboard/dist/` at compile time.
//! At runtime, serves assets with appropriate MIME types and cache headers.
//! Unknown paths fall back to `index.html` for SPA client-side routing.

use axum::{
    http::{header, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use rust_embed::RustEmbed;

// ---------------------------------------------------------------------------
// Embedded assets
// ---------------------------------------------------------------------------

#[derive(RustEmbed)]
#[folder = "dashboard/dist/"]
struct DashboardAssets;

// ---------------------------------------------------------------------------
// Cache control
// ---------------------------------------------------------------------------

/// Return the appropriate `Cache-Control` header value for a path.
///
/// Vite hashes asset filenames (e.g. `assets/index-a1b2c3.js`), so anything
/// under `assets/` can be cached indefinitely. All other files — especially
/// `index.html` — must be revalidated every request so users always load the
/// latest build.
fn cache_control_for(path: &str) -> &'static str {
    if path.contains("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// Serve a static asset from the embedded dashboard build.
/// Falls back to `index.html` for SPA client-side routing.
async fn serve_dashboard(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Try exact path match first
    if let Some(file) = DashboardAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, mime.as_ref())
            .header(header::CACHE_CONTROL, cache_control_for(path))
            .body(axum::body::Body::from(file.data.to_vec()))
            .unwrap();
    }

    // SPA fallback: return index.html for client-side routing
    match DashboardAssets::get("index.html") {
        Some(index) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(axum::body::Body::from(index.data.to_vec()))
            .unwrap(),
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(axum::body::Body::from(
                "Dashboard not built. Run: cd dashboard && npm ci && npm run build",
            ))
            .unwrap(),
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build an axum router that serves the embedded dashboard.
///
/// Uses `.fallback()` so that `/api/v1/*` routes (nested first) take
/// precedence. Any unmatched `GET` returns the SPA.
pub fn dashboard_router() -> Router {
    Router::new().fallback(get(serve_dashboard))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_control_assets() {
        assert_eq!(
            cache_control_for("assets/index-a1b2c3.js"),
            "public, max-age=31536000, immutable"
        );
    }

    #[test]
    fn test_cache_control_index() {
        assert_eq!(cache_control_for("index.html"), "no-cache");
    }

    #[test]
    fn test_cache_control_root() {
        assert_eq!(cache_control_for(""), "no-cache");
    }
}
